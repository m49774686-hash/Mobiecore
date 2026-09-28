require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json({ limit: '1mb' }));

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL || '';
const TMDB_API_KEY = process.env.TMDB_API_KEY || '';
const TMDB_LANGUAGE = process.env.TMDB_LANGUAGE || 'en-US';
const TMDB_MAX_PAGES = Math.max(1, Number(process.env.TMDB_MAX_PAGES || 10));
const NATIVE_HTTP_TIMEOUT_MS = Math.max(8000, Number(process.env.NATIVE_HTTP_TIMEOUT_MS || 20000));
const MOVIEBOX_HOSTS = [
  'https://api6.aoneroom.com',
  'https://api5.aoneroom.com',
  'https://api4.aoneroom.com',
  'https://api4sg.aoneroom.com',
  'https://api3.aoneroom.com',
  'https://api6sg.aoneroom.com',
  'https://api.inmoviebox.com'
];
const MOVIEZONE_GATEWAY_BASE = 'https://moviezone-backend-1.onrender.com';
const MOVIEBOX_SECRET = Buffer.from([0xef,0xa8,0x91,0x97,0x4e,0xec,0xd3,0x14,0x8d,0xf6,0x3a,0xa6,0x11,0x60,0x2d,0xef,0xd1,0x01,0x25,0x9b,0xa5,0x21,0x02,0x2c,0x57,0xae,0x05,0x66,0xbd,0x8e]);
const RETRY_STATUS_CODES = new Set([403,406,407,429,500,502,503,504]);
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';

if (!DATABASE_URL) console.warn('[MovieCore] DATABASE_URL is missing');
if (!TMDB_API_KEY) console.warn('[MovieCore] TMDB_API_KEY is missing');

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : undefined
});

const startedAt = Date.now();
const logs = { tmdb: [], scraping: [] };
const activeScrapes = new Map();
let busy = false;
let tmdbState = { moviePage: 1, tvPage: 1, phase: 'movie', status: 'idle' };

function log(source, level, message) {
  const item = {
    timestamp: new Date().toISOString(),
    level: level || 'info',
    message: String(message)
  };
  const list = logs[source] || logs.tmdb;
  list.push(item);
  if (list.length > 500) list.splice(0, list.length - 500);
  console.log(`[${source.toUpperCase()}] ${item.message}`);
}

function auth(req, res, next) {
  const header = req.headers.authorization || '';
  if (!ADMIN_USERNAME && !ADMIN_PASSWORD) return next();

  const expected = 'Basic ' + Buffer.from(`${ADMIN_USERNAME}:${ADMIN_PASSWORD}`).toString('base64');
  if (header !== expected) {
    res.setHeader('WWW-Authenticate', 'Basic realm="MovieCore Admin"');
    return res.status(401).send('Unauthorized');
  }
  next();
}

async function q(sql, params = []) {
  return pool.query(sql, params);
}

async function schema() {
  await q(`
    CREATE TABLE IF NOT EXISTS moviecore_state (
      key TEXT PRIMARY KEY,
      value_json JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS moviecore_catalog (
      id BIGSERIAL PRIMARY KEY,
      tmdb_id BIGINT NOT NULL,
      media_type TEXT NOT NULL CHECK (media_type IN ('movie','tv')),
      title TEXT,
      original_title TEXT,
      overview TEXT,
      poster_path TEXT,
      backdrop_path TEXT,
      original_language TEXT,
      release_date DATE,
      first_air_date DATE,
      vote_average NUMERIC(5,2),
      vote_count INTEGER,
      tmdb_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (tmdb_id, media_type)
    );

    CREATE TABLE IF NOT EXISTS moviecore_seasons (
      id BIGSERIAL PRIMARY KEY,
      catalog_id BIGINT NOT NULL REFERENCES moviecore_catalog(id) ON DELETE CASCADE,
      season_number INTEGER NOT NULL,
      name TEXT,
      overview TEXT,
      air_date DATE,
      episode_count INTEGER,
      tmdb_json JSONB NOT NULL,
      UNIQUE (catalog_id, season_number)
    );

    CREATE TABLE IF NOT EXISTS moviecore_episodes (
      id BIGSERIAL PRIMARY KEY,
      season_id BIGINT NOT NULL REFERENCES moviecore_seasons(id) ON DELETE CASCADE,
      episode_number INTEGER NOT NULL,
      name TEXT,
      overview TEXT,
      air_date DATE,
      still_path TEXT,
      vote_average NUMERIC(5,2),
      tmdb_json JSONB NOT NULL,
      UNIQUE (season_id, episode_number)
    );

    CREATE TABLE IF NOT EXISTS moviecore_scrape_jobs (
      id BIGSERIAL PRIMARY KEY,
      catalog_id BIGINT NOT NULL REFERENCES moviecore_catalog(id) ON DELETE CASCADE,
      media_type TEXT NOT NULL,
      season_number INTEGER,
      episode_number INTEGER,
      status TEXT NOT NULL DEFAULT 'pending',
      attempts INTEGER NOT NULL DEFAULT 0,
      result_json JSONB,
      error_text TEXT,
      next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (catalog_id, season_number, episode_number)
    );

    CREATE INDEX IF NOT EXISTS idx_moviecore_jobs_ready
      ON moviecore_scrape_jobs(status, next_attempt_at);

    CREATE TABLE IF NOT EXISTS moviecore_links (
      id BIGSERIAL PRIMARY KEY,
      catalog_id BIGINT NOT NULL REFERENCES moviecore_catalog(id) ON DELETE CASCADE,
      media_type TEXT NOT NULL,
      season_number INTEGER,
      episode_number INTEGER,
      provider TEXT,
      video_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
      subtitle_sources JSONB NOT NULL DEFAULT '[]'::jsonb,
      audio_languages TEXT[] NOT NULL DEFAULT '{}',
      subtitle_languages TEXT[] NOT NULL DEFAULT '{}',
      result_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(catalog_id, season_number, episode_number)
    );
  `);

  await q(`
    INSERT INTO moviecore_state(key, value_json)
    VALUES ('tmdb', $1::jsonb)
    ON CONFLICT (key) DO NOTHING
  `, [JSON.stringify(tmdbState)]);
}

async function tmdb(path, params = {}) {
  if (!TMDB_API_KEY) throw new Error('TMDB_API_KEY is not configured');

  const url = new URL(`https://api.themoviedb.org/3${path}`);
  url.searchParams.set('api_key', TMDB_API_KEY);
  url.searchParams.set('language', TMDB_LANGUAGE);

  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  }

  const r = await fetch(url);
  const body = await r.text();
  if (!r.ok) throw new Error(`TMDB ${r.status}: ${body.slice(0, 300)}`);
  return JSON.parse(body);
}

async function saveMovie(item) {
  const details = await tmdb(`/movie/${item.id}`, {
    append_to_response: 'credits,release_dates'
  });

  const result = await q(`
    INSERT INTO moviecore_catalog
      (tmdb_id, media_type, title, original_title, overview, poster_path,
       backdrop_path, original_language, release_date, vote_average, vote_count, tmdb_json)
    VALUES ($1,'movie',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
    ON CONFLICT (tmdb_id, media_type) DO UPDATE SET
      title=EXCLUDED.title,
      original_title=EXCLUDED.original_title,
      overview=EXCLUDED.overview,
      poster_path=EXCLUDED.poster_path,
      backdrop_path=EXCLUDED.backdrop_path,
      original_language=EXCLUDED.original_language,
      release_date=EXCLUDED.release_date,
      vote_average=EXCLUDED.vote_average,
      vote_count=EXCLUDED.vote_count,
      tmdb_json=EXCLUDED.tmdb_json,
      updated_at=NOW()
    RETURNING id
  `, [
    details.id, details.title, details.original_title, details.overview,
    details.poster_path, details.backdrop_path, details.original_language,
    details.release_date || null, details.vote_average ?? null,
    details.vote_count ?? null, JSON.stringify(details)
  ]);

  await enqueueScrape(result.rows[0].id, 'movie', null, null);
  log('tmdb', 'info', `MOVIE COMPLETE | ${details.title || details.id} | tmdb=${details.id}`);
}

async function saveTv(item) {
  const details = await tmdb(`/tv/${item.id}`, {
    append_to_response: 'credits,content_ratings'
  });

  const result = await q(`
    INSERT INTO moviecore_catalog
      (tmdb_id, media_type, title, original_title, overview, poster_path,
       backdrop_path, original_language, first_air_date, vote_average, vote_count, tmdb_json)
    VALUES ($1,'tv',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
    ON CONFLICT (tmdb_id, media_type) DO UPDATE SET
      title=EXCLUDED.title,
      original_title=EXCLUDED.original_title,
      overview=EXCLUDED.overview,
      poster_path=EXCLUDED.poster_path,
      backdrop_path=EXCLUDED.backdrop_path,
      original_language=EXCLUDED.original_language,
      first_air_date=EXCLUDED.first_air_date,
      vote_average=EXCLUDED.vote_average,
      vote_count=EXCLUDED.vote_count,
      tmdb_json=EXCLUDED.tmdb_json,
      updated_at=NOW()
    RETURNING id
  `, [
    details.id, details.name, details.original_name, details.overview,
    details.poster_path, details.backdrop_path, details.original_language,
    details.first_air_date || null, details.vote_average ?? null,
    details.vote_count ?? null, JSON.stringify(details)
  ]);

  const catalogId = result.rows[0].id;

  for (const season of (details.seasons || [])) {
    if (season.season_number < 0) continue;

    const sd = await tmdb(`/tv/${details.id}/season/${season.season_number}`);

    const sr = await q(`
      INSERT INTO moviecore_seasons
        (catalog_id, season_number, name, overview, air_date, episode_count, tmdb_json)
      VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
      ON CONFLICT (catalog_id, season_number) DO UPDATE SET
        name=EXCLUDED.name,
        overview=EXCLUDED.overview,
        air_date=EXCLUDED.air_date,
        episode_count=EXCLUDED.episode_count,
        tmdb_json=EXCLUDED.tmdb_json
      RETURNING id
    `, [
      catalogId, sd.season_number, sd.name, sd.overview,
      sd.air_date || null, sd.episode_count ?? null, JSON.stringify(sd)
    ]);

    for (const ep of (sd.episodes || [])) {
      await q(`
        INSERT INTO moviecore_episodes
          (season_id, episode_number, name, overview, air_date,
           still_path, vote_average, tmdb_json)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
        ON CONFLICT (season_id, episode_number) DO UPDATE SET
          name=EXCLUDED.name,
          overview=EXCLUDED.overview,
          air_date=EXCLUDED.air_date,
          still_path=EXCLUDED.still_path,
          vote_average=EXCLUDED.vote_average,
          tmdb_json=EXCLUDED.tmdb_json
      `, [
        sr.rows[0].id, ep.episode_number, ep.name, ep.overview,
        ep.air_date || null, ep.still_path, ep.vote_average ?? null,
        JSON.stringify(ep)
      ]);

      await enqueueScrape(catalogId, 'tv', sd.season_number, ep.episode_number);
    }
  }

  log('tmdb', 'info', `TV COMPLETE | ${details.name || details.id} | tmdb=${details.id}`);
}

async function enqueueScrape(catalogId, mediaType, season, episode) {
  await q(`
    INSERT INTO moviecore_scrape_jobs
      (catalog_id, media_type, season_number, episode_number)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (catalog_id, season_number, episode_number) DO NOTHING
  `, [catalogId, mediaType, season, episode]);
}

async function tmdbTick() {
  if (tmdbState.status === 'complete') return;

  if (tmdbState.phase === 'movie') {
    if (tmdbState.moviePage > TMDB_MAX_PAGES) {
      tmdbState.phase = 'tv';
      tmdbState.tvPage = 1;
      return;
    }

    const page = tmdbState.moviePage;
    log('tmdb', 'info', `DISCOVER MOVIE | page=${page}`);

    const data = await tmdb('/discover/movie', {
      page,
      sort_by: 'popularity.desc',
      include_adult: false
    });

    for (const item of (data.results || [])) await saveMovie(item);
    tmdbState.moviePage++;
  } else {
    if (tmdbState.tvPage > TMDB_MAX_PAGES) {
      tmdbState.status = 'complete';
      log('tmdb', 'info', 'TMDB INGESTION COMPLETE');
      return;
    }

    const page = tmdbState.tvPage;
    log('tmdb', 'info', `DISCOVER TV | page=${page}`);

    const data = await tmdb('/discover/tv', {
      page,
      sort_by: 'popularity.desc',
      include_adult: false
    });

    for (const item of (data.results || [])) await saveTv(item);
    tmdbState.tvPage++;
  }

  await q(`
    INSERT INTO moviecore_state(key,value_json)
    VALUES ('tmdb',$1::jsonb)
    ON CONFLICT (key) DO UPDATE SET value_json=EXCLUDED.value_json, updated_at=NOW()
  `, [JSON.stringify(tmdbState)]);
}

/*
  Native MovieBox integration.
  No SCRAPER_URL / external scraper ENV is used.
  The provider/network logic is ported from MovieBox-Tui-Gateway-Ready:
  - MovieBox visitor session
  - signed MovieBox requests with x-client-token/x-tr-signature
  - host failover
  - subject search/details for gateway-id discovery
  - MovieZone gateway stream normalization
  - direct PostgreSQL persistence of playable links/subtitles/languages
*/
let movieBoxSession = null;
let movieBoxActiveHost = 0;

function md5Hex(data) { return require('crypto').createHash('md5').update(data).digest('hex'); }
function b64(buf) { return Buffer.from(buf).toString('base64'); }
function generateClientToken(ts) { const t=String(ts); return `${t},${md5Hex([...t].reverse().join(''))}`; }
function sortedQuery(url) {
  const u=new URL(url); const pairs=[...u.searchParams.entries()].sort((a,b)=>a[0].localeCompare(b[0])||a[1].localeCompare(b[1]));
  return pairs.map(([k,v])=>`${k}=${v}`).join('&');
}
function signature(method,url,body,ts) {
  const u=new URL(url); const q=sortedQuery(url); const canonicalUrl=q?`${u.pathname}?${q}`:u.pathname;
  const raw=body==null?'':String(body); const truncated=Buffer.from(raw).subarray(0,102400);
  const bodyHash=body==null?'':md5Hex(truncated); const bodyLen=body==null?'':String(Buffer.byteLength(raw));
  const canonical=[method.toUpperCase(),'application/json','application/json',bodyLen,String(ts),bodyHash,canonicalUrl].join('\\n');
  const sig=require('crypto').createHmac('md5',MOVIEBOX_SECRET).update(canonical).digest();
  return `${ts}|2|${b64(sig)}`;
}
function randomHex(n){let out='';while(out.length<n)out+=Math.floor(Math.random()*16).toString(16);return out.slice(0,n);}
function clientInfo(){
  const android=[['9','PQ3A.190605.03081104'],['10','QP1A.191005.007.A3'],['11','RP1A.200720.011'],['12','S1B.220414.015'],['13','TQ2A.230405.003']][Math.floor(Math.random()*5)];
  const dev=[['23078RKD5C','Redmi'],['2201117TY','Redmi'],['2201117TG','Redmi'],['22101316G','Redmi'],['21121210G','Redmi'],['M2012K11AG','Redmi'],['M2007J20CG','Redmi']][Math.floor(Math.random()*7)];
  const code=50020117+Math.floor(Math.random()*5); const network=Math.random()<.5?'NETWORK_WIFI':'NETWORK_MOBILE';
  const ua=`com.community.oneroom/${code} (Linux; U; Android ${android[0]}; en_US; ${dev[0]}; Build/${android[1]}; Cronet/135.0.7012.3)`;
  const info={package_name:'com.community.oneroom',version_name:'4.0.01.0813.03',version_code:code,os:'android',os_version:android[0],install_ch:'ps',device_id:randomHex(32),install_store:'ps',gaid:`${randomHex(8)}-${randomHex(4)}-${randomHex(4)}-${randomHex(4)}-${randomHex(12)}`,brand:dev[1],model:dev[0],system_language:'en',net:network,region:'US',timezone:'Asia/Kolkata',sp_code:'40401','X-Play-Mode':'2'};
  return {ua,info:JSON.stringify(info)};
}
const movieBoxClientInfo=clientInfo();
function randomSpoofedIp(){const prefixes=['103.241','49.36','117.195','106.198','122.162','157.32','182.70','103.58','27.60','59.90']; const prefix=prefixes[Math.floor(Math.random()*prefixes.length)]; return `${prefix}.${Math.floor(Math.random()*253)+1}.${Math.floor(Math.random()*253)+1}`;}
const movieBoxSpoofedIp=randomSpoofedIp();
function signedHeaders(method,url,body,token){const ts=Date.now();return {'User-Agent':movieBoxClientInfo.ua,Accept:'application/json','Content-Type':'application/json',Connection:'keep-alive','x-client-token':generateClientToken(ts),'x-tr-signature':signature(method,url,body,ts),'x-client-info':movieBoxClientInfo.info,'x-client-status':'0','x-forwarded-for':movieBoxSpoofedIp,...(token?{Authorization:`Bearer ${token}`}:{})};}

async function fetchJson(url, options={}, timeout=NATIVE_HTTP_TIMEOUT_MS){
  const controller=new AbortController(); const timer=setTimeout(()=>controller.abort(),timeout);
  try { const r=await fetch(url,{...options,signal:controller.signal}); const text=await r.text(); let data; try{data=JSON.parse(text);}catch{data={raw:text};} if(!r.ok) throw new Error(`HTTP ${r.status}: ${text.slice(0,300)}`); return data?.data ?? data; }
  catch(e){ if(e?.name==='AbortError') throw new Error(`TIMEOUT after ${timeout}ms`); throw e; }
  finally{clearTimeout(timer);}
}
async function movieBoxRequest(method,path,body=null){
  if(!movieBoxSession){
    let sessionErrors=[];
    for(let i=0;i<MOVIEBOX_HOSTS.length;i++){
      const host=MOVIEBOX_HOSTS[(movieBoxActiveHost+i)%MOVIEBOX_HOSTS.length];
      const url=host+'/wefeed-mobile-bff/user-api/visitor-login';
      try {
        const data=await fetchJson(url,{method:'POST',headers:signedHeaders('POST',url,'{}'),body:'{}'});
        const token=data?.token;
        if(token){ movieBoxSession=token; movieBoxActiveHost=(movieBoxActiveHost+i)%MOVIEBOX_HOSTS.length; break; }
        sessionErrors.push(`${host}: response did not contain token`);
      } catch(e){ sessionErrors.push(`${host}: ${String(e?.message||e).slice(0,180)}`); }
    }
    if(!movieBoxSession){
      throw new Error(`MovieBox visitor session unavailable | ${sessionErrors.join(' || ')}`);
    }
  }
  let lastErr;
  for(let i=0;i<MOVIEBOX_HOSTS.length;i++){
    const host=MOVIEBOX_HOSTS[(movieBoxActiveHost+i)%MOVIEBOX_HOSTS.length]; const url=host+path; const payload=body==null?null:JSON.stringify(body);
    try { const data=await fetchJson(url,{method,headers:signedHeaders(method,url,payload,movieBoxSession),...(payload?{body:payload}:{})}); movieBoxActiveHost=(movieBoxActiveHost+i)%MOVIEBOX_HOSTS.length; return data; }
    catch(e){ lastErr=e; const m=String(e.message||''); if(/HTTP (401|403|429|5\d\d)/.test(m)) continue; }
  }
  movieBoxSession=null; throw lastErr||new Error('MovieBox hosts exhausted');
}
function walkIds(v){let out={}; const visit=x=>{if(!x||typeof x!=='object')return;if(Array.isArray(x)){for(const z of x)visit(z);return;}for(const k of ['imdb_id','imdbId','imdbID','imdb','tmdb_id','tmdbId','tmdbID','tmdb']){if(x[k]!=null&&String(x[k]).trim()){if(/^imdb/i.test(k)&&!out.imdb)out.imdb=String(x[k]).trim();if(/^tmdb/i.test(k)&&!out.tmdb)out.tmdb=String(x[k]).trim();}}for(const z of Object.values(x))visit(z);};visit(v);return out;}
async function findMovieBoxSubject(job){
  const cat=(await q(`SELECT title,original_title,tmdb_json FROM moviecore_catalog WHERE id=$1`,[job.catalog_id])).rows[0];
  const candidates=[cat?.title,cat?.original_title].filter(Boolean);
  const target=String(job.tmdb_id);
  for(const title of candidates){
    const data=await movieBoxRequest('POST','/wefeed-mobile-bff/subject-api/search/v2',{keyword:title,page:1,perPage:15,subjectType:0});
    const arr=[]; const walk=x=>{if(!x||typeof x!=='object')return;if(Array.isArray(x)){x.forEach(walk);return;} if(x.subjectId||x.id||x.subject_id||x.tmdbId||x.tmdb_id||x.imdbId||x.imdb_id)arr.push(x); Object.values(x).forEach(walk);}; walk(data);
    const exact=arr.find(x=>String(x.tmdbId||x.tmdb_id||'')===target||String(x.id||'')===target||String(x.subjectId||x.subject_id||'')===target);
    const pick=exact||arr.find(x=>String(x.title||x.name||'').toLowerCase().trim()===String(title).toLowerCase().trim());
    if(pick){const sid=String(pick.subjectId||pick.subject_id||pick.id||'');if(sid)return sid;}
  }
  return target;
}
async function nativeMovieBoxScrape(job){
  const subjectId=await findMovieBoxSubject(job);
  let details={}; try{details=await movieBoxRequest('GET',`/wefeed-mobile-bff/subject-api/get?subjectId=${encodeURIComponent(subjectId)}`);}catch(_){details={};}
  const ids=walkIds(details); const gatewayId=ids.imdb||ids.tmdb||String(job.tmdb_id);
  const type=job.media_type==='movie'?'movie':'tv';
  const url=new URL(`${MOVIEZONE_GATEWAY_BASE}/api/smart/stream/${type}/${encodeURIComponent(gatewayId)}`);
  if(type==='tv'){url.searchParams.set('season',String(job.season_number));url.searchParams.set('episode',String(job.episode_number));}
  const payload=await fetchJson(url.toString(),{headers:{Accept:'application/json','User-Agent':'MovieCore-Native/1.0'}},NATIVE_HTTP_TIMEOUT_MS);
  if(payload?.success===false) throw new Error(payload.message||payload.error||'Gateway returned success=false');
  const videos=Array.isArray(payload?.video_sources)?payload.video_sources:[];
  const subtitles=Array.isArray(payload?.subtitle_sources)?payload.subtitle_sources:[];
  if(!videos.length) throw new Error('No playable video_sources returned');
  return {success:true,provider:'MovieBox-Tui-native',subject_id:subjectId,gateway_id:gatewayId,video_sources:videos,subtitle_sources:subtitles,audio_languages:payload.audio_languages||[...new Set(videos.map(x=>x.language).filter(Boolean))],subtitle_languages:payload.subtitle_languages||[...new Set(subtitles.map(x=>x.language||x.lanName).filter(Boolean))],raw:payload};
}

async function persistScrapeResult(job,data){
  await q(`INSERT INTO moviecore_links(catalog_id,media_type,season_number,episode_number,provider,video_sources,subtitle_sources,audio_languages,subtitle_languages,result_json,updated_at) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10::jsonb,NOW()) ON CONFLICT(catalog_id,season_number,episode_number) DO UPDATE SET provider=EXCLUDED.provider,video_sources=EXCLUDED.video_sources,subtitle_sources=EXCLUDED.subtitle_sources,audio_languages=EXCLUDED.audio_languages,subtitle_languages=EXCLUDED.subtitle_languages,result_json=EXCLUDED.result_json,updated_at=NOW()`,[job.catalog_id,job.media_type,job.season_number,job.episode_number,data.provider,JSON.stringify(data.video_sources||[]),JSON.stringify(data.subtitle_sources||[]),data.audio_languages||[],data.subtitle_languages||[],JSON.stringify(data)]);
}

async function scraperTick() {
  const result = await q(`
    SELECT j.*, c.tmdb_id, c.title
    FROM moviecore_scrape_jobs j
    JOIN moviecore_catalog c ON c.id=j.catalog_id
    WHERE j.status='pending' AND j.next_attempt_at <= NOW()
    ORDER BY j.id ASC
    LIMIT 2
  `);

  for (const job of result.rows) {
    const taskId = String(job.id);
    activeScrapes.set(taskId, {
      id: taskId,
      title: job.title,
      media_type: job.media_type,
      startedAt: Date.now()
    });

    await q(`
      UPDATE moviecore_scrape_jobs
      SET status='running', attempts=attempts+1, updated_at=NOW()
      WHERE id=$1
    `, [job.id]);

    log('scraping', 'info', `START | ${job.title || job.tmdb_id} | tmdb=${job.tmdb_id}`);

    try {
      const data = await nativeMovieBoxScrape(job);
      await persistScrapeResult(job, data);

      await q(`
        UPDATE moviecore_scrape_jobs
        SET status='complete', result_json=$2::jsonb, error_text=NULL, updated_at=NOW()
        WHERE id=$1
      `, [job.id, JSON.stringify(data)]);

      log('scraping', 'info', `SUCCESS | ${job.title || job.tmdb_id} | links=${(data.video_sources||[]).length} subtitles=${(data.subtitle_sources||[]).length}`);
    } catch (error) {
      await q(`
        UPDATE moviecore_scrape_jobs
        SET status='pending',
            error_text=$2,
            next_attempt_at=NOW() + INTERVAL '5 minutes',
            updated_at=NOW()
        WHERE id=$1
      `, [job.id, error.message]);

      log('scraping', 'error', `FAIL | ${job.title || job.tmdb_id} | ${error.message}`);
    } finally {
      activeScrapes.delete(taskId);
    }
  }
}

async function resetMovieCore() {
  await q(`
    TRUNCATE moviecore_links,
             moviecore_scrape_jobs,
             moviecore_episodes,
             moviecore_seasons,
             moviecore_catalog,
             moviecore_state
    RESTART IDENTITY CASCADE
  `);

  tmdbState = { moviePage: 1, tvPage: 1, phase: 'movie', status: 'idle' };

  await q(`
    INSERT INTO moviecore_state(key,value_json)
    VALUES ('tmdb',$1::jsonb)
  `, [JSON.stringify(tmdbState)]);

  logs.tmdb.length = 0;
  logs.scraping.length = 0;
  activeScrapes.clear();

  log('tmdb', 'info', 'MOVIECORE RESET | cursor reset to movie page 1');
}

async function getStatus() {
  const r = await q(`
    SELECT
      COUNT(*)::int AS catalog_total,
      COUNT(*) FILTER (WHERE media_type='movie')::int AS movies,
      COUNT(*) FILTER (WHERE media_type='tv')::int AS series
    FROM moviecore_catalog
  `);

  const episodes = await q(`SELECT COUNT(*)::int AS count FROM moviecore_episodes`);
  const seasons = await q(`SELECT COUNT(*)::int AS count FROM moviecore_seasons`);

  const pending = await q(`
    SELECT
      COUNT(*) FILTER (WHERE status='pending')::int AS pending,
      COUNT(*) FILTER (WHERE status='running')::int AS running,
      COUNT(*) FILTER (WHERE status='complete')::int AS complete
    FROM moviecore_scrape_jobs
  `);

  const db = r.rows[0];
  const p = pending.rows[0];
  const links = await q(`SELECT COUNT(*)::int AS count, COALESCE(SUM(jsonb_array_length(video_sources)),0)::int AS videos, COALESCE(SUM(jsonb_array_length(subtitle_sources)),0)::int AS subtitles FROM moviecore_links`);

  return {
    catalog_total: db.catalog_total,
    movies: db.movies,
    series: db.series,
    anime: 0,
    animations: 0,
    episodes: episodes.rows[0].count,
    seasons: seasons.rows[0].count,
    scraping: {
      active_threads: [...activeScrapes.values()],
      worker_limit: 2
    },
    pending: p,
    links: links.rows[0],
    tmdb: tmdbState,
    health: {
      pressure: 'healthy',
      memory: process.memoryUsage(),
      uptime: Math.floor((Date.now() - startedAt) / 1000),
      worker_limit: 2,
      worker_max: 2,
      worker_health: 'healthy',
      worker_health_reason: 'normal',
      stuck_workers: 0
    }
  };
}

const adminHtml = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MovieZone Admin v9.0</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,sans-serif;background:#0A0A0A;color:#E0E0E0;min-height:100vh;padding:20px}
.container{max-width:1500px;margin:0 auto}
.header{display:flex;justify-content:space-between;align-items:center;padding:16px 24px;background:#121212;border-radius:14px;border:1px solid #1E1E1E;margin-bottom:20px}
.header h1{font-size:22px;font-weight:800;color:#fff}.header h1 span{color:#E50914}
.status{padding:6px 14px;border-radius:8px;background:#0D2818;color:#4CAF50;font-size:12px;font-weight:700}
.panel{background:#121212;border:1px solid #1E1E1E;border-radius:14px;padding:20px;margin-bottom:20px}
.panel h2{font-size:15px;font-weight:700;color:#fff;margin-bottom:14px;display:flex;align-items:center;gap:10px}
.panel h2:before{content:'';width:4px;height:18px;background:#E50914;border-radius:2px}
.badge{margin-left:auto;font-size:11px;background:#1A1A1A;padding:4px 10px;border-radius:8px;color:#888}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px}
.card{background:#0A0A0A;border:1px solid #1E1E1E;border-radius:10px;padding:14px}
.label{font-size:10px;color:#666;text-transform:uppercase}.value{font-size:22px;font-weight:800;color:#fff;margin-top:6px}
.green .value{color:#4CAF50}.red .value{color:#E50914}.blue .value{color:#4FC3F7}.orange .value{color:#FFB74D}
.terminal{background:#050505;border:1px solid #1d1d1d;border-radius:10px;padding:12px;height:260px;overflow:auto;font:11.5px/1.5 monospace}
.line{padding:3px 4px;border-bottom:1px solid #0f0f0f;white-space:pre-wrap;word-break:break-word}.line.warn{color:#ffc266}.line.error{color:#ff7777}.line.info{color:#b8b8b8}
.btn{padding:8px 14px;border-radius:8px;border:none;font-size:12px;font-weight:700;cursor:pointer;margin-right:8px}
.btn-red{background:#E50914;color:#fff}.btn-dark{background:#1A1A1A;color:#ccc;border:1px solid #333}
.empty{color:#444;text-align:center;padding:24px;font-size:12px}
.active-item{background:#0A0A0A;border:1px solid #1E1E1E;border-radius:10px;padding:12px;margin-bottom:10px}
.active-item .title{font-size:13px;font-weight:700;color:#fff;margin-bottom:6px}
.active-item .meta{font-size:11px;color:#888;display:flex;gap:10px;flex-wrap:wrap}
.badge-s{background:#1A1A1A;padding:2px 8px;border-radius:6px;color:#bbb}
</style>
</head>
<body>
<div class="container">
<div class="header">
<h1>🎬 MovieCore <span>Admin v9.0</span></h1>
<div class="status" id="liveStatus">LIVE</div>
</div>

<div class="panel">
<h2>1. TMDB Content Status <span class="badge" id="tmdbCount">—</span></h2>
<div class="grid" id="tmdbStats"><div class="empty">Loading…</div></div>
</div>

<div class="panel">
<h2>2. Scraping Content (Active Threads) <span class="badge" id="activeCount">—</span></h2>
<div id="activeScrapes"><div class="empty">No active scraping threads</div></div>
</div>

<div class="panel">
<h2>3. Scraping Pending Content <span class="badge" id="pendingCount">—</span></h2>
<button class="btn btn-red" onclick="resetAll()">🗑️ Reset MovieCore</button>
<button class="btn btn-dark" onclick="loadAll()">🔄 Refresh Now</button>
<div class="grid" id="pendingStats" style="margin-top:12px"></div>
</div>

<div class="panel">
<h2>4. TMDB Log Streams <span class="badge" id="tmdbLogCount">—</span></h2>
<div class="terminal" id="tmdbLogs"></div>
</div>

<div class="panel">
<h2>5. Scraping Log Streams <span class="badge" id="scrapingLogCount">—</span></h2>
<div class="terminal" id="scrapingLogs"></div>
</div>

<div class="panel">
<h2>System Health <span class="badge" id="healthBadge">—</span></h2>
<div class="grid" id="healthStats"></div>
</div>
</div>

<script>
async function get(url,options){const r=await fetch(url,options);return r.json()}
function esc(v){return String(v??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]))}

async function loadStatus(){
try{
const d=await get('/admin/api/status');
if(!d.success)return;
const t=d.tmdb||{};
document.getElementById('tmdbCount').textContent=(t.catalog_total||0).toLocaleString()+' in catalog';
document.getElementById('tmdbStats').innerHTML=
'<div class="card blue"><div class="label">Catalog Total</div><div class="value">'+(t.catalog_total||0)+'</div></div>'+
'<div class="card green"><div class="label">Movies</div><div class="value">'+(t.movies||0)+'</div></div>'+
'<div class="card green"><div class="label">Series</div><div class="value">'+(t.series||0)+'</div></div>'+
'<div class="card"><div class="label">Anime</div><div class="value">'+(t.anime||0)+'</div></div>'+
'<div class="card"><div class="label">Animations</div><div class="value">'+(t.animations||0)+'</div></div>'+
'<div class="card blue"><div class="label">Episodes</div><div class="value">'+(t.episodes||0)+'</div></div>'+
'<div class="card"><div class="label">Seasons</div><div class="value">'+(t.seasons||0)+'</div></div>';

const a=d.scraping?.active_threads||[];
document.getElementById('activeCount').textContent=a.length+' active / '+(d.scraping?.worker_limit||0)+' limit';
document.getElementById('activeScrapes').innerHTML=a.length?a.map(x=>'<div class="active-item"><div class="title">'+esc(x.title||x.id)+'</div><div class="meta"><span class="badge-s">Type: '+esc(x.media_type)+'</span><span class="badge-s">Elapsed: '+Math.round((Date.now()-x.startedAt)/1000)+'s</span></div></div>').join(''):'<div class="empty">No active scraping threads</div>';

const p=d.pending||{};
document.getElementById('pendingCount').textContent=((p.pending||0)+(p.running||0)).toLocaleString()+' in queue';
document.getElementById('pendingStats').innerHTML=
'<div class="card orange"><div class="label">Pending</div><div class="value">'+(p.pending||0)+'</div></div>'+
'<div class="card blue"><div class="label">Running</div><div class="value">'+(p.running||0)+'</div></div>'+
'<div class="card green"><div class="label">Complete</div><div class="value">'+(p.complete||0)+'</div></div>';

const h=d.health||{},m=h.memory||{};
document.getElementById('healthBadge').textContent=(h.pressure||'healthy').toUpperCase();
document.getElementById('healthStats').innerHTML=
'<div class="card green"><div class="label">Pressure</div><div class="value">'+esc(h.pressure)+'</div></div>'+
'<div class="card"><div class="label">RSS MB</div><div class="value">'+Math.round((m.rss||0)/1024/1024)+'</div></div>'+
'<div class="card"><div class="label">Heap MB</div><div class="value">'+Math.round((m.heapUsed||0)/1024/1024)+'</div></div>'+
'<div class="card blue"><div class="label">Workers</div><div class="value">'+(h.worker_limit||0)+'</div></div>'+
'<div class="card"><div class="label">Uptime</div><div class="value">'+Math.round((h.uptime||0)/60)+'m</div></div>';
}catch(e){}
}

async function loadLogs(source){
try{
const d=await get('/admin/api/logs?source='+source);
const el=document.getElementById(source==='tmdb'?'tmdbLogs':'scrapingLogs');
const logs=d.logs||[];
document.getElementById(source==='tmdb'?'tmdbLogCount':'scrapingLogCount').textContent=logs.length+' entries';
el.innerHTML=logs.map(x=>'<div class="line '+esc(x.level)+'"><span style="color:#555">'+esc(String(x.timestamp).slice(11,19))+'</span> '+esc(x.message)+'</div>').join('')||'<div class="line info">No logs</div>';
el.scrollTop=el.scrollHeight;
}catch(e){}
}

async function resetAll(){
if(!confirm('Reset MovieCore TMDB catalog and scraper queue and restart from page 1?'))return;
await fetch('/admin/api/reset',{method:'POST'});
await loadAll();
}

async function loadAll(){await Promise.all([loadStatus(),loadLogs('tmdb'),loadLogs('scraping')])}
loadAll();
setInterval(loadStatus,3000);
setInterval(()=>loadLogs('tmdb'),3000);
setInterval(()=>loadLogs('scraping'),3000);
</script>
</body>
</html>`;

app.get('/health', async (_req, res) => {
  try {
    await q('SELECT 1');
    res.json({ ok: true, app: 'MovieCore' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get('/admin', auth, (_req, res) => res.send(adminHtml));

app.get('/admin/api/status', auth, async (_req, res) => {
  try {
    res.json({ success: true, tmdb: await getStatus().then(x => ({
      catalog_total: x.catalog_total,
      movies: x.movies,
      series: x.series,
      anime: x.anime,
      animations: x.animations,
      episodes: x.episodes,
      seasons: x.seasons
    })), scraping: (await getStatus()).scraping, pending: (await getStatus()).pending, health: (await getStatus()).health });
  } catch (e) {
    res.status(500).json({ success:false, error:e.message });
  }
});

app.get('/admin/api/logs', auth, (req, res) => {
  const source = req.query.source === 'scraping' ? 'scraping' : 'tmdb';
  res.json({ success:true, logs: logs[source].slice(-200) });
});

app.post('/admin/api/reset', auth, async (_req, res) => {
  try {
    await resetMovieCore();
    res.json({ success:true });
  } catch (e) {
    log('tmdb','error',`RESET FAIL | ${e.message}`);
    res.status(500).json({ success:false,error:e.message });
  }
});

app.post('/admin/api/tmdb/start', auth, async (_req, res) => {
  try {
    await tmdbTick();
    res.json({ success:true, state:tmdbState });
  } catch(e) {
    log('tmdb','error',e.message);
    res.status(500).json({ success:false,error:e.message });
  }
});

app.get('/api/movies', async (_req,res) => {
  const r=await q(`SELECT * FROM moviecore_catalog WHERE media_type='movie' ORDER BY updated_at DESC LIMIT 100`);
  res.json(r.rows);
});

app.get('/api/tv', async (_req,res) => {
  const r=await q(`SELECT * FROM moviecore_catalog WHERE media_type='tv' ORDER BY updated_at DESC LIMIT 100`);
  res.json(r.rows);
});

app.get('/api/links/:catalogId', async (req,res) => { const r=await q(`SELECT * FROM moviecore_links WHERE catalog_id=$1 ORDER BY COALESCE(season_number,0), COALESCE(episode_number,0)`, [req.params.catalogId]); res.json(r.rows); });

let loopRunning = false;
async function backgroundLoop(){
  if(loopRunning)return;
  loopRunning=true;
  try{
    await tmdbTick();
    await scraperTick();
  }catch(e){
    log('tmdb','error',`ENGINE ERROR | ${e.message}`);
  }finally{
    loopRunning=false;
  }
}

(async()=>{
  try{
    await schema();
    log('tmdb','info','MovieCore started');
    app.listen(PORT,()=>console.log(`MovieCore running on ${PORT}`));
    backgroundLoop();
    setInterval(backgroundLoop, Number(process.env.ENGINE_INTERVAL_MS||5000));
  }catch(e){
    console.error(e);
    process.exit(1);
  }
})();

process.on('SIGINT',async()=>{await pool.end();process.exit(0)});
process.on('SIGTERM',async()=>{await pool.end();process.exit(0)});
