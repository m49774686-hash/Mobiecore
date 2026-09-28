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
const SCRAPER_URL = (process.env.SCRAPER_URL || '').replace(/\/+$/, '');
const SCRAPER_TIMEOUT_MS = Math.max(5000, Number(process.env.SCRAPER_TIMEOUT_MS || 60000));
const ADMIN_USERNAME = process.env.ADMIN_USERNAME || '';
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';

if (!DATABASE_URL) console.warn('[MovieCore] DATABASE_URL is missing');
if (!TMDB_API_KEY) console.warn('[MovieCore] TMDB_API_KEY is missing');
if (!SCRAPER_URL) console.warn('[MovieCore] SCRAPER_URL is missing');

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
  External scraper adapter.
  MovieCore never scrapes providers itself. It calls the already-deployed
  scraper through SCRAPER_URL.

  Supported deployed scraper contract:
    Movie:
      GET /extract?tmdb_id=<id>&type=movie
    TV episode:
      GET /extract?tmdb_id=<id>&type=tv&season=<n>&episode=<n>

  SCRAPER_PATH can override /extract if the deployed scraper uses another
  route, but the default is /extract.
*/
const SCRAPER_PATH = String(process.env.SCRAPER_PATH || '/extract').trim() || '/extract';

async function callExternalScraper(job) {
  if (!SCRAPER_URL) throw new Error('SCRAPER_URL is not configured');

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SCRAPER_TIMEOUT_MS);

  try {
    const params = new URLSearchParams();
    params.set('tmdb_id', String(job.tmdb_id));

    if (job.media_type === 'movie') {
      params.set('type', 'movie');
    } else {
      params.set('type', 'tv');
      params.set('season', String(Number(job.season_number)));
      params.set('episode', String(Number(job.episode_number)));
    }

    const base = `${SCRAPER_URL}${SCRAPER_PATH.startsWith('/') ? SCRAPER_PATH : `/${SCRAPER_PATH}`}`;
    const url = `${base}${base.includes('?') ? '&' : '?'}${params.toString()}`;

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        Accept: 'application/json, text/plain, */*',
        'User-Agent': 'MovieCore/1.0'
      },
      signal: controller.signal
    });

    const text = await response.text();
    let data;
    try { data = JSON.parse(text); } catch { data = { raw: text }; }

    if (!response.ok) {
      const compact = text.replace(/\s+/g, ' ').trim().slice(0, 800);
      throw new Error(`SCRAPER ${response.status} GET ${SCRAPER_PATH}: ${compact}`);
    }

    // Do not treat a scraper-level failure returned with HTTP 200 as success.
    if (data && data.success === false) {
      throw new Error(`SCRAPER returned success=false${data.error ? `: ${String(data.error).slice(0, 600)}` : ''}`);
    }

    return data;
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error(`SCRAPER TIMEOUT after ${SCRAPER_TIMEOUT_MS}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timer);
  }
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
      const data = await callExternalScraper(job);

      await q(`
        UPDATE moviecore_scrape_jobs
        SET status='complete', result_json=$2::jsonb, error_text=NULL, updated_at=NOW()
        WHERE id=$1
      `, [job.id, JSON.stringify(data)]);

      log('scraping', 'info', `SUCCESS | ${job.title || job.tmdb_id}`);
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
    TRUNCATE moviecore_scrape_jobs,
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
