/**
 * Finance 주식 시세 중계 Worker  (2026-11-27)
 * -------------------------------------------------------------
 * Cloudflare 대시보드 → Workers & Pages → (fancy-lake-fe84) → 편집(Quick edit)에서
 * 이 파일 내용 전체를 그대로 붙여 넣고 [Deploy]만 누르면 됩니다. import·설정값 없음.
 *
 * 주소 예시
 *   https://fancy-lake-fe84.nohhw.workers.dev/                      → 상태·도움말
 *   .../quote?symbols=005930.KS,000660.KS,^KS11,KRW=X               → 현재가 묶음
 *   .../history?symbol=005930.KS&range=5y                           → 5년 일별 종가
 *   .../history?symbols=379800.KS,360750.KS&range=5y                → 여러 종목 한 번에
 *
 * 시세 출처: Yahoo Finance(전 세계) → 실패하면 국내는 네이버 금융, 미국은 네이버 해외주식으로 대체.
 * 응답은 10초(시세)·6시간(과거 일봉) 동안 메모리에 캐시해 호출량을 줄입니다.
 */

const ALLOWED_ORIGIN = [
  /^https:\/\/nohhyunwook\.github\.io$/,
  /^http:\/\/localhost(:\d+)?$/,
  /^http:\/\/127\.0\.0\.1(:\d+)?$/
];
const QUOTE_TTL = 12 * 1000;          // 현재가 12초 (앱은 10초마다 물어 캐시와 번갈아 씁니다)
const HIST_TTL = 6 * 60 * 60 * 1000;  // 과거 일봉 6시간
const UPSTREAM_TIMEOUT = 8000;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';

const MEM = new Map();   // key → {at, data}

function corsHeaders(req){
  const origin = req.headers.get('Origin') || '';
  const ok = ALLOWED_ORIGIN.some(re => re.test(origin));
  return {
    'Access-Control-Allow-Origin': ok ? origin : '*',
    'Access-Control-Allow-Methods': 'GET,HEAD,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin'
  };
}
function json(data, req, status, extra){
  const h = Object.assign({'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store'}, corsHeaders(req), extra || {});
  return new Response(JSON.stringify(data), {status: status || 200, headers: h});
}
function num(v){
  if (v === null || v === undefined) return null;
  const n = typeof v === 'number' ? v : Number(String(v).replace(/[^0-9.\-]/g, ''));
  return isFinite(n) ? n : null;
}
async function memo(key, ttl, producer){
  const hit = MEM.get(key);
  if (hit && Date.now() - hit.at < ttl) return hit.data;
  const data = await producer();
  MEM.set(key, {at: Date.now(), data: data});
  if (MEM.size > 800){                       // 메모리 보호 — 오래된 것부터 버립니다
    const keys = Array.from(MEM.keys()).slice(0, 300);
    keys.forEach(k => MEM.delete(k));
  }
  return data;
}
async function upstream(url, opts){
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), UPSTREAM_TIMEOUT);
  try {
    return await fetch(url, Object.assign({signal: ctl.signal, redirect: 'follow'}, opts || {}));
  } finally {
    clearTimeout(timer);
  }
}
function yahooHeaders(){ return {'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*', 'Accept-Language': 'ko,en;q=0.8'}; }
/* query2 가 막히면 query1 로 한 번 더 (Yahoo 는 같은 데이터를 두 호스트로 나눠 줍니다) */
async function yahooJson(path){
  let lastErr = null;
  for (const host of ['query2.finance.yahoo.com', 'query1.finance.yahoo.com']){
    try {
      const r = await upstream('https://' + host + path, {headers: yahooHeaders()});
      if (!r.ok){ lastErr = new Error('yahoo ' + r.status); continue; }
      return await r.json();
    } catch (e){ lastErr = e; }
  }
  throw lastErr || new Error('yahoo failed');
}

/* ---------------- 현재가 ---------------- */
async function yahooQuote(sym){
  const j = await yahooJson('/v8/finance/chart/' + encodeURIComponent(sym) +
            '?range=1d&interval=1m&includePrePost=false');
  const res = j && j.chart && j.chart.result && j.chart.result[0];
  if (!res || !res.meta) throw new Error('no data');
  const m = res.meta;
  let price = num(m.regularMarketPrice);
  const quote = (res.indicators && res.indicators.quote && res.indicators.quote[0]) || {};
  const closes = (quote.close || []).filter(v => num(v) !== null);
  if (price === null && closes.length) price = num(closes[closes.length - 1]);
  let prev = num(m.previousClose);
  if (prev === null) prev = num(m.chartPreviousClose);
  if (prev === null && closes.length > 1) prev = num(closes[0]);
  if (price === null) throw new Error('no price');
  const change = (prev !== null) ? price - prev : num(m.regularMarketChangePercent) !== null ? null : null;
  const pct = (prev) ? (price - prev) / prev * 100 : num(m.regularMarketChangePercent);
  return {
    symbol: sym, name: m.shortName || m.longName || sym, currency: m.currency || '',
    price: price, prevClose: prev, change: (change === null ? null : Math.round(change * 100) / 100),
    changePct: (pct === null ? null : Math.round(pct * 100) / 100),
    marketTime: m.regularMarketTime ? m.regularMarketTime * 1000 : null,
    exchange: m.fullExchangeName || m.exchangeName || '', source: 'yahoo'
  };
}
function isKrCode(sym){ return /^\d{5,6}[A-Z0-9]?\.K[SQ]$/i.test(sym); }
function krCodeOf(sym){ return sym.replace(/\.K[SQ]$/i, ''); }

async function naverQuote(sym){
  const code = krCodeOf(sym);
  const r = await upstream('https://polling.finance.naver.com/api/realtime/domestic/stock/' + encodeURIComponent(code),
                           {headers: {'User-Agent': UA, 'Accept': 'application/json'}});
  if (!r.ok) throw new Error('naver ' + r.status);
  const j = await r.json();
  const d = j && j.datas && j.datas[0];
  if (!d) throw new Error('no data');
  const over = d.overMarketPriceInfo || {};
  const price = num(over.overPrice) !== null ? num(over.overPrice) : num(d.closePrice);
  const prevChg = num(over.overPrice) !== null ? num(over.compareToPreviousClosePrice) : num(d.compareToPreviousClosePrice);
  const rate = num(over.overPrice) !== null ? num(over.fluctuationsRatio) : num(d.fluctuationsRatio);
  if (price === null) throw new Error('no price');
  const prev = (prevChg === null) ? null : price - prevChg;
  return {
    symbol: sym, name: d.stockName || sym, currency: 'KRW',
    price: price, prevClose: prev, change: prevChg, changePct: rate,
    marketTime: Date.parse(d.localTradedAt || '') || null,
    exchange: (d.stockExchangeType && d.stockExchangeType.nameKor) || '', source: 'naver'
  };
}
/* 미국 종목 대체 출처 — 네이버 해외주식(나스닥 .O, 뉴욕 .N, 아멕스 .A, ETF는 접미사 없음) */
async function naverWorldQuote(sym){
  const cands = /\.(O|N|A)$/i.test(sym) ? [sym] : [sym + '.O', sym + '.N', sym + '.A', sym];
  for (const c of cands){
    try {
      const r = await upstream('https://api.stock.naver.com/stock/' + encodeURIComponent(c) + '/basic',
                               {headers: {'User-Agent': UA, 'Accept': 'application/json'}});
      if (!r.ok) continue;
      const d = await r.json();
      const price = num(d.closePrice);
      if (price === null || !d.stockName) continue;
      const chg = num(d.compareToPreviousClosePrice);
      return {
        symbol: sym, name: d.stockName || sym,
        currency: (d.currencyType && d.currencyType.code) || 'USD',
        price: price, prevClose: chg === null ? null : price - chg, change: chg,
        changePct: num(d.fluctuationsRatio),
        marketTime: Date.parse(d.localTradedAt || '') || null,
        exchange: (d.stockExchangeType && d.stockExchangeType.nameKor) || '', source: 'naver'
      };
    } catch (e){}
  }
  throw new Error('네이버 해외주식에 없음');
}
async function oneQuote(sym){
  try {
    return await memo('q:' + sym, QUOTE_TTL, () => yahooQuote(sym));
  } catch (e) {
    if (isKrCode(sym)){
      try { return await memo('nq:' + sym, QUOTE_TTL, () => naverQuote(sym)); } catch (e2){}
    } else if (/^[A-Za-z][A-Za-z.\-]{0,9}$/.test(sym)){
      try { return await memo('nw:' + sym, QUOTE_TTL, () => naverWorldQuote(sym)); } catch (e2){}
    }
    throw e;
  }
}
async function pool(items, limit, task){
  const out = new Array(items.length), errs = new Array(items.length);
  let next = 0;
  const run = async () => {
    for (;;){
      const i = next++;
      if (i >= items.length) return;
      try { out[i] = await task(items[i], i); } catch (e){ errs[i] = e; }
    }
  };
  await Promise.all(Array.from({length: Math.max(1, Math.min(limit, items.length))}, run));
  return {out, errs};
}
async function handleQuote(url, req){
  const raw = (url.searchParams.get('symbols') || url.searchParams.get('symbol') || '').trim();
  const syms = raw.split(',').map(s => s.trim()).filter(Boolean).slice(0, 60);
  if (!syms.length) return json({ok: false, error: 'symbols 파라미터가 필요합니다 (예: ?symbols=005930.KS,^KS11)'}, req, 400);
  const {out, errs} = await pool(syms, 8, oneQuote);
  const quotes = [], errors = [];
  for (let i = 0; i < syms.length; i++){
    const q = out[i];
    if (q && q.price !== null) quotes.push(q);
    else errors.push({symbol: syms[i], error: String((errs[i] && errs[i].message) || '시세 없음')});
  }
  return json({ok: quotes.length > 0, ts: Date.now(), count: quotes.length, quotes: quotes, errors: errors}, req, quotes.length ? 200 : 502);
}

/* ---------------- 과거 일봉 ---------------- */
function rangeToStart(range){
  const m = /^(\d+)(y|mo|d)$/.exec(String(range || '5y'));
  const d = new Date();
  if (!m) return new Date(d.getTime() - 5 * 365 * 86400000);
  const n = parseInt(m[1], 10);
  if (m[2] === 'y') d.setFullYear(d.getFullYear() - n);
  else if (m[2] === 'mo') d.setMonth(d.getMonth() - n);
  else d.setDate(d.getDate() - n);
  return d;
}
async function yahooHistory(sym, range){
  const rng = /^(1mo|3mo|6mo|1y|2y|3y|5y|10y|max)$/.test(range) ? range : '5y';
  const j = await yahooJson('/v8/finance/chart/' + encodeURIComponent(sym) +
            '?range=' + rng + '&interval=1d&includePrePost=false');
  const res = j && j.chart && j.chart.result && j.chart.result[0];
  if (!res || !res.timestamp) throw new Error('no data');
  const closes = (res.indicators && res.indicators.quote && res.indicators.quote[0] && res.indicators.quote[0].close) || [];
  const out = [];
  for (let i = 0; i < res.timestamp.length; i++){
    const c = num(closes[i]);
    if (c !== null) out.push([res.timestamp[i] * 1000, Math.round(c * 100) / 100]);
  }
  if (!out.length) throw new Error('no closes');
  return {symbol: sym, currency: (res.meta && res.meta.currency) || '', name: (res.meta && (res.meta.shortName || res.meta.longName)) || sym, source: 'yahoo', candles: out};
}
async function naverHistory(sym, range){
  const code = krCodeOf(sym);
  const f = d => d.getFullYear() + String(d.getMonth() + 1).padStart(2, '0') + String(d.getDate()).padStart(2, '0');
  const start = f(rangeToStart(range)), end = f(new Date());
  const u = 'https://api.finance.naver.com/siseJson.naver?symbol=' + encodeURIComponent(code) +
            '&requestType=1&startTime=' + start + '&endTime=' + end + '&timeframe=day';
  const r = await upstream(u, {headers: {'User-Agent': UA, 'Accept': 'text/plain,*/*'}});
  if (!r.ok) throw new Error('naver ' + r.status);
  const txt = await r.text();
  const rows = txt.match(/\[[^\]]*\]/g) || [];
  const out = [];
  rows.forEach(line => {
    const parts = line.replace(/^\[|\]$/g, '').split(',').map(s => s.trim().replace(/^'|'$/g, ''));
    if (parts.length < 5) return;
    const d = parts[0], close = num(parts[4]);
    if (!/^\d{8}$/.test(d) || close === null) return;
    const t = Date.UTC(+d.slice(0, 4), +d.slice(4, 6) - 1, +d.slice(6, 8), 6, 30);  // 15:30 KST 마감
    out.push([t, close]);
  });
  if (!out.length) throw new Error('no data');
  out.sort((a, b) => a[0] - b[0]);
  return {symbol: sym, currency: 'KRW', name: sym, source: 'naver', candles: out};
}
async function oneHistory(sym, range){
  try {
    return await memo('h:' + sym + ':' + range, HIST_TTL, () => yahooHistory(sym, range));
  } catch (e) {
    if (isKrCode(sym)){
      try { return await memo('nh:' + sym + ':' + range, HIST_TTL, () => naverHistory(sym, range)); } catch (e2){}
    }
    throw e;
  }
}
async function handleHistory(url, req){
  const range = (url.searchParams.get('range') || '5y').trim();
  const raw = (url.searchParams.get('symbols') || url.searchParams.get('symbol') || '').trim();
  const syms = raw.split(',').map(s => s.trim()).filter(Boolean).slice(0, 40);
  if (!syms.length) return json({ok: false, error: 'symbol 파라미터가 필요합니다 (예: ?symbol=005930.KS&range=5y)'}, req, 400);
  const {out, errs} = await pool(syms, 6, s => oneHistory(s, range));
  const series = {}, errors = [];
  for (let i = 0; i < syms.length; i++){
    const h = out[i];
    if (h && h.candles && h.candles.length) series[syms[i]] = h;
    else errors.push({symbol: syms[i], error: String((errs[i] && errs[i].message) || '과거 시세 없음')});
  }
  const single = syms.length === 1 ? (series[syms[0]] || null) : null;
  return json({
    ok: Object.keys(series).length > 0, ts: Date.now(), range: range,
    symbol: syms.length === 1 ? syms[0] : undefined,
    currency: single ? single.currency : undefined,
    candles: single ? single.candles : undefined,
    series: series, errors: errors
  }, req, Object.keys(series).length ? 200 : 502);
}

export default {
  async fetch(request){
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, {status: 204, headers: corsHeaders(request)});
    if (request.method !== 'GET' && request.method !== 'HEAD')
      return json({ok: false, error: 'GET 만 지원합니다'}, request, 405);

    const path = url.pathname.replace(/\/+$/, '');
    try {
      if (path === '' || path === '/index.html'){
        const t = url.searchParams.get('test');
        if (t){
          try {
            const q = await oneQuote(t);
            return json({ok: true, test: t, quote: q}, request);
          } catch (e){
            return json({ok: false, test: t, error: String(e.message || e)}, request, 502);
          }
        }
        const body = [
          'Finance 주식 시세 중계 Worker — 정상 동작 중입니다.',
          '',
          'GET /quote?symbols=005930.KS,000660.KS,^KS11,^GSPC,^IXIC,KRW=X',
          'GET /history?symbol=005930.KS&range=5y',
          'GET /history?symbols=379800.KS,360750.KS&range=5y',
          'GET /?test=005930.KS   ← 한 종목 시험',
          '',
          '웹앱 ⚙ 설정 → 동기화 → 주식 시세 서버 주소에 이 Worker 주소를 넣으면 앱이 10초마다 시세를 받아옵니다.'
        ].join('\n');
        return new Response(body, {status: 200, headers: Object.assign({'Content-Type': 'text/plain; charset=utf-8'}, corsHeaders(request))});
      }
      if (path === '/quote' || path === '/quotes') return await handleQuote(url, request);
      if (path === '/history' || path === '/candles') return await handleHistory(url, request);
      return json({ok: false, error: '알 수 없는 경로: ' + path}, request, 404);
    } catch (e){
      return json({ok: false, error: String((e && e.message) || e)}, request, 500);
    }
  }
};
