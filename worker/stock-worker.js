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
 *   POST .../ingest                                                 → 카드·은행 알림 실시간 반영 (2026-11-27 추가)
 *
 * 시세 출처: Yahoo Finance(전 세계) → 실패하면 국내는 네이버 금융, 미국은 네이버 해외주식으로 대체.
 * 응답은 10초(시세)·6시간(과거 일봉) 동안 메모리에 캐시해 호출량을 줄입니다.
 *
 * 실시간 반영(/ingest)은 아래 환경변수를 씁니다 — 코드에는 값이 없습니다.
 *   GITHUB_TOKEN : NOHHYUNWOOK/card-budget-data (비공개) Contents 읽기·쓰기 토큰
 *   INGEST_KEY   : 폰(MacroDroid)이 함께 보내는 비밀 문자열
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
    'Access-Control-Allow-Methods': 'GET,HEAD,POST,OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type,X-Ingest-Key',
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

/* ============================================================
   실시간 가계부 반영 — POST /ingest  (2026-11-27)
   ------------------------------------------------------------
   폰의 MacroDroid 가 카드·은행 알림 문구를 이 주소로 그대로 보내면
   곧바로 비공개 저장소 NOHHYUNWOOK/card-budget-data 의
   data/YYYY-MM.json(카드) · data/bank-YYYY-MM.json(은행) 에 새 줄 하나를 더합니다.
   기존 줄·순서(order)·삭제 표시(deleted)·제외(excluded)·결제일(dueDates)은 그대로 둡니다.

   입력 (아무 형태나 가능)
     POST /ingest         헤더 X-Ingest-Key: <INGEST_KEY>
     body  text/plain     알림 문구 그대로
     body  application/json  {"title":"...","text":"..."} 또는 {"message":"..."} (+ "key" 가능)
     ?key=<INGEST_KEY>    헤더 대신 주소에 붙여도 됩니다

   응답
     200 {ok:true, added:1, file, row}          → 새로 반영됨
     200 {ok:true, skipped:"duplicate"}         → 이미 있는 거래
     200 {ok:true, skipped:"no_keyword"}        → 광고·안내·테스트 문자
     200 {ok:true, skipped:"excluded_account"}  → 우리은행 055*** 이외 계좌
     401 / 409 / 502                            → 인증 실패 / 동시 저장 충돌 / 저장소 오류
   ============================================================ */

const GH_API = 'https://api.github.com';
const GH_OWNER = 'NOHHYUNWOOK';
const GH_REPO = 'card-budget-data';
const GH_BRANCH = 'main';
const INGEST_BODY_MAX = 8 * 1024;
const KST_MS = 9 * 60 * 60 * 1000;

/* ---------- 작은 도우미 ---------- */
function pad2(n){ return String(n).padStart(2, '0'); }
function kstOf(ms){
  const d = new Date((ms || Date.now()) + KST_MS);
  return {y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate(), hh: d.getUTCHours(), mi: d.getUTCMinutes()};
}
function dateStrOf(p){ return p.y + '-' + pad2(p.m) + '-' + pad2(p.d); }
function hhmmOf(p){ return pad2(p.hh) + ':' + pad2(p.mi); }
function fmtNum(n){ return String(Math.round(+n || 0)).replace(/\B(?=(\d{3})+(?!\d))/g, ','); }
function nowIso(){ return new Date().toISOString(); }
function safeEqual(a, b){
  const x = String(a == null ? '' : a), y = String(b == null ? '' : b);
  if (!x || !y || x.length !== y.length) return false;
  let r = 0;
  for (let i = 0; i < x.length; i++) r |= x.charCodeAt(i) ^ y.charCodeAt(i);
  return r === 0;
}

/* 결제월 — 앱과 같은 규칙: 전월 13일 ~ 당월 12일 사용분이 그 달 결제분 (13일 이후는 다음 달) */
function billingMonthOf(dateStr){
  const [y, m, d] = String(dateStr).split('-').map(Number);
  if (d >= 13) return m === 12 ? (y + 1) + '-01' : y + '-' + pad2(m + 1);
  return y + '-' + pad2(m);
}
function rid(){
  const A = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < 10; i++) s += A[Math.floor(Math.random() * A.length)];
  return 'r' + s;
}

/* ---------- 문자 정리 ---------- */
function normSms(t){
  let s = String(t == null ? '' : t);
  try { if (s.normalize) s = s.normalize('NFKC'); } catch (e) {}   /* ＧＳ２５ → GS25 */
  return s.replace(/\u00a0|\u3000/g, ' ').replace(/\r\n?/g, '\n');
}
const KEYWORD_RE = /(승인|결제|입금|출금|취소)/;

function cardIssuerOf(t){
  if (/KB\s*국민카드|KB\s*국민|KB\s*Pay|KB\s*페이/i.test(t)) return 'KB국민카드';
  if (/우리카드/.test(t)) return '우리카드';
  if (/현대카드/.test(t)) return '현대카드';
  if (/삼성카드/.test(t)) return '삼성카드';
  return '';
}

/* 금액 후보 — '누적·잔액·한도' 뒤 숫자는 거래금액이 아니므로 뺍니다 */
function amountCandidates(t){
  const out = [];
  const re = /(\d[\d,]*)\s*원/g;
  let m;
  while ((m = re.exec(t))){
    const pre = t.slice(Math.max(0, m.index - 10), m.index);
    const excl = /(누적|잔액|한도|잔여|가능|합계|총액|금액)\s*:?\s*$/.test(pre);
    out.push({v: Math.abs(Number(String(m[1]).replace(/,/g, ''))) || 0, excluded: excl, index: m.index});
  }
  return out;
}

/* 날짜·시각 — '11/27 19:00', '11/25 19:57:54', '11월 27일 오후 7:00' 지원. 없으면 지금(KST) */
function findDateTime(t, nowMs){
  const base = kstOf(nowMs);
  let mo = null, da = null, hh = null, mi = null, guessed = true;
  let m = /(\d{1,2})\s*\/\s*(\d{1,2})\s+(\d{1,2}):(\d{2})(?::\d{2})?/.exec(t);
  if (m){ mo = +m[1]; da = +m[2]; hh = +m[3]; mi = +m[4]; guessed = false; }
  else {
    m = /(\d{1,2})\s*월\s*(\d{1,2})\s*일\s*(오전|오후)?\s*(\d{1,2}):(\d{2})/.exec(t);
    if (m){
      mo = +m[1]; da = +m[2]; hh = +m[4]; mi = +m[5]; guessed = false;
      if (m[3] === '오후' && hh < 12) hh += 12;
      if (m[3] === '오전' && hh === 12) hh = 0;
    } else {
      m = /(\d{1,2})\s*\/\s*(\d{1,2})/.exec(t);
      if (m){ mo = +m[1]; da = +m[2]; hh = base.hh; mi = base.mi; }
    }
  }
  if (mo === null || !(mo >= 1 && mo <= 12) || !(da >= 1 && da <= 31)){
    return {date: dateStrOf(base), time: hhmmOf(base), guessed: true};
  }
  if (hh === null || !(hh >= 0 && hh <= 23) || !(mi >= 0 && mi <= 59)){ hh = base.hh; mi = base.mi; }
  let y = base.y;
  let cand = Date.UTC(y, mo - 1, da, hh, mi) - KST_MS;
  if (cand > (nowMs || Date.now()) + 2 * 86400000){ y -= 1; cand = Date.UTC(y, mo - 1, da, hh, mi) - KST_MS; }  /* 연말·연초 */
  return {date: y + '-' + pad2(mo) + '-' + pad2(da), time: pad2(hh) + ':' + pad2(mi), guessed: guessed};
}

/* 가맹점 — 카드사 이름·승인/일시불·날짜·금액·누적·가린이름을 걷어낸 나머지 */
function cleanMerchant(t){
  const s = t
    .replace(/\[[^\]]*\]/g, ' ')
    .replace(/\(주\)|\(유\)|㈜/g, ' ')
    .replace(/누적\s*:?\s*[\d,]+\s*원?/g, ' ')
    .replace(/잔액\s*:?\s*[\d,]+\s*원?/g, ' ')
    .replace(/\d{1,2}\s*\/\s*\d{1,2}\s+(\d{1,2}):(\d{2})(?::\d{2})?/g, ' ')
    .replace(/\d{1,2}\s*월\s*\d{1,2}\s*일\s*(오전|오후)?\s*\d{1,2}:\d{2}/g, ' ')
    .replace(/[\d,]+\s*원/g, ' ')
    .replace(/(KB국민카드|KB\s*국민|KB\s*Pay|KB\s*페이|우리카드|현대카드|삼성카드)\s*\d{0,4}/gi, ' ')
    .replace(/[가-힣]\s*\*\s*[가-힣]?\s*님?/g, ' ')     /* 노*욱님·김*수 */
    .replace(/(일시불|할부|체크|신용|승인|결제|취소|정상|거절)/g, ' ')
    .replace(/[\/|·:;,\-–]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  return s.slice(0, 60);
}

/* ---------- 분류 ----------
   사용자의 실제 분류 규칙은 비공개 저장소의 data/rules.json(앱 ⚙ 설정 → 분류 규칙)을 그대로 읽어 쓵니다.
   공개 저장소에 개인 단골 가맹점 이름을 넣지 않기 위해서입니다. 읽지 못했을 때만 아래 일반 규칙으로 대신합니다. */
const CAT_COLORS = {생활비: '#3b82f6', 용돈: '#f59e0b', 기타: '#94a3b8', 보험료: '#8b5cf6', 생활공과금: '#f43f5e', EV9충전: '#10b981', '하이패스/세차': '#06b6d4'};
const FALLBACK_RULES = [
  {category: 'EV9충전', keywords: ['전기차충전', 'EV충전', '한국자동차환경협회'], min: null, max: null},
  {category: '하이패스/세차', keywords: ['하이패스', '통행료', '한국도로공사'], min: null, max: null},
  {category: '보험료', keywords: ['보험'], min: null, max: null},
  {category: '생활공과금', keywords: ['통신요금', '유선상품', '브로드밴드', '넷플릭스', '멤버십', '렌탈'], min: null, max: null},
  {category: '기타', keywords: ['지방세', '국세', '관세', '세금', '지자체세입금'], min: null, max: null},
  {category: '용돈', keywords: ['CU', '씨유', 'GS25', '지에스25', '세븐일레븐', '편의점'], min: null, max: 10000},
  {category: '생활비', keywords: ['CU', '씨유', 'GS25', '지에스25', '세븐일레븐', '편의점'], min: 12000, max: null},
  {category: '용돈', keywords: ['주차', '파크', '티머니', '버스', '지하철'], min: null, max: null},
  {category: '생활비', keywords: ['식당', '음식점', '카페', '커피', '스타벅스', '베이커리', '마트'], min: null, max: null},
  {category: '생활비', keywords: ['쿠팡'], min: null, max: 99999},
  {category: '기타', keywords: ['쿠팡'], min: 100000, max: null}
];
/* 같은 결제수단을 다르게 적는 이름 (규칙 키워드 → 문자에 실제로 찍히는 말) */
const KEYWORD_SYNONYMS = {
  '구글플레이': ['구글페이'],
  '토스페이먼츠': ['토스페이'],
  '우아한형제들': ['배달의민족', '배민']
};
function classify(merchant, amount, rules){
  const m = String(merchant || '');
  const amt = Math.abs(+amount || 0);
  for (const r of (rules || [])){
    const kws = (r.keywords || []).filter(Boolean);
    if (!kws.length) continue;
    const hit = kws.some(k => m.indexOf(k) >= 0 || (KEYWORD_SYNONYMS[k] || []).some(s => m.indexOf(s) >= 0));
    if (!hit) continue;
    if (r.min != null && amt < r.min) continue;
    if (r.max != null && amt > r.max) continue;
    return r.category;
  }
  return '기타';   /* 앱과 같은 기본값 (autoCategory 가 null 이면 기타) */
}
async function loadRules(env){
  const hit = MEM.get('ingest:rules');
  if (hit && Date.now() - hit.at < 60 * 1000) return hit.data;
  let rules = null;
  try {
    const f = await ghGetFile(env, 'data/rules.json');
    if (f){
      const j = JSON.parse(f.text);
      if (j && Array.isArray(j.rules) && j.rules.length) rules = j.rules;
    }
  } catch (e) {}
  if (!rules) rules = FALLBACK_RULES;
  MEM.set('ingest:rules', {at: Date.now(), data: rules});
  return rules;
}

/* ---------- 중복 판정 ---------- */
/* 앱(merchKey)과 같은 방식으로 가맹점 이름을 비교용으로 줄입니다 — 법인 표기·공백·기호 차이 무시 */
function merchKey(s){
  return String(s == null ? '' : s).normalize('NFKC').toLowerCase()
    .replace(/\(주\)|\(유\)|주식회사?|㈜|\(합자\)|\(유한\)/g, '')
    .replace(/[\s\u00a0\u3000]+/g, '')
    .replace(/[()\[\]{}.,·・\-_\/\\'"`~!@#$%^&*+=|<>?:;]/g, '')
    .trim();
}
function sameMerchant(a, b){
  const x = merchKey(a), y = merchKey(b);
  if (!x || !y) return false;
  return x === y || x.indexOf(y) >= 0 || y.indexOf(x) >= 0;
}
function rowClock(r){
  let m = /^\s*(\d{1,2}):(\d{2})\s*$/.exec(String((r && r.time) || ''));
  if (!m) m = /(\d{1,2}):(\d{2})/.exec(String((r && r.memo) || ''));
  if (!m) return null;
  const hh = +m[1], mi = +m[2];
  if (hh > 23 || mi > 59) return null;
  return hh * 60 + mi;
}
function isDupCard(rows, n){
  return rows.some(r => {
    if (String(r.card || '') !== n.card) return false;
    if (String(r.date || '').slice(0, 10) !== n.date) return false;
    if (Math.round(+r.amount || 0) !== n.amount) return false;
    if (sameMerchant(r.merchant, n.merchant)) return true;
    const a = rowClock(r), b = rowClock(n);
    return a !== null && b !== null && Math.abs(a - b) <= 5;   /* 5분 이내 같은 금액 = 같은 거래 */
  });
}
function isDupBank(rows, n){
  return rows.some(r => {
    if (String(r.account || '') !== n.account) return false;
    if (String(r.date || '').slice(0, 10) !== n.date) return false;
    if (Math.round(+r.amount || 0) !== n.amount) return false;
    if (sameMerchant(r.name || r.item, n.name)) return true;
    const a = rowClock(r), b = rowClock(n);
    return a !== null && b !== null && Math.abs(a - b) <= 5;
  });
}
function cardTombstoned(doc, n){
  const list = Array.isArray(doc && doc.deleted) ? doc.deleted : [];
  return list.some(x => {
    const k = String(typeof x === 'string' ? x : ((x && (x.k || x.key)) || ''));
    const p = k.split('|');
    if (p.length !== 5) return false;
    return p[0] === n.month && p[1] === n.card && p[2] === n.date &&
           String(p[4]) === String(n.amount) && sameMerchant(p[3], n.merchant);
  });
}
function bankTombstoned(doc, n){
  const list = Array.isArray(doc && doc.deleted) ? doc.deleted : [];
  return list.some(x => {
    const k = String(typeof x === 'string' ? x : ((x && (x.k || x.key)) || ''));
    const p = k.split('|');
    if (p.length === 4) return p[0] === n.account && p[1] === n.date && String(p[3]) === String(n.amount) && sameMerchant(p[2], n.name);
    if (p.length === 3) return p[0] === n.account && String(p[2]) === String(n.amount) && sameMerchant(p[1], n.name);
    return false;
  });
}

/* ---------- 문자 해석 ---------- */
function parseSms(title, text, nowMs){
  const raw = normSms((title ? title + '\n' : '') + (text || ''));
  if (!raw.trim()) return {skip: 'empty'};
  if (/메시지\s*전송\s*테스트/.test(raw)) return {skip: 'no_keyword', detail: 'test_message'};
  if (!KEYWORD_RE.test(raw)) return {skip: 'no_keyword'};

  /* 1) 우리은행 입출금 — 055*** 계좌만 */
  const woori = /우리WON뱅킹|우리은행\s*입출금|우리은행\s*알림/.test(raw) || (/\[(입금|출금)\]/.test(raw) && /계좌\s*잔액/.test(raw));
  if (woori){
    const dirM = /\[(입금|출금)\]/.exec(raw);
    if (!dirM) return {skip: 'unrecognized_bank'};
    const accM = /(\d{2,4}-\d{2,4}-\d{2,3})\s*\*{2,4}/.exec(raw);
    if (!accM) return {skip: 'no_account'};
    if (!/055$/.test(accM[1])) return {skip: 'excluded_account', detail: '우리은행 ' + accM[1].slice(0, 4) + '-***' };
    const amts = amountCandidates(raw).filter(a => !a.excluded);
    if (!amts.length) return {skip: 'no_amount'};
    const dir = dirM[1] === '입금' ? 'in' : 'out';
    const amount = (dir === 'in' ? 1 : -1) * amts[0].v;
    const when = findDateTime(raw, nowMs);
    const after = raw.slice(dirM.index + dirM[0].length);
    let name = after.replace(/[\d,]+\s*원[\s\S]*$/, '').replace(/\s+/g, ' ').trim();
    name = name.slice(0, 60);
    const balM = /잔액\s*([\d,]+)\s*원/.exec(raw);
    const balance = balM ? Number(balM[1].replace(/,/g, '')) : null;
    return {
      kind: 'bank', account: '우리은행', date: when.date, time: when.time, guessedTime: when.guessed,
      name: name || '우리은행', amount: amount, dir: dir, balance: balance,
      item: bankItemOf('우리은행', name, dir)
    };
  }

  /* 2) 토스뱅크 입출금 */
  const toss = /\[토스\]|토스뱅크\s*입출금|토스뱅크\s*통장/.test(raw) && !/토스페이먼츠/.test(raw);
  if (toss && /입금|출금/.test(raw)){
    const amts = amountCandidates(raw).filter(a => !a.excluded);
    if (!amts.length) return {skip: 'no_amount'};
    const dir = /출금/.test(raw) ? 'out' : 'in';
    const amount = (dir === 'in' ? 1 : -1) * amts[0].v;
    const when = findDateTime(raw, nowMs);
    let name = raw
      .replace(/\[[^\]]*\]/g, ' ')
      .replace(/잔액\s*:?\s*[\d,]+\s*원?/g, ' ')
      .replace(/[\d,]+\s*원/g, ' ')
      .replace(/(입금|출금|승인|결제|취소)/g, ' ')
      .replace(/\s+/g, ' ')
      .trim().slice(0, 60);
    const balM = /잔액\s*([\d,]+)\s*원/.exec(raw);
    return {
      kind: 'bank', account: '토스 입출금', date: when.date, time: when.time, guessedTime: when.guessed,
      name: name || '토스뱅크', amount: amount, dir: dir,
      balance: balM ? Number(balM[1].replace(/,/g, '')) : null,
      item: bankItemOf('토스 입출금', name, dir)
    };
  }

  /* 3) 카드 승인·취소 */
  const issuer = cardIssuerOf(raw);
  if (!issuer) return {skip: 'unknown_card'};
  const amts = amountCandidates(raw).filter(a => !a.excluded);
  if (!amts.length) return {skip: 'no_amount'};
  const isCancel = /취소/.test(raw);
  const amount = (isCancel ? -1 : 1) * amts[0].v;
  const when = findDateTime(raw, nowMs);
  const merchant = cleanMerchant(raw);
  if (!merchant) return {skip: 'no_merchant'};
  const payType = /할부/.test(raw) ? '할부' : (/체크/.test(raw) ? '체크' : '일시불');
  return {
    kind: 'card', card: issuer, date: when.date, time: when.time, guessedTime: when.guessed,
    merchant: merchant, amount: amount, isCancel: isCancel, payType: payType
  };
}

function bankItemOf(account, name, dir){
  const n = String(name || '');
  if (account === '토스 입출금'){
    if (/내\s*토스뱅크/.test(n) || /우리/.test(n)) return dir === 'out' ? '우리보내기' : '우리받기';
    return '기타';
  }
  if (/(삼성카드|KB\s*국민|현대카드|우리카드|롯데카드|신한카드|하나카드|카드\(주\)|카드요금)/i.test(n)) return '카드요금';
  if (/토뱅|토스뱅크|토스/.test(n)) return dir === 'out' ? '모으기채우기' : '모으기꺼내기';
  return '기타현금사용';
}

/* ---------- 저장소(비공개) 읽기·쓰기 ---------- */
function ghHeaders(env){
  return {
    'Authorization': 'Bearer ' + env.GITHUB_TOKEN,
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'User-Agent': 'finance-ingest-worker'
  };
}
function ghUrl(path){
  return GH_API + '/repos/' + GH_OWNER + '/' + GH_REPO + '/contents/' +
         String(path).split('/').map(encodeURIComponent).join('/');
}
function b64ToText(b64){
  const bin = atob(String(b64 || '').replace(/\s/g, ''));
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return new TextDecoder('utf-8').decode(bytes);
}
function textToB64(s){
  const bytes = new TextEncoder().encode(s);
  let bin = '';
  for (let i = 0; i < bytes.length; i += 4096){
    bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 4096));
  }
  return btoa(bin);
}
async function ghGetFile(env, path){
  const r = await fetch(ghUrl(path) + '?ref=' + GH_BRANCH, {headers: ghHeaders(env), cache: 'no-store'});
  if (r.status === 404) return null;
  if (!r.ok) throw new Error('GitHub 읽기 실패 (' + r.status + ')');
  const j = await r.json();
  if (!j || typeof j.content !== 'string') throw new Error('GitHub 응답에 content 가 없습니다');
  return {sha: j.sha, text: b64ToText(j.content)};
}
async function ghPutFile(env, path, text, sha, message){
  const body = {message: message, content: textToB64(text), branch: GH_BRANCH};
  if (sha) body.sha = sha;
  const r = await fetch(ghUrl(path), {
    method: 'PUT', cache: 'no-store',
    headers: Object.assign(ghHeaders(env), {'Content-Type': 'application/json'}),
    body: JSON.stringify(body)
  });
  const txt = await r.text();
  let j = null;
  try { j = JSON.parse(txt); } catch (e) {}
  const msg = (j && j.message) || ('HTTP ' + r.status);
  if (r.ok) return {ok: true, status: r.status, message: msg};
  const conflict = r.status === 409 || (r.status === 422 && /sha|conflict/i.test(msg));
  return {ok: false, status: r.status, message: msg, conflict: conflict};
}

async function newMonthDoc(env, p){
  let settings = null;
  try {
    const s = await ghGetFile(env, 'data/settings.json');
    if (s) settings = JSON.parse(s.text);
  } catch (e) {}
  if (p.kind === 'card'){
    const names = (settings && Array.isArray(settings.cats) && settings.cats.length) ? settings.cats : Object.keys(CAT_COLORS);
    const colors = (settings && settings.colors) || {};
    return {
      v: 1, month: p.month, updatedAt: nowIso(),
      categories: names.map(n => ({name: n, color: colors[n] || CAT_COLORS[n] || '#64748b'})),
      dueDates: {}, deleted: [], rows: []
    };
  }
  const accounts = (settings && Array.isArray(settings.accounts) && settings.accounts.length)
    ? settings.accounts : ['우리은행', '토스 입출금', '토스 모으기'];
  return {
    v: 1, kind: 'bank', month: p.month, updatedAt: nowIso(), accounts: accounts, cardLink: true,
    moveNames: {}, moveNamesAt: {}, transfer: {}, rowOrder: {}, rowOrderAt: {}, hiddenMoves: {}, hiddenMovesAt: {},
    budgetFormula: {}, itemDir: {}, settlementFormula: {}, rows: [], budgets: {}, budgetAt: {}, map: {}, settlement: {}, deleted: []
  };
}

function makeRow(p){
  const t = nowIso();
  if (p.kind === 'card'){
    const md = p.date.slice(5).replace('-', '/');
    const stamp = 'SMS자동전달 · ' + md + ' ' + p.time;
    return {
      id: rid(), month: p.month, card: p.card, date: p.date, time: p.time,
      merchant: p.merchant, amount: p.amount, category: p.category,
      memo: (p.isCancel ? '취소 · ' : '') + stamp,
      cardName: '', payType: p.payType, excluded: false, highlight: '',
      createdAt: t, editedAt: t
    };
  }
  const md = p.date.slice(5).replace('-', '/');
  const memo = 'SMS자동전달 · ' + md + ' ' + p.time + (p.balance != null ? ' (잔액 ' + fmtNum(p.balance) + '원)' : '');
  return {
    id: rid(), month: p.month, account: p.account, date: p.date,
    item: p.item, name: p.name, amount: p.amount,
    memo: memo.slice(0, 80), highlight: '',
    createdAt: t, editedAt: t
  };
}

/* ---------- 한 건 저장 (409 sha 충돌은 최대 2회 다시 시도) ---------- */
async function ingestWrite(env, p){
  const path = p.kind === 'card' ? 'data/' + p.month + '.json' : 'data/bank-' + p.month + '.json';
  let lastErr = '';
  for (let attempt = 0; attempt < 3; attempt++){
    const cur = await ghGetFile(env, path);
    let doc = null;
    if (cur){
      try { doc = JSON.parse(cur.text); } catch (e){ return {status: 502, body: {ok: false, error: '장부 파일을 읽지 못했습니다', file: path}}; }
    } else {
      doc = await newMonthDoc(env, p);
    }
    if (!doc || typeof doc !== 'object') return {status: 502, body: {ok: false, error: '장부 파일 형식이 아닙니다', file: path}};
    const rows = Array.isArray(doc.rows) ? doc.rows : [];

    if (p.kind === 'card' ? isDupCard(rows, p) : isDupBank(rows, p))
      return {status: 200, body: {ok: true, skipped: 'duplicate', file: path, month: p.month}};
    if (p.kind === 'card' ? cardTombstoned(doc, p) : bankTombstoned(doc, p))
      return {status: 200, body: {ok: true, skipped: 'deleted', file: path, month: p.month}};

    const row = makeRow(p);
    doc.rows = rows;
    doc.rows.push(row);
    doc.updatedAt = nowIso();

    const label = p.kind === 'card' ? (p.card + ' ' + p.merchant) : (p.account + ' ' + p.name);
    const msg = '실시간 반영: ' + label + ' ' + fmtNum(Math.abs(p.amount)) + '원 (' + p.date + ' ' + p.time + ')';
    const put = await ghPutFile(env, path, JSON.stringify(doc), cur ? cur.sha : null, msg.slice(0, 120));
    if (put.ok) return {status: 200, body: {ok: true, added: 1, file: path, month: p.month, row: row, attempt: attempt + 1}};
    if (!put.conflict) return {status: 502, body: {ok: false, error: 'GitHub 저장 실패: ' + put.message, file: path, status: put.status}};
    lastErr = put.message;
  }
  return {status: 409, body: {ok: false, error: '다른 기기가 같은 파일을 동시에 저장했습니다 — 잠시 뒤 다시 보내 주세요', detail: lastErr, file: path}};
}

/* ---------- 입력 본문 해석 (JSON·텍스트·폼 모두) ---------- */
function readIngestBody(raw, ctype){
  const out = {title: '', text: '', key: ''};
  const ct = String(ctype || '').toLowerCase();
  const body = String(raw || '');
  const trim = body.trim();
  if (trim.charAt(0) === '{' || ct.indexOf('json') >= 0){
    try {
      const j = JSON.parse(trim);
      if (j && typeof j === 'object'){
        out.title = String(j.title || j.subject || j.app || '');
        out.text = String(j.text || j.message || j.body || j.content || j.msg || '');
        out.key = String(j.key || j.ingestKey || j.token || '');
        if (!out.text && !out.title) out.text = body;
        return out;
      }
    } catch (e) {}
  }
  if (ct.indexOf('x-www-form-urlencoded') >= 0){
    try {
      const p = new URLSearchParams(trim);
      const t = p.get('text') || p.get('message') || p.get('body') || '';
      const ti = p.get('title') || p.get('subject') || '';
      if (t || ti){
        out.title = ti; out.text = t; out.key = p.get('key') || '';
        return out;
      }
    } catch (e) {}
  }
  out.text = body;
  return out;
}

async function handleIngest(request, env){
  const url = new URL(request.url);
  if (request.method === 'GET' || request.method === 'HEAD'){
    const help = [
      '실시간 가계부 반영 — POST /ingest',
      '',
      '헤더 X-Ingest-Key: <INGEST_KEY> (또는 ?key=)',
      'body: text/plain 알림 문구  또는  JSON {"title":"...","text":"..."}',
      '',
      '카드 승인·취소 → data/YYYY-MM.json (결제월: 전월 13일~당월 12일)',
      '우리은행 055*** · 토스뱅크 입출금 → data/bank-YYYY-MM.json',
      '중복·광고·제외계좌는 200 {ok:true, skipped:"..."} 로 조용히 넘어갑니다.'
    ].join('\n');
    return new Response(help, {status: 200, headers: Object.assign({'Content-Type': 'text/plain; charset=utf-8'}, corsHeaders(request))});
  }
  if (request.method !== 'POST') return json({ok: false, error: 'POST 만 지원합니다'}, request, 405);
  if (!env || !env.INGEST_KEY) return json({ok: false, error: 'Worker 환경변수 INGEST_KEY 가 없습니다'}, request, 500);
  if (!env.GITHUB_TOKEN) return json({ok: false, error: 'Worker 환경변수 GITHUB_TOKEN 이 없습니다'}, request, 500);

  const raw = await request.text();
  if (raw.length > INGEST_BODY_MAX) return json({ok: false, error: '본문이 너무 큽니다'}, request, 413);
  const body = readIngestBody(raw, request.headers.get('Content-Type') || '');
  const key = request.headers.get('X-Ingest-Key') || url.searchParams.get('key') || body.key || '';
  if (!safeEqual(key, env.INGEST_KEY)) return json({ok: false, error: '인증 실패 — X-Ingest-Key 를 확인하세요'}, request, 401);

  const parsed = parseSms(body.title, body.text, Date.now());
  if (parsed.skip) return json({ok: true, skipped: parsed.skip, detail: parsed.detail || undefined}, request, 200);

  parsed.month = parsed.kind === 'card' ? billingMonthOf(parsed.date) : parsed.date.slice(0, 7);
  if (parsed.kind === 'card') parsed.category = classify(parsed.merchant, parsed.amount, await loadRules(env));
  const res = await ingestWrite(env, parsed);
  return json(res.body, request, res.status);
}

export default {
  async fetch(request, env){
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, {status: 204, headers: corsHeaders(request)});

    const path = url.pathname.replace(/\/+$/, '');
    try {
      if (path === '/ingest') return await handleIngest(request, env || {});
      if (request.method !== 'GET' && request.method !== 'HEAD')
        return json({ok: false, error: '이 경로는 GET 만 지원합니다'}, request, 405);
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
          'POST /ingest   ← 폰 알림(MacroDroid)을 가계부에 실시간 반영 (X-Ingest-Key 필요)',
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
