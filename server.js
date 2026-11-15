import express from 'express';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Server } from 'socket.io';

// הגדרות ברירת מחדל (אפשר לשנות מגלגל השיניים באתר)
const cfg = {
  user: (process.env.TIKTOK_USER || 'cryam.23').replace(/^@/, ''),
  step: Number(process.env.LIKE_STEP) || 50, // כרטיס לייקים כל X לייקים
};
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.static(path.join(path.dirname(fileURLToPath(import.meta.url)), 'public')));
app.get('/health', (_, res) => res.send('ok'));
const server = http.createServer(app);
const io = new Server(server);
server.listen(PORT, '0.0.0.0', () => console.log(`[boot] הפורט ${PORT} פתוח`));

let status = { state: 'connecting', user: cfg.user };
const setStatus = (state, msg = '') => {
  status = { state, user: cfg.user, msg };
  io.emit('status', status);
  console.log('[status]', state, msg);
};

const likes = new Map(), followed = new Set(), shared = new Set();
let cur = null; // החיבור הנוכחי
let wake = null; // מעיר את ההמתנה בין ניסיונות
const sleep = (ms) => new Promise((r) => { wake = r; setTimeout(r, ms); });

function applyConfig(c = {}) {
  let userChanged = false;
  const u = String(c.user || '').trim().replace(/^@/, '');
  if (u && /^[A-Za-z0-9._]{2,24}$/.test(u) && u !== cfg.user) {
    cfg.user = u;
    userChanged = true;
    likes.clear(); followed.clear(); shared.clear();
  }
  const st = Math.floor(Number(c.step));
  if (st >= 1 && st <= 100000) cfg.step = st;
  io.emit('config', cfg);
  if (userChanged) {
    console.log('[config] משתמש חדש:', cfg.user);
    try { cur?.c.disconnect(); } catch {}
    cur?.end?.();
    wake?.();
  }
}

io.on('connection', (s) => {
  s.emit('status', status);
  s.emit('config', cfg);
  s.on('config', applyConfig);
});

function pickPic(d) {
  const u = d.user || {};
  const all = [
    d.profilePictureUrl, u.profilePictureUrl, u.profilePicture?.url, u.profilePicture?.urls,
    u.avatarLarger?.urlListList, u.avatarMedium?.urlListList, u.avatarThumb?.urlListList,
    u.avatarLarger?.urls, u.avatarMedium?.urls, u.avatarThumb?.urls,
  ].flat(2).filter((x) => typeof x === 'string' && x.startsWith('http'));
  return all.find((x) => !/\.heic/i.test(x)) || all[0] || '';
}
const uid = (d) => d.uniqueId || d.user?.uniqueId || '';
function send(kind, d, extra = {}) {
  const user = uid(d);
  if (!user) return;
  io.emit('event', { kind, user, nick: d.nickname || d.user?.nickname || '', pic: pickPic(d), ts: Date.now(), ...extra });
}

function attach(c) {
  c.on('error', (e) => console.error('[error]', e?.message || e));
  c.on('follow', (d) => {
    const id = uid(d);
    if (!id || followed.has(id)) return;
    followed.add(id);
    send('follow', d);
  });
  c.on('share', (d) => {
    const id = uid(d);
    if (!id || shared.has(id)) return;
    shared.add(id);
    send('share', d);
  });
  c.on('like', (d) => {
    const id = uid(d);
    if (!id) return;
    const prev = likes.get(id) || 0;
    const total = prev + (d.likeCount || 1);
    likes.set(id, total);
    const step = cfg.step;
    if (Math.floor(total / step) > Math.floor(prev / step)) send('like', d, { count: Math.floor(total / step) * step });
  });
  c.on('gift', (d) => {
    if (d.giftType === 1 && !d.repeatEnd) return; // רצף מתנות עדיין רץ
    send('gift', d, {
      gift: d.giftName || d.giftDetails?.giftName || d.extendedGiftInfo?.name || d.describe || 'Gift',
      count: d.repeatCount || 1,
    });
  });
}

let Conn;
try {
  const mod = await import('tiktok-live-connector');
  const lib = mod.TikTokLiveConnection || mod.WebcastPushConnection ? mod : mod.default;
  Conn = lib.TikTokLiveConnection || lib.WebcastPushConnection;
  console.log(`[boot] הספרייה נטענה, מאזין ל-@${cfg.user}`);
} catch (e) {
  console.error('[boot] טעינת הספרייה נכשלה:', e);
}

for (;;) {
  if (!Conn) {
    setStatus('offline', 'ספריית טיקטוק לא נטענה');
    await sleep(60000);
    continue;
  }
  const user = cfg.user;
  const c = new Conn(user, {
    processInitialData: false,
    enableExtendedGiftInfo: true,
    ...(process.env.SIGN_API_KEY && { signApiKey: process.env.SIGN_API_KEY }),
  });
  attach(c);
  let end;
  const ended = new Promise((resolve) => {
    end = resolve;
    c.on('disconnected', resolve);
    c.on('streamEnd', () => { likes.clear(); followed.clear(); shared.clear(); resolve(); });
  });
  cur = { c, end };
  try {
    setStatus('connecting');
    await c.connect();
    if (user === cfg.user) {
      setStatus('live');
      await ended;
    }
  } catch (e) {
    if (user === cfg.user) setStatus('offline', String(e?.message || e).slice(0, 160));
  }
  try { c.disconnect(); } catch {}
  cur = null;
  if (status.state !== 'offline') setStatus('offline');
  await sleep(15000); // מנסה שוב כל 15 שניות עד שהלייב נפתח
}
