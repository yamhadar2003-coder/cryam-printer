import express from 'express';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Server } from 'socket.io';

const USER = (process.env.TIKTOK_USER || 'cryam.23').replace(/^@/, '');
const STEP = Number(process.env.LIKE_STEP) || 50; // כרטיס לייקים כל X לייקים
const PORT = process.env.PORT || 3000;

const app = express();
app.use(express.static(path.join(path.dirname(fileURLToPath(import.meta.url)), 'public')));
app.get('/health', (_, res) => res.send('ok'));
const server = http.createServer(app);
const io = new Server(server);
server.listen(PORT, '0.0.0.0', () => console.log(`[boot] הפורט ${PORT} פתוח`));

let status = { state: 'connecting', user: USER };
const setStatus = (state, msg = '') => {
  status = { state, user: USER, msg };
  io.emit('status', status);
  console.log('[status]', state, msg);
};
io.on('connection', (s) => s.emit('status', status));

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

const likes = new Map(), followed = new Set(), shared = new Set();

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
    if (Math.floor(total / STEP) > Math.floor(prev / STEP)) send('like', d, { count: Math.floor(total / STEP) * STEP });
  });
  c.on('gift', (d) => {
    if (d.giftType === 1 && !d.repeatEnd) return; // רצף מתנות עדיין רץ
    send('gift', d, {
      gift: d.giftName || d.giftDetails?.giftName || d.extendedGiftInfo?.name || d.describe || 'Gift',
      count: d.repeatCount || 1,
    });
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let Conn;
try {
  const mod = await import('tiktok-live-connector');
  const lib = mod.TikTokLiveConnection || mod.WebcastPushConnection ? mod : mod.default;
  Conn = lib.TikTokLiveConnection || lib.WebcastPushConnection;
  console.log(`[boot] הספרייה נטענה, מאזין ל-@${USER}`);
} catch (e) {
  console.error('[boot] טעינת הספרייה נכשלה:', e);
}

for (;;) {
  if (!Conn) {
    setStatus('offline', 'ספריית טיקטוק לא נטענה');
    await sleep(60000);
    continue;
  }
  const c = new Conn(USER, {
    processInitialData: false,
    enableExtendedGiftInfo: true,
    ...(process.env.SIGN_API_KEY && { signApiKey: process.env.SIGN_API_KEY }),
  });
  attach(c);
  const ended = new Promise((resolve) => {
    c.on('disconnected', resolve);
    c.on('streamEnd', () => { likes.clear(); followed.clear(); shared.clear(); resolve(); });
  });
  try {
    setStatus('connecting');
    await c.connect();
    setStatus('live');
    await ended;
  } catch (e) {
    setStatus('offline', String(e?.message || e).slice(0, 160));
  }
  try { c.disconnect(); } catch {}
  setStatus('offline');
  await sleep(15000); // מנסה שוב כל 15 שניות עד שהלייב נפתח
}
