import { Telegraf, Markup } from "telegraf";
import Database from "better-sqlite3";
import crypto from "crypto";

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_ID = Number(process.env.ADMIN_ID);

const PROXY_SERVER = process.env.PROXY_SERVER || "45.140.146.233";
const PROXY_PORT = process.env.PROXY_PORT || "443";
const PROXY_SECRET = process.env.PROXY_SECRET || "";

if (!BOT_TOKEN) throw new Error("BOT_TOKEN missing");
if (!ADMIN_ID) throw new Error("ADMIN_ID missing");

const db = new Database("/data/bot.db");

// ==================== DATABASE SCHEMA ====================

db.exec(`
CREATE TABLE IF NOT EXISTS users (
  tg_id INTEGER PRIMARY KEY,
  username TEXT,
  first_name TEXT,
  last_name TEXT,
  status TEXT NOT NULL DEFAULT 'new',
  device_limit INTEGER DEFAULT 0,
  devices_used INTEGER NOT NULL DEFAULT 0,
  expires_at INTEGER DEFAULT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  menu_msg_id INTEGER DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS requests (
  id TEXT PRIMARY KEY,
  tg_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_sessions (
  req_id TEXT PRIMARY KEY,
  admin_id INTEGER NOT NULL,
  device_limit INTEGER DEFAULT 2,
  expires_days INTEGER DEFAULT 30,
  step TEXT DEFAULT 'selecting',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_ps_state (
  tg_id INTEGER NOT NULL,
  stage TEXT NOT NULL,
  last_idx INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (tg_id, stage)
);
`);

// lightweight migrations
try { db.prepare("ALTER TABLE users ADD COLUMN menu_msg_id INTEGER").run(); } catch {}

const now = () => Math.floor(Date.now() / 1000);

// ==================== SAFE ERROR HANDLING ====================

const SAFE_ERRORS = [
  'message to edit not found',
  'message can\'t be edited',
  'message is not modified',
  'query is too old',
  'bot was blocked by the user',
  'chat not found',
  'message to delete not found',
  'user is deactivated',
  'retry after',
  'ETELEGRAM',
  'Bad Request',
  'Forbidden'
];

function isSafeError(err) {
  if (!err) return true;
  const msg = (err.message || err.description || String(err)).toLowerCase();
  return SAFE_ERRORS.some(pattern => msg.includes(pattern.toLowerCase()));
}

async function safeAnswerCbQuery(ctx, text, opts = {}) {
  try {
    return await ctx.answerCbQuery(text, opts);
  } catch (err) {
    if (!isSafeError(err)) console.error("[safeAnswerCbQuery]", err);
  }
}

async function safeReply(ctx, text, opts = {}) {
  try {
    return await ctx.reply(text, opts);
  } catch (err) {
    if (!isSafeError(err)) console.error("[safeReply]", err);
  }
}

async function safeEditMessageText(ctx, text, opts = {}) {
  try {
    if (ctx.callbackQuery?.message) {
      return await ctx.editMessageText(text, opts);
    }
  } catch (err) {
    if (!isSafeError(err)) console.error("[safeEditMessageText]", err);
  }
}

async function safeDeleteMessage(bot, chatId, messageId) {
  try {
    if (chatId && messageId) {
      await bot.telegram.deleteMessage(chatId, messageId);
    }
  } catch (err) {
    // silently ignore deletion errors
  }
}

async function safeSendMessage(bot, chatId, text, opts = {}) {
  try {
    return await bot.telegram.sendMessage(chatId, text, opts);
  } catch (err) {
    if (!isSafeError(err)) console.error("[safeSendMessage]", err);
  }
}

// ==================== TEXT CONTENT ====================

const PS = {
  start: [
    "P.S. Если связь вдруг «случайно» стала капризной — у нас есть свои маленькие лайфхаки 😉",
    "P.S. Тут всё максимально просто. Даже если миру вокруг нравится усложнять.",
    "P.S. Я не спорю с реальностью. Я просто делаю так, чтобы она работала.",
    "P.S. Ничего незаконного. Просто стабильность.",
    "P.S. Если ты это читаешь — значит, ты из тех, кто выбирает рабочие решения. Уважаю."
  ],
  end: [
    "Мы только что сделали ваш интернет чуточку свободнее",
    "Готово. Мы только что сделали ваш интернет чуточку свободнее",
    "Подключение завершено. Мы только что сделали ваш интернет чуточку свободнее"
  ]
};

function pickUniquePs(stage, tgId) {
  const arr = PS[stage] || [];
  if (!arr.length) return "";

  const row = db.prepare("SELECT last_idx FROM user_ps_state WHERE tg_id=? AND stage=?").get(tgId, stage);
  let idx = Math.floor(Math.random() * arr.length);

  if (arr.length > 1 && row && idx === row.last_idx) {
    idx = (idx + 1) % arr.length;
  }

  db.prepare(`
    INSERT INTO user_ps_state(tg_id, stage, last_idx, updated_at)
    VALUES(?, ?, ?, ?)
    ON CONFLICT(tg_id, stage) DO UPDATE SET
      last_idx=excluded.last_idx,
      updated_at=excluded.updated_at
  `).run(tgId, stage, idx, now());

  return arr[idx];
}

// ==================== DATABASE HELPERS ====================

function upsertUser(from) {
  const t = now();
  db.prepare(`
    INSERT INTO users(tg_id, username, first_name, last_name, status, created_at, updated_at)
    VALUES(?,?,?,?, 'new', ?,?)
    ON CONFLICT(tg_id) DO UPDATE SET
      username=excluded.username,
      first_name=excluded.first_name,
      last_name=excluded.last_name,
      updated_at=excluded.updated_at
  `).run(from.id, from.username || null, from.first_name || null, from.last_name || null, t, t);
}

function getUser(tgId) {
  return db.prepare("SELECT * FROM users WHERE tg_id=?").get(tgId);
}

function setUserStatus(tgId, status) {
  db.prepare("UPDATE users SET status=?, updated_at=? WHERE tg_id=?").run(status, now(), tgId);
}

function setUserAccess(tgId, { deviceLimit, expiresAt }) {
  db.prepare("UPDATE users SET status='approved', device_limit=?, expires_at=?, updated_at=? WHERE tg_id=?")
    .run(deviceLimit, expiresAt ?? null, now(), tgId);
}

function revokeUser(tgId) {
  db.prepare("UPDATE users SET status='revoked', updated_at=? WHERE tg_id=?").run(now(), tgId);
}

function banUser(tgId) {
  db.prepare("UPDATE users SET status='banned', updated_at=? WHERE tg_id=?").run(now(), tgId);
}

function fmtUser(u) {
  const uname = u.username ? `@${u.username}` : "(no username)";
  const name = `${u.first_name || ""} ${u.last_name || ""}`.trim();
  return `${name} ${uname} | id:${u.tg_id}`;
}

function fmtUserCard(u) {
  const statusEmoji = {
    new: "🆕",
    pending: "⏳",
    approved: "✅",
    denied: "❌",
    banned: "🚫",
    revoked: "🔒"
  };
  
  let lines = [
    `${statusEmoji[u.status] || "❓"} ${fmtUser(u)}`,
    `Статус: ${u.status}`,
  ];
  
  if (u.status === 'approved') {
    lines.push(`Лимит устройств: ${u.device_limit || '∞'}`);
    lines.push(`Использовано: ${u.devices_used}`);
    if (u.expires_at) {
      const daysLeft = Math.ceil((u.expires_at - now()) / 86400);
      lines.push(`Истекает: ${new Date(u.expires_at * 1000).toLocaleDateString('ru-RU')} (${daysLeft} дн.)`);
    } else {
      lines.push(`Срок: без ограничений`);
    }
  }
  
  lines.push(`Зарегистрирован: ${new Date(u.created_at * 1000).toLocaleDateString('ru-RU')}`);
  
  return lines.join("\n");
}

function isApproved(u) {
  if (!u) return false;
  if (Number(u.tg_id) === ADMIN_ID) return true;
  if (u.status !== "approved") return false;
  if (u.expires_at && now() > u.expires_at) return false;
  return true;
}

// ==================== PROXY URLS ====================

function proxyUrl() {
  if (!PROXY_SECRET) return null;
  return `https://t.me/proxy?server=${PROXY_SERVER}&port=${PROXY_PORT}&secret=${PROXY_SECRET}`;
}

function adminProxyUrl() {
  if (!PROXY_SECRET) return null;
  return `https://t.me/proxy?server=${PROXY_SERVER}&port=${PROXY_PORT}&secret=${PROXY_SECRET}`;
}

// ==================== ADMIN SESSIONS (PERSISTENT) ====================

function createAdminSession(reqId, adminId, deviceLimit = 2, expiresDays = 30) {
  const t = now();
  db.prepare(`
    INSERT INTO admin_sessions (req_id, admin_id, device_limit, expires_days, step, created_at, updated_at)
    VALUES (?, ?, ?, ?, 'selecting', ?, ?)
    ON CONFLICT(req_id) DO UPDATE SET
      admin_id = excluded.admin_id,
      device_limit = excluded.device_limit,
      expires_days = excluded.expires_days,
      step = 'selecting',
      updated_at = excluded.updated_at
  `).run(reqId, adminId, deviceLimit, expiresDays, t, t);
}

function getAdminSession(reqId) {
  return db.prepare("SELECT * FROM admin_sessions WHERE req_id=?").get(reqId);
}

function updateAdminSession(reqId, updates) {
  const fields = [];
  const values = [];
  
  if (updates.deviceLimit !== undefined) {
    fields.push("device_limit = ?");
    values.push(updates.deviceLimit);
  }
  if (updates.expiresDays !== undefined) {
    fields.push("expires_days = ?");
    values.push(updates.expiresDays);
  }
  if (updates.step !== undefined) {
    fields.push("step = ?");
    values.push(updates.step);
  }
  
  if (fields.length === 0) return;
  
  values.push(now(), reqId);
  db.prepare(`UPDATE admin_sessions SET ${fields.join(", ")}, updated_at = ? WHERE req_id = ?`).run(...values);
}

function deleteAdminSession(reqId) {
  db.prepare("DELETE FROM admin_sessions WHERE req_id=?").run(reqId);
}

function cleanupOldSessions(maxAgeHours = 24) {
  const cutoff = now() - (maxAgeHours * 3600);
  db.prepare("DELETE FROM admin_sessions WHERE updated_at < ?").run(cutoff);
}

// ==================== KEYBOARDS ====================

function userMenu(opts = {}) {
  const { approved = false } = opts;
  const rows = [];
  if (!approved) {
    rows.push([Markup.button.callback("Запросить доступ", "req_access")]);
  } else {
    rows.push([
      Markup.button.callback("⚡ TURBO", "get_turbo"),
      Markup.button.callback("🧱 STABLE", "get_stable")
    ]);
    rows.push([Markup.button.callback("🛡️ Оба профиля", "get_profiles")]);
    rows.push([Markup.button.callback("Инструкция", "howto")]);
  }
  return Markup.inlineKeyboard(rows);
}

function adminMainMenu() {
  return Markup.inlineKeyboard([
    [Markup.button.callback("📋 Список заявок", "admin_list_requests")],
    [Markup.button.callback("⏳ Зависшие заявки", "admin_stuck_requests")],
    [Markup.button.callback("📊 Статистика", "admin_stats")],
    [Markup.button.callback("👥 Клиенты сейчас", "admin_clients")]
  ]);
}

function adminRequestListItem(reqId, userSummary) {
  return Markup.inlineKeyboard([
    [Markup.button.callback(`👤 ${userSummary.substring(0, 30)}...`, `admin_view_req:${reqId}`)]
  ]);
}

function adminRequestCard(reqId) {
  return Markup.inlineKeyboard([
    [Markup.button.callback("⚡ Быстро выдать (5 устр / без срока)", `admin_quickgrant:${reqId}`)],
    [
      Markup.button.callback("✅ Одобрить (кастом)", `admin_approve:${reqId}`),
      Markup.button.callback("❌ Отказать", `admin_deny:${reqId}`)
    ],
    [
      Markup.button.callback("🧱 Забанить", `admin_ban:${reqId}`),
      Markup.button.callback("🔍 Профиль", `admin_profile:${reqId}`)
    ],
    [Markup.button.callback("« К списку заявок", "admin_list_requests")]
  ]);
}

function adminDeviceLimitPicker(reqId) {
  return Markup.inlineKeyboard([
    [
      Markup.button.callback("1 📱", `admin_setdev:${reqId}:1`),
      Markup.button.callback("2 📱", `admin_setdev:${reqId}:2`),
      Markup.button.callback("3 📱", `admin_setdev:${reqId}:3`),
      Markup.button.callback("5 📱", `admin_setdev:${reqId}:5`)
    ],
    [
      Markup.button.callback("10 📱", `admin_setdev:${reqId}:10`),
      Markup.button.callback("∞", `admin_setdev:${reqId}:0`)
    ],
    [Markup.button.callback("« Отмена", `admin_cancel:${reqId}`)]
  ]);
}

function adminExpiryPicker(reqId) {
  return Markup.inlineKeyboard([
    [
      Markup.button.callback("7 дней", `admin_setexp:${reqId}:7`),
      Markup.button.callback("30 дней", `admin_setexp:${reqId}:30`),
      Markup.button.callback("90 дней", `admin_setexp:${reqId}:90`)
    ],
    [
      Markup.button.callback("1 год", `admin_setexp:${reqId}:365`),
      Markup.button.callback("Без срока ♾️", `admin_setexp:${reqId}:0`)
    ],
    [Markup.button.callback("« Назад к лимиту", `admin_back_dev:${reqId}`)],
    [Markup.button.callback("« Отмена", `admin_cancel:${reqId}`)]
  ]);
}

function adminConfirmPicker(reqId, deviceLimit, expiresDays) {
  const expText = expiresDays === 0 ? "Без срока" : `${expiresDays} дней`;
  return Markup.inlineKeyboard([
    [Markup.button.callback(`✅ Выдать: ${deviceLimit} устр., ${expText}`, `admin_confirm:${reqId}`)],
    [Markup.button.callback("🔄 Изменить лимит", `admin_back_dev:${reqId}`)],
    [Markup.button.callback("🔄 Изменить срок", `admin_back_exp:${reqId}`)],
    [Markup.button.callback("« Отмена", `admin_cancel:${reqId}`)]
  ]);
}

function adminStuckActions(reqId) {
  return Markup.inlineKeyboard([
    [
      Markup.button.callback("✅ Одобрить", `admin_approve:${reqId}`),
      Markup.button.callback("❌ Отказать", `admin_deny:${reqId}`)
    ],
    [
      Markup.button.callback("🧱 Забанить", `admin_ban:${reqId}`),
      Markup.button.callback("🔄 Новая заявка", `admin_reopen:${reqId}`)
    ],
    [Markup.button.callback("« К списку", "admin_stuck_requests")]
  ]);
}

// ==================== MENU RENDERING ====================

async function renderMenu(ctx, { text, keyboard }) {
  const userId = ctx.from.id;
  const u = getUser(userId);
  const chatId = (ctx.chat && ctx.chat.id) ? ctx.chat.id : userId;

  if (u && u.menu_msg_id) {
    try {
      await ctx.telegram.editMessageText(chatId, u.menu_msg_id, undefined, text, {
        reply_markup: keyboard.reply_markup,
        parse_mode: "HTML"
      });
      return u.menu_msg_id;
    } catch (e) {
      await safeDeleteMessage(bot, chatId, u.menu_msg_id);
    }
  }

  const sent = await ctx.telegram.sendMessage(chatId, text, {
    reply_markup: keyboard.reply_markup,
    parse_mode: "HTML"
  });
  db.prepare("UPDATE users SET menu_msg_id=?, updated_at=? WHERE tg_id=?").run(sent.message_id, now(), userId);
  return sent.message_id;
}

async function renderMenuForUser(userId, { text, keyboard }) {
  const u = getUser(userId);
  const chatId = userId;

  if (u && u.menu_msg_id) {
    try {
      await bot.telegram.editMessageText(chatId, u.menu_msg_id, undefined, text, {
        reply_markup: keyboard.reply_markup,
        parse_mode: "HTML"
      });
      return u.menu_msg_id;
    } catch (e) {
      await safeDeleteMessage(bot, chatId, u.menu_msg_id);
    }
  }

  const sent = await bot.telegram.sendMessage(chatId, text, {
    reply_markup: keyboard.reply_markup,
    parse_mode: "HTML"
  });
  db.prepare("UPDATE users SET menu_msg_id=?, updated_at=? WHERE tg_id=?").run(sent.message_id, now(), userId);
  return sent.message_id;
}

// ==================== BOT INSTANCE ====================

const bot = new Telegraf(BOT_TOKEN);

async function configureBotCommands() {
  try {
    // Hide commands globally for regular users
    await bot.telegram.setMyCommands([], { scope: { type: 'default' } });

    // Show extended command menu only in admin chat
    await bot.telegram.setMyCommands([
      { command: 'admin', description: 'Админ-панель' },
      { command: 'stats', description: 'Статистика' },
      { command: 'clients', description: 'Клиенты сейчас' },
      { command: 'diag', description: 'Диагностика режима' },
      { command: 'turbo', description: 'Быстрый профиль' },
      { command: 'stable', description: 'Резервный профиль' },
      { command: 'safe', description: 'Показать оба профиля' }
    ], { scope: { type: 'chat', chat_id: ADMIN_ID } });
  } catch (err) {
    console.error('[configureBotCommands]', err?.message || err);
  }
}

function requireAdmin(ctx) {
  if (ctx.from?.id !== ADMIN_ID) {
    safeReply(ctx, "Нет доступа");
    return false;
  }
  return true;
}

// ==================== USER HANDLERS ====================

bot.start(async (ctx) => {
  upsertUser(ctx.from);
  const u = getUser(ctx.from.id);
  if (u?.status === "banned") {
    return safeReply(ctx, "Доступ закрыт");
  }

  const approved = isApproved(u);

  const startText = approved
    ? `Привет! Доступ уже активен ✅\n\nВыбери режим:\n• ⚡ TURBO — быстрее\n• 🧱 STABLE — надёжнее при плохом маршруте\n\n${pickUniquePs("start", ctx.from.id)}`
    : `Привет! Я помогаю подключиться к прокси, чтобы связь работала стабильно.\n\nКак это работает:\n1) Нажми «Запросить доступ»\n2) Я подтвержу\n\n⚠️ Важно: С включённым VPN MTProto‑прокси часто не работает.\n\n${pickUniquePs("start", ctx.from.id)}`;

  await renderMenu(ctx, {
    text: startText,
    keyboard: userMenu({ approved })
  });
});

bot.action("req_access", async (ctx) => {
  upsertUser(ctx.from);
  const u = getUser(ctx.from.id);
  if (u.status === "banned") {
    return safeAnswerCbQuery(ctx, "Доступ закрыт", { show_alert: true });
  }
  if (u.status === "approved" && isApproved(u)) {
    return safeAnswerCbQuery(ctx, "У тебя уже есть доступ");
  }

  // Check if there's already a pending request
  const existingPending = db.prepare("SELECT * FROM requests WHERE tg_id=? AND status='pending' ORDER BY created_at DESC LIMIT 1").get(ctx.from.id);
  if (existingPending) {
    await safeAnswerCbQuery(ctx, "У тебя уже есть активная заявка");

    // Ping admin again with direct link to existing pending request
    const nu = getUser(ctx.from.id);
    await safeSendMessage(
      bot,
      ADMIN_ID,
      `🔔 Повторный пинг по заявке\n${fmtUser(nu)}\nreq: ${existingPending.id.slice(0, 8)}`,
      { reply_markup: Markup.inlineKeyboard([[Markup.button.callback("Открыть заявку", `admin_view_req:${existingPending.id}`)]]) .reply_markup }
    );

    return renderMenu(ctx, {
      text: `Заявка уже отправлена и ожидает проверки ⏳\n\nЯ повторно пинганула админа по твоей заявке ✅`,
      keyboard: userMenu({ approved: false })
    });
  }

  const reqId = crypto.randomUUID();
  db.prepare("INSERT INTO requests(id,tg_id,status,created_at) VALUES(?,?, 'pending', ?)").run(reqId, ctx.from.id, now());
  setUserStatus(ctx.from.id, "pending");

  await safeAnswerCbQuery(ctx, "Отправлено");
  await renderMenu(ctx, {
    text: `Заявка отправлена ✅\n\nКак только одобрю — сразу открою тебе нужные кнопки подключения.`,
    keyboard: userMenu({ approved: false })
  });

  const nu = getUser(ctx.from.id);
  await safeSendMessage(bot, ADMIN_ID, `🆕 Новая заявка на доступ\n${fmtUser(nu)}`, adminMainMenu());
});

bot.action(/get_proxy|get_profiles|get_turbo|get_stable/, async (ctx) => {
  upsertUser(ctx.from);
  const u = getUser(ctx.from.id);
  if (!isApproved(u)) {
    return safeAnswerCbQuery(ctx, "Нет доступа (или истёк)", { show_alert: true });
  }

  if (!PROXY_SECRET) {
    return safeAnswerCbQuery(ctx, "Прокси временно недоступен (секрет не задан)", { show_alert: true });
  }

  if (ctx.from.id !== ADMIN_ID && u.device_limit > 0 && u.devices_used >= u.device_limit) {
    return safeAnswerCbQuery(ctx, "Лимит устройств исчерпан. Попроси апдейт", { show_alert: true });
  }
  if (ctx.from.id !== ADMIN_ID && u.devices_used === 0) {
    db.prepare("UPDATE users SET devices_used = devices_used + 1, updated_at=? WHERE tg_id=?").run(now(), u.tg_id);
  }

  const { turboUrl, stableUrl } = buildProxyUrls();
  const action = ctx.match?.[0] || ctx.callbackQuery?.data || "get_profiles";

  let text = "";
  let keyboard;

  if (action === "get_turbo") {
    text = `⚡ TURBO профиль\n\nНажми кнопку ниже для подключения\n\nМы только что сделали ваш интернет чуточку свободнее`;
    keyboard = Markup.inlineKeyboard([[Markup.button.url("Подключить TURBO", turboUrl)], [Markup.button.callback("Показать оба профиля", "get_profiles")]]);
  } else if (action === "get_stable") {
    text = `🧱 STABLE профиль\n\nНажми кнопку ниже для подключения\n\nМы только что сделали ваш интернет чуточку свободнее`;
    keyboard = Markup.inlineKeyboard([[Markup.button.url("Подключить STABLE", stableUrl)], [Markup.button.callback("Показать оба профиля", "get_profiles")]]);
  } else {
    text = `Доступ активен ✅\n\nВыбери режим подключения:\n• TURBO — быстрее\n• STABLE — надёжнее при плохом маршруте\n\nМы только что сделали ваш интернет чуточку свободнее`;
    keyboard = Markup.inlineKeyboard([
      [Markup.button.url("⚡ Подключить TURBO", turboUrl)],
      [Markup.button.url("🧱 Подключить STABLE", stableUrl)],
      [Markup.button.callback("Какой выбрать?", "howto")]
    ]);
  }

  await safeAnswerCbQuery(ctx, "Ок");
  await renderMenu(ctx, { text, keyboard });
});

bot.action("howto", async (ctx) => {
  await safeAnswerCbQuery(ctx);
  const approved = isApproved(getUser(ctx.from.id));
  await renderMenu(ctx, {
    text: `Как выбрать режим:

• TURBO — используй первым (быстрее)
• STABLE — если видео/медиа лагают или не открываются

Ручное подключение:
1) Telegram → Настройки → Данные и память → Прокси
2) «Добавить прокси» → MTProto
3) Вставь Server / Port / Secret

После включения прокси отключи внешний VPN, если он мешает.` ,
    keyboard: userMenu({ approved })
  });
});

// ==================== LEGACY ADMIN HANDLERS (backward compat) ====================

bot.action(/approve:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req || req.status !== "pending") {
    return safeAnswerCbQuery(ctx, "Уже обработано или не найдено");
  }

  // Start new flow - select device limit
  createAdminSession(reqId, ctx.from.id, 2, 30);
  
  await safeAnswerCbQuery(ctx, "Начинаем настройку доступа");
  await safeEditMessageText(ctx, 
    `⚙️ Настройка доступа для заявки\n\nШаг 1/3: Выбери лимит устройств`,
    { reply_markup: adminDeviceLimitPicker(reqId).reply_markup }
  );
});

bot.action(/deny:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req || req.status !== "pending") {
    return safeAnswerCbQuery(ctx, "Уже обработано");
  }

  db.prepare("UPDATE requests SET status='denied' WHERE id=?").run(reqId);
  setUserStatus(req.tg_id, 'denied');
  deleteAdminSession(reqId);

  await safeAnswerCbQuery(ctx, "Отклонено");
  await safeEditMessageText(ctx, "❌ Заявка отклонена", { reply_markup: adminMainMenu().reply_markup });
  await safeSendMessage(bot, req.tg_id, "Сорри, доступ не выдан");
});

bot.action(/banreq:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req) return safeAnswerCbQuery(ctx, "Не нашла заявку");

  banUser(req.tg_id);
  db.prepare("UPDATE requests SET status='banned' WHERE id=?").run(reqId);
  deleteAdminSession(reqId);

  await safeAnswerCbQuery(ctx, "Забанен");
  await safeEditMessageText(ctx, "🧱 Пользователь забанен", { reply_markup: adminMainMenu().reply_markup });
  await safeSendMessage(bot, req.tg_id, "Доступ закрыт");
});

// Legacy setdev/setexp handlers - redirect to new flow
bot.action(/setdev:(.+):(\d+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const n = Number(ctx.match[2]);
  
  let session = getAdminSession(reqId);
  if (!session) {
    createAdminSession(reqId, ctx.from.id, n, 30);
    session = getAdminSession(reqId);
  } else {
    updateAdminSession(reqId, { deviceLimit: n });
  }
  
  await safeAnswerCbQuery(ctx, `Лимит: ${n === 0 ? '∞' : n}`);
  await safeEditMessageText(ctx,
    `⚙️ Настройка доступа\n\nЛимит устройств: ${n === 0 ? '∞' : n}\n\nШаг 2/3: Выбери срок действия`,
    { reply_markup: adminExpiryPicker(reqId).reply_markup }
  );
});

bot.action(/setexp:(.+):(\d+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const d = Number(ctx.match[2]);
  
  let session = getAdminSession(reqId);
  if (!session) {
    createAdminSession(reqId, ctx.from.id, 2, d);
    session = getAdminSession(reqId);
  } else {
    updateAdminSession(reqId, { expiresDays: d });
  }
  
  const deviceLimit = session?.device_limit ?? 2;
  const expText = d === 0 ? "Без срока" : `${d} дней`;
  
  await safeAnswerCbQuery(ctx, `Срок: ${expText}`);
  await safeEditMessageText(ctx,
    `⚙️ Подтверждение выдачи доступа\n\n📱 Лимит устройств: ${deviceLimit === 0 ? '∞' : deviceLimit}\n📅 Срок: ${expText}\n\nШаг 3/3: Подтверди выдачу`,
    { reply_markup: adminConfirmPicker(reqId, deviceLimit, d).reply_markup }
  );
});

// ==================== NEW ADMIN PANEL HANDLERS ====================

// Main menu
bot.action("admin_menu", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeAnswerCbQuery(ctx);
  await safeEditMessageText(ctx, "🔧 Админ-панель", { reply_markup: adminMainMenu().reply_markup });
});

// List pending requests
bot.action("admin_list_requests", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeAnswerCbQuery(ctx);
  
  const pending = db.prepare(`
    SELECT r.*, u.username, u.first_name, u.last_name 
    FROM requests r 
    JOIN users u ON r.tg_id = u.tg_id 
    WHERE r.status = 'pending' 
    ORDER BY r.created_at DESC
  `).all();
  
  if (pending.length === 0) {
    return safeEditMessageText(ctx, "✅ Нет ожидающих заявок", { reply_markup: adminMainMenu().reply_markup });
  }
  
  let text = `📋 Ожидающие заявки (${pending.length}):\n\n`;
  const keyboard = { inline_keyboard: [] };
  
  for (const req of pending) {
    const name = `${req.first_name || ""} ${req.last_name || ""}`.trim();
    const username = req.username ? `@${req.username}` : `id:${req.tg_id}`;
    const time = new Date(req.created_at * 1000).toLocaleString('ru-RU', { hour: '2-digit', minute: '2-digit', day: '2-digit', month: '2-digit' });
    text += `• ${name} (${username}) — ${time}\n`;
    keyboard.inline_keyboard.push([Markup.button.callback(`👤 ${name || username}`, `admin_view_req:${req.id}`)]);
  }
  
  keyboard.inline_keyboard.push([Markup.button.callback("« В меню", "admin_menu")]);
  
  await safeEditMessageText(ctx, text, { reply_markup: keyboard });
});

// View specific request
bot.action(/admin_view_req:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  await safeAnswerCbQuery(ctx);
  
  const req = db.prepare(`
    SELECT r.*, u.* 
    FROM requests r 
    JOIN users u ON r.tg_id = u.tg_id 
    WHERE r.id = ?
  `).get(reqId);
  
  if (!req) {
    return safeEditMessageText(ctx, "❌ Заявка не найдена", { reply_markup: adminMainMenu().reply_markup });
  }
  
  const time = new Date(req.created_at * 1000).toLocaleString('ru-RU');
  const text = `🔍 Заявка #${reqId.slice(0, 8)}\n\n${fmtUserCard(req)}\n\nСоздана: ${time}`;
  
  if (req.status === 'pending') {
    await safeEditMessageText(ctx, text, { reply_markup: adminRequestCard(reqId).reply_markup });
  } else {
    await safeEditMessageText(ctx, text + `\n\n⚠️ Заявка уже обработана (статус: ${req.status})`, { reply_markup: adminMainMenu().reply_markup });
  }
});

// Quick grant default profile (5 devices, unlimited)
bot.action(/admin_quickgrant:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];

  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req || req.status !== "pending") {
    deleteAdminSession(reqId);
    return safeAnswerCbQuery(ctx, "Уже обработано");
  }

  const deviceLimit = 5;
  const expiresDays = 0;
  const expiresAt = null;

  db.prepare("UPDATE requests SET status='approved' WHERE id=?").run(reqId);
  setUserAccess(req.tg_id, { deviceLimit, expiresAt });
  deleteAdminSession(reqId);

  await safeAnswerCbQuery(ctx, "Выдано: 5 устройств, без срока");
  await safeEditMessageText(ctx,
    `✅ Доступ выдан быстро\n\n📱 Лимит: 5\n📅 Срок: без ограничений`,
    { reply_markup: adminMainMenu().reply_markup }
  );

  await safeSendMessage(bot, req.tg_id,
    `Доступ выдан ✅\nЛимит устройств: 5\nСрок: без ограничений\n\n${pickUniquePs("end", req.tg_id)}`
  );

  await renderMenuForUser(req.tg_id, {
    text: `Привет! Доступ активен ✅\n\nВыбери нужный режим подключения ниже.\n\n${pickUniquePs("start", req.tg_id)}`,
    keyboard: userMenu({ approved: true })
  });
});

// Start approve flow from admin panel
bot.action(/admin_approve:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  
  if (!req || req.status !== "pending") {
    return safeAnswerCbQuery(ctx, "Уже обработано или не найдено");
  }
  
  createAdminSession(reqId, ctx.from.id, 5, 0);
  
  await safeAnswerCbQuery(ctx, "Начинаем настройку");
  await safeEditMessageText(ctx,
    `⚙️ Настройка доступа для заявки #${reqId.slice(0, 8)}\n\nШаг 1/3: Выбери лимит устройств`,
    { reply_markup: adminDeviceLimitPicker(reqId).reply_markup }
  );
});

// Device limit selection
bot.action(/admin_setdev:(.+):(\d+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const n = Number(ctx.match[2]);
  
  const session = getAdminSession(reqId);
  if (!session) {
    createAdminSession(reqId, ctx.from.id, n, 30);
  } else {
    updateAdminSession(reqId, { deviceLimit: n });
  }
  
  await safeAnswerCbQuery(ctx, `Лимит: ${n === 0 ? '∞' : n}`);
  await safeEditMessageText(ctx,
    `⚙️ Настройка доступа\n\n📱 Лимит устройств: ${n === 0 ? '∞' : n}\n\nШаг 2/3: Выбери срок действия`,
    { reply_markup: adminExpiryPicker(reqId).reply_markup }
  );
});

// Back to device selection
bot.action(/admin_back_dev:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  await safeAnswerCbQuery(ctx);
  await safeEditMessageText(ctx,
    `⚙️ Настройка доступа\n\nШаг 1/3: Выбери лимит устройств`,
    { reply_markup: adminDeviceLimitPicker(reqId).reply_markup }
  );
});

// Expiry selection
bot.action(/admin_setexp:(.+):(\d+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const d = Number(ctx.match[2]);
  
  const session = getAdminSession(reqId);
  if (!session) {
    createAdminSession(reqId, ctx.from.id, 2, d);
  } else {
    updateAdminSession(reqId, { expiresDays: d });
  }
  
  const deviceLimit = session?.device_limit ?? 2;
  const expText = d === 0 ? "Без срока" : `${d} дней`;
  
  await safeAnswerCbQuery(ctx, `Срок: ${expText}`);
  await safeEditMessageText(ctx,
    `⚙️ Подтверждение выдачи доступа\n\n📱 Лимит устройств: ${deviceLimit === 0 ? '∞' : deviceLimit}\n📅 Срок: ${expText}\n\nШаг 3/3: Подтверди выдачу`,
    { reply_markup: adminConfirmPicker(reqId, deviceLimit, d).reply_markup }
  );
});

// Back to expiry selection
bot.action(/admin_back_exp:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  await safeAnswerCbQuery(ctx);
  await safeEditMessageText(ctx,
    `⚙️ Настройка доступа\n\nШаг 2/3: Выбери срок действия`,
    { reply_markup: adminExpiryPicker(reqId).reply_markup }
  );
});

// Confirm and grant access
bot.action(/admin_confirm:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req || req.status !== "pending") {
    deleteAdminSession(reqId);
    return safeAnswerCbQuery(ctx, "Уже обработано");
  }
  
  const session = getAdminSession(reqId);
  if (!session) {
    return safeAnswerCbQuery(ctx, "Сессия устарела, начни заново");
  }
  
  const deviceLimit = session.device_limit ?? 2;
  const expiresDays = session.expires_days ?? 30;
  const expiresAt = expiresDays === 0 ? null : now() + expiresDays * 86400;
  
  db.prepare("UPDATE requests SET status='approved' WHERE id=?").run(reqId);
  setUserAccess(req.tg_id, { deviceLimit, expiresAt });
  deleteAdminSession(reqId);
  
  const expText = expiresDays === 0 ? "Без срока" : `${expiresDays} дней`;
  
  await safeAnswerCbQuery(ctx, "Доступ выдан!");
  await safeEditMessageText(ctx,
    `✅ Доступ выдан\n\n📱 Лимит: ${deviceLimit === 0 ? '∞' : deviceLimit}\n📅 Срок: ${expText}`,
    { reply_markup: adminMainMenu().reply_markup }
  );
  
  await safeSendMessage(bot, req.tg_id,
    `Доступ выдан ✅\nЛимит устройств: ${deviceLimit === 0 ? '∞' : deviceLimit}\nСрок: ${expText}\n\n${pickUniquePs("end", req.tg_id)}`
  );
  
  await renderMenuForUser(req.tg_id, {
    text: `Привет! Доступ активен ✅\n\nВыбери нужный режим подключения ниже.\n\n${pickUniquePs("start", req.tg_id)}`,
    keyboard: userMenu({ approved: true })
  });
});

// Cancel approval flow
bot.action(/admin_cancel:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  deleteAdminSession(reqId);
  await safeAnswerCbQuery(ctx, "Отменено");
  await safeEditMessageText(ctx, "❌ Выдача доступа отменена", { reply_markup: adminMainMenu().reply_markup });
});

// Deny request
bot.action(/admin_deny:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  
  if (!req || req.status !== "pending") {
    return safeAnswerCbQuery(ctx, "Уже обработано");
  }
  
  db.prepare("UPDATE requests SET status='denied' WHERE id=?").run(reqId);
  setUserStatus(req.tg_id, 'denied');
  deleteAdminSession(reqId);
  
  await safeAnswerCbQuery(ctx, "Отклонено");
  await safeEditMessageText(ctx, "❌ Заявка отклонена", { reply_markup: adminMainMenu().reply_markup });
  await safeSendMessage(bot, req.tg_id, "Сорри, доступ не выдан");
});

// Ban user
bot.action(/admin_ban:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  
  if (!req) return safeAnswerCbQuery(ctx, "Не найдено");
  
  banUser(req.tg_id);
  db.prepare("UPDATE requests SET status='banned' WHERE id=?").run(reqId);
  deleteAdminSession(reqId);
  
  await safeAnswerCbQuery(ctx, "Забанен");
  await safeEditMessageText(ctx, "🧱 Пользователь забанен", { reply_markup: adminMainMenu().reply_markup });
  await safeSendMessage(bot, req.tg_id, "Доступ закрыт");
});

// View user profile
bot.action(/admin_profile:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  await safeAnswerCbQuery(ctx);
  
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req) return safeEditMessageText(ctx, "❌ Заявка не найдена", { reply_markup: adminMainMenu().reply_markup });
  
  const u = getUser(req.tg_id);
  if (!u) return safeEditMessageText(ctx, "❌ Пользователь не найден", { reply_markup: adminMainMenu().reply_markup });
  
  const text = `👤 Профиль пользователя\n\n${fmtUserCard(u)}\n\nЗаявка #${reqId.slice(0, 8)}`;
  
  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("« К заявке", `admin_view_req:${reqId}`)],
    [Markup.button.callback("« В меню", "admin_menu")]
  ]);
  
  await safeEditMessageText(ctx, text, { reply_markup: keyboard.reply_markup });
});

// Stuck requests handling
bot.action("admin_stuck_requests", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeAnswerCbQuery(ctx);
  
  // Show all pending requests, but mark which ones are truly "stuck" (>1h)
  const oneHourAgo = now() - 3600;
  const pendingAll = db.prepare(`
    SELECT r.*, u.username, u.first_name, u.last_name 
    FROM requests r 
    JOIN users u ON r.tg_id = u.tg_id 
    WHERE r.status = 'pending'
    ORDER BY r.created_at ASC
  `).all();

  const stuckCount = pendingAll.filter(r => r.created_at < oneHourAgo).length;

  let text = `⏳ Pending / зависшие заявки\n\n`;
  text += `Всего pending: ${pendingAll.length}\n`;
  text += `Старые (>1ч): ${stuckCount}\n\n`;

  if (pendingAll.length === 0) {
    text += "✅ Сейчас нет pending-заявок";
    return safeEditMessageText(ctx, text, { reply_markup: adminMainMenu().reply_markup });
  }

  const keyboard = { inline_keyboard: [] };

  for (const req of pendingAll) {
    const name = `${req.first_name || ""} ${req.last_name || ""}`.trim();
    const username = req.username ? `@${req.username}` : `id:${req.tg_id}`;
    const ageSec = now() - req.created_at;
    const ageText = ageSec >= 3600 ? `${Math.floor(ageSec / 3600)}ч` : `${Math.max(1, Math.floor(ageSec / 60))}м`;
    const icon = req.created_at < oneHourAgo ? "🔧" : "🆕";
    text += `• ${name} (${username}) — ${ageText}\n`;
    keyboard.inline_keyboard.push([Markup.button.callback(`${icon} ${name || username} (${ageText})`, `admin_stuck_view:${req.id}`)]);
  }
  
  keyboard.inline_keyboard.push([Markup.button.callback("« В меню", "admin_menu")]);
  
  await safeEditMessageText(ctx, text, { reply_markup: keyboard });
});

// View stuck request
bot.action(/admin_stuck_view:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  await safeAnswerCbQuery(ctx);
  
  const req = db.prepare(`
    SELECT r.*, u.* 
    FROM requests r 
    JOIN users u ON r.tg_id = u.tg_id 
    WHERE r.id = ?
  `).get(reqId);
  
  if (!req) {
    return safeEditMessageText(ctx, "❌ Заявка не найдена", { reply_markup: adminMainMenu().reply_markup });
  }
  
  const hoursAgo = Math.floor((now() - req.created_at) / 3600);
  const text = `⏳ Зависшая заявка #${reqId.slice(0, 8)}\n\n${fmtUserCard(req)}\n\nСоздана: ${hoursAgo} часов назад\n\n⚠️ Эта заявка висит долго. Можно одобрить, отклонить или создать новую.`;
  
  await safeEditMessageText(ctx, text, { reply_markup: adminStuckActions(reqId).reply_markup });
});

// Reopen/create new request for user
bot.action(/admin_reopen:(.+)/, async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const reqId = ctx.match[1];
  
  const req = db.prepare("SELECT * FROM requests WHERE id=?").get(reqId);
  if (!req) return safeAnswerCbQuery(ctx, "Не найдено");
  
  // Mark old request as superseded
  db.prepare("UPDATE requests SET status='superseded' WHERE id=?").run(reqId);
  
  // Create new request
  const newReqId = crypto.randomUUID();
  db.prepare("INSERT INTO requests(id,tg_id,status,created_at) VALUES(?,?, 'pending', ?)").run(newReqId, req.tg_id, now());
  setUserStatus(req.tg_id, "pending");
  
  await safeAnswerCbQuery(ctx, "Создана новая заявка");
  await safeEditMessageText(ctx, `🔄 Создана новая заявка #${newReqId.slice(0, 8)} для пользователя`, { reply_markup: adminMainMenu().reply_markup });
  
  // Notify user
  await safeSendMessage(bot, req.tg_id, "Твоя зависшая заявка была переоткрыта. Жди подтверждения ✅");
});

function formatClientName(u) {
  const name = `${u.first_name || ""} ${u.last_name || ""}`.trim();
  return name || (u.username ? `@${u.username}` : `id:${u.tg_id}`);
}

async function renderAdminClients(ctx, mode = "edit") {
  const users = db.prepare(`
    SELECT tg_id, username, first_name, last_name, status, device_limit, devices_used, expires_at, updated_at
    FROM users
    WHERE status='approved'
    ORDER BY updated_at DESC
    LIMIT 100
  `).all();

  const active = users.filter(u => !u.expires_at || u.expires_at > now());
  const expired = users.length - active.length;

  let text = `👥 Клиенты сейчас\n\n✅ Активных: ${active.length}\n⌛ Истекших: ${expired}\n\n`;

  if (!users.length) {
    text += "Пока нет одобренных клиентов";
    if (mode === "reply") {
      return safeReply(ctx, text, adminMainMenu());
    }
    return safeEditMessageText(ctx, text, { reply_markup: adminMainMenu().reply_markup });
  }

  const rows = [];
  for (const u of users) {
    const isActive = !u.expires_at || u.expires_at > now();
    const icon = isActive ? "✅" : "⌛";
    const username = u.username ? `@${u.username}` : "без username";
    const expText = u.expires_at ? new Date(u.expires_at * 1000).toLocaleDateString('ru-RU') : "без срока";
    const limText = u.device_limit === 0 ? "∞" : String(u.device_limit || 0);

    text += `${icon} ${formatClientName(u)} (${username})\n`;
    text += `   устр: ${u.devices_used}/${limText} • срок: ${expText}\n`;

    if (u.username) {
      rows.push([Markup.button.url(`${icon} ${username}`, `https://t.me/${u.username}`)]);
    } else {
      rows.push([Markup.button.url(`${icon} id:${u.tg_id}`, `tg://user?id=${u.tg_id}`)]);
    }
  }

  rows.push([Markup.button.callback("🔄 Обновить", "admin_clients")]);
  rows.push([Markup.button.callback("« В меню", "admin_menu")]);

  if (mode === "reply") {
    return safeReply(ctx, text, { reply_markup: Markup.inlineKeyboard(rows).reply_markup });
  }
  return safeEditMessageText(ctx, text, { reply_markup: Markup.inlineKeyboard(rows).reply_markup });
}

// Stats
bot.action("admin_stats", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeAnswerCbQuery(ctx);
  
  const total = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  const approved = db.prepare("SELECT COUNT(*) as count FROM users WHERE status='approved'").get().count;
  const pending = db.prepare("SELECT COUNT(*) as count FROM requests WHERE status='pending'").get().count;
  const banned = db.prepare("SELECT COUNT(*) as count FROM users WHERE status='banned'").get().count;
  const denied = db.prepare("SELECT COUNT(*) as count FROM users WHERE status='denied'").get().count;
  
  // Expiring soon (within 7 days)
  const weekFromNow = now() + 7 * 86400;
  const expiringSoon = db.prepare("SELECT COUNT(*) as count FROM users WHERE status='approved' AND expires_at > ? AND expires_at < ?").get(now(), weekFromNow).count;
  
  const text = `📊 Статистика:\n\n👥 Всего пользователей: ${total}\n✅ Активных доступов: ${approved}\n⏳ Ожидают проверки: ${pending}\n❌ Отклонено: ${denied}\n🚫 Забанено: ${banned}\n\n⚠️ Истекает в течение 7 дней: ${expiringSoon}`;

  const keyboard = Markup.inlineKeyboard([
    [Markup.button.callback("👥 Юзернеймы и профили", "admin_stats_users")],
    [Markup.button.callback("👥 Клиенты сейчас", "admin_clients")],
    [Markup.button.callback("« В меню", "admin_menu")]
  ]);
  
  await safeEditMessageText(ctx, text, { reply_markup: keyboard.reply_markup });
});

bot.action("admin_stats_users", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeAnswerCbQuery(ctx);

  const users = db.prepare(`
    SELECT tg_id, username, first_name, last_name, status
    FROM users
    ORDER BY updated_at DESC
    LIMIT 80
  `).all();

  if (!users.length) {
    return safeEditMessageText(ctx, "Пользователи пока не найдены", { reply_markup: adminMainMenu().reply_markup });
  }

  let text = `👥 Пользователи (последние ${users.length})\nНажми кнопку — откроется профиль в Telegram\n\n`;
  const rows = [];

  for (const u of users) {
    const name = `${u.first_name || ""} ${u.last_name || ""}`.trim() || `id:${u.tg_id}`;
    const username = u.username ? `@${u.username}` : "без username";
    const emoji = u.status === 'approved' ? '✅' : (u.status === 'pending' ? '⏳' : '•');
    text += `${emoji} ${name} (${username})\n`;

    if (u.username) {
      rows.push([Markup.button.url(`${emoji} ${username}`, `https://t.me/${u.username}`)]);
    } else {
      rows.push([Markup.button.url(`${emoji} ${name}`, `tg://user?id=${u.tg_id}`)]);
    }
  }

  rows.push([Markup.button.callback("« Назад к статистике", "admin_stats")]);
  await safeEditMessageText(ctx, text, { reply_markup: Markup.inlineKeyboard(rows).reply_markup });
});

bot.action("admin_clients", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeAnswerCbQuery(ctx);
  await renderAdminClients(ctx, "edit");
});

// ==================== COMMANDS ====================

bot.command("admin", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await safeReply(ctx, "🔧 Админ-панель", adminMainMenu());
});

bot.command("stats", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  const total = db.prepare("SELECT COUNT(*) as count FROM users").get().count;
  const approved = db.prepare("SELECT COUNT(*) as count FROM users WHERE status='approved'").get().count;
  const pending = db.prepare("SELECT COUNT(*) as count FROM requests WHERE status='pending'").get().count;
  
  await safeReply(ctx, `📊 Статистика:\nВсего пользователей: ${total}\nАктивных доступов: ${approved}\nОжидают проверки: ${pending}`);
});

bot.command("clients", async (ctx) => {
  if (!requireAdmin(ctx)) return;
  await renderAdminClients(ctx, "reply");
});

function buildProxyUrls() {
  return {
    turboUrl: `https://t.me/proxy?server=${PROXY_SERVER}&port=${PROXY_PORT}&secret=${PROXY_SECRET}`,
    stableUrl: `https://t.me/proxy?server=${PROXY_SERVER}&port=443&secret=${PROXY_SECRET}`,
  };
}

function requireApprovedUser(ctx) {
  upsertUser(ctx.from);
  const u = getUser(ctx.from.id);
  if (!isApproved(u)) {
    safeReply(ctx, "Команда доступна после подтверждения доступа");
    return null;
  }
  if (!PROXY_SECRET) {
    safeReply(ctx, "Прокси временно недоступен");
    return null;
  }
  return u;
}

// /safe, /turbo, /stable, /diag are hidden from non-admin command menu,
// but can be used by approved users if they know the command names.
bot.command("safe", async (ctx) => {
  const u = requireApprovedUser(ctx);
  if (!u) return;
  const { turboUrl, stableUrl } = buildProxyUrls();

  await safeReply(ctx,
`🛡️ Профили подключения

1) TURBO (быстрый, по умолчанию):\n${turboUrl}

2) STABLE (резервный, если сеть режет turbo):\n${stableUrl}

Рекомендация: держи оба профиля в Telegram и переключайся при деградации.`
  );
});

bot.command("turbo", async (ctx) => {
  const u = requireApprovedUser(ctx);
  if (!u) return;
  const { turboUrl } = buildProxyUrls();
  await safeReply(ctx, `⚡ TURBO профиль:\n${turboUrl}`);
});

bot.command("stable", async (ctx) => {
  const u = requireApprovedUser(ctx);
  if (!u) return;
  const { stableUrl } = buildProxyUrls();
  await safeReply(ctx, `🧱 STABLE профиль:\n${stableUrl}`);
});

bot.command("diag", async (ctx) => {
  const u = requireApprovedUser(ctx);
  if (!u) return;
  const { turboUrl, stableUrl } = buildProxyUrls();

  // Lightweight recommendation heuristic for user-facing guidance.
  // If default port is not 443, we assume turbo-first with stable fallback.
  const turboPort = String(PROXY_PORT || "443");
  const recommendation = turboPort === "443"
    ? "Сейчас дефолт уже STABLE (443). Если медиа норм — оставайся на нём."
    : `Сейчас рекомендую TURBO (${turboPort}). Если видео начнут тупить — переключись на STABLE (443).`;

  await safeReply(ctx,
`🧪 Диагностика режима (быстрая)

${recommendation}

TURBO:\n${turboUrl}

STABLE:\n${stableUrl}

Правило: если текст/фото идут, а видео тупят — пробуй STABLE.`
  );
});

// ==================== ERROR HANDLING ====================

bot.catch(async (err, ctx) => {
  console.error("[bot error]", err);
  try {
    if (ctx?.callbackQuery) {
      await safeAnswerCbQuery(ctx, "Ошибка, попробуй ещё раз", { show_alert: true });
    }
  } catch {}
});

// ==================== CLEANUP ON STARTUP ====================

// Clean up old sessions on startup
cleanupOldSessions();

// ==================== LAUNCH ====================

await configureBotCommands();

bot.launch();
process.on("SIGINT", () => bot.stop("SIGINT"));
process.on("SIGTERM", () => bot.stop("SIGTERM"));
