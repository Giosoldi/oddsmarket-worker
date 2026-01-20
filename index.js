import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

// Configuration
const ODDSMARKET_API_KEY = process.env.ODDSMARKET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// OddsMarket WebSocket endpoints
const WS_URL = `wss://api-pr.oddsmarket.org/v4/odds_ws`;

// Bookmaker IDs: 1xbet (21), Sisal (103)
const BOOKMAKER_IDS = [21, 103];
const SPORT_ID = 7; // Soccer

// ============ SERIE A TEAM MAPPING ============
// Mappa manuale: nome 1xbet (russo translitterato) -> nome standardizzato
// Aggiungi qui le mappature per ogni squadra
const TEAM_MAP = {
  // === SERIE A 2024/25 ===
  // Formato: "nome1xbet": "nomeStandard"
  // Esempi da completare con i nomi reali dal feed 1xbet:

  // Squadre già mappate (aggiungi altre man mano le scopri)
  napoli: "napoli",
  juventus: "juventus",
  inter: "inter",
  milan: "milan",
  roma: "roma",
  lazio: "lazio",
  atalanta: "atalanta",
  fiorentina: "fiorentina",
  torino: "torino",
  bologna: "bologna",
  udinese: "udinese",
  empoli: "empoli",
  lecce: "lecce",
  monza: "monza",
  cagliari: "cagliari",
  genoa: "genoa",
  verona: "verona",
  parma: "parma",
  como: "como",
  venezia: "venezia",

  // Varianti russe comuni (aggiungi qui i nomi che trovi nei log)
  // "наполи": "napoli",
  // "ювентус": "juventus",
  // etc.
};

// ============ MARKET MAPPING ============
// Mercati da confrontare tra 1xbet e Sisal
// ============ CANONICAL MARKET DEFINITIONS ============
// UNICA FONTE DI VERITÀ

const CANONICAL_MARKETS = {
  SHOTS: "SHOTS",
  SHOTS_ON_TARGET: "SHOTS_ON_TARGET",
  FOULS: "FOULS",
  OFFSIDES: "OFFSIDES",
  CORNERS: "CORNERS",
};

// Scope standard
const MARKET_SCOPE = {
  MATCH: "MATCH", // 1X2
  TOTAL: "TOTAL", // Over/Under totale
  TEAM1: "TEAM1", // Casa
  TEAM2: "TEAM2", // Trasferta
};

// Tipo selezione
const MARKET_TYPE = {
  H2H: "1X2",
  OU: "OU",
};

// 1xbet betId -> mercato standardizzato
const ONEXBET_MARKETS = {
  // ===== H2H (1X2) =====
  11: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.MATCH, type: MARKET_TYPE.H2H },
  12: { canonical: CANONICAL_MARKETS.SHOTS, scope: MARKET_SCOPE.MATCH, type: MARKET_TYPE.H2H },
  13: { canonical: CANONICAL_MARKETS.SHOTS_ON_TARGET, scope: MARKET_SCOPE.MATCH, type: MARKET_TYPE.H2H },
  14: { canonical: CANONICAL_MARKETS.FOULS, scope: MARKET_SCOPE.MATCH, type: MARKET_TYPE.H2H },
  751: { canonical: CANONICAL_MARKETS.OFFSIDES, scope: MARKET_SCOPE.MATCH, type: MARKET_TYPE.H2H },

  // ===== TOTAL OVER / UNDER =====
  739: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  740: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },

  7778: { canonical: CANONICAL_MARKETS.SHOTS_ON_TARGET, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  7779: { canonical: CANONICAL_MARKETS.SHOTS_ON_TARGET, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },

  7786: { canonical: CANONICAL_MARKETS.SHOTS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  7787: { canonical: CANONICAL_MARKETS.SHOTS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },

  7792: { canonical: CANONICAL_MARKETS.FOULS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  7793: { canonical: CANONICAL_MARKETS.FOULS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },

  7798: { canonical: CANONICAL_MARKETS.OFFSIDES, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  7799: { canonical: CANONICAL_MARKETS.OFFSIDES, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  741: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  742: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
};

// Sisal codiceScommessa -> mercato standardizzato
const SISAL_MARKETS = {
  // H2H
  127: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.MATCH, type: MARKET_TYPE.H2H },

  // TOTAL OU
  9942: { canonical: CANONICAL_MARKETS.CORNERS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  28319: { canonical: CANONICAL_MARKETS.SHOTS_ON_TARGET, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  28320: { canonical: CANONICAL_MARKETS.SHOTS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  28321: { canonical: CANONICAL_MARKETS.FOULS, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
  28322: { canonical: CANONICAL_MARKETS.OFFSIDES, scope: MARKET_SCOPE.TOTAL, type: MARKET_TYPE.OU },
};

// Sisal codiceEsito -> selezione
const SISAL_SELECTIONS = {
  1: "1",
  2: "2",
  3: "X",
  9: "Over",
  14: "Under",
};

// ============ SERIE A LEAGUE FILTER ============
// Nomi del campionato Serie A come appaiono nel feed
const SERIE_A_LEAGUES = [
  "italy. serie a",
  "italy serie a",
  "serie a",
  "италия. серия а",
  "italy. serie a. round",
  "italian serie a",
];

function isSerieALeague(league) {
  if (!league) return false;
  const normalized = league.toLowerCase().trim();
  return SERIE_A_LEAGUES.some((l) => normalized.includes(l) || l.includes(normalized));
}

// ============ TEAM NAME NORMALIZATION ============
// Semplice: cerca nella mappa, altrimenti pulisci il nome

function normalizeTeamName(name) {
  if (!name) return null;

  // Pulisci e normalizza
  let cleaned = name.toLowerCase().trim();

  // Rimuovi prefissi/suffissi comuni
  cleaned = cleaned
    .replace(/\b(fc|ac|ssc|as|ss|afc|sc|cf|ud|us)\b/gi, "")
    .replace(/[^a-zа-яёà-ü0-9\s]/gi, "") // mantieni lettere + numeri
    .replace(/\s+/g, " ")
    .trim();

  // Cerca nella mappa manuale
  if (TEAM_MAP[cleaned]) {
    return TEAM_MAP[cleaned];
  }

  // Prova anche senza spazi
  const noSpaces = cleaned.replace(/\s/g, "");
  if (TEAM_MAP[noSpaces]) {
    return TEAM_MAP[noSpaces];
  }

  // Se non trovato, restituisci il nome pulito (per logging)
  return cleaned;
}

function parseEventName(eventName) {
  if (!eventName) return null;

  const separators = [" - ", " – ", " — ", " vs ", " v "];

  for (const sep of separators) {
    if (eventName.includes(sep)) {
      const parts = eventName.split(sep);
      if (parts.length >= 2) {
        return {
          home: parts[0].trim(),
          away: parts[parts.length - 1].trim(),
        };
      }
    }
  }

  return null;
}

// ============ MATCH KEY GENERATION ============
// Crea una chiave univoca per il match basata sui nomi normalizzati

function roundStartTime(timestamp) {
  if (!timestamp) return "unknown";

  try {
    const date = new Date(timestamp);
    if (isNaN(date.getTime()) || date.getFullYear() < 2020) {
      // Timestamp non valido, usa data corrente arrotondata
      const now = new Date();
      const roundedMinutes = Math.floor(now.getMinutes() / 30) * 30;
      now.setMinutes(roundedMinutes, 0, 0);
      return now.toISOString().slice(0, 16);
    }

    // Arrotonda a 30 minuti
    const roundedMinutes = Math.floor(date.getMinutes() / 30) * 30;
    date.setMinutes(roundedMinutes, 0, 0);
    return date.toISOString().slice(0, 16);
  } catch (e) {
    return "unknown";
  }
}

function buildMatchKey(eventName, rawStartTime) {
  const parsed = parseEventName(eventName);
  if (!parsed) return null;

  const home = normalizeTeamName(parsed.home);
  const away = normalizeTeamName(parsed.away);
  const roundedTime = roundStartTime(rawStartTime);

  if (!home || !away) return null;

  return `${home}_${away}_${roundedTime}`;
}

function buildDisplayName(eventName) {
  const parsed = parseEventName(eventName);
  if (!parsed) return eventName;

  return `${parsed.home.trim()} - ${parsed.away.trim()}`;
}

// ============ MAPPING FUNCTIONS ============

function map1xbetBetId(betId) {
  const mapped = ONEXBET_MARKETS[betId];
  if (mapped) return mapped;
  return null; // Non salvare mercati non mappati
}

function mapSisalMarket(codice) {
  return SISAL_MARKETS[codice] || null;
}

function mapSisalSelection(codiceEsito) {
  return SISAL_SELECTIONS[codiceEsito] || `E${codiceEsito}`;
}

function getBookmakerName(id) {
  if (id === 21) return "1xbet";
  if (id === 103) return "Sisal";
  return `Bookmaker_${id}`;
}

// ============ SUPABASE & WEBSOCKET ============

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

let ws = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 5000;

// Cache eventi
const eventsCache = new Map();

// Tracking per debug
const unmappedTeams = new Map(); // Squadre non trovate nella mappa

function connect() {
  console.log("Connecting to OddsMarket WebSocket...", WS_URL);

  ws = new WebSocket(WS_URL, { perMessageDeflate: true });

  ws.on("open", () => {
    console.log("Connected to OddsMarket WebSocket");
    reconnectAttempts = 0;

    ws.send(JSON.stringify({ cmd: "authorization", msg: ODDSMARKET_API_KEY }));
    console.log("Sent authorization message");
  });

  ws.on("message", async (data) => {
    try {
      const message = JSON.parse(data.toString());

      if (message.cmd === "authorized") {
        console.log("✅ Authorization successful");
        ws.send(
          JSON.stringify({
            cmd: "subscribe",
            msg: { bookmakerIds: BOOKMAKER_IDS, sportIds: [SPORT_ID] },
          }),
        );
      } else if (message.cmd === "subscribed") {
        console.log("✅ Subscription successful");
      } else if (message.cmd === "outcomes") {
        await processOutcomes(message.msg);
      } else if (message.cmd === "bookmaker_events") {
        await processBookmakerEvents(message.msg);
      } else if (message.cmd === "error") {
        console.error("OddsMarket error:", message.msg);
      }
    } catch (error) {
      console.error("Error processing message:", error);
    }
  });

  ws.on("error", (error) => console.error("WebSocket error:", error.message));

  ws.on("close", (code, reason) => {
    console.log(`WebSocket closed: ${code} - ${reason}`);
    attemptReconnect();
  });
}

function attemptReconnect() {
  if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    reconnectAttempts++;
    console.log(`Reconnecting in ${RECONNECT_DELAY / 1000}s... (attempt ${reconnectAttempts})`);
    setTimeout(connect, RECONNECT_DELAY);
  } else {
    console.error("Max reconnect attempts reached. Exiting.");
    process.exit(1);
  }
}

// Ping per mantenere la connessione
setInterval(() => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ cmd: "ping", msg: Date.now().toString() }));
  }
}, 30000);

// ============ PROCESS BOOKMAKER EVENTS ============

async function processBookmakerEvents(data) {
  if (!data) return;

  const events = Array.isArray(data) ? data : [data];
  let serieACount = 0;

  for (const event of events) {
    if (!event) continue;

    let eventId, eventName, startsAt, league, bookmakerId;

    if (Array.isArray(event)) {
      eventId = event[0];
      bookmakerId = event[1];
      eventName = event[4] || `Event ${eventId}`;
      league = event[6] || event[7] || "";
      startsAt = event[3] ? new Date(event[3] * 1000).toISOString() : null;
    } else if (typeof event === "object") {
      eventId = event.id || event.eventId;
      eventName = event.name || event.eventName;
      startsAt = event.starts_at || event.startsAt;
      league = event.league || event.leagueName;
      bookmakerId = event.bookmaker_id || event.bookmakerId;
    }

    // FILTRO SERIE A
    if (!isSerieALeague(league)) continue;

    serieACount++;

    if (eventId && bookmakerId) {
      eventsCache.set(String(eventId), {
        name: eventName,
        startsAt: startsAt,
        league: league,
        bookmakerId: bookmakerId,
      });

      // Log squadre per debugging mappatura
      const parsed = parseEventName(eventName);
      if (parsed) {
        const homeNorm = normalizeTeamName(parsed.home);
        const awayNorm = normalizeTeamName(parsed.away);

        // Se non è nella mappa, logga per aggiunta manuale
        if (!TEAM_MAP[homeNorm]) {
          if (!unmappedTeams.has(homeNorm)) {
            unmappedTeams.set(homeNorm, parsed.home);
            console.log(`⚠️ UNMAPPED TEAM: "${parsed.home}" -> normalized: "${homeNorm}"`);
          }
        }
        if (!TEAM_MAP[awayNorm]) {
          if (!unmappedTeams.has(awayNorm)) {
            unmappedTeams.set(awayNorm, parsed.away);
            console.log(`⚠️ UNMAPPED TEAM: "${parsed.away}" -> normalized: "${awayNorm}"`);
          }
        }
      }
    }
  }

  if (serieACount > 0) {
    console.log(`✅ Serie A events cached: ${serieACount}, total cache: ${eventsCache.size}`);
  }

  // Pulizia cache periodica
  if (eventsCache.size > 2000) {
    const keysToDelete = [...eventsCache.keys()].slice(0, 500);
    keysToDelete.forEach((k) => eventsCache.delete(k));
  }
}

// ============ PROCESS OUTCOMES ============

async function processOutcomes(data) {
  if (!data) return;

  const outcomes = Array.isArray(data) ? data : [data];
  const oddsRecords = [];

  for (const outcome of outcomes) {
    if (!Array.isArray(outcome) || outcome.length < 12) continue;

    const internalEventId = outcome[1];
    const odds = outcome[11];
    const infoString = outcome[15];

    // Recupera info evento dalla cache
    const eventInfo = eventsCache.get(String(internalEventId));
    if (!eventInfo) continue; // Skip se non è Serie A (non in cache)

    let bookmakerId = eventInfo.bookmakerId;
    let canonicalMarket = null;
    let marketScope = null;
    let marketKind = null;
    let selection = null;

    // Identifica bookmaker dall'info string
    if (!bookmakerId && typeof infoString === "string") {
      if (infoString.includes("codiceScommessa=")) bookmakerId = 103;
      else if (infoString.includes("betId=")) bookmakerId = 21;
    }

    if (!bookmakerId) continue;

    // Parse mercati per bookmaker
    if (typeof infoString === "string") {
      if (bookmakerId === 21) {
        // 1XBET
        const betMatch = infoString.match(/betId=(\d+)/);
        const betValueMatch = infoString.match(/betValue=([0-9]+\.?[0-9]*)/);

        if (betMatch) {
          const betId = parseInt(betMatch[1]);
          const mapped = map1xbetBetId(betId);

          if (!mapped) continue; // Skip mercati non mappati

          canonicalMarket = mapped.canonical;
          marketScope = mapped.scope;
          marketKind = mapped.type;

          if (marketKind === MARKET_TYPE.H2H && betValueMatch) {
  const betValue = parseFloat(betValueMatch[1]);
  if (betValue === 1) selection = "1";
  else if (betValue === 2) selection = "2";
  else if (betValue === 0 || betValue === 3) selection = "X";
}

if (marketKind === MARKET_TYPE.OU && betValueMatch) {
  const line = parseFloat(betValueMatch[1]);
  selection = infoString.includes("Over")
  ? `Over ${line}`
  : `Under ${line}`;

}

      } else if (bookmakerId === 103) {
        // SISAL
        const marketMatch = infoString.match(/codiceScommessa=(\d+)/);
        const esitoMatch = infoString.match(/codiceEsito=(\d+)/);
        const lineMatch = infoString.match(/(?:handicap|line|spread|punti)=([0-9]+\.?[0-9]*)/i);

        if (marketMatch) {
          const codice = parseInt(marketMatch[1]);
          const mapped = mapSisalMarket(codice);
          if (!mapped) continue;

          canonicalMarket = mapped.canonical;
          marketScope = mapped.scope;
          marketKind = mapped.type;
        }

        if (esitoMatch) {
          let baseSel = mapSisalSelection(parseInt(esitoMatch[1]));
          if (lineMatch && (baseSel === "Over" || baseSel === "Under")) {
            const lineValue = parseFloat(lineMatch[1]);
            const normalizedLine = lineValue > 50 ? lineValue / 10 : lineValue;
            selection = `${baseSel} ${normalizedLine}`;
          } else {
            selection = baseSel;
          }
        }
      }
    }

    if (!canonicalMarket || !marketScope || !marketKind || !selection) continue;
    if (typeof odds !== "number" || odds <= 1 || odds >= 1000) continue;

    const matchKey = buildMatchKey(eventInfo.name, eventInfo.startsAt);
    const displayName = buildDisplayName(eventInfo.name);

    // Costruisci market_type come: CANONICAL_SCOPE_TYPE (es: "CORNERS_TOTAL_OU")
    const fullMarketType = `${canonicalMarket}_${marketScope}_${marketKind}`;

    oddsRecords.push({
      event_id: String(internalEventId),
      event_name: eventInfo.name,
      event_time: eventInfo.startsAt || null,
      league: eventInfo.league || "Serie A",
      sport_id: SPORT_ID,
      bookmaker_id: bookmakerId,
      bookmaker_name: getBookmakerName(bookmakerId),
      market_type: fullMarketType, // es: "CORNERS_TOTAL_OU", "SHOTS_MATCH_1X2"
      selection: selection,
      odds: parseFloat(odds),
      updated_at: new Date().toISOString(),
      match_key: matchKey,
      raw_start_time: eventInfo.startsAt || null,
      display_name: displayName,
      kickoff_time: null,
    });
  }

  if (oddsRecords.length > 0) {
    await saveOddsRecords(oddsRecords);
  }
}

// ============ SAVE TO DATABASE ============

const writeQueue = [];
let isProcessingQueue = false;
const BATCH_SIZE = 50;
const WRITE_DELAY_MS = 200;

async function saveOddsRecords(records) {
  // Deduplica per match_key + bookmaker + market_type + selection
  const uniqueMap = new Map();
  for (const record of records) {
    const key = `${record.match_key}_${record.bookmaker_id}_${record.market_type}_${record.selection}`;
    uniqueMap.set(key, record);
  }

  const uniqueRecords = [...uniqueMap.values()];

  // Aggiungi alla coda
  writeQueue.push(...uniqueRecords);
  console.log(`📝 Queued ${uniqueRecords.length} records, queue size: ${writeQueue.length}`);

  processWriteQueue();
}

async function processWriteQueue() {
  if (isProcessingQueue || writeQueue.length === 0) return;

  isProcessingQueue = true;

  while (writeQueue.length > 0) {
    const batch = writeQueue.splice(0, BATCH_SIZE);

    try {
      // Usa la constraint esistente nella tabella
      const { error } = await supabase.from("live_odds").upsert(batch, {
        onConflict: "event_id,bookmaker_id,market_type,selection",
        ignoreDuplicates: false,
      });

      if (error) {
        console.error("Supabase upsert error:", error.message);
      } else {
        // Conta per bookmaker
        const counts = { "1xbet": 0, Sisal: 0 };
        batch.forEach((r) => {
          if (r.bookmaker_id === 21) counts["1xbet"]++;
          else if (r.bookmaker_id === 103) counts["Sisal"]++;
        });
        console.log(`✅ Saved ${batch.length} records (1xbet: ${counts["1xbet"]}, Sisal: ${counts["Sisal"]})`);
      }
    } catch (err) {
      console.error("Database error:", err.message);
    }

    // Delay tra batch
    await new Promise((resolve) => setTimeout(resolve, WRITE_DELAY_MS));
  }

  isProcessingQueue = false;
}

// ============ LOGGING & HEALTH ============

// Log periodico delle squadre non mappate
setInterval(() => {
  if (unmappedTeams.size > 0) {
    console.log("\n========== UNMAPPED TEAMS (add to TEAM_MAP) ==========");
    [...unmappedTeams.entries()].slice(0, 20).forEach(([norm, orig]) => {
      console.log(`  "${norm}": "standardName", // Original: "${orig}"`);
    });
    console.log("=======================================================\n");
  }
}, 120000); // Ogni 2 minuti

// Health check server per Railway
const PORT = process.env.PORT || 3000;
import http from "http";

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({
      status: "ok",
      eventsCache: eventsCache.size,
      writeQueue: writeQueue.length,
      unmappedTeams: unmappedTeams.size,
    }),
  );
});

server.listen(PORT, () => {
  console.log(`Health server on port ${PORT}`);
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("SIGTERM received, closing...");
  if (ws) ws.close();
  server.close();
  process.exit(0);
});

// Start
connect();
console.log("🚀 OddsMarket Worker started - SERIE A ONLY mode");
console.log(`📋 Team mappings loaded: ${Object.keys(TEAM_MAP).length}`);
console.log(`📋 1xbet markets loaded: ${Object.keys(ONEXBET_MARKETS).length}`);
console.log(`📋 Sisal markets loaded: ${Object.keys(SISAL_MARKETS).length}`);
