import WebSocket from "ws";
import { createClient } from "@supabase/supabase-js";

// Configuration
const ODDSMARKET_API_KEY = process.env.ODDSMARKET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// OddsMarket WebSocket endpoints (CORRECT URLs from official docs)
// Prematch: wss://api-pr.oddsmarket.org/v4/odds_ws
// Live: wss://api-lv.oddsmarket.org/v4/odds_ws
const WS_URL = `wss://api-pr.oddsmarket.org/v4/odds_ws`;

// Bookmaker IDs from trial: 1xbet (21), Sisal (103) - Pinnacle NOT in tariff!
const BOOKMAKER_IDS = [21, 103];
// Soccer sport ID
const SPORT_ID = 7;

// ============ STATISTICAL MARKETS ONLY ============
// Save: Shots, Shots on Target, Fouls, Offsides, Corners
// Including: Total + Team 1 (Home) + Team 2 (Away) versions

// Allowed statistical market prefixes (standardized names)
const STATISTICAL_MARKETS = [
  "Shots", // Tiri totali
  "Shots On Target", // Tiri in porta
  "Shots Home", // Tiri squadra casa
  "Shots Away", // Tiri squadra trasferta
  "Shots On Target Home", // Tiri in porta casa
  "Shots On Target Away", // Tiri in porta trasferta
  "Fouls", // Falli totali
  "Fouls Home", // Falli casa
  "Fouls Away", // Falli trasferta
  "Offsides", // Fuorigiochi totali
  "Offsides Home", // Fuorigiochi casa
  "Offsides Away", // Fuorigiochi trasferta
  "Corners", // Angoli totali
  "Corners Home", // Angoli casa
  "Corners Away", // Angoli trasferta
];

function isStatisticalMarket(marketType) {
  if (!marketType) return false;
  return STATISTICAL_MARKETS.some((stat) => marketType.toLowerCase().includes(stat.toLowerCase()));
}

// ============ 1XBET STATISTICAL BETID CLASSIFICATION ============
// These betIds indicate statistical markets even when not mapped to named markets
// Based on analysis of OddsMarket feed structure and Railway logs
const ONEXBET_STAT_GROUPS = {
  // H2H: Chi vince il confronto (1=Home, 2=Away, 0/3=Draw)
  // Includes main 1X2 markets and statistical H2H variants
  H2H: [
    1, 2, 3, 4, 5, 6, 7, 8,         // Main match markets (1X2, DC, etc.)
    11, 12, 13, 14,                  // Statistical H2H: Corners, Shots, Shots OT, Fouls
    17, 19, 20, 22,                  // Additional statistical H2H variants
    181, 182, 183,                   // Extended statistical markets
    424, 426,                        // Yellow Cards H2H variants
    506,                             // Additional market
    751, 755, 3829,                  // Offsides and other H2H
  ],
  // OVER_UNDER: Mercati con linea dinamica (betValue contiene la linea)
  OVER_UNDER: [9, 10, 18, 3827, 3828, 3830],
};

// Check if a 1xbet betId is a statistical market
function is1xbetStatisticalBetId(betId) {
  return ONEXBET_STAT_GROUPS.H2H.includes(betId) || ONEXBET_STAT_GROUPS.OVER_UNDER.includes(betId);
}

// Get the statistical group for a 1xbet betId
function get1xbetStatGroup(betId) {
  if (ONEXBET_STAT_GROUPS.H2H.includes(betId)) return 'H2H_STATS';
  if (ONEXBET_STAT_GROUPS.OVER_UNDER.includes(betId)) return 'STATS_OU';
  return null;
}

// 1xbet betId mapping - STATISTICAL MARKETS (Total + Team-specific)
// H2H markets use betValue to determine selection (1=Home, 2=Away, 3=Draw)
const ONEXBET_MARKETS = {
  // ===== H2H STATISTICAL MARKETS (1X2 - Chi vince il confronto) =====
  // Discovered from Railway logs: betId 11,12,13,14,424 have betValue=1,2,3 (H2H pattern)
  
  // betId=11 - H2H market (most frequent, count=12574)
  11: { market: "Corners", selection: "H2H", isH2H: true },
  
  // betId=12 - H2H market (very frequent, count=12574)
  12: { market: "Shots", selection: "H2H", isH2H: true },
  
  // betId=13 - H2H market (count=11748)
  13: { market: "Shots On Target", selection: "H2H", isH2H: true },
  
  // betId=14 - H2H market (count=11763)
  14: { market: "Fouls", selection: "H2H", isH2H: true },
  
  // betId=424 - H2H market (has betValue=3 for Draw)
  424: { market: "Yellow Cards", selection: "H2H", isH2H: true },
  
  // betId=751 - H2H market (count=3213)
  751: { market: "Offsides", selection: "H2H", isH2H: true },

  // ===== CORNERS - Over/Under =====
  // Total
  739: { market: "Corners", selection: "Over 7.5" },
  740: { market: "Corners", selection: "Under 7.5" },
  741: { market: "Corners", selection: "Over 8.5" },
  742: { market: "Corners", selection: "Under 8.5" },
  743: { market: "Corners", selection: "Over 9.5" },
  744: { market: "Corners", selection: "Under 9.5" },
  749: { market: "Corners", selection: "Over 10.5" },
  750: { market: "Corners", selection: "Under 10.5" },
  753: { market: "Corners", selection: "Over 11.5" },
  754: { market: "Corners", selection: "Under 11.5" },
  763: { market: "Corners", selection: "Over 12.5" },
  764: { market: "Corners", selection: "Under 12.5" },
  // Home team corners
  765: { market: "Corners Home", selection: "Over 3.5" },
  766: { market: "Corners Home", selection: "Under 3.5" },
  767: { market: "Corners Home", selection: "Over 4.5" },
  768: { market: "Corners Home", selection: "Under 4.5" },
  769: { market: "Corners Home", selection: "Over 5.5" },
  770: { market: "Corners Home", selection: "Under 5.5" },
  // Away team corners
  771: { market: "Corners Away", selection: "Over 3.5" },
  772: { market: "Corners Away", selection: "Under 3.5" },
  773: { market: "Corners Away", selection: "Over 4.5" },
  774: { market: "Corners Away", selection: "Under 4.5" },
  775: { market: "Corners Away", selection: "Over 5.5" },
  776: { market: "Corners Away", selection: "Under 5.5" },

  // ===== SHOTS ON TARGET - Tiri in porta =====
  // Total
  7778: { market: "Shots On Target", selection: "Over 5.5" },
  7779: { market: "Shots On Target", selection: "Under 5.5" },
  7780: { market: "Shots On Target", selection: "Over 6.5" },
  7781: { market: "Shots On Target", selection: "Under 6.5" },
  7782: { market: "Shots On Target", selection: "Over 7.5" },
  7783: { market: "Shots On Target", selection: "Under 7.5" },
  7784: { market: "Shots On Target", selection: "Over 8.5" },
  7785: { market: "Shots On Target", selection: "Under 8.5" },
  // Home team shots on target
  7810: { market: "Shots On Target Home", selection: "Over 2.5" },
  7811: { market: "Shots On Target Home", selection: "Under 2.5" },
  7812: { market: "Shots On Target Home", selection: "Over 3.5" },
  7813: { market: "Shots On Target Home", selection: "Under 3.5" },
  7814: { market: "Shots On Target Home", selection: "Over 4.5" },
  7815: { market: "Shots On Target Home", selection: "Under 4.5" },
  // Away team shots on target
  7820: { market: "Shots On Target Away", selection: "Over 2.5" },
  7821: { market: "Shots On Target Away", selection: "Under 2.5" },
  7822: { market: "Shots On Target Away", selection: "Over 3.5" },
  7823: { market: "Shots On Target Away", selection: "Under 3.5" },
  7824: { market: "Shots On Target Away", selection: "Over 4.5" },
  7825: { market: "Shots On Target Away", selection: "Under 4.5" },

  // ===== SHOTS TOTAL - Tiri totali =====
  // Total
  7786: { market: "Shots", selection: "Over 20.5" },
  7787: { market: "Shots", selection: "Under 20.5" },
  7788: { market: "Shots", selection: "Over 22.5" },
  7789: { market: "Shots", selection: "Under 22.5" },
  7790: { market: "Shots", selection: "Over 24.5" },
  7791: { market: "Shots", selection: "Under 24.5" },
  // Home team shots
  7830: { market: "Shots Home", selection: "Over 10.5" },
  7831: { market: "Shots Home", selection: "Under 10.5" },
  7832: { market: "Shots Home", selection: "Over 11.5" },
  7833: { market: "Shots Home", selection: "Under 11.5" },
  7834: { market: "Shots Home", selection: "Over 12.5" },
  7835: { market: "Shots Home", selection: "Under 12.5" },
  // Away team shots
  7840: { market: "Shots Away", selection: "Over 10.5" },
  7841: { market: "Shots Away", selection: "Under 10.5" },
  7842: { market: "Shots Away", selection: "Over 11.5" },
  7843: { market: "Shots Away", selection: "Under 11.5" },
  7844: { market: "Shots Away", selection: "Over 12.5" },
  7845: { market: "Shots Away", selection: "Under 12.5" },

  // ===== FOULS - Falli =====
  // Total
  7792: { market: "Fouls", selection: "Over 20.5" },
  7793: { market: "Fouls", selection: "Under 20.5" },
  7794: { market: "Fouls", selection: "Over 22.5" },
  7795: { market: "Fouls", selection: "Under 22.5" },
  7796: { market: "Fouls", selection: "Over 24.5" },
  7797: { market: "Fouls", selection: "Under 24.5" },
  // Home team fouls
  7850: { market: "Fouls Home", selection: "Over 10.5" },
  7851: { market: "Fouls Home", selection: "Under 10.5" },
  7852: { market: "Fouls Home", selection: "Over 11.5" },
  7853: { market: "Fouls Home", selection: "Under 11.5" },
  7854: { market: "Fouls Home", selection: "Over 12.5" },
  7855: { market: "Fouls Home", selection: "Under 12.5" },
  // Away team fouls
  7860: { market: "Fouls Away", selection: "Over 10.5" },
  7861: { market: "Fouls Away", selection: "Under 10.5" },
  7862: { market: "Fouls Away", selection: "Over 11.5" },
  7863: { market: "Fouls Away", selection: "Under 11.5" },
  7864: { market: "Fouls Away", selection: "Over 12.5" },
  7865: { market: "Fouls Away", selection: "Under 12.5" },

  // ===== OFFSIDES - Fuorigiochi =====
  // Total
  7798: { market: "Offsides", selection: "Over 2.5" },
  7799: { market: "Offsides", selection: "Under 2.5" },
  7800: { market: "Offsides", selection: "Over 3.5" },
  7801: { market: "Offsides", selection: "Under 3.5" },
  7802: { market: "Offsides", selection: "Over 4.5" },
  7803: { market: "Offsides", selection: "Under 4.5" },
  // Home team offsides
  7870: { market: "Offsides Home", selection: "Over 1.5" },
  7871: { market: "Offsides Home", selection: "Under 1.5" },
  7872: { market: "Offsides Home", selection: "Over 2.5" },
  7873: { market: "Offsides Home", selection: "Under 2.5" },
  // Away team offsides
  7880: { market: "Offsides Away", selection: "Over 1.5" },
  7881: { market: "Offsides Away", selection: "Under 1.5" },
  7882: { market: "Offsides Away", selection: "Over 2.5" },
  7883: { market: "Offsides Away", selection: "Under 2.5" },
};

function map1xbetBetId(betId) {
  const mapped = ONEXBET_MARKETS[betId];
  if (mapped) {
    return mapped;
  }
  // Fallback: return raw betId for unmapped markets
  return { market: `Bet${betId}`, selection: "Unknown" };
}

// Sisal codiceScommessa mapping - H2H STATISTICAL MARKETS
// These are Head-to-Head markets (1=Home wins, X=Draw, 2=Away wins)
// Codes discovered from live OddsMarket feed
const SISAL_MARKETS = {
  // ===== H2H STATISTICAL MARKETS (discovered from live data) =====
  127: "Corners", // Chi vince corner (H2H)
  // More codes will be discovered from live feed logging

  // Legacy Over/Under codes (may not be available via OddsMarket)
  9942: "Corners",
  9943: "Corners Home",
  9944: "Corners Away",
  28319: "Shots On Target",
  28323: "Shots On Target Home",
  28324: "Shots On Target Away",
  28320: "Shots",
  28325: "Shots Home",
  28326: "Shots Away",
  28321: "Fouls",
  28327: "Fouls Home",
  28328: "Fouls Away",
  28322: "Offsides",
  28329: "Offsides Home",
  28330: "Offsides Away",
};

function mapSisalMarket(codice) {
  const mapped = SISAL_MARKETS[codice];
  if (mapped) {
    return mapped;
  }
  return `M${codice}`;
}

// Sisal codiceEsito mapping
const SISAL_SELECTIONS = {
  1: "1", // Home
  2: "2", // Away
  3: "X", // Draw
  4: "1X", // Home or Draw
  5: "12", // Home or Away
  6: "X2", // Draw or Away
  9: "Over", // Over
  14: "Under", // Under
  15: "Yes", // Yes (BTTS)
  18: "No", // No (BTTS)
  21: "Over 0.5",
};

function mapSisalSelection(codiceEsito) {
  const mapped = SISAL_SELECTIONS[codiceEsito];
  if (mapped) {
    return mapped;
  }
  return `E${codiceEsito}`;
}

// ============ MATCH KEY GENERATION ============
// Cross-bookmaker event matching using normalized team names + kickoff time
// Event IDs from OddsMarket are NOT shared between bookmakers!

// Cyrillic to Latin transliteration map
const CYRILLIC_MAP = {
  'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e',
  'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
  'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
  'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '',
  'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
};

function transliterateCyrillic(text) {
  return text.toLowerCase().split('').map(char => CYRILLIC_MAP[char] || char).join('');
}

// Team name normalization map for known variations
const TEAM_NAME_MAP = {
  // Italian teams (Russian -> Latin)
  'наполи': 'napoli', 'ювентус': 'juventus', 'интер': 'inter',
  'милан': 'milan', 'рома': 'roma', 'лацио': 'lazio',
  'аталанта': 'atalanta', 'фиорентина': 'fiorentina', 'торино': 'torino',
  'болонья': 'bologna', 'удинезе': 'udinese', 'сассуоло': 'sassuolo',
  'эмполи': 'empoli', 'салернитана': 'salernitana', 'лечче': 'lecce',
  'монца': 'monza', 'фрозиноне': 'frosinone', 'кальяри': 'cagliari',
  'дженоа': 'genoa', 'верона': 'verona', 'сампдория': 'sampdoria',
  
  // Spanish teams
  'барселона': 'barcelona', 'реал мадрид': 'real madrid', 'атлетико': 'atletico',
  'севилья': 'sevilla', 'валенсия': 'valencia', 'сельта': 'celta',
  'райо вальекано': 'rayo vallecano', 'бетис': 'betis', 'вильярреал': 'villarreal',
  'реал сосьедад': 'real sociedad', 'атлетик бильбао': 'athletic bilbao',
  'жирона': 'girona', 'осасуна': 'osasuna', 'алавес': 'alaves',
  
  // English teams
  'манчестер юнайтед': 'manchester united', 'манчестер сити': 'manchester city',
  'ливерпуль': 'liverpool', 'челси': 'chelsea', 'арсенал': 'arsenal',
  'тоттенхэм': 'tottenham', 'ньюкасл': 'newcastle', 'вест хэм': 'west ham',
  
  // German teams
  'бавария': 'bayern', 'боруссия дортмунд': 'borussia dortmund',
  'лейпциг': 'leipzig', 'байер': 'leverkusen',
};

// ============ TEAM NAME SEMANTIC NORMALIZATION ============
// Semantic alias map for common transliteration errors (Russian -> English)
const TEAM_ALIAS = {
  yorksiti: 'yorkcity',
  yorkciti: 'yorkcity',
  manchesteryunaited: 'manchesterunited',
  manchesteryunayted: 'manchesterunited',
  manchestersiti: 'manchestercity',
  bristolsiti: 'bristolcity',
  norwichsiti: 'norwichcity',
  leicestersiti: 'leicestercity',
  stouksiti: 'stokecity',
  hullsiti: 'hullcity',
  swansisiti: 'swanseacity',
  kardiffsiti: 'cardiffcity',
  birminghamsiti: 'birminghamcity',
  sautendyunaited: 'southendunited',
  sautendyunayted: 'southendunited',
  sheffildyunaited: 'sheffieldunited',
  sheffildyunayted: 'sheffieldunited',
  nyukaslyunaited: 'newcastleunited',
  nyukaslyunayted: 'newcastleunited',
  liidsyunaited: 'leedsunited',
  liidsyunayted: 'leedsunited',
  vestkhemyunaited: 'westhamunited',
  vestkhemyunayted: 'westhamunited',
};

function normalizeTeamName(name) {
  if (!name) return '';
  
  let normalized = name.toLowerCase().trim();
  
  // Step 1: Check for known team name mappings first
  for (const [russian, latin] of Object.entries(TEAM_NAME_MAP)) {
    if (normalized.includes(russian)) {
      normalized = normalized.replace(russian, latin);
    }
  }
  
  // Step 2: Transliterate any remaining Cyrillic
  normalized = transliterateCyrillic(normalized);
  
  // Step 3: Basic cleanup
  normalized = normalized
    .replace(/\b(fc|ac|ssc|as|ss|afc|sc|cf|cd|ud|rc|rcd|real|sporting|atletico|athletic)\b/gi, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
  
  // Step 4: Semantic suffix normalization
  normalized = normalized
    .replace(/siti$/, 'city')
    .replace(/citi$/, 'city')
    .replace(/yunaited$/, 'united')
    .replace(/yunayted$/, 'united');
  
  // Step 5: Check alias map for exact matches
  if (TEAM_ALIAS[normalized]) {
    normalized = TEAM_ALIAS[normalized];
  }
  
  return normalized;
}

function parseEventName(eventName) {
  if (!eventName) return null;
  
  // Common separators: " - ", " – ", " vs ", " v "
  const separators = [' - ', ' – ', ' — ', ' vs ', ' v '];
  
  for (const sep of separators) {
    if (eventName.includes(sep)) {
      const parts = eventName.split(sep);
      if (parts.length >= 2) {
        return {
          home: parts[0].trim(),
          away: parts[parts.length - 1].trim()
        };
      }
    }
  }
  
  return null;
}

// ============ TIMESTAMP HANDLING ============
// OddsMarket timestamps are NOT reliable kickoff times
// raw_start_time: stored as-is for matching (internal use only)
// kickoff_time: left null (not auto-derived to avoid wrong dates)

function roundRawStartTime(timestamp) {
  // This function rounds the raw OddsMarket timestamp for MATCHING purposes only
  // The result is NOT a real kickoff time and should NOT be displayed to users
  if (!timestamp) return 'unknown';
  
  const date = new Date(timestamp);
  if (isNaN(date.getTime())) return 'unknown';
  
  // Round to nearest 5 minutes for matching tolerance
  const minutes = date.getMinutes();
  const roundedMinutes = Math.round(minutes / 5) * 5;
  date.setMinutes(roundedMinutes);
  date.setSeconds(0);
  date.setMilliseconds(0);
  
  return date.toISOString();
}

function buildMatchKey(eventName, rawStartTime) {
  // match_key is built from normalized team names + rounded raw timestamp
  // This is for MATCHING only, NOT for display
  const parsed = parseEventName(eventName);
  if (!parsed) return null;
  
  const home = normalizeTeamName(parsed.home);
  const away = normalizeTeamName(parsed.away);
  const roundedTime = roundRawStartTime(rawStartTime);
  
  if (!home || !away) return null;
  
  return `${home}_${away}_${roundedTime}`;
}

// Generate display-friendly team name (for UI, not matching)
function buildDisplayName(eventName) {
  const parsed = parseEventName(eventName);
  if (!parsed) return eventName;
  
  // Just clean up the original name without aggressive normalization
  const cleanHome = parsed.home.trim();
  const cleanAway = parsed.away.trim();
  
  return `${cleanHome} - ${cleanAway}`;
}

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

let ws = null;
let reconnectAttempts = 0;
let fieldDefinitions = null; // Store field definitions from OddsMarket
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 5000;

function connect() {
  console.log("Connecting to OddsMarket WebSocket...", WS_URL);

  // IMPORTANT: Must enable permessage-deflate compression (required by OddsMarket)
  ws = new WebSocket(WS_URL, {
    perMessageDeflate: true,
  });

  ws.on("open", () => {
    console.log("Connected to OddsMarket WebSocket");
    reconnectAttempts = 0;

    // Step 1: Send authorization message (API key via JSON, not header)
    const authMessage = {
      cmd: "authorization",
      msg: ODDSMARKET_API_KEY,
    };

    ws.send(JSON.stringify(authMessage));
    console.log("Sent authorization message");
  });

  ws.on("message", async (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log("Received message type:", message.cmd);

      if (message.cmd === "authorized") {
        console.log("Authorization successful:", message.msg);

        // Step 2: Subscribe to odds feed after authorization
        const subscribeMessage = {
          cmd: "subscribe",
          msg: {
            bookmakerIds: BOOKMAKER_IDS,
            sportIds: [SPORT_ID],
          },
        };

        ws.send(JSON.stringify(subscribeMessage));
        console.log("Sent subscribe message:", subscribeMessage.msg);
      } else if (message.cmd === "subscribed") {
        console.log("Subscription successful");
      } else if (message.cmd === "fields") {
        // Store field definitions for parsing arrays
        console.log("Received field definitions:", JSON.stringify(message.msg).substring(0, 500));
        fieldDefinitions = message.msg;
      } else if (message.cmd === "outcomes") {
        // Process outcomes data (odds updates)
        await processOutcomes(message.msg);
      } else if (message.cmd === "bookmaker_events") {
        // Process bookmaker events data
        await processBookmakerEvents(message.msg);
      } else if (message.cmd === "data" || message.cmd === "update") {
        // Legacy format fallback
        await processOddsData(message.msg);
      } else if (message.cmd === "error") {
        console.error("OddsMarket error:", message.msg);
      } else if (message.cmd === "pong") {
        console.log("Pong received");
      }
    } catch (error) {
      console.error("Error processing message:", error);
    }
  });

  ws.on("error", (error) => {
    console.error("WebSocket error:", error.message);
  });

  ws.on("close", (code, reason) => {
    console.log(`WebSocket closed: ${code} - ${reason}`);
    attemptReconnect();
  });
}

function attemptReconnect() {
  if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    reconnectAttempts++;
    console.log(
      `Reconnecting in ${RECONNECT_DELAY / 1000}s... (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`,
    );
    setTimeout(connect, RECONNECT_DELAY);
  } else {
    console.error("Max reconnect attempts reached. Exiting.");
    process.exit(1);
  }
}

// Keep connection alive with ping
setInterval(() => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ cmd: "ping", msg: Date.now().toString() }));
  }
}, 30000);

// Store events temporarily by ID for reference
const eventsCache = new Map();

// Buffer for outcomes that arrived before their events
const pendingOutcomes = [];
const PENDING_BUFFER_MAX = 1000;
const PENDING_PROCESS_INTERVAL = 2000; // Process pending every 2 seconds

// ============ LEAGUE TRACKING FOR DEBUGGING ============
// Track events by bookmaker and league for diagnostics
const leagueStats = {
  "1xbet": {},
  "Sisal": {},
};
let lastLeagueLogTime = Date.now();
const LEAGUE_LOG_INTERVAL = 60000; // Log every 60 seconds

function trackEventByLeague(bookmakerId, league, eventName) {
  const bmName = bookmakerId === 21 ? "1xbet" : bookmakerId === 103 ? "Sisal" : "Other";
  if (!leagueStats[bmName]) return;
  
  const normalizedLeague = league || "Unknown";
  if (!leagueStats[bmName][normalizedLeague]) {
    leagueStats[bmName][normalizedLeague] = { count: 0, samples: [] };
  }
  leagueStats[bmName][normalizedLeague].count++;
  
  // Keep max 3 sample event names per league
  if (leagueStats[bmName][normalizedLeague].samples.length < 3) {
    leagueStats[bmName][normalizedLeague].samples.push(eventName);
  }
}

function logLeagueStats() {
  const now = Date.now();
  if (now - lastLeagueLogTime < LEAGUE_LOG_INTERVAL) return;
  lastLeagueLogTime = now;
  
  console.log("\n========== LEAGUE DISTRIBUTION REPORT ==========");
  
  for (const [bookmaker, leagues] of Object.entries(leagueStats)) {
    const sortedLeagues = Object.entries(leagues)
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 15); // Top 15 leagues
    
    console.log(`\n📚 ${bookmaker} - ${sortedLeagues.length} leagues:`);
    
    for (const [league, data] of sortedLeagues) {
      console.log(`   ${data.count.toString().padStart(4)} events | ${league}`);
      if (data.samples.length > 0) {
        console.log(`         Sample: ${data.samples[0]}`);
      }
    }
  }
  
  // Check for common leagues between bookmakers
  const onexbetLeagues = new Set(Object.keys(leagueStats["1xbet"]));
  const sisalLeagues = new Set(Object.keys(leagueStats["Sisal"]));
  const commonLeagues = [...onexbetLeagues].filter(l => sisalLeagues.has(l));
  
  console.log(`\n🔗 Common leagues: ${commonLeagues.length}`);
  if (commonLeagues.length > 0) {
    commonLeagues.slice(0, 10).forEach(l => {
      const count1x = leagueStats["1xbet"][l]?.count || 0;
      const countSisal = leagueStats["Sisal"][l]?.count || 0;
      console.log(`   ${l}: 1xbet=${count1x}, Sisal=${countSisal}`);
    });
  }
  
  console.log("=================================================\n");
}

// OddsMarket uses compact array format. Based on observed data:
// bookmaker_events array: [eventId, bookmakerId, active, timestamp, eventName, sportId, league, ...]
// outcomes array: strings like "MTg5OTMxMjIwNnw0MDQs..." which are base64 encoded

async function processOutcomes(data) {
  if (!data) return;

  console.log("Processing outcomes, count:", Array.isArray(data) ? data.length : 1);

  const outcomes = Array.isArray(data) ? data : [data];
  const oddsRecords = [];

  // DEBUG: Log first outcome structure
  if (outcomes.length > 0) {
    const first = outcomes[0];
    console.log("First outcome type:", typeof first);
    if (typeof first === "object" && first !== null && !Array.isArray(first)) {
      console.log("First outcome keys:", Object.keys(first).join(", "));
      console.log("First outcome data:", JSON.stringify(first).substring(0, 500));
    } else if (Array.isArray(first)) {
      console.log("First outcome is array, length:", first.length);
      console.log("First outcome array:", JSON.stringify(first).substring(0, 500));
      // Log key indices for debugging bookmaker extraction
      console.log(
        "Index 1 (bookmakerId?):",
        first[1],
        "Index 11 (odds?):",
        first[11],
        "Index 15 (info?):",
        typeof first[15] === "string" ? first[15].substring(0, 100) : first[15],
      );
    } else if (typeof first === "string") {
      console.log("First outcome (raw):", first.substring(0, 100));
    }
  }

  for (const outcome of outcomes) {
    if (!outcome) continue;

    // Handle STRING format (base64 encoded or pipe-delimited)
    if (typeof outcome === "string") {
      let decoded = outcome;

      // Try base64 decode if it looks like base64
      if (/^[A-Za-z0-9+/=]+$/.test(outcome) && outcome.length > 20) {
        try {
          decoded = Buffer.from(outcome, "base64").toString("utf8");
        } catch (e) {
          // Not base64, use as-is
        }
      }

      // Now parse the decoded/raw string (pipe-delimited format)
      if (decoded.includes("|")) {
        const parts = decoded.split("|");

        // OddsMarket format appears to be:
        // eventId|bookmakerId|marketId|outcomeId|odds|...
        // or similar positional format

        let eventId = null;
        let bookmakerId = null;
        let odds = null;
        let selection = "Unknown";

        // Try to extract from parts
        for (let i = 0; i < parts.length; i++) {
          const part = parts[i];
          const num = parseFloat(part);

          // Large number could be eventId
          if (!eventId && !isNaN(num) && num > 1000000) {
            eventId = String(Math.floor(num));
          }
          // Small number 1-200 could be bookmakerId
          else if (!bookmakerId && !isNaN(num) && num >= 1 && num <= 200 && Number.isInteger(num)) {
            bookmakerId = num;
          }
          // Decimal between 1-100 could be odds
          else if (!odds && !isNaN(num) && num > 1 && num < 100 && part.includes(".")) {
            odds = num;
          }
        }

        if (eventId && odds) {
          const eventInfo = eventsCache.get(eventId) || {};
          oddsRecords.push({
            event_id: eventId,
            event_name: eventInfo.name || `Event ${eventId}`,
            event_time: eventInfo.startsAt || null,
            league: eventInfo.league || "Soccer",
            sport_id: SPORT_ID,
            bookmaker_id: bookmakerId || 21,
            bookmaker_name: getBookmakerName(bookmakerId || 21),
            market_type: "Match Winner",
            selection: selection,
            odds: odds,
            updated_at: new Date().toISOString(),
          });
        }
      }
    }
    // Handle OBJECT format
    else if (typeof outcome === "object" && !Array.isArray(outcome)) {
      const eventId = outcome.event_id || outcome.eventId || outcome.eid;
      const bookmakerId = outcome.bookmaker_id || outcome.bookmakerId || outcome.bid;
      const odds = outcome.odds || outcome.price || outcome.value;
      const marketType = outcome.market || outcome.marketName || "Match Winner";
      const selection = outcome.outcome || outcome.outcomeName || outcome.sel || "Unknown";

      if (eventId && bookmakerId && odds) {
        const eventInfo = eventsCache.get(String(eventId)) || {};
        oddsRecords.push({
          event_id: String(eventId),
          event_name: eventInfo.name || `Event ${eventId}`,
          event_time: eventInfo.startsAt || null,
          league: eventInfo.league || "Soccer",
          sport_id: SPORT_ID,
          bookmaker_id: Number(bookmakerId),
          bookmaker_name: getBookmakerName(Number(bookmakerId)),
          market_type: String(marketType),
          selection: String(selection),
          odds: parseFloat(odds),
          updated_at: new Date().toISOString(),
        });
      }
    }
    // Handle ARRAY format - OddsMarket uses 18-element arrays
    // Format: [encodedId, internalEventId, ?, period, marketTypeId, ?, ?, null, ?, null, bool, ODDS, ?, bool, ?, "eventId=X&...", timestamp, null]
    // IMPORTANT: internalEventId at index 1 matches what we get from bookmaker_events!
    else if (Array.isArray(outcome) && outcome.length >= 12) {
      const internalEventId = outcome[1]; // This matches bookmaker_events[0]!
      const odds = outcome[11]; // Odds at index 11
      const infoString = outcome[15]; // Contains external eventId and market details
      const period = outcome[3] || "Regular time";

      // Get bookmaker from events cache using INTERNAL event ID
      const eventInfo = eventsCache.get(String(internalEventId)) || {};
      let bookmakerId = eventInfo.bookmakerId;
      let eventName = eventInfo.name;

      // CRITICAL FIX: If no bookmakerId from cache, detect from info string
      // Sisal uses "codiceScommessa", 1xbet uses "betId"
      if (!bookmakerId && typeof infoString === "string") {
        if (infoString.includes("codiceScommessa=")) {
          bookmakerId = 103; // Sisal
        } else if (infoString.includes("betId=")) {
          bookmakerId = 21; // 1xbet
        }
      }

      // If still no bookmakerId, buffer for later
      if (!bookmakerId) {
        if (pendingOutcomes.length < PENDING_BUFFER_MAX) {
          pendingOutcomes.push(outcome);
        }
        continue;
      }

      // Extract external eventId for storage (for matching across bookmakers)
      let externalEventId = null;
      let marketType = period;
      let selection = "Unknown";

      if (typeof infoString === "string") {
        // Parse external eventId (used by the bookmaker)
        const eventMatch = infoString.match(/eventId=(\d+)/);
        if (eventMatch) {
          externalEventId = eventMatch[1];
        }

        // --- BOOKMAKER-SPECIFIC PARSING ---
        if (bookmakerId === 21) {
          // 1xbet: uses betId for market type + betValue for dynamic line
          const betMatch = infoString.match(/betId=(\d+)/);
          const betValueMatch = infoString.match(/betValue=([0-9]+\.?[0-9]*)/);

          if (betMatch) {
            const betId = parseInt(betMatch[1]);
            const mapped = map1xbetBetId(betId);
            const statGroup = get1xbetStatGroup(betId);
            
            // CRITICAL: For 1xbet, use betId-based classification instead of market name
            // If betId is in ONEXBET_STAT_GROUPS, it's a statistical market
            if (!statGroup && mapped.market.startsWith("Bet")) {
              // Not a statistical betId and not mapped - skip
              continue;
            }
            
            // Use mapped market if available, otherwise use raw betId marker
            if (mapped.market.startsWith("Bet")) {
              // Not mapped but is statistical - save with betId identifier
              marketType = `1xbet_stat_${betId}`;
            } else {
              marketType = mapped.market;
            }

            // Log ALL 1xbet betIds to discover H2H markets
            if (!global.onexbetLog) global.onexbetLog = new Map();
            if (!global.onexbetLog.has(betId)) {
              const betValueStr = betValueMatch ? betValueMatch[1] : "none";
              const isKnown = !mapped.market.startsWith("Bet");
              const isStatBetId = !!statGroup;
              console.log(
                `🔍 1XBET betId=${betId} betValue=${betValueStr} mapped=${mapped.market}|${mapped.selection} known=${isKnown} statGroup=${statGroup || "none"} event="${eventName || "unknown"}"`,
              );
              global.onexbetLog.set(betId, { betValue: betValueStr, market: mapped.market, statGroup, count: 1 });
            } else {
              // Increment count for known betIds
              const entry = global.onexbetLog.get(betId);
              entry.count++;
              global.onexbetLog.set(betId, entry);
            }

            // Log periodic summary of discovered betIds (every 500 outcomes)
            if (!global.onexbetLogCounter) global.onexbetLogCounter = 0;
            global.onexbetLogCounter++;
            if (global.onexbetLogCounter % 500 === 0) {
              const unmapped = [...global.onexbetLog.entries()].filter(([id, data]) => data.market.startsWith("Bet") || data.market.startsWith("1xbet_stat"));
              const saved = [...global.onexbetLog.entries()].filter(([id, data]) => data.statGroup);
              console.log(`📊 1XBET STATS: ${saved.length} statistical betIds saved, ${unmapped.length} raw betIds:`);
              unmapped.slice(0, 10).forEach(([id, data]) => {
                console.log(`   betId=${id} betValue=${data.betValue} statGroup=${data.statGroup || "none"} count=${data.count}`);
              });
            }

            // Construct selection based on statistical group
            if (statGroup === 'H2H_STATS') {
              // H2H markets - betValue indicates selection (1=Home, 2=Away, 0/3=Draw)
              if (betValueMatch) {
                const betValue = parseFloat(betValueMatch[1]);
                if (betValue === 1) {
                  selection = "1"; // Home wins
                } else if (betValue === 2) {
                  selection = "2"; // Away wins
                } else if (betValue === 0 || betValue === 3) {
                  selection = "X"; // Draw (both 0 and 3 map to X for Sisal compatibility)
                } else {
                  selection = String(betValue); // Fallback
                }
              } else if (mapped.isH2H) {
                // Use mapped H2H logic as fallback
                selection = mapped.selection;
              } else {
                selection = "UNK";
              }
            } else if (statGroup === 'STATS_OU') {
              // Over/Under markets - betValue is the line
              if (betValueMatch && parseFloat(betValueMatch[1]) > 0) {
                const betValue = parseFloat(betValueMatch[1]);
                selection = `line ${betValue}`;
              } else {
                selection = "UNK";
              }
            } else if (mapped.isH2H && betValueMatch) {
              // Mapped H2H (like Corners H2H)
              const betValue = parseFloat(betValueMatch[1]);
              if (betValue === 1) {
                selection = "1";
              } else if (betValue === 2) {
                selection = "2";
              } else if (betValue === 0 || betValue === 3) {
                selection = "X";
              } else {
                selection = String(betValue);
              }
            } else if (betValueMatch && parseFloat(betValueMatch[1]) > 0) {
              // Mapped Over/Under
              const betValue = parseFloat(betValueMatch[1]);
              if (mapped.selection === "Over" || mapped.selection === "Under") {
                selection = `${mapped.selection} ${betValue}`;
              } else {
                selection = mapped.selection;
              }
            } else {
              selection = mapped.selection;
            }
          } else {
            // No betId found - skip for 1xbet
            continue;
          }
        } else if (bookmakerId === 103) {
          // Sisal: uses codiceScommessa + codiceEsito
          const marketMatch = infoString.match(/codiceScommessa=(\d+)/);
          const esitoMatch = infoString.match(/codiceEsito=(\d+)/);

          // Try to extract line/handicap value from infoString - look for common patterns
          // Sisal may use: punti, quota, linea, handicap, spread, over, under followed by number
          const lineMatch = infoString.match(
            /(?:punti|quota|linea|handicap|spread|over|under|valore|riferimento)=?[\s:]*([0-9]+\.?[0-9]*)/i,
          );

          if (marketMatch) {
            const codice = parseInt(marketMatch[1]);
            marketType = mapSisalMarket(codice);

            // Log ALL unique Sisal market + selection combinations to discover available markets
            if (!global.sisalMarketLog) global.sisalMarketLog = new Set();
            const esitoCode = esitoMatch ? esitoMatch[1] : "none";
            const logKey = `${codice}:${esitoCode}`;
            if (!global.sisalMarketLog.has(logKey) && global.sisalMarketLog.size < 100) {
              console.log(
                `📊 Sisal market: codiceScommessa=${codice}, codiceEsito=${esitoCode}, eventName=${eventName || "unknown"}`,
              );
              global.sisalMarketLog.add(logKey);
            }
          }
          if (esitoMatch) {
            let baseSel = mapSisalSelection(esitoMatch[1]);

            // If we found a line and selection is Over/Under, append the line
            if (lineMatch && (baseSel === "Over" || baseSel === "Under")) {
              const lineValue = parseFloat(lineMatch[1]);
              const normalizedLine = lineValue > 50 ? lineValue / 10 : lineValue;
              selection = `${baseSel} ${normalizedLine}`;
            } else {
              selection = baseSel;
            }
          }
          
          // For Sisal, still use isStatisticalMarket filter
          if (!isStatisticalMarket(marketType)) {
            continue;
          }
        }
      }

      // CRITICAL: Use ONLY internalEventId (outcome[1]) for storage
      // This is the OddsMarket internal ID that matches across ALL bookmakers
      // The externalEventId is bookmaker-specific and MUST NOT be used for matching
      const eventIdForStorage = String(internalEventId);

      // Only process valid bookmakers
      const validBookmakers = [21, 103];

      if (bookmakerId && validBookmakers.includes(bookmakerId) && typeof odds === "number" && odds > 1 && odds < 1000) {
        // Use eventName from cache if available, otherwise use Event ID as fallback
        const finalEventName = eventName || `Event ${eventIdForStorage}`;
        
        // Generate match_key for cross-bookmaker matching
        const matchKey = buildMatchKey(finalEventName, eventInfo.startsAt);
        
        // Generate display name (cleaned, for UI)
        const displayName = buildDisplayName(finalEventName);
        
        // Store raw start time as-is (for matching reference, not real kickoff)
        const rawStartTime = eventInfo.startsAt || null;

        oddsRecords.push({
          event_id: eventIdForStorage,
          event_name: finalEventName,
          event_time: eventInfo.startsAt || null,
          league: eventInfo.league || "Soccer",
          sport_id: SPORT_ID,
          bookmaker_id: bookmakerId,
          bookmaker_name: getBookmakerName(bookmakerId),
          market_type: String(marketType),
          selection: selection,
          odds: parseFloat(odds),
          updated_at: new Date().toISOString(),
          match_key: matchKey,
          // NEW: Separate fields for timestamp handling
          raw_start_time: rawStartTime, // Original OddsMarket value (for matching only)
          display_name: displayName,    // Clean name for UI display
          kickoff_time: null,           // Left null - not auto-derived to avoid wrong dates
        });
      }
    }
  }

  if (oddsRecords.length > 0) {
    console.log(`✅ Parsed ${oddsRecords.length} outcome records, saving...`);
    await saveOddsRecords(oddsRecords);
  }

  // Log pending buffer status
  if (pendingOutcomes.length > 0) {
    console.log(`📦 Buffered ${pendingOutcomes.length} outcomes waiting for event cache`);
  }
}

async function processBookmakerEvents(data) {
  if (!data) return;

  console.log("Processing bookmaker_events, count:", Array.isArray(data) ? data.length : 1);

  const events = Array.isArray(data) ? data : [data];
  let added = 0;
  const bookmakerCounts = {};

  for (const event of events) {
    if (!event) continue;

    let eventId, eventName, startsAt, league, bookmakerId;

    // If it's an array (OddsMarket compact format)
    if (Array.isArray(event)) {
      // Based on log: [1899312206,21,true,411201800,"Понте-Прета - Вело Клуб СП",...]
      // Format: [eventId, bookmakerId, active, timestamp, eventName, sportId?, league?, ...]
      eventId = event[0];
      bookmakerId = event[1];
      // event[2] is boolean (active)
      // event[3] is timestamp
      eventName = event[4] || `Event ${eventId}`;
      // Try to find league in remaining elements
      league = event[6] || event[7] || "Soccer";
      startsAt = event[3] ? new Date(event[3] * 1000).toISOString() : null;
    }
    // If it's an object
    else if (typeof event === "object") {
      eventId = event.id || event.eventId || event.eid;
      eventName = event.name || event.eventName || `${event.home || "Home"} vs ${event.away || "Away"}`;
      startsAt = event.starts_at || event.startsAt || event.start_time;
      league = event.league || event.leagueName || event.tournament || "Soccer";
      bookmakerId = event.bookmaker_id || event.bookmakerId;
    }

    if (eventId && bookmakerId) {
      eventsCache.set(String(eventId), {
        name: eventName,
        startsAt: startsAt,
        league: league,
        bookmakerId: bookmakerId,
      });
      added++;

      // Track bookmaker distribution
      const bmName = getBookmakerName(bookmakerId);
      bookmakerCounts[bmName] = (bookmakerCounts[bmName] || 0) + 1;
      
      // Track league distribution for debugging
      trackEventByLeague(bookmakerId, league, eventName);
    }
  }

  // Log which bookmakers we're receiving events from
  const bmSummary = Object.entries(bookmakerCounts)
    .map(([k, v]) => `${k}:${v}`)
    .join(", ");
  console.log(
    `✅ Event cache updated: +${added} events, total: ${eventsCache.size} | Bookmakers: ${bmSummary || "none"}`,
  );
  
  // Periodically log league stats
  logLeagueStats();

  // Keep cache clean (max 5000 events)
  if (eventsCache.size > 5000) {
    const keysToDelete = [...eventsCache.keys()].slice(0, 1000);
    keysToDelete.forEach((k) => eventsCache.delete(k));
    console.log("🧹 Cleaned event cache, new size:", eventsCache.size);
  }

  // Process any pending outcomes now that we have more events
  if (pendingOutcomes.length > 0 && added > 0) {
    await processPendingOutcomes();
  }
}

// Process buffered outcomes that were waiting for event cache
async function processPendingOutcomes() {
  if (pendingOutcomes.length === 0) return;

  console.log(`🔄 Processing ${pendingOutcomes.length} pending outcomes...`);

  const toProcess = pendingOutcomes.splice(0, pendingOutcomes.length);
  const oddsRecords = [];
  let stillPending = 0;

  for (const outcome of toProcess) {
    if (!Array.isArray(outcome) || outcome.length < 12) continue;

    const internalEventId = outcome[1];
    const odds = outcome[11];
    const infoString = outcome[15];
    const period = outcome[3] || "Regular time";

    const eventInfo = eventsCache.get(String(internalEventId)) || {};
    let bookmakerId = eventInfo.bookmakerId;
    let eventName = eventInfo.name;

    // Detect bookmaker from info string if not in cache
    if (!bookmakerId && typeof infoString === "string") {
      if (infoString.includes("codiceScommessa=")) {
        bookmakerId = 103; // Sisal
      } else if (infoString.includes("betId=")) {
        bookmakerId = 21; // 1xbet
      }
    }

    // Still no bookmakerId? Re-buffer (up to limit)
    if (!bookmakerId) {
      if (stillPending < 100) {
        pendingOutcomes.push(outcome);
        stillPending++;
      }
      continue;
    }

    let externalEventId = null;
    let marketType = period;
    let selection = "Unknown";

    if (typeof infoString === "string") {
      const eventMatch = infoString.match(/eventId=(\d+)/);
      if (eventMatch) externalEventId = eventMatch[1];

      if (bookmakerId === 21) {
        const betMatch = infoString.match(/betId=(\d+)/);
        const betValueMatch = infoString.match(/betValue=([0-9]+\\.?[0-9]*)/);
        if (betMatch) {
          const mapped = map1xbetBetId(parseInt(betMatch[1]));
          marketType = mapped.market;
          // Use betValue as dynamic line if available
          // 🔧 FIX H2H 1xbet (pending outcomes)
          if (mapped.isH2H && betValueMatch) {
            const betValue = parseFloat(betValueMatch[1]);
            if (betValue === 1) selection = "1";
            else if (betValue === 2) selection = "2";
            else if (betValue === 0) selection = "X";
          }

          if (betValueMatch && betValueMatch[1] && parseFloat(betValueMatch[1]) > 0) {
            const betValue = parseFloat(betValueMatch[1]);
            if (mapped.selection === "Over" || mapped.selection === "Under") {
              selection = `${mapped.selection} ${betValue}`;
            } else {
              selection = mapped.selection;
            }
          } else {
            selection = mapped.selection;
          }
        }
      } else if (bookmakerId === 103) {
        // Sisal: uses codiceScommessa + codiceEsito
        const marketMatch = infoString.match(/codiceScommessa=(\d+)/);
        const esitoMatch = infoString.match(/codiceEsito=(\d+)/);
        const lineMatch = infoString.match(
          /(?:handicap|line|spread|punti|quota_riferimento|valore)=([0-9]+\.?[0-9]*)/i,
        );

        if (marketMatch) marketType = mapSisalMarket(parseInt(marketMatch[1]));
        if (esitoMatch) {
          let baseSel = mapSisalSelection(esitoMatch[1]);
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

    const eventIdForStorage = externalEventId || String(internalEventId);
    const validBookmakers = [21, 103];

    if (!isStatisticalMarket(marketType)) continue;

    if (bookmakerId && validBookmakers.includes(bookmakerId) && typeof odds === "number" && odds > 1 && odds < 1000) {
      const finalEventName = eventInfo.name || `Event ${eventIdForStorage}`;
      const matchKey = buildMatchKey(finalEventName, eventInfo.startsAt);
      const displayName = buildDisplayName(finalEventName);
      
      oddsRecords.push({
        event_id: eventIdForStorage,
        event_name: finalEventName,
        event_time: eventInfo.startsAt || null,
        league: eventInfo.league || "Soccer",
        sport_id: SPORT_ID,
        bookmaker_id: bookmakerId,
        bookmaker_name: getBookmakerName(bookmakerId),
        market_type: String(marketType),
        selection: selection,
        odds: parseFloat(odds),
        updated_at: new Date().toISOString(),
        match_key: matchKey,
        raw_start_time: eventInfo.startsAt || null,
        display_name: displayName,
        kickoff_time: null,
      });
    }
  }

  if (oddsRecords.length > 0) {
    console.log(`✅ Recovered ${oddsRecords.length} records from pending buffer`);
    await saveOddsRecords(oddsRecords);
  }

  if (stillPending > 0) {
    console.log(`📦 Still ${stillPending} outcomes pending`);
  }
}

async function processOddsData(data) {
  if (!data) return;

  const oddsRecords = [];
  console.log("Legacy data format:", JSON.stringify(data).substring(0, 500));

  const events = Array.isArray(data) ? data : data.events || [data];

  for (const event of events) {
    if (!event) continue;

    const eventId = event.id || event.eventId || `${event.home}_${event.away}`;
    const eventName = event.name || `${event.home || "Team1"} vs ${event.away || "Team2"}`;
    const eventTime = event.startsAt || event.starts_at || event.startTime || null;
    const league = event.league || event.leagueName || event.tournament || "Unknown";

    const markets = event.markets || event.odds || [];

    for (const market of markets) {
      const marketType = market.name || market.marketName || market.type || "Unknown";
      const outcomes = market.outcomes || market.odds || [];

      for (const outcome of outcomes) {
        const bookmakerId = outcome.bookmakerId || outcome.bookmaker_id || event.bookmakerId;
        const odds = outcome.odds || outcome.price || outcome.value;
        const selection = outcome.name || outcome.outcomeName || outcome.selection || "Unknown";

        if (bookmakerId && odds) {
          oddsRecords.push({
            event_id: String(eventId),
            event_name: eventName,
            event_time: eventTime,
            league: league,
            sport_id: SPORT_ID,
            bookmaker_id: Number(bookmakerId),
            bookmaker_name: getBookmakerName(Number(bookmakerId)),
            market_type: String(marketType),
            selection: String(selection),
            odds: parseFloat(odds),
            updated_at: new Date().toISOString(),
          });
        }
      }
    }
  }

  if (oddsRecords.length > 0) {
    await saveOddsRecords(oddsRecords);
  }
}

// ============ GLOBAL WRITE QUEUE ============
// CRITICAL: All writes go through this queue to prevent socket saturation
// Only ONE upsert at a time, with mandatory delay between writes

const writeQueue = [];
let isProcessingQueue = false;

const BATCH_SIZE = 25; // Maximum records per upsert
const WRITE_DELAY_MS = 400; // Mandatory delay between writes (300-500ms range)
const MAX_RETRIES = 2; // Maximum retry attempts per batch
const RETRY_DELAYS = [300, 600]; // Backoff delays for retries (ms)

// Helper function to sleep
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Add records to the global write queue
function enqueueWrite(records) {
  if (!records || records.length === 0) return;
  writeQueue.push(...records);
  console.log(`📥 Enqueued ${records.length} records, queue size: ${writeQueue.length}`);
  
  // Trigger processing if not already running
  if (!isProcessingQueue) {
    processWriteQueue();
  }
}

// Process the write queue - ONE batch at a time
async function processWriteQueue() {
  if (isProcessingQueue) return;
  isProcessingQueue = true;
  
  console.log(`🔄 Write queue processor started, ${writeQueue.length} records pending`);
  
  while (writeQueue.length > 0) {
    // Take a batch from the queue
    const batch = writeQueue.splice(0, BATCH_SIZE);
    
    // Execute single upsert with retry
    await executeSingleUpsert(batch);
    
    // MANDATORY delay before next write
    if (writeQueue.length > 0) {
      await sleep(WRITE_DELAY_MS);
    }
  }
  
  isProcessingQueue = false;
  console.log(`✅ Write queue processor finished`);
}

// Execute a single upsert with retry logic
async function executeSingleUpsert(batch) {
  const batchStart = Date.now();
  
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const { error, data } = await supabase.from("live_odds").upsert(batch, {
        onConflict: "event_id,bookmaker_id,market_type,selection",
        ignoreDuplicates: false,
      });

      if (error) {
        // Check for Cloudflare HTML error
        const errorStr = error?.message || String(error);
        if (isCloudflareError(errorStr)) {
          console.warn(`⚠️ Cloudflare 522 error (batch of ${batch.length}), retrying...`);
          throw new Error('CLOUDFLARE_522');
        }
        throw error;
      }
      
      const duration = Date.now() - batchStart;
      console.log(`✅ Upsert: ${batch.length} records in ${duration}ms`);
      return true;
      
    } catch (error) {
      const errorMsg = error?.message || String(error);
      
      // Handle Cloudflare HTML responses
      if (isCloudflareError(errorMsg)) {
        console.warn(`⚠️ Cloudflare error detected, attempt ${attempt + 1}/${MAX_RETRIES + 1}`);
      }
      
      const isRetryable = errorMsg.includes('fetch failed') || 
                         errorMsg.includes('socket') || 
                         errorMsg.includes('ECONNRESET') ||
                         errorMsg.includes('timeout') ||
                         errorMsg.includes('CLOUDFLARE') ||
                         errorMsg.includes('522');
      
      if (attempt < MAX_RETRIES && isRetryable) {
        const delay = RETRY_DELAYS[attempt] || 600;
        console.log(`⚠️ Upsert attempt ${attempt + 1} failed, retrying in ${delay}ms`);
        await sleep(delay);
      } else {
        // Log concise error, not full HTML
        const shortError = errorMsg.substring(0, 150);
        console.error(`❌ Upsert failed after ${attempt + 1} attempts: ${shortError}`);
        return false;
      }
    }
  }
  
  return false;
}

// Detect Cloudflare HTML error responses
function isCloudflareError(text) {
  if (!text) return false;
  const str = String(text).toLowerCase();
  return str.includes('cloudflare') || 
         str.includes('<!doctype') || 
         str.includes('<html') ||
         str.includes('522') ||
         str.includes('error 522');
}

// ============ CHANGE DETECTION ============
// Track recent upserts to avoid redundant writes
const recentUpserts = new Map();
const UPSERT_CACHE_TTL = 60000; // 60 seconds (increased for better dedup)

function getRecordKey(record) {
  return `${record.event_id}|${record.bookmaker_id}|${record.market_type}|${record.selection}`;
}

function getRecordHash(record) {
  // Include odds in hash to detect value changes
  return `${record.odds}`;
}

function hasRecordChanged(record) {
  const key = getRecordKey(record);
  const hash = getRecordHash(record);
  
  const cached = recentUpserts.get(key);
  if (cached) {
    // Record exists in cache - check if odds changed
    if (cached.hash === hash && Date.now() - cached.time < UPSERT_CACHE_TTL) {
      return false; // Odds unchanged, skip write
    }
  }
  
  // Record changed or new - update cache
  recentUpserts.set(key, { hash, time: Date.now() });
  return true;
}

// Periodically clean upsert cache
setInterval(() => {
  const now = Date.now();
  let cleaned = 0;
  for (const [key, value] of recentUpserts.entries()) {
    if (now - value.time > UPSERT_CACHE_TTL * 2) {
      recentUpserts.delete(key);
      cleaned++;
    }
  }
  if (cleaned > 0) {
    console.log(`🧹 Cleaned ${cleaned} stale cache entries, remaining: ${recentUpserts.size}`);
  }
}, 60000);

// ============ OPTIMIZED SAVE FUNCTION ============
async function saveOddsRecords(oddsRecords) {
  const startTime = Date.now();
  
  // Step 1: Deduplicate records by unique key
  const uniqueMap = new Map();
  for (const record of oddsRecords) {
    const key = getRecordKey(record);
    uniqueMap.set(key, record);
  }

  const uniqueRecords = Array.from(uniqueMap.values());
  
  // Step 2: Filter out unchanged records (MANDATORY - avoid redundant writes)
  const changedRecords = uniqueRecords.filter(hasRecordChanged);
  
  const skippedCount = uniqueRecords.length - changedRecords.length;
  
  if (changedRecords.length === 0) {
    if (skippedCount > 0) {
      console.log(`⏭️ All ${skippedCount} records unchanged, no writes needed`);
    }
    return;
  }
  
  console.log(`📊 ${changedRecords.length} changed records (${skippedCount} unchanged skipped)`);
  
  // Step 3: Enqueue to global write queue (serialized processing)
  enqueueWrite(changedRecords);
  
  const duration = Date.now() - startTime;
  console.log(`📥 Enqueue complete in ${duration}ms`);
  
  // Log match_key stats for cross-bookmaker matching verification
  logMatchKeyStats(changedRecords);
}

// ============ MATCH KEY LOGGING ============
function logMatchKeyStats(records) {
  const matchKeyGroups = new Map();
  
  for (const record of records) {
    if (record.match_key) {
      if (!matchKeyGroups.has(record.match_key)) {
        matchKeyGroups.set(record.match_key, { bookmakers: new Set(), eventName: record.event_name });
      }
      matchKeyGroups.get(record.match_key).bookmakers.add(record.bookmaker_name);
    }
  }
  
  // Find cross-bookmaker matches
  const crossBookmakerMatches = [...matchKeyGroups.entries()]
    .filter(([_, data]) => data.bookmakers.size > 1);
  
  if (crossBookmakerMatches.length > 0) {
    console.log(`\n🔗 CROSS-BOOKMAKER MATCHES FOUND: ${crossBookmakerMatches.length}`);
    crossBookmakerMatches.slice(0, 5).forEach(([matchKey, data]) => {
      console.log(`   MATCH_KEY: ${matchKey}`);
      console.log(`   Event: ${data.eventName}`);
      console.log(`   Bookmakers: ${[...data.bookmakers].join(', ')}`);
    });
  }
  
  // Log new match_key creations
  const matchKeysBy1xbet = records.filter(r => r.bookmaker_id === 21 && r.match_key).length;
  const matchKeysBySisal = records.filter(r => r.bookmaker_id === 103 && r.match_key).length;
  
  if (matchKeysBy1xbet > 0 || matchKeysBySisal > 0) {
    console.log(`📍 MATCH_KEY CREATED: 1xbet=${matchKeysBy1xbet}, Sisal=${matchKeysBySisal}`);
  }
}

function getBookmakerName(id) {
  const names = {
    1: "Pinnacle",
    21: "1xbet",
    103: "Sisal",
  };
  return names[id] || `Bookmaker ${id}`;
}

// Health check server for Railway
import { createServer } from "http";
const PORT = process.env.PORT || 3000;

createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(
    JSON.stringify({
      status: "running",
      connected: ws?.readyState === WebSocket.OPEN,
      reconnectAttempts,
      wsUrl: WS_URL,
    }),
  );
}).listen(PORT, () => {
  console.log(`Health check server running on port ${PORT}`);
});

// Log write queue stats periodically
setInterval(() => {
  if (writeQueue.length > 0 || isProcessingQueue) {
    console.log(`📊 Write queue: ${writeQueue.length} pending, processing: ${isProcessingQueue}`);
  }
}, 30000);

// Start connection
console.log("Starting OddsMarket Worker...");
console.log("API Key:", ODDSMARKET_API_KEY ? "Configured" : "MISSING");
console.log("Supabase URL:", SUPABASE_URL ? "Configured" : "MISSING");
console.log("Bookmakers:", BOOKMAKER_IDS.map(getBookmakerName).join(", "));
console.log("Write config: batch=" + BATCH_SIZE + ", delay=" + WRITE_DELAY_MS + "ms, retries=" + MAX_RETRIES);
connect();

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("Received SIGTERM, closing connection...");
  console.log(`Write queue has ${writeQueue.length} pending records`);
  if (ws) ws.close();
  process.exit(0);
});
