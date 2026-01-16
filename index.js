import WebSocket from 'ws';
import { createClient } from '@supabase/supabase-js';

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

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

let ws = null;
let reconnectAttempts = 0;
let fieldDefinitions = null; // Store field definitions from OddsMarket
const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_DELAY = 5000;

function connect() {
  console.log('Connecting to OddsMarket WebSocket...', WS_URL);
  
  // IMPORTANT: Must enable permessage-deflate compression (required by OddsMarket)
  ws = new WebSocket(WS_URL, {
    perMessageDeflate: true
  });

  ws.on('open', () => {
    console.log('Connected to OddsMarket WebSocket');
    reconnectAttempts = 0;
    
    // Step 1: Send authorization message (API key via JSON, not header)
    const authMessage = {
      cmd: 'authorization',
      msg: ODDSMARKET_API_KEY
    };
    
    ws.send(JSON.stringify(authMessage));
    console.log('Sent authorization message');
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log('Received message type:', message.cmd);
      
      if (message.cmd === 'authorized') {
        console.log('Authorization successful:', message.msg);
        
        // Step 2: Subscribe to odds feed after authorization
        const subscribeMessage = {
          cmd: 'subscribe',
          msg: {
            bookmakerIds: BOOKMAKER_IDS,
            sportIds: [SPORT_ID]
          }
        };
        
        ws.send(JSON.stringify(subscribeMessage));
        console.log('Sent subscribe message:', subscribeMessage.msg);
        
      } else if (message.cmd === 'subscribed') {
        console.log('Subscription successful');
        
      } else if (message.cmd === 'fields') {
        // Store field definitions for parsing arrays
        console.log('Received field definitions:', JSON.stringify(message.msg).substring(0, 500));
        fieldDefinitions = message.msg;
        
      } else if (message.cmd === 'outcomes') {
        // Process outcomes data (odds updates)
        await processOutcomes(message.msg);
        
      } else if (message.cmd === 'bookmaker_events') {
        // Process bookmaker events data
        await processBookmakerEvents(message.msg);
        
      } else if (message.cmd === 'data' || message.cmd === 'update') {
        // Legacy format fallback
        await processOddsData(message.msg);
        
      } else if (message.cmd === 'error') {
        console.error('OddsMarket error:', message.msg);
        
      } else if (message.cmd === 'pong') {
        console.log('Pong received');
      }
    } catch (error) {
      console.error('Error processing message:', error);
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`WebSocket closed: ${code} - ${reason}`);
    attemptReconnect();
  });
}

function attemptReconnect() {
  if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
    reconnectAttempts++;
    console.log(`Reconnecting in ${RECONNECT_DELAY/1000}s... (attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
    setTimeout(connect, RECONNECT_DELAY);
  } else {
    console.error('Max reconnect attempts reached. Exiting.');
    process.exit(1);
  }
}

// Keep connection alive with ping
setInterval(() => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ cmd: 'ping', msg: Date.now().toString() }));
  }
}, 30000);

// Store events temporarily by ID for reference
const eventsCache = new Map();

// OddsMarket uses compact array format. Based on observed data:
// bookmaker_events array: [eventId, bookmakerId, active, timestamp, eventName, sportId, league, ...]
// outcomes array: strings like "MTg5OTMxMjIwNnw0MDQs..." which are base64 encoded

async function processOutcomes(data) {
  if (!data) return;
  
  console.log('Processing outcomes, count:', Array.isArray(data) ? data.length : 1);
  
  const outcomes = Array.isArray(data) ? data : [data];
  const oddsRecords = [];
  
  // DEBUG: Log first outcome structure
  if (outcomes.length > 0) {
    const first = outcomes[0];
    console.log('First outcome type:', typeof first);
    if (typeof first === 'object' && first !== null && !Array.isArray(first)) {
      console.log('First outcome keys:', Object.keys(first).join(', '));
      console.log('First outcome data:', JSON.stringify(first).substring(0, 500));
    } else if (Array.isArray(first)) {
      console.log('First outcome is array, length:', first.length);
      console.log('First outcome array:', JSON.stringify(first).substring(0, 500));
      // Log key indices for debugging bookmaker extraction
      console.log('Index 1 (bookmakerId?):', first[1], 'Index 11 (odds?):', first[11], 'Index 15 (info?):', typeof first[15] === 'string' ? first[15].substring(0, 100) : first[15]);
    } else if (typeof first === 'string') {
      console.log('First outcome (raw):', first.substring(0, 100));
    }
  }
  
  for (const outcome of outcomes) {
    if (!outcome) continue;
    
    // Handle STRING format (base64 encoded or pipe-delimited)
    if (typeof outcome === 'string') {
      let decoded = outcome;
      
      // Try base64 decode if it looks like base64
      if (/^[A-Za-z0-9+/=]+$/.test(outcome) && outcome.length > 20) {
        try {
          decoded = Buffer.from(outcome, 'base64').toString('utf8');
        } catch (e) {
          // Not base64, use as-is
        }
      }
      
      // Now parse the decoded/raw string (pipe-delimited format)
      if (decoded.includes('|')) {
        const parts = decoded.split('|');
        
        // OddsMarket format appears to be:
        // eventId|bookmakerId|marketId|outcomeId|odds|...
        // or similar positional format
        
        let eventId = null;
        let bookmakerId = null;
        let odds = null;
        let selection = 'Unknown';
        
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
          else if (!odds && !isNaN(num) && num > 1 && num < 100 && part.includes('.')) {
            odds = num;
          }
        }
        
        if (eventId && odds) {
          const eventInfo = eventsCache.get(eventId) || {};
          oddsRecords.push({
            event_id: eventId,
            event_name: eventInfo.name || `Event ${eventId}`,
            event_time: eventInfo.startsAt || null,
            league: eventInfo.league || 'Soccer',
            sport_id: SPORT_ID,
            bookmaker_id: bookmakerId || 21,
            bookmaker_name: getBookmakerName(bookmakerId || 21),
            market_type: 'Match Winner',
            selection: selection,
            odds: odds,
            updated_at: new Date().toISOString()
          });
        }
      }
    }
    // Handle OBJECT format
    else if (typeof outcome === 'object' && !Array.isArray(outcome)) {
      const eventId = outcome.event_id || outcome.eventId || outcome.eid;
      const bookmakerId = outcome.bookmaker_id || outcome.bookmakerId || outcome.bid;
      const odds = outcome.odds || outcome.price || outcome.value;
      const marketType = outcome.market || outcome.marketName || 'Match Winner';
      const selection = outcome.outcome || outcome.outcomeName || outcome.sel || 'Unknown';
      
      if (eventId && bookmakerId && odds) {
        const eventInfo = eventsCache.get(String(eventId)) || {};
        oddsRecords.push({
          event_id: String(eventId),
          event_name: eventInfo.name || `Event ${eventId}`,
          event_time: eventInfo.startsAt || null,
          league: eventInfo.league || 'Soccer',
          sport_id: SPORT_ID,
          bookmaker_id: Number(bookmakerId),
          bookmaker_name: getBookmakerName(Number(bookmakerId)),
          market_type: String(marketType),
          selection: String(selection),
          odds: parseFloat(odds),
          updated_at: new Date().toISOString()
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
      const period = outcome[3] || 'Regular time';
      
      // Get bookmaker from events cache using INTERNAL event ID
      const eventInfo = eventsCache.get(String(internalEventId)) || {};
      const bookmakerId = eventInfo.bookmakerId;
      
      // Extract external eventId for storage (for matching across bookmakers)
      let externalEventId = null;
      let marketType = period;
      let selection = 'Unknown';
      
      if (typeof infoString === 'string') {
        // Parse external eventId (used by the bookmaker)
        const eventMatch = infoString.match(/eventId=(\d+)/);
        if (eventMatch) {
          externalEventId = eventMatch[1];
        }
        
        // Parse selection/bet type from selectionId or betId
        const selMatch = infoString.match(/selectionId=(\d+)/);
        const betMatch = infoString.match(/betId=(\d+)/);
        if (selMatch) {
          selection = `Selection ${selMatch[1]}`;
        } else if (betMatch) {
          selection = `Bet ${betMatch[1]}`;
        }
        
        // Parse market type from codiceScommessa (shared across bookmakers!)
        const marketMatch = infoString.match(/codiceScommessa=(\d+)/);
        if (marketMatch) {
          marketType = `M${marketMatch[1]}`; // Use consistent format M{code}
        }
        
        // Parse selection from codiceEsito (the actual bet outcome code)
        const esitoMatch = infoString.match(/codiceEsito=(\d+)/);
        if (esitoMatch) {
          selection = `E${esitoMatch[1]}`; // E1=Home, E2=Away, EX=Draw typically
        }
      }
      
      // Use external eventId for storage (to match across different bookmakers)
      const eventIdForStorage = externalEventId || String(internalEventId);
      
      // Only process if we have valid bookmakerId (must be 21 or 103) and odds
      const validBookmakers = [21, 103]; // 1xbet, Sisal
      if (bookmakerId && validBookmakers.includes(bookmakerId) && typeof odds === 'number' && odds > 1 && odds < 1000) {
        oddsRecords.push({
          event_id: eventIdForStorage,
          event_name: eventInfo.name || `Event ${eventIdForStorage}`,
          event_time: eventInfo.startsAt || null,
          league: eventInfo.league || 'Soccer',
          sport_id: SPORT_ID,
          bookmaker_id: bookmakerId,
          bookmaker_name: getBookmakerName(bookmakerId),
          market_type: String(marketType),
          selection: selection,
          odds: parseFloat(odds),
          updated_at: new Date().toISOString()
        });
      }
    }
  }
  
  if (oddsRecords.length > 0) {
    console.log(`✅ Parsed ${oddsRecords.length} outcome records, saving...`);
    await saveOddsRecords(oddsRecords);
  } else {
    console.log('⚠️ No valid outcomes parsed from', outcomes.length, 'items');
  }
}

async function processBookmakerEvents(data) {
  if (!data) return;
  
  console.log('Processing bookmaker_events, count:', Array.isArray(data) ? data.length : 1);
  
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
      league = event[6] || event[7] || 'Soccer';
      startsAt = event[3] ? new Date(event[3] * 1000).toISOString() : null;
    }
    // If it's an object
    else if (typeof event === 'object') {
      eventId = event.id || event.eventId || event.eid;
      eventName = event.name || event.eventName || `${event.home || 'Home'} vs ${event.away || 'Away'}`;
      startsAt = event.starts_at || event.startsAt || event.start_time;
      league = event.league || event.leagueName || event.tournament || 'Soccer';
      bookmakerId = event.bookmaker_id || event.bookmakerId;
    }
    
    if (eventId && bookmakerId) {
      eventsCache.set(String(eventId), {
        name: eventName,
        startsAt: startsAt,
        league: league,
        bookmakerId: bookmakerId
      });
      added++;
      
      // Track bookmaker distribution
      const bmName = getBookmakerName(bookmakerId);
      bookmakerCounts[bmName] = (bookmakerCounts[bmName] || 0) + 1;
    }
  }
  
  // Log which bookmakers we're receiving events from
  const bmSummary = Object.entries(bookmakerCounts).map(([k, v]) => `${k}:${v}`).join(', ');
  console.log(`✅ Event cache updated: +${added} events, total: ${eventsCache.size} | Bookmakers: ${bmSummary || 'none'}`);
  
  // Keep cache clean (max 5000 events)
  if (eventsCache.size > 5000) {
    const keysToDelete = [...eventsCache.keys()].slice(0, 1000);
    keysToDelete.forEach(k => eventsCache.delete(k));
    console.log('🧹 Cleaned event cache, new size:', eventsCache.size);
  }
}

async function processOddsData(data) {
  if (!data) return;
  
  const oddsRecords = [];
  console.log('Legacy data format:', JSON.stringify(data).substring(0, 500));
  
  const events = Array.isArray(data) ? data : (data.events || [data]);
  
  for (const event of events) {
    if (!event) continue;
    
    const eventId = event.id || event.eventId || `${event.home}_${event.away}`;
    const eventName = event.name || `${event.home || 'Team1'} vs ${event.away || 'Team2'}`;
    const eventTime = event.startsAt || event.starts_at || event.startTime || null;
    const league = event.league || event.leagueName || event.tournament || 'Unknown';
    
    const markets = event.markets || event.odds || [];
    
    for (const market of markets) {
      const marketType = market.name || market.marketName || market.type || 'Unknown';
      const outcomes = market.outcomes || market.odds || [];
      
      for (const outcome of outcomes) {
        const bookmakerId = outcome.bookmakerId || outcome.bookmaker_id || event.bookmakerId;
        const odds = outcome.odds || outcome.price || outcome.value;
        const selection = outcome.name || outcome.outcomeName || outcome.selection || 'Unknown';
        
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
            updated_at: new Date().toISOString()
          });
        }
      }
    }
  }
  
  if (oddsRecords.length > 0) {
    await saveOddsRecords(oddsRecords);
  }
}

async function saveOddsRecords(oddsRecords) {
  // Deduplicate records by unique key before upserting
  const uniqueMap = new Map();
  for (const record of oddsRecords) {
    const key = `${record.event_id}|${record.bookmaker_id}|${record.market_type}|${record.selection}`;
    // Keep the latest record (last one wins)
    uniqueMap.set(key, record);
  }
  
  const uniqueRecords = Array.from(uniqueMap.values());
  console.log(`Upserting ${uniqueRecords.length} unique records to Supabase (from ${oddsRecords.length} total)...`);
  
  const { error } = await supabase
    .from('live_odds')
    .upsert(uniqueRecords, { 
      onConflict: 'event_id,bookmaker_id,market_type,selection',
      ignoreDuplicates: false 
    });
  
  if (error) {
    console.error('Error saving to Supabase:', error);
  } else {
    console.log(`✅ Saved ${uniqueRecords.length} odds records`);
  }
}

function getBookmakerName(id) {
  const names = {
    1: 'Pinnacle',
    21: '1xbet',
    103: 'Sisal'
  };
  return names[id] || `Bookmaker ${id}`;
}

// Health check server for Railway
import { createServer } from 'http';
const PORT = process.env.PORT || 3000;

createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ 
    status: 'running',
    connected: ws?.readyState === WebSocket.OPEN,
    reconnectAttempts,
    wsUrl: WS_URL
  }));
}).listen(PORT, () => {
  console.log(`Health check server running on port ${PORT}`);
});

// Start connection
console.log('Starting OddsMarket Worker...');
console.log('API Key:', ODDSMARKET_API_KEY ? 'Configured' : 'MISSING');
console.log('Supabase URL:', SUPABASE_URL ? 'Configured' : 'MISSING');
console.log('Bookmakers:', BOOKMAKER_IDS.map(getBookmakerName).join(', '));
connect();

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, closing connection...');
  if (ws) ws.close();
  process.exit(0);
});
