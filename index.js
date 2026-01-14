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

// Bookmaker IDs from trial: Pinnacle (1), 1xbet (21), Sisal (103)
const BOOKMAKER_IDS = [1, 21, 103];
// Soccer sport ID
const SPORT_ID = 7;

// Initialize Supabase client
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

let ws = null;
let reconnectAttempts = 0;
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
        console.log('Received field definitions');
        
      } else if (message.cmd === 'data' || message.cmd === 'update') {
        // Process odds data
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

async function processOddsData(data) {
  if (!data) return;
  
  const oddsRecords = [];
  
  // Handle different data formats from OddsMarket
  // The data structure may vary - log it to understand format
  console.log('Processing data:', JSON.stringify(data).substring(0, 500));
  
  // If data is an array of events
  const events = Array.isArray(data) ? data : (data.events || [data]);
  
  for (const event of events) {
    if (!event) continue;
    
    const eventId = event.id || event.eventId || `${event.home}_${event.away}`;
    const eventName = event.name || `${event.home || 'Team1'} vs ${event.away || 'Team2'}`;
    const eventTime = event.startsAt || event.starts_at || event.startTime || null;
    const league = event.league || event.leagueName || event.tournament || 'Unknown';
    
    // Process markets/outcomes
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
            bookmaker_id: bookmakerId,
            bookmaker_name: getBookmakerName(bookmakerId),
            market_type: marketType,
            selection: selection,
            odds: parseFloat(odds),
            updated_at: new Date().toISOString()
          });
        }
      }
    }
  }
  
  if (oddsRecords.length > 0) {
    console.log(`Saving ${oddsRecords.length} odds records...`);
    
    const { error } = await supabase
      .from('live_odds')
      .upsert(oddsRecords, { 
        onConflict: 'event_id,bookmaker_id,market_type,selection',
        ignoreDuplicates: false 
      });
    
    if (error) {
      console.error('Error saving to Supabase:', error);
    } else {
      console.log(`Saved ${oddsRecords.length} odds records`);
    }
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
