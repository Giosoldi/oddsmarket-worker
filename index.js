import WebSocket from 'ws';
import { createClient } from '@supabase/supabase-js';

// Configuration
const ODDSMARKET_API_KEY = process.env.ODDSMARKET_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

// OddsMarket WebSocket endpoint
const WS_URL = `wss://api.oddsmarket.org/v4/odds_feed`;

// Bookmaker IDs from trial: 1xbet (21), Sisal (103), Pinnacle (1)
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
  console.log('Connecting to OddsMarket WebSocket...');
  
  ws = new WebSocket(WS_URL, {
    perMessageDeflate: true, // Enable compression
    headers: {
      'Authorization': `Bearer ${ODDSMARKET_API_KEY}`
    }
  });

  ws.on('open', () => {
    console.log('Connected to OddsMarket WebSocket');
    reconnectAttempts = 0;
    
    // Subscribe to odds feed
    const subscribeMessage = {
      type: 'subscribe',
      data: {
        bookmaker_ids: BOOKMAKER_IDS,
        sport_id: SPORT_ID,
        is_prematch: true
      }
    };
    
    ws.send(JSON.stringify(subscribeMessage));
    console.log('Subscribed to odds feed:', subscribeMessage);
  });

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log('Received message type:', message.type);
      
      if (message.type === 'odds' || message.type === 'odds_update') {
        await processOddsData(message.data);
      } else if (message.type === 'error') {
        console.error('OddsMarket error:', message.data);
      } else if (message.type === 'subscribed') {
        console.log('Successfully subscribed:', message.data);
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

async function processOddsData(data) {
  if (!data || !data.events) return;
  
  const oddsRecords = [];
  
  for (const event of data.events) {
    const eventName = `${event.home_team} vs ${event.away_team}`;
    const eventTime = event.starts_at;
    const league = event.league_name || 'Unknown';
    
    for (const market of event.markets || []) {
      for (const outcome of market.outcomes || []) {
        oddsRecords.push({
          event_id: event.id,
          event_name: eventName,
          event_time: eventTime,
          league: league,
          sport_id: SPORT_ID,
          bookmaker_id: outcome.bookmaker_id,
          bookmaker_name: getBookmakerName(outcome.bookmaker_id),
          market_type: market.name,
          selection: outcome.name,
          odds: outcome.odds,
          updated_at: new Date().toISOString()
        });
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
    reconnectAttempts
  }));
}).listen(PORT, () => {
  console.log(`Health check server running on port ${PORT}`);
});

// Start connection
console.log('Starting OddsMarket Worker...');
console.log('Bookmakers:', BOOKMAKER_IDS.map(getBookmakerName).join(', '));
connect();

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, closing connection...');
  if (ws) ws.close();
  process.exit(0);
});
