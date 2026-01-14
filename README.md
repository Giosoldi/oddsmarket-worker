# OddsMarket WebSocket Worker

Worker Node.js che gestisce la connessione WebSocket con compressione `permessage-deflate` e salva i dati odds in Supabase.

## Setup

### 1. Clona questo codice in un nuovo repository

### 2. Deploy su Railway (gratuito)

1. Vai su [railway.app](https://railway.app)
2. Crea nuovo progetto → Deploy from GitHub
3. Connetti il repository
4. Aggiungi le variabili d'ambiente:

```env
ODDSMARKET_API_KEY=0058f8135eae783e2eb18b68dfc31fa9
SUPABASE_URL=https://cdqfxqsqfrqqnojrlrll.supabase.co
SUPABASE_SERVICE_KEY=<your-service-role-key>
```

### 3. Il worker si connetterà automaticamente e inizierà a salvare i dati

## Architettura

```
OddsMarket WebSocket (con compressione)
         ↓
   Node.js Worker (Railway)
         ↓
   Supabase Database
         ↓
   Frontend React (polling/realtime)
```

## File

- `index.js` - Worker principale
- `package.json` - Dipendenze
