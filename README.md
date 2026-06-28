# Night Echo Online

A Node.js + WebSocket version of Night Echo.

## Local run

```bash
npm install
npm start
```

Open `http://localhost:8787/?room=echo`.

## Deploy notes

This app needs a long-running Node web service because multiplayer rooms use WebSockets at `/ws`.

Recommended cloud settings:

- Build command: `npm install`
- Start command: `npm start`
- Node service port: use the platform-provided `PORT` environment variable

Do not deploy this as a static-only site; WebSocket rooms need `server.js` running.