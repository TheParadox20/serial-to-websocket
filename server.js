import express from 'express';
import path from 'path';
import cors from 'cors';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';
import http from 'http';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';

dotenv.config();

const users = { janedoe: { userID: 1 } };
const clients = [];

const app = express();
app.use(cors());
app.use(bodyParser.json());
const port = process.env.PORT || 80;
const serialPortPath = process.env.SERIAL_PORT;
const serialBaudRate = Number(process.env.SERIAL_BAUD_RATE || 115200);

// sendFile will go here
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
app.use(express.static(path.join(__dirname, 'frontend', 'dist')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const broadcast = (payload) => {
    const message = JSON.stringify(payload);

    clients.forEach(client => {
        if (client.readyState === 1) {
            client.send(message);
        }
    });
};

const sendClientCount = () => {
    broadcast({
        type: 'users',
        data: clients.length,
    });
};

const parseSerialPayload = (rawData) => {
    const text = rawData.trim();

    if (!text) {
        return null;
    }

    try {
        return JSON.parse(text);
    } catch {
        return text;
    }
};

const normalizeIncomingPayload = (payload) => {
    if (payload === undefined || payload === null) {
        return null;
    }

    if (typeof payload === 'string') {
        return parseSerialPayload(payload);
    }

    return payload;
};

const setupSerialBridge = async () => {
    if (!serialPortPath) {
        console.log('Serial bridge disabled: set SERIAL_PORT in .env to enable serial input.');
        return;
    }

    try {
        const { SerialPort } = await import('serialport');
        const { ReadlineParser } = await import('@serialport/parser-readline');

        const serialPort = new SerialPort({
            path: serialPortPath,
            baudRate: serialBaudRate,
        });

        const parser = serialPort.pipe(new ReadlineParser({ delimiter: '\n' }));

        serialPort.on('open', () => {
            console.log(`Serial port open on ${serialPortPath} @ ${serialBaudRate} baud`);
        });

        serialPort.on('error', (error) => {
            console.error('Serial port error:', error.message);
        });

        parser.on('data', (line) => {
            const trimmed = line.trim();
            if (!trimmed) return;

            if (trimmed.startsWith('#')) {
                console.log('Serial log:', trimmed);
                return;
            }

            let payload;
            try {
                payload = JSON.parse(trimmed);
            } catch {
                console.log('Serial non-JSON ignored:', trimmed);
                return;
            }

            console.log('Serial data received:', payload);
            broadcast({
                type: 'serial',
                data: payload,
                receivedAt: new Date().toISOString(),
            });
        });
    } catch (error) {
        console.error('Serial bridge unavailable:', error.message);
        console.error('Install "serialport" and "@serialport/parser-readline" to enable serial forwarding.');
    }
};

//start server
server.listen(port, async () => {
    console.log('Server started at http://localhost:' + port);
    console.log('WebSocket server started at ws://localhost:' + port);
    await setupSerialBridge();
});

app.get('/', (req, res) => {
    console.log(req.query);
    res.send({ message: 'Express says Hello World!' });
});

app.post('/api/broadcast', (req, res) => {
    const payload = normalizeIncomingPayload(req.body?.data ?? req.body?.message ?? req.body);
    console.log('payload ::',payload)

    if (payload === null) {
        res.status(400).send({
            message: 'Request body must include data to broadcast.',
        });
        return;
    }

    const websocketPayload = {
        type: req.body?.type || 'serial',
        data: payload,
        receivedAt: new Date().toISOString(),
        source: 'api',
    };

    console.log('API broadcast received:', websocketPayload);
    broadcast(websocketPayload);

    res.send({
        message: 'Broadcast sent.',
        clients: clients.length,
        payload: websocketPayload,
    });
});

// Event handler for connection
wss.on('connection', (ws) => {
    clients.push(ws);
    console.log('Client connected');
    sendClientCount();

    ws.on('message', (message) => {
        console.log(`Received message: ${message}`);
        const msg = JSON.parse(message);

        if (msg.type === 'setup') {
            // users[msg.data.username].client = ws;
        }
        else if (msg.type === 'ping') {
            console.log('ping');
            ws.send(JSON.stringify({
                type: 'pong',
            }));
        }
        else if (msg.type === 'text') {
            broadcast({
                type: 'text',
                data: msg.data,
            });
        }
        else if (msg.type === 'logout') {
            delete users[msg.data.username];
        }
    });

    ws.on('close', () => {
        console.log('Client disconnected');
        const clientIndex = clients.indexOf(ws);

        if (clientIndex >= 0) {
            clients.splice(clientIndex, 1);
        }

        sendClientCount();
    });
});