import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import express from 'express';
import { Server } from 'socket.io';

import { kafkaClient } from './kafka-client.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const AUTH_SERVER_URL = process.env.AUTH_SERVER_URL ?? 'http://localhost:8001';
const CLIENT_ID = process.env.OIDC_CLIENT_ID ?? 'location-tracking';

async function getUserFromToken(token) {
  if (!token) return null;

  const response = await fetch(`${AUTH_SERVER_URL}/o/userinfo`, {
    headers: {
      authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) return null;
  return response.json();
}

async function main() {
  const PORT = process.env.PORT ?? 8000;

  const app = express();
  const server = http.createServer(app);
  const io = new Server();

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`KafkaConsumer Data Received`, { data });
      io.emit('server:location:update', {
        id: data.id,
        name: data.name,
        email: data.email,
        latitude: data.latitude,
        longitude: data.longitude,
        updatedAt: data.updatedAt,
      });
      await heartbeat();
    },
  });

  io.attach(server);

  io.use(async (socket, next) => {
    try {
      const token = socket.handshake.auth?.token;
      const user = await getUserFromToken(token);

      if (!user) {
        next(new Error('Authentication required'));
        return;
      }

      socket.data.user = user;
      next();
    } catch (error) {
      next(error);
    }
  });

  io.on('connection', (socket) => {
    const user = socket.data.user;
    console.log(`[Socket:${socket.id}]: ${user.email} connected`);

    socket.on('client:location:update', async (locationData) => {
      const { latitude, longitude } = locationData;

      if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
        socket.emit('server:error', { message: 'Invalid location payload.' });
        return;
      }

      await kafkaProducer.send({
        topic: 'location-updates',
        messages: [
          {
            key: user.sub,
            value: JSON.stringify({
              id: user.sub,
              name: user.name || user.email,
              email: user.email,
              latitude,
              longitude,
              updatedAt: new Date().toISOString(),
            }),
          },
        ],
      });
    });
  });

  app.use(express.static(path.resolve(__dirname, 'public')));

  app.get('/health', (req, res) => {
    return res.json({ healthy: true });
  });

  app.get('/auth/config', (req, res) => {
    const redirectUri = `${req.protocol}://${req.get('host')}/`;
    const loginUrl = new URL('/o/authenticate', AUTH_SERVER_URL);
    loginUrl.searchParams.set('client_id', CLIENT_ID);
    loginUrl.searchParams.set('redirect_uri', redirectUri);

    return res.json({
      clientId: CLIENT_ID,
      authServerUrl: AUTH_SERVER_URL,
      loginUrl: loginUrl.toString(),
    });
  });

  app.get('/auth/me', async (req, res) => {
    try {
      const authHeader = req.headers.authorization;
      const token = authHeader?.startsWith('Bearer ')
        ? authHeader.slice(7)
        : null;
      const user = await getUserFromToken(token);

      if (!user) {
        res.status(401).json({ message: 'Authentication required.' });
        return;
      }

      res.json(user);
    } catch {
      res.status(401).json({ message: 'Authentication required.' });
    }
  });

  server.listen(PORT, () =>
    console.log(`Server running on http://localhost:${PORT}`),
  );
}

main();
