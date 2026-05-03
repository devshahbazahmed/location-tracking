# Location Tracking with OIDC and Kafka

This project runs a live location tracking app protected by the local OIDC auth service. Authenticated browser clients send location updates over Socket.IO, the app publishes those updates to Kafka, and the same app consumes the Kafka topic to broadcast live map updates back to connected users.

## Services

- **Location app**: Express, Socket.IO, KafkaJS, and the Leaflet frontend in `public/index.html`.
- **OIDC auth service**: Express, Postgres, Drizzle, and RS256 JWTs in `oidc-auth/`.
- **Kafka**: Local single-node Kafka broker from `docker-compose.yml`.
- **Postgres**: Local database for auth users from `docker-compose.yml`.

## Prerequisites

- Node.js 18 or newer
- pnpm
- Docker

## Local Setup

Start Kafka and Postgres:

```bash
docker compose up -d
```

Install dependencies:

```bash
pnpm install
cd oidc-auth
pnpm install
cd ..
```

Create the OIDC database tables:

```bash
cd oidc-auth
DATABASE_URL="postgres://admin:admin@localhost:5432/oidc_auth" pnpm db:migrate
cd ..
```

Create the Kafka topic:

```bash
pnpm kafka:setup
```

## Running the App

Terminal 1, start the OIDC auth server on port `8001`:

```bash
cd oidc-auth
DATABASE_URL="postgres://admin:admin@localhost:5432/oidc_auth" PORT=8001 pnpm dev
```

Terminal 2, start the location app on port `8000`:

```bash
AUTH_SERVER_URL="http://localhost:8001" PORT=8000 pnpm dev
```

Open:

```text
http://localhost:8000
```

The frontend redirects unauthenticated users to the OIDC sign-in page. Create an account, sign in, and the auth service redirects back to the location app with a JWT. The app stores that token in local storage, validates it through `/auth/me`, and uses it in the Socket.IO connection.

## Optional Processor

Run the database processor to observe Kafka messages that would be inserted into a locations table:

```bash
pnpm db-processor
```

At the moment this processor logs normalized location records:

```js
{
  userId,
  name,
  email,
  latitude,
  longitude,
  updatedAt
}
```

## Configuration

Root app environment variables:

- `PORT`: location app port, default `8000`
- `AUTH_SERVER_URL`: OIDC auth server URL, default `http://localhost:8001`
- `OIDC_CLIENT_ID`: client id sent to the auth server, default `location-tracking`
- `KAFKA_CLIENT_ID`: Kafka client id, default `location-tracking`
- `KAFKA_BROKERS`: comma-separated broker list, default `localhost:9092`

Auth service environment variables:

- `PORT`: auth server port, default `8001`
- `DATABASE_URL`: Postgres connection string, for local Docker use `postgres://admin:admin@localhost:5432/oidc_auth`

## Auth and Kafka Flow

1. The browser loads `http://localhost:8000`.
2. If no token exists, the frontend calls `/auth/config` and redirects to `http://localhost:8001/o/authenticate`.
3. The auth service signs in the user and redirects back with a signed JWT.
4. The location app validates the token through the auth service `/o/userinfo` endpoint.
5. Socket.IO accepts the connection only when the token is valid.
6. Location updates are published to the Kafka `location-updates` topic with the OIDC user id, name, and email.
7. The location app consumes `location-updates` and broadcasts live map updates to all authenticated clients.
