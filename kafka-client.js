import { Kafka } from 'kafkajs';

export const kafkaClient = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID ?? 'location-tracking',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
});
