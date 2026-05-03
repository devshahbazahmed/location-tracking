import { kafkaClient } from './kafka-client.js';

async function init() {
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `database-processor`,
  });
  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: true,
  });

  kafkaConsumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      console.log(`INSERT INTO DB LOCATION`, {
        userId: data.id,
        name: data.name,
        email: data.email,
        latitude: data.latitude,
        longitude: data.longitude,
        updatedAt: data.updatedAt,
      });
      await heartbeat();
    },
  });
}

init();
