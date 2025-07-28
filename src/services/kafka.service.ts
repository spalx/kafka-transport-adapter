import {
  Kafka,
  Producer,
  Consumer,
  Admin,
  logLevel,
  LogEntry,
  KafkaMessage,
} from 'kafkajs';
import { logger, kafkaLogger } from 'common-loggers-pkg';

class KafkaService {
  private topicHandlers: Record<string, (message: KafkaMessage) => Promise<void>> = {};
  private isProducerConnected: boolean = false;
  private isConsumerConnected: boolean = false;

  private producer!: Producer;
  private consumer!: Consumer;
  private admin!: Admin;
  private readonly retryLimit = 3;

  constructor() {
    const clientId: string = 'client_id'; //TODO
    const broker: string = 'kafka:9092'; //TODO
    const kafka = new Kafka({
      clientId,
      brokers: [broker],
      logLevel: logLevel.ERROR,
      logCreator: this.createLogger(),
    });
    this.producer = kafka.producer();
    this.consumer = kafka.consumer({ groupId: `${clientId}-group` });
    this.admin = kafka.admin();
  }

  async createTopics(
    topics: {
      topic: string;
      numPartitions: number;
      replicationFactor: number;
    }[]
  ): Promise<void> {
    await this.admin.connect();

    const existingTopics = await this.admin.listTopics();

    // Expand the original topics to include "did." prefixed versions
    const allTopics = topics.flatMap(({ topic, numPartitions, replicationFactor }) => [
      { topic, numPartitions, replicationFactor },
      {
        topic: `did.${topic}`,
        numPartitions,
        replicationFactor,
      },
    ]);

    const newTopics = allTopics.filter(
      ({ topic }) => !existingTopics.includes(topic)
    );

    if (newTopics.length > 0) {
      await this.admin.createTopics({
        topics: newTopics,
      });
    }

    await this.admin.disconnect();
  }

  async connectProducer(): Promise<void> {
    if (this.isProducerConnected) {
      return;
    }

    try {
      await this.producer.connect();
      this.isProducerConnected = true;
      logger.info('Producer connected to Kafka');
    } catch (error) {
      logger.error('Error initializing Kafka Producer:', error);
    }
  }

  async sendMessage(topic: string, message: object): Promise<void> {
    await this.connectProducer();
    await this.producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    });

    kafkaLogger.info(`Message sent: ${JSON.stringify(message)} to ${topic}`);
  }

  async disconnectProducer(): Promise<void> {
    if (!this.isProducerConnected) {
      return;
    }

    await this.producer.disconnect();
    logger.info('Kafka producer disconnected');
  }

  async connectConsumer(): Promise<void> {
    if (this.isConsumerConnected) {
      return;
    }
    try {
      await this.consumer.connect();
      this.isConsumerConnected = true;
      logger.info('Consumer connected to Kafka');
    } catch (error) {
      logger.error('Error initializing Kafka Consumer:', error);
    }
  }

  subscribe(
    topics: Record<string, (message: KafkaMessage) => Promise<void>>
  ): void {
    Object.assign(this.topicHandlers, topics);
  }

  async runConsumer(): Promise<void> {
    if (!Object.keys(this.topicHandlers).length) {
      return;
    }

    await this.connectConsumer();
    const topicsKeys = Object.keys(this.topicHandlers);

    await this.consumer.subscribe({
      topics: topicsKeys,
      fromBeginning: false,
    });
    logger.info('Subscribed to Kafka topics: ' + topicsKeys.join(', '));

    await this.consumer.run({
      eachMessage: async ({ topic, message }) => {
        kafkaLogger.info(
          `Received message: ${message.value?.toString()} from ${topic}`
        );

        if (this.topicHandlers[topic] && message.value) {
          try {
            const parsedMessage = JSON.parse(message.value.toString());
            const retryCount = parsedMessage.retryCount || 0;

            try {
              await this.topicHandlers[topic](parsedMessage);
              kafkaLogger.info(
                `Message processed successfully for topic ${topic}`
              );
            } catch (processingError) {
              kafkaLogger.error(
                `Error processing message for topic ${topic}:`,
                processingError
              );
              if (retryCount < this.retryLimit) {
                // Retry the message by sending it back to the same topic
                kafkaLogger.warn(
                  `Retrying message for topic ${topic} (Attempt: ${
                    retryCount + 1
                  })`
                );

                await this.producer.send({
                  topic,
                  messages: [
                    {
                      value: JSON.stringify({
                        ...parsedMessage,
                        retryCount: retryCount + 1,
                      }),
                    },
                  ],
                });
              } else {
                kafkaLogger.error(
                  `Max retry attempts reached for topic ${topic}}`
                );
              }
            }
          } catch (parsingError) {
            kafkaLogger.error(
              `Failed to parse message for topic ${topic}`,
              parsingError
            );
          }
        } else {
          kafkaLogger.warn(`No handler defined for topic: ${topic}`);
        }
      },
    });
  }

  async disconnectConsumer(): Promise<void> {
    if (!this.isConsumerConnected) {
      return;
    }

    await this.consumer.disconnect();
    logger.info('Kafka consumer disconnected');
  }

  private createLogger() {
    const kafkaLogLevelMap: Record<logLevel, string> = {
      [logLevel.ERROR]: 'error',
      [logLevel.WARN]: 'warn',
      [logLevel.INFO]: 'info',
      [logLevel.DEBUG]: 'debug',
      [logLevel.NOTHING]: '',
    };

    return (entryLevel: logLevel) =>
      ({ namespace, level, label, log }: LogEntry) => {
        const { message, ...extra } = log;

        // Map Kafka log levels to Winston and log the message
        logger.log({
          level: kafkaLogLevelMap[level] || 'info',
          message: `[${namespace}] ${label}: ${message}`,
          ...extra, // Include additional log details
        });
      };
  }
}

export default new KafkaService();
