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
  private isConsumerJoined: boolean = false;

  private producer: Producer;
  private consumer: Consumer;
  private admin: Admin;

  constructor(broker: string, clientId: string) {
    const kafka = new Kafka({
      clientId,
      brokers: [broker],
      logLevel: logLevel.ERROR,
      logCreator: this.createLogger(),
    });
    this.producer = kafka.producer();
    this.consumer = kafka.consumer({ groupId: `${clientId}-group` });
    this.admin = kafka.admin();

    const { GROUP_JOIN } = this.consumer.events;
    this.consumer.on(GROUP_JOIN, () => {
      logger.info('Kafka consumer joined');
      this.isConsumerJoined = true;
    });
  }

  async createTopics(topics: { topic: string; numPartitions: number; replicationFactor: number; }[]): Promise<void> {
    await this.admin.connect();

    const existingTopics = await this.admin.listTopics();

    const newTopics = topics.filter(
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

  subscribe(topics: Record<string, (message: KafkaMessage) => Promise<void>>): void {
    Object.assign(this.topicHandlers, topics);
  }

  async runConsumer(): Promise<void> {
    if (!Object.keys(this.topicHandlers).length) {
      return;
    }

    await this.connectConsumer();
    const topicsKeys = Object.keys(this.topicHandlers);

    await this.consumer.subscribe({ topics: topicsKeys, fromBeginning: false });
    logger.info('Subscribed to Kafka topics: ' + topicsKeys.join(', '));

    await this.consumer.run({
      eachMessage: async ({ topic, message }) => {
        kafkaLogger.info(`Received message: ${message.value?.toString()} from ${topic}`);

        if (this.topicHandlers[topic] && message.value) {
          try {
            const parsedMessage = JSON.parse(message.value.toString());

            try {
              await this.topicHandlers[topic](parsedMessage);
              kafkaLogger.info(`Message processed successfully for topic ${topic}`);
            } catch (processingError) {
              kafkaLogger.error(`Error processing message for topic ${topic}:`, processingError);
            }
          } catch (parsingError) {
            kafkaLogger.error(`Failed to parse message for topic ${topic}`, parsingError);
          }
        } else {
          kafkaLogger.warn(`No handler defined for topic: ${topic}`);
        }
      },
    });
  }

  async disconnectConsumer(): Promise<void> {
    if (!this.isConsumerConnected || !this.isConsumerJoined) {
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

export default KafkaService;
