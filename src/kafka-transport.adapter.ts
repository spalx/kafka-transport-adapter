import { v4 as uuidv4 } from 'uuid';
import { CorrelatedRequestDTO, CorrelatedResponseDTO, TransportAdapter, transportService } from 'transport-pkg';

import KafkaService from './services/kafka.service';

class KafkaTransportAdapter implements TransportAdapter {
  private pendingResponses: Map<string, (message: CorrelatedResponseDTO) => void> = new Map();
  private kafkaService: KafkaService;

  constructor(clientId: string) {
    this.kafkaService = new KafkaService('kafka:9092', clientId);
  }

  async init(): Promise<void> {
    const actionsToProduce: string[] = transportService.getSendableActions();
    const actionsToConsume: Record<string, (data: CorrelatedRequestDTO) => Promise<void>> = transportService.getReceivableActions();

    const allActions = Array.from(
      new Set([
        ...actionsToProduce,
        ...Object.keys(actionsToConsume)
      ])
    );

    const createTopics = allActions.map(action => ({
      topic: action,
      numPartitions: 1,
      replicationFactor: 1
    }));

    await this.kafkaService.createTopics(createTopics);

    for (const action in actionsToConsume) {
      this.kafkaService.subscribe({
        [action]: async (message: object) => {
          actionsToConsume[action](message as CorrelatedRequestDTO);
        }
      });
    }

    for (const action of actionsToProduce) {
      this.kafkaService.subscribe({
        [`did.${action}`]: async (message: object) => {
          const response = message as CorrelatedResponseDTO;
          if (response.request_id && this.pendingResponses.has(response.request_id)) {
            const resolve = this.pendingResponses.get(response.request_id);
            if (resolve) {
              resolve(response);
              this.pendingResponses.delete(response.request_id);
            }
          }
        }
      });
    }

    await this.kafkaService.connectProducer();
    await this.kafkaService.runConsumer();
  }

  async shutdown(): Promise<void> {
    await this.kafkaService.disconnectProducer();
    await this.kafkaService.disconnectConsumer();
  }

  async send(data: CorrelatedRequestDTO, timeout?: number): Promise<CorrelatedResponseDTO> {
    if (!data.request_id) {
      data.request_id = uuidv4();
    }

    return new Promise<CorrelatedResponseDTO>(async (resolve, reject) => {
      const timer = setTimeout(() => {
        if (data.request_id) {
          this.pendingResponses.delete(data.request_id);
        }
        reject(new Error(`Timeout waiting for Kafka response on ${data.action}`));
      }, timeout || 10000);

      if (data.request_id) {
        this.pendingResponses.set(data.request_id, (msg) => {
          clearTimeout(timer);
          resolve(msg);
        });
      }

      try {
        await this.kafkaService.sendMessage(data.action, data);
      } catch (err) {
        clearTimeout(timer);
        if (data.request_id) {
          this.pendingResponses.delete(data.request_id);
        }
        reject(err);
      }
    });
  }

  async sendResponse(data: CorrelatedResponseDTO): Promise<void> {
    await this.kafkaService.sendMessage(`did.${data.action}`, data);
  }
}

export default KafkaTransportAdapter;
