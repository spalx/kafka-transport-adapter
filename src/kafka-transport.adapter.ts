import { v4 as uuidv4 } from 'uuid';
import { CorrelatedRequestDTO, CorrelatedResponseDTO, TransportAdapter } from 'transport-pkg';
import { ZodError } from 'zod';

import kafkaService from './services/kafka.service';
import { KafkaTopic } from './types/kafka';

class KafkaTransportAdapter implements TransportAdapter {
  private pendingResponses: Map<string, (message: CorrelatedResponseDTO) => void> = new Map();
  private topics: KafkaTopic[] = [];

  constructor(topics: KafkaTopic[]) {
    this.topics = topics;
  }

  async init(): Promise<void> {
    await this.prepareTopics();
    await kafkaService.connectProducer();
    await kafkaService.runConsumer();
  }

  async shutdown(): Promise<void> {
    await kafkaService.disconnectProducer();
    await kafkaService.disconnectConsumer();
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
        await kafkaService.sendMessage(data.action, data);
      } catch (err) {
        clearTimeout(timer);
        if (data.request_id) {
          this.pendingResponses.delete(data.request_id);
        }
        reject(err);
      }
    });
  }

  async sendResponse(data: CorrelatedRequestDTO, error: unknown | null): Promise<void> {
    let errorMessage = '';
    let status = 0;
    if (error !== null) {
      errorMessage = error instanceof Error ? error.message : 'Internal Server Error';
      if (error instanceof ZodError) {
        status = 400;
        errorMessage = error.errors.map(e => e.message).join(', ');
      } else if (this.isErrorWithCode(error)) {
        status = error.code;
      } else {
        status = 500;
      }
    }

    const response: CorrelatedResponseDTO = {
      correlation_id: data.correlation_id,
      request_id: data.request_id,
      action: data.action,
      data: data.data,
      status: status,
      error: errorMessage
    };

    await kafkaService.sendMessage(`did.${data.action}`, response);
  }

  private async prepareTopics(): Promise<void> {
    for (const topicInfo of this.topics) {
      const topicName = topicInfo.topic;

      await kafkaService.createTopics([{
        topic: topicName,
        numPartitions: topicInfo.num_partitions,
        replicationFactor: topicInfo.replication_factor
      }]);

      kafkaService.subscribe({
        [topicName]: async (message: object) => {
          topicInfo.on_message(message as CorrelatedRequestDTO);
        }
      });

      kafkaService.subscribe({
        [`did.${topicName}`]: async (message: object) => {
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
  }

  private isErrorWithCode(error: unknown): error is { code: number } {
    return (
      typeof error === 'object' &&
      error !== null &&
      Object.prototype.hasOwnProperty.call(error, 'code') &&
      typeof (error as Record<string, unknown>).code === 'number'
    );
  }
}

export default KafkaTransportAdapter;
