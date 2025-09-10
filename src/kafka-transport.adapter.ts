import { v4 as uuidv4 } from 'uuid';
import { CorrelatedRequestDTO, CorrelatedResponseDTO, TransportAdapter, transportService } from 'transport-pkg';
import { IAppPkg, AppRunPriority } from 'app-life-cycle-pkg';
import { BadRequestError } from 'rest-pkg';

import KafkaService from './services/kafka.service';

class KafkaTransportAdapter extends TransportAdapter implements IAppPkg {
  private kafkaService: KafkaService;

  constructor(brokerUrl: string, clientId: string) {
    super();

    this.kafkaService = new KafkaService(brokerUrl, clientId);
  }

  async init(): Promise<void> {
    const actionsToProduce: string[] = transportService.getBroadcastableActions();
    const actionsToConsume: Record<string, (data: CorrelatedRequestDTO) => Promise<void>> = transportService.getSubscribedActions();

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
          await actionsToConsume[action](message as CorrelatedRequestDTO);
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

  getPriority(): number {
    return AppRunPriority.Lowest;
  }

  async broadcast(data: CorrelatedRequestDTO): Promise<void> {
    if (!data.request_id) {
      data.request_id = uuidv4();
    }

    const actionsToProduce: string[] = transportService.getBroadcastableActions();
    if (!actionsToProduce.includes(data.action)) {
      throw new BadRequestError(`Invalid action provided: ${data.action}`);
    }

    await this.kafkaService.sendMessage(data.action, data);
  }
}

export default KafkaTransportAdapter;
