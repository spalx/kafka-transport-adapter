import { CorrelatedMessage, TransportAdapter, transportService } from 'transport-pkg';
import { IAppPkg, AppRunPriority } from 'app-life-cycle-pkg';

import KafkaService from './services/kafka.service';

class KafkaTransportAdapter extends TransportAdapter implements IAppPkg {
  private kafkaService: KafkaService;

  constructor(brokerUrl: string, clientId: string) {
    super();

    this.kafkaService = new KafkaService(brokerUrl, clientId);
  }

  async init(): Promise<void> {
    const actionsToProduce: string[] = transportService.getBroadcastableActions();
    const actionsToConsume: Record<string, (req: CorrelatedMessage) => Promise<void>> = transportService.getSubscribedBroadcastableActions();

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
          await actionsToConsume[action](message as CorrelatedMessage);
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

  async broadcast(req: CorrelatedMessage): Promise<void> {
    await this.kafkaService.sendMessage(req.action, req);
  }
}

export default KafkaTransportAdapter;
