import { CorrelatedMessage, TransportAdapter, transportService } from 'transport-pkg';
import { IAppPkg, AppRunPriority } from 'app-life-cycle-pkg';
import { serviceDiscoveryService, ServiceDTO } from 'service-discovery-pkg';

import KafkaService from './services/kafka.service';

class KafkaTransportAdapter extends TransportAdapter implements IAppPkg {
  private kafkaService: KafkaService | null = null;
  private clientId: string = '';

  constructor(clientId: string) {
    super();

    this.clientId = clientId;
  }

  async init(): Promise<void> {
    const kafkaService: ServiceDTO = await serviceDiscoveryService.getService('kafka');

    this.kafkaService = new KafkaService(`${kafkaService.host}:${kafkaService.port}`, this.clientId);

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
          await actionsToConsume[action](CorrelatedMessage.fromObject(message));
        }
      });
    }

    await this.kafkaService.connectProducer();
    await this.kafkaService.runConsumer();
  }

  async shutdown(): Promise<void> {
    await this.kafkaService?.disconnectProducer();
    await this.kafkaService?.disconnectConsumer();
  }

  getPriority(): number {
    return AppRunPriority.Lowest;
  }

  getName(): string {
    return 'kafka-transport-adapter';
  }

  getDependencies(): IAppPkg[] {
    return [
      transportService,
      serviceDiscoveryService
    ];
  }

  async broadcast(req: CorrelatedMessage): Promise<void> {
    await this.kafkaService?.sendMessage(req.action, req);
  }
}

export default KafkaTransportAdapter;
