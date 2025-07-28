import { CorrelatedRequestDTO } from 'transport-pkg';

export interface KafkaTopic {
  topic: string;
  num_partitions: number;
  replication_factor: number;
  on_message: (message: CorrelatedRequestDTO) => Promise<void>;
}
