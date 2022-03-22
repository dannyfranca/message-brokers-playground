import { kafkaBootstrapServers } from '@/config'
import { Kafka } from 'kafkajs'

export const createKafkaClient = (clientId?: string) =>
  new Kafka({
    clientId,
    brokers: kafkaBootstrapServers?.split(',') ?? [],
  })
