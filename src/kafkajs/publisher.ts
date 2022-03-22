import { Kafka, Producer } from 'kafkajs'

import { Events } from '@/typings'
import { createKafkaClient } from './client'

export const createPublisher = (kafka?: Kafka) =>
  new KafkaJsPublisher<Events>(kafka ?? createKafkaClient())

export class KafkaJsPublisher<E> {
  private producer: Producer

  constructor(private kafka: Kafka) {
    this.producer = kafka.producer({
      allowAutoTopicCreation: true,
      idempotent: true,
    })
  }

  async start() {
    await this.producer.connect()
  }

  async stop() {
    await this.producer.disconnect()
  }

  async publish<K extends keyof E & string>(
    topic: K,
    message: E[K],
    key?: string
  ) {
    await this.producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(message) }],
      acks: -1,
    })
  }
}
