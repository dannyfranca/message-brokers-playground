import { Kafka, Consumer } from 'kafkajs'

import { Events } from '@/typings'
import { createKafkaClient } from './client'

export const createSubscriber = (groupId: string, kafka?: Kafka) =>
  new KafkaJsSubscriber<Events>({
    groupId,
    kafka: kafka ?? createKafkaClient(),
  })

export class KafkaJsSubscriber<E> {
  private consumer: Consumer
  private handlers = {} as { [K in keyof E]: (payload: E[K]) => any }
  private kafka: Kafka

  constructor({
    groupId,
    handlers,
    kafka,
  }: {
    groupId: string
    handlers?: { [K in keyof E]: (payload: E[K]) => any }
    kafka: Kafka
  }) {
    this.kafka = kafka
    this.consumer = this.kafka.consumer({
      allowAutoTopicCreation: true,
      groupId,
    })
    if (handlers) this.handlers = { ...handlers }
  }

  async start() {
    await this.consumer.connect()
    for (const event of Object.keys(this.handlers))
      await this.consumer.subscribe({ topic: event, fromBeginning: true })

    this.consumer.run({
      eachMessage: async ({ topic, message }) =>
        this.handlers[topic as keyof E](
          JSON.parse(message.value?.toString() ?? JSON.stringify(undefined))
        ),
    })
  }

  async stop() {
    await this.consumer.disconnect()
  }

  async subscribe<K extends keyof E>(
    event: K & string,
    handler: (payload: E[K]) => any
  ) {
    this.handlers[event] = handler
  }
}
