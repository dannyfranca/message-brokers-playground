import { connect, Connection, Channel } from 'amqplib'

import { Events } from '@/typings'
import { rabbitMqUrl } from '@/config'

export const createPublisher = () => new RabbitMqPublisher<Events>()

export class RabbitMqPublisher<E> {
  private conn: Connection | undefined
  private channel: Channel | undefined
  private starting = false
  private assertions: { [k: string]: boolean } = {}

  async start() {
    if (this.starting || this.channel) return
    this.starting = true
    try {
      this.conn = await connect(rabbitMqUrl)
      this.channel = await this.conn.createChannel()
    } catch (error) {
      this.starting = false
      throw error
    }
    this.starting = false
  }

  async stop() {
    await this.channel!.close()
    await this.conn!.close()
    delete this.channel
    delete this.conn
  }

  async publish<K extends keyof E & string>(queue: K, message: E[K]) {
    if (!this.assertions[queue]) {
      await this.channel!.assertQueue(queue)
      this.assertions[queue] = true
    }
    await this.channel!.sendToQueue(
      queue,
      Buffer.from(JSON.stringify(message)),
      { mandatory: true }
    )
  }
}
