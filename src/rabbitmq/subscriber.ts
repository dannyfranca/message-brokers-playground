import { connect, Connection, Channel } from 'amqplib'

import { rabbitMqUrl } from '@/config'
import { Events } from '@/typings'

export const createSubscriber = () => new RabbitMqSubscriber<Events>()

export class RabbitMqSubscriber<E> {
  private handlers = {} as { [K in keyof E]: (payload: E[K]) => any }
  private conn: Connection | undefined
  private channel: Channel | undefined
  private starting = false
  private assertions: { [k: string]: boolean } = {}

  constructor(options?: {
    handlers?: { [K in keyof E]: (payload: E[K]) => any }
  }) {
    if (options?.handlers) this.handlers = { ...options.handlers }
  }

  async start() {
    if (this.starting || this.channel) return
    this.starting = true
    try {
      this.conn = await connect(rabbitMqUrl)
      this.channel = await this.conn.createChannel()
      for (const queue of Object.keys(this.handlers)) {
        if (!this.assertions[queue]) {
          await this.channel!.assertQueue(queue)
          this.assertions[queue] = true
        }
        const ch = this.channel!
        await ch.consume(queue, async (message) => {
          if (!message) return
          try {
            await this.handlers[queue as keyof E](
              JSON.parse(
                message.content?.toString() ?? JSON.stringify(undefined)
              )
            )
            ch.ack(message)
          } catch {
            ch.nack(message)
          }
        })
      }
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

  async subscribe<K extends keyof E>(
    queue: K & string,
    handler: (payload: E[K]) => any
  ) {
    this.handlers[queue] = handler
  }
}
