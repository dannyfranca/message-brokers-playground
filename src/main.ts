import { handlers } from './handlers'
import { createPublisher } from './rabbitmq/publisher'
import { createSubscriber } from './rabbitmq/subscriber'

async function bootstrap() {
  const producer = createPublisher()
  const subscriber = createSubscriber()

  await producer.start()
  console.log('app running')

  await producer.publish('test', { message: 'my message' })
  await subscriber.subscribe('test', handlers.test)
  await subscriber.start()
  await producer.stop()
  console.log('app stopping')
}
bootstrap()
