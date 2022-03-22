import { HandlerObject } from './typings'

export const handlers: HandlerObject = {
  test: (payload) => {
    console.log('---')
    console.log(payload)
    console.log('---')
  },
}
