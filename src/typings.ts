export type MessageInput = { [k: string]: any }

export interface Events {
  test: { message: string }
}

export type Handler<K extends keyof Events> = (payload: Events[K]) => any
export type HandlerObject = { [K in keyof Events]: Handler<K> }
