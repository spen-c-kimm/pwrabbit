import { Connection, Channel, ConsumeMessage } from 'amqplib'

interface Credentials {
  RABBIT_USER: string
  RABBIT_PASSWORD: string
  RABBIT_HOST: string
  RABBIT_PORT: string
}

interface ConnectOptions {
  exchange?: string
  queue?: string
  credentials: Credentials
}

interface SendOptions {
  exchange: string
  queue: string
  type: string
  data: Record<string, any>
}

interface Events {
  [key: string]: (message: ConsumeMessage) => Promise<void>
}

export default interface PurpleWaveRabbit {
  connection: Connection | null
  channel: Channel | null
  connect: (options: ConnectOptions) => Promise<void>
  reconnect: (options: ConnectOptions) => Promise<void>
  send: (options: SendOptions) => Promise<void>
  listen: (queue: string, events: Events) => Promise<void>
}
