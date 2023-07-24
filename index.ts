// RabbitMQ packages
import amqplib, { Connection, Channel, ConsumeMessage } from 'amqplib'
import { EventEmitter } from 'events'

// Logger packages
import { createLogger, transports } from 'winston'
import { readFileSync } from 'fs'
import { format } from 'logform'
import 'winston-daily-rotate-file'

// Type declarations
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

interface PurpleWaveRabbit {
  connection: Connection | null
  channel: Channel | null
  connect: (options: ConnectOptions) => Promise<void>
  reconnect: (options: ConnectOptions) => Promise<void>
  send: (options: SendOptions) => Promise<void>
  listen: (queue: string, events: Events) => Promise<void>
}

// Logger implementation
const pkg = JSON.parse(readFileSync('./package.json', 'utf8'))
const { combine, timestamp, label, metadata, json, errors } = format

const combinedTransport = new transports.DailyRotateFile({
  filename: `${pkg.name}-combined-%DATE%.log`,
  dirname: `/var/log/services/${pkg.name}/`,
  datePattern: 'YYYY-MM-DD-HH',
  zippedArchive: true,
  maxSize: '20m',
  maxFiles: '14d',
  createSymlink: true,
  symlinkName: 'combined.log',
})

const errorTransport = new transports.DailyRotateFile({
  filename: `${pkg.name}-error-%DATE%.log`,
  dirname: `/var/log/services/${pkg.name}/`,
  datePattern: 'YYYY-MM-DD-HH',
  zippedArchive: true,
  level: 'error',
  maxSize: '20m',
  maxFiles: '14d',
  createSymlink: true,
  symlinkName: 'errors.log',
})

const logger = createLogger({
  level: 'info',
  format: combine(
    label({ label: pkg.name }),
    errors({ stack: true }),
    timestamp({
      format: 'YYYY-MM-DD HH:mm:ss',
    }),
    metadata(),
    json()
  ),
  transports: [new transports.Console(), combinedTransport, errorTransport],
})

// Set the default max listeners to 100
EventEmitter.defaultMaxListeners = 100

// Rabbit implementation
const Rabbit: PurpleWaveRabbit = {
  connection: null,
  channel: null,

  // Make a rabbit connection and assert the given exchange/queue
  connect: async options => {
    try {
      // Extract the exchange, queue, and credentials from the options
      const { exchange, queue, credentials } = options

      // Extract the connection credentials
      const { RABBIT_USER, RABBIT_PASSWORD, RABBIT_HOST, RABBIT_PORT } =
        credentials

      // Get the rabbit connection, otherwise make one
      Rabbit.connection = Rabbit.connection
        ? Rabbit.connection
        : await amqplib.connect(
            `amqp://${RABBIT_USER}:${RABBIT_PASSWORD}@${RABBIT_HOST}:${RABBIT_PORT}/`
          )

      // Add event listeners to reconnect when the connection is lost
      Rabbit.connection.once('close', async () => {
        logger.error('Rabbit connection closed')
        Rabbit.reconnect(options)
      })

      Rabbit.connection.once('error', async error => {
        logger.error('Rabbit connection error', error)
        Rabbit.reconnect(options)
      })

      // Get the rabbit channel, otherwise make one
      Rabbit.channel = Rabbit.channel
        ? Rabbit.channel
        : await Rabbit.connection.createChannel()

      if (queue) {
        // Assert the dead letter exchange
        await Rabbit.channel.assertExchange('dlx_exchange', 'direct', {
          durable: true,
        })

        // Assert the dead letter queue
        await Rabbit.channel.assertQueue('dlx_queue', { durable: true })

        // Bind the dead letter queue to the dead letter exchange
        await Rabbit.channel.bindQueue(
          'dlx_queue',
          'dlx_exchange',
          'dlx_routing_key'
        )

        // Assert the queue with the dead letter exchange
        await Rabbit.channel.assertQueue(queue, {
          durable: true,
          arguments: {
            'x-dead-letter-exchange': 'dlx_exchange',
            'x-dead-letter-routing-key': 'dlx_routing_key',
          },
        })

        logger.info(`Asserted queue ${queue}`)
      }

      if (exchange) {
        // Assert the exchange
        await Rabbit.channel.assertExchange(exchange, 'fanout', {
          durable: true,
        })

        logger.info(`Asserted exchange ${exchange}`)
      }

      // If both the exchange and queue were specified then bind the queue to the exchange
      if (exchange && queue) {
        await Rabbit.channel.bindQueue(queue, exchange, '')

        logger.info(`Bound queue ${queue} to the ${exchange} exchange`)
      }
    } catch (error) {
      logger.error('Rabbit.connect', error)
    }
  },

  // Reconnect to rabbit after waiting 5 seconds
  reconnect: async options => {
    try {
      logger.info('Attempting to reconnect in 5 seconds...')
      setTimeout(() => Rabbit.connect(options), 5000)
    } catch (error) {
      logger.error('Rabbit.reconnect', error)
    }
  },

  // Send a message to the given exchange/queue
  send: async options => {
    try {
      // If there is no channel then end execution
      if (!Rabbit.channel) throw new Error('Channel not found')

      // Extract the exchange, queue, type, and data from the options
      const { exchange, queue, type, data } = options

      // Legacy data structure to support message lib
      const legacyData = Buffer.from(
        JSON.stringify({
          ...data,
          source: exchange,
          eventType: type,
          data,
        })
      )

      // Publish the message to the queue
      if (queue) Rabbit.channel.sendToQueue(queue, legacyData)

      // Publish the message to the exchange
      if (exchange) Rabbit.channel.publish(exchange, '', legacyData, { type })
    } catch (error) {
      logger.error('Rabbit.send failed', options, error)
    }
  },

  // Consume a message from the queue
  listen: async (queue, events) => {
    try {
      // If there is no channel then end execution
      if (!Rabbit.channel) throw new Error('Channel not found')

      // Consume messages that are sent to the queue
      await Rabbit.channel.consume(queue, async message => {
        if (message) {
          // Parse out the properties and data from the message
          const { properties, content } = message
          const data = JSON.parse(content.toString())
          const eventType = properties.type || data.eventType

          // Grab the event from the events object
          const event = events[eventType]

          // If the event doesn't exist then acknowledge the message and end execution
          if (!event) return Rabbit.channel?.ack(message)

          const callback = async (retry = 0) => {
            try {
              // Process the message
              await event(data)

              // If no error was thrown then acknowledge the message
              Rabbit.channel?.ack(message)
            } catch (error) {
              logger.error(
                `Message type ${eventType} failed on attempt ${
                  retry + 1
                } on the ${queue} queue at ${new Date()}`
              )

              // Attempt the event callback a maximum of 5 times
              if (retry < 5) callback(retry + 1)
              // Send the message to the dead letter queue with the original queue name in the headers
              else Rabbit.channel?.reject(message, false)
            }
          }

          await callback()
        }
      })
    } catch (error) {
      logger.error('Rabbit.listen', error)
    }
  },
}

export default Rabbit
