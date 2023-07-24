import { readFileSync } from 'fs'
import { createLogger, transports } from 'winston'
import { format } from 'logform'
import 'winston-daily-rotate-file'

const pkg = JSON.parse(readFileSync('./package.json', 'utf8'))
const { combine, timestamp, label, metadata, json, errors } = format

const logger = (() => {
  if (!process.env.NODE_ENV || process.env.NODE_ENV === 'testing') {
    return { info: console.log, error: console.error }
  }

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

  return createLogger({
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
})()

export default logger
