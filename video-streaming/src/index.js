/***
It streams videos from storage to be watched by the user.
external cloud storage -> video-storage -> video-streaming -> gateway -> user UI
                                                |
                         												-> RabbitMQ (viewed message) -> history
***/
//Load the express library (the de facto standard framework for building HTTP servers on Node.js).
const express = require("express");
//To send and receive messages with RabbitMQ.
const amqp = require("amqplib");
//Load the http library to forward an HTTP request from one container to another.
const http = require("http");
const winston = require('winston');

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_NAME = "video-streaming";
const SVC_DNS_RABBITMQ = process.env.SVC_DNS_RABBITMQ;
const SVC_DNS_VIDEO_STORAGE = process.env.SVC_DNS_VIDEO_STORAGE;
const PORT = process.env.PORT && parseInt(process.env.PORT) || 3000;
const MAX_RETRIES = process.env.MAX_RETRIES && parseInt(process.env.MAX_RETRIES) || 10;
let READINESS_PROBE = false;

/***
Resume Operation
----------------
The resume operation strategy intercepts unexpected errors and responds by allowing the process to
continue.
***/
process.on('uncaughtException',
err => {
  logger.error(`${SVC_NAME} - Uncaught exception.`);
  logger.error(`${SVC_NAME} - ${err}`);
  logger.error(`${SVC_NAME} - ${err.stack}`);
})

/***
Abort and Restart
-----------------
***/
// process.on("uncaughtException",
// err => {
//   console.error("Uncaught exception:");
//   console.error(err && err.stack || err);
//   process.exit(1);
// })

//Winston requires at least one transport (location to save the log) to create a log.
const logConfiguration = {
  transports: [ new winston.transports.Console() ],
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSSSS' }),
    winston.format.printf(msg => `${msg.timestamp} ${msg.level} ${msg.message}`)
  ),
  exitOnError: false
}

//Create a logger and pass it the Winston configuration object.
const logger = winston.createLogger(logConfiguration);

/***
Unlike most other programming languages or runtime environments, Node.js doesn't have a built-in
special "main" function to designate the entry point of a program.

Accessing the main module
-------------------------
When a file is run directly from Node.js, require.main is set to its module. That means that it is
possible to determine whether a file has been run directly by testing require.main === module.
***/
if (require.main === module) {
  main()
  .then(() => {
    READINESS_PROBE = true;
    logger.info(`${SVC_NAME} - Microservice is listening on port "${PORT}"!`);
  })
  .catch(err => {
    logger.error(`${SVC_NAME} - Microservice failed to start.`);
    logger.error(`${SVC_NAME} - ${err}`);
    logger.error(`${SVC_NAME} - ${err.stack}`);
  });
}

function main() {
  //Throw an exception if any required environment variables are missing.
  if (process.env.SVC_DNS_RABBITMQ === undefined) {
    throw new Error('Please specify the name of the service DNS for RabbitMQ in the environment variable SVC_DNS_RABBITMQ.');
  }
  else if (process.env.SVC_DNS_VIDEO_STORAGE === undefined) {
    throw new Error('Please specify the service DNS in the environment variable SVC_DNS_VIDEO_STORAGE.');
  }
  //Display a message if any optional environment variables are missing.
  else {
    if (process.env.PORT === undefined) {
      logger.info(`${SVC_NAME} - The environment variable PORT for the HTTP server is missing; using port ${PORT}.`);
    }
    //
    if (process.env.MAX_RETRIES === undefined) {
      logger.info(`${SVC_NAME} - The environment variable MAX_RETRIES is missing; using MAX_RETRIES=${MAX_RETRIES}.`);
    }
  }
  //Notify when server has started.
  return requestWithRetry(connectToRabbitMQ, SVC_DNS_RABBITMQ, MAX_RETRIES)  //Connect to RabbitMQ...
  .then(channel => {                  //then...
    return startHttpServer(channel);  //start the HTTP server.
  });
}

function connectToRabbitMQ(url, currentRetry) {
  logger.info(`${SVC_NAME} - Connecting (${currentRetry}) to 'RabbitMQ' at ${url}.`);
  /***
  return connect()
    .then(conn =>
    {
      //Create a RabbitMQ messaging channel.
      return conn.createChannel()
        .then(channel =>
        {
          //Assert that we have a "viewed" exchange.
          return channel.assertExchange('viewed', 'fanout')
            .then(() =>
            {
              return channel;
            });
        });
    });
  ***/
  return amqp.connect(url)
  .then(conn => {
    logger.info(`${SVC_NAME} - Connected to RabbitMQ.`);
    //Create a RabbitMQ messaging channel.
    return conn.createChannel()
    .then(channel => {
      //Assert that we have a "viewed" queue.
      return channel.assertQueue('viewed', { exclusive: false })
      .then(() => {
        return channel;
      });
    });
  });
}

async function sleep(timeout) {
  return new Promise(resolve => {
    setTimeout(() => { resolve(); }, timeout);
  });
}

async function requestWithRetry(func, url, maxRetry) {
  for (let currentRetry = 0;;) {
    try {
      ++currentRetry;
      return await func(url, currentRetry);
    }
    catch(err) {
      if (currentRetry === maxRetry) {
        //Save the error from the most recent attempt.
        lastError = err;
        logger.info(`${SVC_NAME} - Maximum number of ${maxRetry} retries has been reached.`);
        break;
      }
      const timeout = (Math.pow(2, currentRetry) - 1) * 100;
      logger.log(`${SVC_NAME} - Waiting ${timeout}ms...`);
      await sleep(timeout);
    }
  }
  //Throw the error from the last attempt; let the error bubble up to the caller.
  throw lastError;
}

//Start the HTTP server.
function startHttpServer(channel) {
  //Notify when the server has started.
  return new Promise(resolve => {
    setupHandlers(channel);
    app.listen(PORT,
    () => {
      //HTTP server is listening, resolve the promise.
      resolve();
    });
  });
}

//Setup event handlers.
function setupHandlers(channel) {
  //Readiness probe.
  app.get('/readiness',
  (req, res) => {
    res.sendStatus(READINESS_PROBE === true ? 200 : 500);
  });
  //
  //Route for streaming video.
  app.get('/video',
  (req, res) => {
    const cid = req.headers['X-Correlation-Id'];
    const videoId = req.query.id;
    logger.info(`${SVC_NAME} ${cid} - Retrieving video '${videoId}' from the video storage service.`);
    //Forward the request to the video storage microservice.
    const forwardReq = http.request({
      host: SVC_DNS_VIDEO_STORAGE,
      path: `/video?id=${videoId}`,
      method: 'GET',
      headers: req.headers
    },
    forwardRes => {
      res.writeHeader(forwardRes.statusCode, forwardRes.headers);
      forwardRes.pipe(res);
    });
    req.pipe(forwardReq);
    //Send "viewed" message to indicate this video has been watched.
    //sendMultipleRecipientMessage(channel, videoId);
    sendSingleRecipientMessage(channel, videoId, cid);
  });
}

/***
function sendMultipleRecipientMessage(channel, videoId)
{
  console.log('Publishing message on "viewed" exchange.');
  const msg = { video: { id: videoId } };
  const jsonMsg = JSON.stringify(msg);
  //Publish message to the "viewed" exchange.
  channel.publish('viewed', '', Buffer.from(jsonMsg));
}
***/

function sendSingleRecipientMessage(channel, videoId, cid) {
  logger.info(`${SVC_NAME} ${cid} - Publishing message on "viewed" queue.`);
  //Define the message payload. This is the data that will be sent with the message.
  const msg = { video: { id: videoId, cid: cid } };
  //Convert the message to the JSON format.
  const jsonMsg = JSON.stringify(msg);
  //In RabbitMQ a message can never be sent directly to the queue, it always needs to go through an
  //exchange. Use the default exchange identified by an empty string; publish the message to the
  //"viewed" queue.
  channel.publish('', 'viewed', Buffer.from(jsonMsg));
}
