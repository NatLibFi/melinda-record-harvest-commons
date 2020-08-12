/**
*
* @licstart  The following is the entire license notice for the JavaScript code in this file.
*
* Shared modules for Melinda record harvest microservices
*
* Copyright (C) 2020 University Of Helsinki (The National Library Of Finland)
*
* This file is part of melinda-record-harvest-commons-js
*
* melinda-record-harvest-commons-js program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* melinda-record-harvest-commons-js is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.k
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
* @licend  The above is the entire license notice
* for the JavaScript code in this file.
*
*/

import moment from 'moment';
import createDebugLogger from 'debug';
import {MongoClient} from 'mongodb';
import {connect as amqpConnect} from 'amqplib';

export const statuses = {
  harvestPending: 'harvestPending',
  harvestDone: 'harvestDone',
  harvestError: 'harvestError',
  postProcessingDone: 'postProcessingDone'
};

export default ({mongoUri, amqpUri}) => {
  const debug = createDebugLogger('@natlibfi/melinda-record-harvest-commons');

  return {readState, writeState, handleQueues};

  async function readState() {
    const {client, db} = await getMongoClient();
    const doc = await db.collection('state').findOne({}) || {};

    await client.close();

    return {
      status: statuses.pendingHarvest,
      ...formatDoc(),
      timestamp: doc.timestamp ? moment(doc.timestamp) : undefined
    };

    function formatDoc() {
      return Object.entries(doc).
        filter(([k]) => k === '_id' === false)
        .reduce((a, [k, v]) => ({...a, [k]: v}), {});
    }
  }

  async function writeState(state, records = []) {
    const connection = await amqpConnect(amqpUri);
    const channel = await connection.createConfirmChannel();

    await channel.assertQueue('records', {durable: true});
    const {messageCount: totalTempMessageCount} = await channel.assertQueue('records-temp', {durable: true});

    await queueRecords(records.slice(), records.length);

    const timestamp = moment();
    const {client, db} = await getMongoClient();
    const doc = {
      $set: {...state, timestamp: timestamp.toDate()}
    };

    debug('Writing state');
    await db.collection('state').updateOne({}, doc, {upsert: true});
    await client.close();

    if (records.length > 0) {
      debug('Requeuing temp records');
      await handleTemporaryRecords({channel, timestamp, totalMessageCount: totalTempMessageCount});
      await channel.close();
      return connection.close();
    }

    await channel.close();
    return connection.close();

    async function queueRecords(records, totalCount, timestamp = moment()) {
      const [record] = records;

      if (record) {
        debug(`Sending record ${totalCount - records.length}/${totalCount} to temporary queue`);

        const content = Buffer.from(JSON.stringify(record.toObject()));
        await sendMessage({channel, content, queue: 'records-temp', timestamp: timestamp.valueOf()});
        return queueRecords(records.slice(1), totalCount, timestamp);
      }
    }
  }

  async function handleQueues() {
    const connection = await amqpConnect(amqpUri);
    const channel = await connection.createConfirmChannel();
    const {timestamp} = await readState();

    await channel.assertQueue('records', {durable: true});
    const {messageCount: totalMessageCount} = await channel.assertQueue('records-temp', {durable: true});

    const {discardedMessageCount, forwardedMessageCount} = await handleTemporaryRecords({channel, timestamp, totalMessageCount});
    debug(`Forwarded ${forwardedMessageCount} messages, discarded ${discardedMessageCount}`);

    await channel.close();
    return connection.close();
  }

  async function handleTemporaryRecords({channel, totalMessageCount, timestamp, forwardedMessageCount = 0, discardedMessageCount = 0}) {
    const message = await channel.get('records-temp');

    if (message) {
      debug(`Handling message ${forwardedMessageCount + discardedMessageCount + 1}/${totalMessageCount}`);
      const messageTimestamp = moment(message.properties.timestamp);

      if (timestamp && timestamp.isAfter(messageTimestamp)) {
        await sendMessage({channel, queue: 'records', content: message.content});
        await channel.ack(message);
        return handleTemporaryRecords({channel, totalMessageCount, timestamp, discardedMessageCount, forwardedMessageCount: forwardedMessageCount + 1});
      }

      await channel.ack(message);
      return handleTemporaryRecords({channel, totalMessageCount, timestamp, forwardedMessageCount, discardedMessageCount: discardedMessageCount + 1});
    }

    return {discardedMessageCount, forwardedMessageCount};
  }

  function sendMessage({channel, queue, content, ...options}) {
    return new Promise((resolve, reject) => {
      channel.sendToQueue(queue, content, {
        ...options, persistent: true
      }, err => err ? reject(err) : resolve());
    });
  }

  /* Async function getRecords() {
    const {timestamp: stateTimestamp} = await readState();
    const connection = await amqpConnect(amqpUri);
    const channel = await connection.createChannel();

    await channel.assertQueue('records', {durable: true});

    const emitter = new class extends EventEmitter {}();
    consume();
    return emitter;

    function consume() {
      channel.consume(activeQueue, message => {
        const {messageTimestamp, record} = parse();

        if (moment(messageTimestamp).isAfter(stateTimestamp)) {
          emitter.removeAllListeners('record');
          emitter.removeAllListeners('end');
          emitter.emit('error', new Error('Message is newer than state'));
          channel.nack(message);
          return;
        }

        const {ack} = channel;
        emitter.emit('record', {record, ack});

        function parse() {
          const {content, properties: {timestamp}} = mesage;
          const recordObj = JSON.parse(content.toString());
          return {
            messageTimestamp: moment(timestamp),
            record: new MarcRecord(recordObj, {subfieldValues: false, subfields: false})
          };
        }
      });
    }
  }*/

  async function getMongoClient() {
    const client = new MongoClient(mongoUri, {useUnifiedTopology: true});
    await client.connect();
    const db = client.db();
    return {client, db};
  }
};
