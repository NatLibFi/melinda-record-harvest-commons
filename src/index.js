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
import {createPool as createDbPool} from 'mariadb';

export const statuses = {
  harvestPending: 'harvestPending',
  harvestDone: 'harvestDone',
  harvestError: 'harvestError',
  postProcessingDone: 'postProcessingDone'
};

export default async ({db}) => {
  const debug = createDebugLogger('@natlibfi/melinda-record-harvest-commons');
  const dbPool = await initializeDb();

  return {readState, writeState, getPool, close};

  function getPool() {
    return dbPool;
  }

  async function close() {
    await dbPool.end();
  }

  async function readState() {
    const connection = await dbPool.getConnection();
    const [{status, timestamp, resumption_token: token, resumption_cursor: cursor} = {}] = await connection.query('SELECT * FROM state');

    await connection.end();

    return {
      status: status || statuses.harvestPending,
      timestamp: timestamp ? moment(timestamp) : undefined,
      resumptionToken: token ? {token, cursor} : undefined
    };
  }

  async function writeState({status, error, timestamp = moment(), resumptionToken = {}} = {}, records = []) {
    debug('Writing state');

    const connection = await dbPool.getConnection();

    if (records.length > 0) {
      await connection.beginTransaction();

      await insertRecords();
      await updateState();

      await connection.commit();
      await connection.end();
      return;
    }

    await updateState();
    await connection.end();

    function updateState() {
      const parameters = [
        status,
        timestamp.toDate(),
        resumptionToken.token || null,
        resumptionToken.cursor || null,
        error || null
      ];

      return connection.query(`REPLACE INTO state SET
        id=0,
        status=?,
        timestamp=?,
        resumption_token=?,
        resumption_cursor=?,
        error=?`, parameters);
    }

    async function insertRecords() {
      // This returns a SQL error for some reason if the number of inserts is too long. So far can only be reproduced with the exact same payload so not filing a bug. Yet.
      //await connection.batch('INSERT INTO records SET id=?, record=?', records.map(({identifier, record}) => [identifier, record]));

      if (records.length > 500) {
        await connection.batch('INSERT INTO records SET id=?, record=?', records.slice(0, 500).map(toValues));
        return connection.batch('INSERT INTO records SET id=?, record=?', records.slice(500).map(toValues));
      }

      return connection.batch('INSERT INTO records SET id=?, record=?', records.map(toValues));

      function toValues({identifier, record}) {
        return [identifier, record];
      }
    }
  }

  async function initializeDb() {
    const dbPool = createDbPool({
      host: db.host,
      port: db.port,
      database: db.database,
      user: db.username,
      password: db.password,
      connectionLimit: db.connectionLimit
    });

    const connection = await dbPool.getConnection();

    await connection.query(`CREATE TABLE IF NOT EXISTS state (
      id INT NOT NULL,
      status ENUM('harvestPending', 'harvestDone', 'harvestError', 'postProcessingDone') NOT NULL,
      timestamp DATETIME NOT NULL,
      resumption_token VARCHAR(200),
      resumption_cursor INT,
      error TEXT,
      PRIMARY KEY (id)
    )`);

    await connection.query(`CREATE TABLE IF NOT EXISTS records (
      id INT NOT NULL UNIQUE,
      record JSON NOT NULL,
      PRIMARY KEY (id)
    )`);

    await connection.end();
    return dbPool;
  }
};
