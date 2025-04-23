const fs = require('fs');
const csv = require('fast-csv');
const db = require('../db/queries');
const logger = require('../utils/logger');

async function loadCSV(filePath) {
  const stream = fs.createReadStream(filePath);
  const parser = csv.parse({ headers: true });

  stream.pipe(parser)
    .on('data', async (row) => {
      try {
        await db.upsertCustomer(row);
        await db.upsertProduct(row);
        await db.insertOrder(row);
        await db.insertOrderItem(row);
      } catch (err) {
        logger.error('Row processing error:', err);
      }
    })
    .on('end', async () => {
      await db.logRefresh('success', 'CSV load completed');
      logger.info('CSV load complete');
    })
    .on('error', async (err) => {
      await db.logRefresh('error', err.message);
      logger.error('CSV load failed:', err);
    });
}

module.exports = loadCSV;
