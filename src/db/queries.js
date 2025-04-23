
const pool = require('./dbconnect');

async function upsertCustomer(row) {
  await pool.query(`
    INSERT INTO customers (customer_id, name, email, address)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (customer_id) DO UPDATE
    SET name = EXCLUDED.name, email = EXCLUDED.email, address = EXCLUDED.address
  `, [row['Customer ID'], row['Customer Name'], row['Customer Email'], row['Customer Address']]);
}

async function upsertProduct(row) {
  await pool.query(`
    INSERT INTO products (product_id, name, category)
    VALUES ($1, $2, $3)
    ON CONFLICT (product_id) DO UPDATE
    SET name = EXCLUDED.name, category = EXCLUDED.category
  `, [row['Product ID'], row['Product Name'], row['Category']]);
}

async function insertOrder(row) {
  await pool.query(`
    INSERT INTO orders (order_id, customer_id, date_of_sale, region, payment_method, shipping_cost)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (order_id) DO NOTHING
  `, [row['Order ID'], row['Customer ID'], row['Date of Sale'], row['Region'], row['Payment Method'], row['Shipping Cost']]);
}

async function insertOrderItem(row) {
  await pool.query(`
    INSERT INTO order_items (order_id, product_id, quantity_sold, unit_price, discount)
    VALUES ($1, $2, $3, $4, $5)
  `, [row['Order ID'], row['Product ID'], row['Quantity Sold'], row['Unit Price'], row['Discount']]);
}

async function logRefresh(status, message) {
  await pool.query(`
    INSERT INTO refresh_logs (status, message)
    VALUES ($1, $2)
  `, [status, message]);
}

module.exports = {
  pool,
  upsertCustomer,
  upsertProduct,
  insertOrder,
  insertOrderItem,
  logRefresh
};
