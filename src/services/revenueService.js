// sales-api/src/services/revenueService.js
const db = require('../db/queries');

// Total Revenue Calculation
async function getTotalRevenue(start, end) {
  const result = await db.pool.query(
    `SELECT SUM(quantity_sold * unit_price * (1 - discount)) AS revenue
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.date_of_sale BETWEEN $1 AND $2`,
    [start, end]
  );
  return result.rows[0].revenue || 0;
}

// Revenue by Category
async function getRevenueByCategory(start, end) {
  const result = await db.pool.query(
    `SELECT p.category, SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) AS revenue
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY p.category
     ORDER BY revenue DESC`,
    [start, end]
  );
  return result.rows;
}

// Revenue by Product
async function getRevenueByProduct(start, end) {
  const result = await db.pool.query(
    `SELECT p.name AS product_name, SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) AS revenue
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY p.name
     ORDER BY revenue DESC`,
    [start, end]
  );
  return result.rows;
}

// Revenue by Region
async function getRevenueByRegion(start, end) {
  const result = await db.pool.query(
    `SELECT o.region, SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) AS revenue
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY o.region
     ORDER BY revenue DESC`,
    [start, end]
  );
  return result.rows;
}

// Top N Products by Quantity Sold - Overall
async function getTopNProductsOverall(start, end, limit = 10) {
  const result = await db.pool.query(
    `SELECT p.name AS product_name, SUM(oi.quantity_sold) AS quantity_sold
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY p.name
     ORDER BY quantity_sold DESC
     LIMIT $3`,
    [start, end, limit]
  );
  return result.rows;
}

// Top N Products by Category
async function getTopNProductsByCategory(start, end, limit = 10) {
  const result = await db.pool.query(
    `SELECT p.category, p.name AS product_name, SUM(oi.quantity_sold) AS quantity_sold
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY p.category, p.name
     ORDER BY quantity_sold DESC
     LIMIT $3`,
    [start, end, limit]
  );
  return result.rows;
}

// Top N Products by Region
async function getTopNProductsByRegion(start, end, limit = 10) {
  const result = await db.pool.query(
    `SELECT o.region, p.name AS product_name, SUM(oi.quantity_sold) AS quantity_sold
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY o.region, p.name
     ORDER BY quantity_sold DESC
     LIMIT $3`,
    [start, end, limit]
  );
  return result.rows;
}

// Total Number of Customers
async function getTotalCustomers(start, end) {
  const result = await db.pool.query(
    `SELECT COUNT(DISTINCT customer_id) AS total_customers
     FROM orders o
     WHERE o.date_of_sale BETWEEN $1 AND $2`,
    [start, end]
  );
  return result.rows[0].total_customers;
}

// Total Number of Orders
async function getTotalOrders(start, end) {
  const result = await db.pool.query(
    `SELECT COUNT(*) AS total_orders
     FROM orders o
     WHERE o.date_of_sale BETWEEN $1 AND $2`,
    [start, end]
  );
  return result.rows[0].total_orders;
}

// Average Order Value
async function getAverageOrderValue(start, end) {
  const result = await db.pool.query(
    `SELECT AVG(total_value) AS avg_order_value
     FROM (
       SELECT SUM(quantity_sold * unit_price * (1 - discount)) AS total_value
       FROM order_items oi
       JOIN orders o ON o.order_id = oi.order_id
       WHERE o.date_of_sale BETWEEN $1 AND $2
       GROUP BY o.order_id
     ) AS order_totals`,
    [start, end]
  );
  return result.rows[0].avg_order_value || 0;
}

// Profit Margin by Product
async function getProfitMarginByProduct(start, end) {
  const result = await db.pool.query(
    `SELECT p.name AS product_name, 
            SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) AS revenue,
            SUM(oi.quantity_sold * oi.unit_price) AS total_sales,
            (SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) - SUM(oi.quantity_sold * oi.unit_price)) / SUM(oi.quantity_sold * oi.unit_price) AS profit_margin
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     JOIN products p ON p.product_id = oi.product_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY p.name
     ORDER BY profit_margin DESC`,
    [start, end]
  );
  return result.rows;
}

// Customer Lifetime Value (CLV) - An estimate of the total revenue from a customer
async function getCustomerLifetimeValue(start, end) {
  const result = await db.pool.query(
    `SELECT customer_id, 
            SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) AS clv
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY customer_id
     ORDER BY clv DESC`,
    [start, end]
  );
  return result.rows;
}

// Customer Segmentation - Segmenting customers based on their total spending (CLV)
async function getCustomerSegmentation(start, end) {
  const result = await db.pool.query(
    `SELECT customer_id,
            SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) AS total_spent,
            CASE
              WHEN SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) > 1000 THEN 'High'
              WHEN SUM(oi.quantity_sold * oi.unit_price * (1 - oi.discount)) BETWEEN 500 AND 1000 THEN 'Medium'
              ELSE 'Low'
            END AS customer_segment
     FROM order_items oi
     JOIN orders o ON o.order_id = oi.order_id
     WHERE o.date_of_sale BETWEEN $1 AND $2
     GROUP BY customer_id
     ORDER BY total_spent DESC`,
    [start, end]
  );
  return result.rows;
}

module.exports = {
  getTotalRevenue,
  getRevenueByCategory,
  getRevenueByProduct,
  getRevenueByRegion,
  getTopNProductsOverall,
  getTopNProductsByCategory,
  getTopNProductsByRegion,
  getTotalCustomers,
  getTotalOrders,
  getAverageOrderValue,
  getProfitMarginByProduct,
  getCustomerLifetimeValue,
  getCustomerSegmentation
};
