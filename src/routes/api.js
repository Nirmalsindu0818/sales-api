const express = require('express');
const loadCSV = require('../loaders/csvLoader');
const revenueService = require('../services/revenueService');
const router = express.Router();

router.post('/refresh', async (req, res) => {
  try {
    await loadCSV('./data/sales.csv');
    res.status(200).send({ message: 'Data refresh triggered.' });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

const extractRange = (req) => [req.query.start_date, req.query.end_date];

router.get('/revenue/total', async (req, res) => {
  try {
    const total = await revenueService.getTotalRevenue(...extractRange(req));
    res.send({ ...req.query, total_revenue: total });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/revenue/by-category', async (req, res) => {
  try {
    const results = await revenueService.getRevenueByCategory(...extractRange(req));
    res.send({ ...req.query, revenue_by_category: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/revenue/by-product', async (req, res) => {
  try {
    const results = await revenueService.getRevenueByProduct(...extractRange(req));
    res.send({ ...req.query, revenue_by_product: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/revenue/by-region', async (req, res) => {
  try {
    const results = await revenueService.getRevenueByRegion(...extractRange(req));
    res.send({ ...req.query, revenue_by_region: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

// New endpoints
router.get('/top-products/overall', async (req, res) => {
  try {
    const results = await revenueService.getTopProductsOverall(...extractRange(req));
    res.send({ ...req.query, top_products: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/top-products/by-category', async (req, res) => {
  try {
    const results = await revenueService.getTopProductsByCategory(...extractRange(req));
    res.send({ ...req.query, top_products_by_category: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/top-products/by-region', async (req, res) => {
  try {
    const results = await revenueService.getTopProductsByRegion(...extractRange(req));
    res.send({ ...req.query, top_products_by_region: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/customers/total', async (req, res) => {
  try {
    const results = await revenueService.getTotalCustomers(...extractRange(req));
    res.send({ ...req.query, total_customers: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/orders/total', async (req, res) => {
  try {
    const results = await revenueService.getTotalOrders(...extractRange(req));
    res.send({ ...req.query, total_orders: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/orders/average-value', async (req, res) => {
  try {
    const results = await revenueService.getAverageOrderValue(...extractRange(req));
    res.send({ ...req.query, average_order_value: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/products/profit-margin', async (req, res) => {
  try {
    const results = await revenueService.getProfitMarginByProduct(...extractRange(req));
    res.send({ ...req.query, profit_margin_by_product: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/customers/lifetime-value', async (req, res) => {
  try {
    const results = await revenueService.getCustomerLifetimeValue(...extractRange(req));
    res.send({ ...req.query, customer_lifetime_value: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

router.get('/customers/segmentation', async (req, res) => {
  try {
    const results = await revenueService.getCustomerSegmentation(...extractRange(req));
    res.send({ ...req.query, customer_segmentation: results });
  } catch (error) {
    res.status(500).send({ error: error.message });
  }
});

module.exports = router;