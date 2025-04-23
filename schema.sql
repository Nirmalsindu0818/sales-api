CREATE TABLE IF NOT EXISTS customers (
  customer_id TEXT PRIMARY KEY,
  name TEXT,
  email TEXT,
  address TEXT
);

CREATE TABLE IF NOT EXISTS products (
  product_id TEXT PRIMARY KEY,
  name TEXT,
  category TEXT
);

CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT REFERENCES customers(customer_id),
  date_of_sale DATE,
  region TEXT,
  payment_method TEXT,
  shipping_cost NUMERIC
);

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id SERIAL PRIMARY KEY,
  order_id TEXT REFERENCES orders(order_id),
  product_id TEXT REFERENCES products(product_id),
  quantity_sold INTEGER,
  unit_price NUMERIC,
  discount NUMERIC
);

CREATE TABLE IF NOT EXISTS refresh_logs (
  id SERIAL PRIMARY KEY,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  status TEXT,
  message TEXT
);
