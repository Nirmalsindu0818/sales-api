const { Pool } = require('pg');

const pool = new Pool({
    host: 'localhost', // Your database host
    user: 'nirmal', // Your database username
    password: 'nirmal', // Your database password
    database: 'test', // Your database name
    port: 5432, // Default PostgreSQL port
});

module.exports = pool;