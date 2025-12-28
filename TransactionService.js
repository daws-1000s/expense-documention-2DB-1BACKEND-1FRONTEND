const mysql = require('mysql2');
const dbcreds = require('./DbConfig');

// Parse DB_HOST (comma-separated) and create connections
const dbHosts = (process.env.DB_HOST || dbcreds.DB_HOST).split(',');
const connections = dbHosts.map(host => mysql.createConnection({
    host: host.trim(),
    user: process.env.DB_USER || dbcreds.DB_USER,
    password: process.env.DB_PWD || dbcreds.DB_PWD,
    database: process.env.DB_DATABASE || dbcreds.DB_DATABASE
}));

// Utility: execute query on all DBs (for writes)
function executeOnAllDBs(query, callback) {
    connections.forEach(con => {
        con.query(query, (err, result) => {
            if (err) console.error(`Error on host ${con.config.host}:`, err.message);
        });
    });
    if (callback) callback();
}

// Utility: execute query on first available DB (for reads)
function executeOnOneDB(query, callback) {
    const con = connections[0]; // read from first DB
    con.query(query, (err, result) => {
        if (err) {
            console.error(`Error reading from host ${con.config.host}:`, err.message);
            callback([]);
        } else {
            callback(result);
        }
    });
}

// Add a transaction to all DBs
function addTransaction(amount, desc) {
    const sql = `INSERT INTO transactions (amount, description) VALUES ('${amount}','${desc}')`;
    executeOnAllDBs(sql);
    return 200;
}

// Get all transactions from first DB
function getAllTransactions(callback) {
    const sql = "SELECT * FROM transactions";
    executeOnOneDB(sql, callback);
}

// Find a transaction by ID from first DB
function findTransactionById(id, callback) {
    const sql = `SELECT * FROM transactions WHERE id = ${id}`;
    executeOnOneDB(sql, callback);
}

// Delete all transactions from all DBs
function deleteAllTransactions(callback) {
    const sql = "DELETE FROM transactions";
    executeOnAllDBs(sql, callback);
}

// Delete a transaction by ID from all DBs
function deleteTransactionById(id, callback) {
    const sql = `DELETE FROM transactions WHERE id = ${id}`;
    executeOnAllDBs(sql, callback);
}

module.exports = {
    addTransaction,
    getAllTransactions,
    findTransactionById,
    deleteAllTransactions,
    deleteTransactionById
};
