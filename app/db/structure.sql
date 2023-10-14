CREATE DATABASE metasync_production;

\c metasync_production 

-- Create the "users" table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(30) NOT NULL,
    email VARCHAR(30) UNIQUE,
    password VARCHAR(100) NOT NULL
);

-- Create the "connections" table
CREATE TABLE IF NOT EXISTS connections (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    adapter VARCHAR(30) NOT NULL,
    conn_name VARCHAR(100) NOT NULL,
    host VARCHAR(100) NOT NULL,
    port INTEGER NOT NULL,
    user VARCHAR(200) NOT NULL,
    password VARCHAR(200) NOT NULL,
    db_name VARCHAR(200) NOT NULL,
    notes VARCHAR(200),
    debezium_config JSON,
    FOREIGN KEY (user_id) REFERENCES users (id)
);
