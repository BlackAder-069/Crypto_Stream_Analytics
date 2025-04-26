CREATE DATABASE IF NOT EXISTS cryptodb;
USE cryptodb;

CREATE TABLE IF NOT EXISTS stream_agg (
    start_time DATETIME,
    end_time DATETIME,
    asset_id_base VARCHAR(10),
    asset_id_quote VARCHAR(10),
    update_count INT,
    avg_rate DOUBLE,
    min_rate DOUBLE,
    max_rate DOUBLE,
    sum_rate DOUBLE
);

CREATE TABLE IF NOT EXISTS raw_coin_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    asset_id_base VARCHAR(10),
    asset_id_quote VARCHAR(10),
    rate DOUBLE,
    time DATETIME
);

CREATE TABLE IF NOT EXISTS coin_update_count (
    start_time DATETIME,
    end_time DATETIME,
    asset_id_base VARCHAR(10),
    asset_id_quote VARCHAR(10),
    update_count INT
);