DROP TABLE IF EXISTS aldykarinacp_coderhouse.stage_api_alphavantage;

CREATE TABLE stage_api_alphavantage(

    date TIMESTAMP,
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    symbol VARCHAR(10),
    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
