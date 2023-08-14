CREATE DATABASE IF NOT EXISTS a_share_stock_db;

CREATE TABLE IF NOT EXISTS a_share_stock_db.a_share_stock_daily_price
(
    `date`   Date,
    `code`   String COMMENT '股票代码',
    `open`   Float32 COMMENT '开盘价',
    `high`   Float32 COMMENT '最高价',
    `low`    Float32 COMMENT '最低价',
    `close`  Float32 COMMENT '收盘价',
    `volume` Float64 COMMENT '成交量',
    `amount` Float64 COMMENT '成交额'
) ENGINE = ReplacingMergeTree()
      ORDER BY (javaHash(code), date);