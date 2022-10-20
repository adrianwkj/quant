create table stock_hist(
    id serial primary key,
    symbol text,
    trade_date date,
    open float,
    close float,
    high float,
    low float,
    volume int,
    turnover float,
    amplitude float,
    change_percent float,
    change float,
    turnover_rate float
)

create table industry_hist(
    id serial primary key,
    symbol text,
    trade_date date,
    open float,
    close float,
    high float,
    low float,
    volume int,
    turnover float,
    amplitude float,
    change_percent float,
    change float,
    turnover_rate float
)

create table stock_hist_minute(
    id serial primary key,
    symbol text,
    period text,
    trade_datetime timestamp,
    open float,
    close float,
    high float,
    low float,
    latest float,
    volume int,
    turnover float,
    amplitude float,
    change_percent float,
    change float,
    turnover_rate float
)
