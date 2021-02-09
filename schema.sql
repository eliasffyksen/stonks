/*V0*/

CREATE TABLE Tickers (
    ticker VARCHAR(16) NOT NULL,
    PRIMARY KEY (ticker)
);

CREATE TABLE OHLC (
    date DATE NOT NULL,
    ticker VARCHAR(16) NOT NULL,
    volume INT NOT NULL,
    open DECIMAL(20,20) NOT NULL,
    high DECIMAL(20,20) NOT NULL,
    low DECIMAL(20,20) NOT NULL,
    close DECIMAL(20,20) NOT NULL,
    PRIMARY KEY (date, ticker),
    FOREIGN KEY (ticker) REFERENCES Tickers(ticker)
);

CREATE TABLE OHLC_updates (
    date_of_data DATE NOT NULL,
    date_of_update DATETIME NOT NULL,
    PRIMARY KEY(date_of_data)
);
