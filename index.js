require('dotenv').config();

const first_date = '2019-01-10';
const dbPath = 'data/db.sqlite.gz';
const apiKey = process.env.POLYGONIO_API_KEY;
const host = 'https://api.polygon.io';

const fetch = require('node-fetch');

if (!apiKey) {
    console.error('No POLYGONIO_API_KEY in .env file');
    process.exit(1);
}

let db = null;
(async () => {
    db = await require('./db')(dbPath);
    process.on('SIGINT', () => {
        db.close().catch(err => {
            console.error('Error closing database on SIGINT:', err.message);
        });
    });
    await updateOHLC();
    db.close().catch(err => {
        console.error('Error closing database on exit:', err.message);
    });
})();

function sleep(sec) {
    return new Promise((res, rej) => {
        setTimeout(res, sec * 1000);
    });
}

function formatDate(d) {
    var month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2) 
        month = '0' + month;
    if (day.length < 2) 
        day = '0' + day;

    return [year, month, day].join('-');
}

function getOHCL(date) {
    return fetch(`${host}/v2/aggs/grouped/locale/us/market/stocks/${date}?apiKey=${apiKey}`)
        .then(res => res.json());
}

function insertOHLC(date) {
    return getOHCL(date)
        .then(async res => {
            switch (res.status) {
                case 'OK': break;
                case 'DELAYED':
                    console.log('Delayed OHLC status response for', date + ', not updating database');
                    return;
                default:
                    console.error('OHCL Failure (date', date,'):', res);
                    return;
            }
            if (!res.count) {
                console.log('No OHLC results for day', date);
            } else {
                console.log(res.count, 'OHLC results for date', date);

                for (const stock of res.results) {
                    try {
                        await db.run(`INSERT INTO Tickers(ticker) VALUES ('${stock.T}')`);
                    } catch (e) {}
                    try {
                        await db.run(`INSERT INTO OHLC(date, ticker, volume, open, high, low, close)
                                    VALUES ('${date}', '${stock.T}', ${stock.v}, ${stock.o}, ${stock.h}, ${stock.l}, ${stock.c})`);
                    } catch (e) {}
                }
                console.log('Done inserting OHLC for', date);
            }
            try {
                await db.run(`INSERT INTO OHLC_updates(date_of_data, date_of_update) VALUES ('${date}', CURRENT_TIMESTAMP)`);
            } catch (e) {
                console.error('Failed to updates OHLC_update table:', e.message);
            }
        });
}

async function updateOHLC() {
    const success = db && await db.do(async () => {
        const datesInDb = (await db.all('SELECT DISTINCT date_of_data FROM OHLC_updates'))
                            .map(e => e.date_of_data);
        let curDate = new Date(first_date);
        const today = formatDate(new Date());
        const promiseList = [];
        let dbOpen = true;
        while (dbOpen) {
            const curDateFormatted = formatDate(curDate);
            if (curDateFormatted == today)
                break;
            dbOpen = await db.do(async () => {
                if (datesInDb.includes(curDateFormatted)) {
                    console.log(curDateFormatted, 'already in OHLC DB');
                } else {
                    console.log('Fetching OHLC for', curDateFormatted);
                    promiseList.push(insertOHLC(curDateFormatted));
                    await sleep(12);
                }
                curDate.setDate(curDate.getDate() + 1);
            });
        }
        await Promise.allSettled(promiseList);   
    });
    if (!success) {
        console.log('Failed to update OHLC, DB not open');
    }
}


