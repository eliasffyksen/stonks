require('dotenv').config();
const r = require('rethinkdb');
const dt = require('node-datetime');

const first_date = '2019-01-10';
const apiKey = process.env.POLYGONIO_API_KEY;
const host = 'https://api.polygon.io';

const fetch = require('node-fetch');

if (!apiKey) {
    console.error('No POLYGONIO_API_KEY in .env file');
    process.exit(1);
}

let conn = null;

(async () => {
    conn = await r.connect({ db: 'stonks' });
    await updateOHLC();
    conn.close();
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
                    await r.table('OHLC').insert({
                        id: `${stock.T}:${date}`,
                        ticker: stock.T,
                        date,
                        c: stock.c,
                        h: stock.h,
                        l: stock.l,
                        o: stock.o,
                        v: stock.v
                    }).run(conn);
                }
                console.log('Done inserting OHLC for', date);
            }
            await r.table('OHLCUpdates').insert({ id: date, updated: dt.create().format('Y-m-d H:M:S')}).run(conn);
        });
}

async function updateOHLC() {
    // const datesInDb = (await db.all('SELECT DISTINCT date_of_data FROM OHLC_updates'))
    //                     .map(e => e.date_of_data);
    const datesInDb = (await r.table('OHLCUpdates').pluck('id').run(conn).then(res => res.toArray())).map(e => e.id);
    let curDate = new Date(first_date);
    const today = formatDate(new Date());
    const promiseList = [];
    while (true) {
        const curDateFormatted = formatDate(curDate);
        if (curDateFormatted == today)
            break;

        if (datesInDb.includes(curDateFormatted)) {
            console.log(curDateFormatted, 'already in OHLC DB');
        } else {
            console.log('Fetching OHLC for', curDateFormatted);
            promiseList.push(insertOHLC(curDateFormatted));
            await sleep(12);
        }
        curDate.setDate(curDate.getDate() + 1);
    }
    console.log('Waiting for all promises to resolve');
    await Promise.allSettled(promiseList);   
}


