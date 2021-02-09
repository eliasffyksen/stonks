
const r = require('rethinkdb');

(async () => {
    const conn = await r.connect({
        db: 'stonks'
    });
    const db = await require('./db')('./data/db.sqlite.gz');
    db.do(async () => {

        // Tickers
        let rows = await db.all('SELECT * FROM Tickers');;
        const tickerData = rows.reduce((acc, row) => {
            acc.push({id: row['ticker']});
            return acc;
        }, []);
        await r.table('tickers').insert(tickerData).run(conn);

        // OHCL
        rows = await db.all('SELECT * FROM OHLC');
        const OHLCData = rows.reduce((acc, row) => {
            acc.push({
                id: `${row['ticker']}:${row['date']}`,
                ticker: row['ticker'],
                date: row['date'],
                v: row['volume'],
                o: row['open'],
                h: row['high'],
                l: row['low'],
                c: row['close']
            });
            return acc;
        }, []);

        console.log('staring inserts');
        while (OHLCData.length) {
            const insertData = OHLCData.splice(0, 1E4);
            await r.table('OHLC').insert(insertData).run(conn);
            console.log('Inserted 10k');
        }
        console.log('done with all inserts');
    });
    db.close(false);
})();