const r = require('rethinkdb');

const tables = [
    'tickers',
    'OHLC'
];

(async () => {
    const conn = await r.connect();

    // Init db stonks
    let res = await r.dbList().run(conn);
    if (!res.includes('stonks')) {
        await r.dbCreate('stonks').run(conn);
        console.log('Created db stonks');
    }
    conn.use('stonks');

    res = await r.tableList().run(conn);
    for (table of tables) {
        if (!res.includes(table)) {
            await r.tableCreate(table).run(conn);
            console.log('Create table', table);
        }
    }
})();
