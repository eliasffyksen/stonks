
(async () => {
    const db = await require('./db')('data/db.sqlite.gz');
    db.do(async () => {
        console.log('Loading OHLC data');
        const rows = await db.all('SELECT * FROM OHLC');
        console.log('Mapping OHLC data');
        const OHLC = rows.reduce((map, row) => {
            if (!map.has(row.ticker)) {
                map.set(row.ticker, [row]);
            } else {
                const arr = map.get(row.ticker);
                arr.push(row);
            }
            return map;
        }, new Map());
        console.log('Sorting OHLC data');
        for (const [ticker, series] of OHLC) {
            series.sort((a, b) => (a.date > b.date) ? 1 : -1);
        }
        let num = 0;
        let den = 0;
        for (const [ticker, series] of OHLC) {
            
            console.log('Evaluating', ticker);
            for (let i = 1; i < series.length; i++) {
                const lcolse = series[i - 1].close;
                const close = series[i].close;
                den++;
                if (close < lcolse) {
                    num++;
                }
            }
        }
        console.log('Ratio', num / den);
    });
    db.close(false);
})();