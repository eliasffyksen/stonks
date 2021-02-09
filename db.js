
const sqlite3 = require('sqlite3').verbose();
const tmp = require('tmp');
const fs = require('fs');
const zlib = require('zlib');

function tmpFile() {
    return new Promise((res, rej) => {
        tmp.file({keep: true}, (err, tmpPath, fd, rmcb) => {
            if (err) {
                rej(err);
                return;
            }
            fs.close(fd, (err) => {
                if (err) {
                    rej(err);
                    return;
                }
                res(tmpPath);
            });
        });
    });
}

function gunzip(from, to) {
    return new Promise((res, rej) => {
        const is = fs.createReadStream(from);
        const os = fs.createWriteStream(to);
        is.pipe(zlib.createGunzip()).pipe(os).on('finish', (err) => {
            if (err) {
                rej(err);
                return;
            }
            is.close();
            os.close();
            res();
        });
    });
}

function gzip(from, to) {
    return new Promise((res, rej) => {
        const is = fs.createReadStream(from);
        const os = fs.createWriteStream(to);
        is.pipe(zlib.createGzip()).pipe(os).on('finish', (err) => {
            if (err) {
                rej(err);
                return;
            }
            is.close();
            os.close();
            res();
        });
    });
}

function fsUnlink(path) {
    return new Promise((res, rej) => {
        fs.unlink(path, (err) => {
            if (err) {
                rej(err);
                return;
            }
            res();
        });
    });
}

module.exports = async (path) => {
    const tmpPath = await tmpFile();
    console.log('Decompressing DB', path, 'into', tmpPath);
    await gunzip(path, tmpPath);
    const db = new sqlite3.Database(tmpPath);
    let open = true;

    let nextTransactionId = 0;
    const transactions = new Map();

    console.log('DB initialized');

    return {
        do: async (cb) => {
            if (!open) {
                return false;
            }
            const pId = nextTransactionId++;
            const p = new Promise(async (res, rej) => {
                await cb();
                transactions.delete(pId);
                res();
            });
            transactions.set(pId, p);
            await p;
            return true;
        },
        run: (...args) => {
            return new Promise(function (res, rej) {
                db.run(...args, function (err) {
                    if (err) {
                        rej(err);
                        return;
                    }
                    res(this);
                });
            });
        },
        all: (...args) => {
            return new Promise(function (res, rej) {
                db.all(...args, function (err, rows) {
                    if (err) {
                        rej(err);
                        return;
                    }
                    res(rows);
                });
            });
        },
        each: (...args) => {
            return new Promise((res, rej) => {
                const cb = args.pop(args.length - 1);
                const promiseList = [];
                db.each(...args, (err, row) => {
                    if (err) {
                        rej(err);
                        return;
                    }
                    promiseList.push(new Promise(async (res, rej) => {
                        await cb(row);
                        res();
                    }));
                }, async (err, n) => {
                    if (err) {
                        rej(err);
                        return;
                    }
                    await Promise.allSettled(promiseList);
                    res(n);
                });
            });
        },
        close: (save = true) => {
            return new Promise(async (res, rej) => {
                console.log('Closing DB', path);
                if (!open) {
                    return;
                }
                open = false;
                if (transactions.size) {
                    console.log('Waiting for', transactions.size, 'transactions');
                    await Promise.allSettled(transactions.values());
                }
                db.close(async (err) => {
                    try {
                        if (err) {
                            rej(err);
                            return;
                        }
                        if (save) {
                            console.log('Compressing DB', tmpPath, 'into', path);
                            await gzip(tmpPath, path);
                            console.log('Done compressing DB', path);
                        }
                        console.log('Deleting tmp DB', tmpPath);
                        await fsUnlink(tmpPath);
                        res();
                    } catch (e) {
                        rej(e);
                    }
                });
            });
        }
    };
}
