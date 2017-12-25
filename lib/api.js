var fs = require('fs');
var http = require('http');
var url = require("url");
var zlib = require('zlib');

var async = require('async');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet);
var charts = require('./charts.js');
var authSid = Math.round(Math.random() * 10000000000) + '' + Math.round(Math.random() * 10000000000);

var logSystem = 'api';
require('./exceptionWriter.js')(logSystem);

var redisCommands = [
    ['zremrangebyscore', config.coin + ':hashrate', '-inf', ''],
    ['zrange', config.coin + ':hashrate', 0, -1],
    ['hgetall', config.coin + ':stats'],
    ['zrange', config.coin + ':blocks:candidates', 0, -1, 'WITHSCORES'],
    ['zrevrange', config.coin + ':blocks:matured', 0, config.api.blocks - 1, 'WITHSCORES'],
    ['hgetall', config.coin + ':shares:roundCurrent'],
    ['hgetall', config.coin + ':stats'],
    ['zcard', config.coin + ':blocks:matured'],
    ['zrevrange', config.coin + ':payments:all', 0, config.api.payments - 1, 'WITHSCORES'],
    ['zcard', config.coin + ':payments:all'],
    ['keys', config.coin + ':payments:*']
];

var currentStats = "";
var currentStatsCompressed = "";

var minerStats = {};
var minersHashrate = {};

var minerDetailedStats = {};

var liveConnections = {};
var addressConnections = {};



function collectStats(){

    var startTime = Date.now();
    var redisFinished;
    var daemonFinished;

    var windowTime = (((Date.now() / 1000) - config.api.hashrateWindow) | 0).toString();
    redisCommands[0][3] = '(' + windowTime;

    async.parallel({
        pool: function(callback){
            redisClient.multi(redisCommands).exec(function(error, replies){

                redisFinished = Date.now();
                var dateNowSeconds = Date.now() / 1000 | 0;

                if (error){
                    log('error', logSystem, 'Error getting redis data %j', [error]);
                    callback(true);
                    return;
                }

                var data = {
                    stats: replies[2],
                    blocks: replies[3].concat(replies[4]),
                    totalBlocks: parseInt(replies[7]) + (replies[3].length / 2),
                    payments: replies[8],
                    totalPayments: parseInt(replies[9]),
                    totalMinersPaid: replies[10].length - 1
                };

                var hashrates = replies[1];

                minerStats = {};
                minersHashrate = {};
                minerDetailedStats = {};
                var minersDetailedHashrate = {};

                for (var i = 0; i < hashrates.length; i++) {
                    var hashParts = hashrates[i].split(':');
                    var address = hashParts[1];
                    var hashrate = parseInt(hashParts[0]);
                    var ip = hashParts[3] || '';
                    var rigName = hashParts[4] || '';
                    minersHashrate[address] = (minersHashrate[address] || 0) + hashrate;

                    // Detailed miner hashrates.
                    if (!(address in minersDetailedHashrate)) {
                        minersDetailedHashrate[address] = { 'ip' : {}, 'name' : {} };
                    }

                    if (!(ip in minersDetailedHashrate[address].ip)) {
                        minersDetailedHashrate[address].ip[ip] = 0;
                    }

                    if (!(rigName in minersDetailedHashrate[address].name)) {
                        minersDetailedHashrate[address].name[rigName] = 0;
                    }

                    minersDetailedHashrate[address].ip[ip] += hashrate;
                    minersDetailedHashrate[address].name[rigName] += hashrate;
                }

                var totalShares = 0;

                for (var miner in minersHashrate){
                    var shares = minersHashrate[miner];
                    totalShares += shares;
                    minersHashrate[miner] = Math.round(shares / config.api.hashrateWindow);
                    minerStats[miner] = getReadableHashRateString(minersHashrate[miner]);
                }

                for (var miner in minersDetailedHashrate) {
                    var rigs = minersDetailedHashrate[miner];

                    // By IP
                    for (var rigIp in rigs.ip) {
                        var shares = minersDetailedHashrate[miner].ip[rigIp];
                        var hashrate = Math.round(shares / config.api.hashrateWindow);
                        var worker = { 
                            'ip' : rigIp,
                            'difficulty' : '',
                            'hashrate' : getReadableHashRateString(hashrate),
                            'hashes' : shares,
                            'accepted' : '',
                            'expired' : '',
                            'bad' : '',
                            'lastSeen' : '',
                            'status' : ''
                        };

                        if (!(miner in minerDetailedStats)) {
                            minerDetailedStats[miner] = { 'ip' : [], 'name' : [] };
                        }

                        minerDetailedStats[miner].ip.push(worker);
                    }
                    
                    // By rig name
                    for (var rigName in rigs.name) {
                        var shares = minersDetailedHashrate[miner].name[rigName];
                        var hashrate = Math.round(shares / config.api.hashrateWindow);
                        var worker = { 
                            'name' : rigName,
                            'difficulty' : '',
                            'hashrate' : getReadableHashRateString(hashrate),
                            'hashes' : shares,
                            'accepted' : '',
                            'expired' : '',
                            'bad' : '',
                            'lastSeen' : '',
                            'status' : ''
                        };

                        if (!(miner in minerDetailedStats)) {
                            minerDetailedStats[miner] = { 'ip' : [], 'name' : [] };
                        }

                        minerDetailedStats[miner].name.push(worker);
                    }
                }

                data.miners = Object.keys(minerStats).length;
                data.hashrate = Math.round(totalShares / config.api.hashrateWindow);

                data.roundHashes = 0;

                if (replies[5]){
                    for (var miner in replies[5]){
                        if (config.poolServer.slushMining.enabled) {
                            data.roundHashes +=  parseInt(replies[5][miner]) / Math.pow(Math.E, ((data.lastBlockFound - dateNowSeconds) / config.poolServer.slushMining.weight)); //TODO: Abstract: If something different than lastBlockfound is used for scoreTime, this needs change. 
                        }
                        else {
                            data.roundHashes +=  parseInt(replies[5][miner]);
                        }
                    }
                }

                if (replies[6]) {
                    data.lastBlockFound = replies[6].lastBlockFound;
                }

                callback(null, data);
            });
        },
        network: function(callback){
            apiInterfaces.rpcDaemon('getlastblockheader', {}, function(error, reply){
                daemonFinished = Date.now();
                if (error){
                    log('error', logSystem, 'Error getting daemon data %j', [error]);
                    callback(true);
                    return;
                }
                var blockHeader = reply.block_header;
                callback(null, {
                    difficulty: blockHeader.difficulty,
                    height: blockHeader.height,
                    timestamp: blockHeader.timestamp,
                    reward: blockHeader.reward,
                    hash:  blockHeader.hash
                });
            });
        },
        config: function(callback){
            callback(null, {
                ports: getPublicPorts(config.poolServer.ports),
                hashrateWindow: config.api.hashrateWindow,
                fee: config.blockUnlocker.poolFee,
                coin: config.coin,
                coinUnits: config.coinUnits,
                coinDifficultyTarget: config.coinDifficultyTarget,
                symbol: config.symbol,
                depth: config.blockUnlocker.depth,
                donation: donations,
                version: version,
                minPaymentThreshold: config.payments.minPayment,
                denominationUnit: config.payments.denomination,
                blockTime: config.coinDifficultyTarget,
                slushMiningEnabled: config.poolServer.slushMining.enabled,
                weight: config.poolServer.slushMining.weight
            });
        },
        charts: charts.getPoolChartsData
    }, function(error, results){

        log('info', logSystem, 'Stat collection finished: %d ms redis, %d ms daemon', [redisFinished - startTime, daemonFinished - startTime]);

        if (error){
            log('error', logSystem, 'Error collecting all stats');
        }
        else{
            currentStats = JSON.stringify(results);
            zlib.deflateRaw(currentStats, function(error, result){
                currentStatsCompressed = result;
                broadcastLiveStats();
            });

        }

        setTimeout(collectStats, config.api.updateInterval * 1000);
    });

}

function getPublicPorts(ports){
    return ports.filter(function(port) {
        return !port.hidden;
    });
}

function getReadableHashRateString(hashrate){
    var i = 0;
    var byteUnits = [' H', ' KH', ' MH', ' GH', ' TH', ' PH' ];
    while (hashrate > 1000){
        hashrate = hashrate / 1000;
        i++;
    }
    return hashrate.toFixed(2) + byteUnits[i];
}

function broadcastLiveStats(){

    log('info', logSystem, 'Broadcasting to %d visitors and %d address lookups', [Object.keys(liveConnections).length, Object.keys(addressConnections).length]);

    for (var uid in liveConnections){
        var res = liveConnections[uid];
        res.end(currentStatsCompressed);
    }

    var redisCommands = [];
    var offsets = {};
    var offset = 0;

    for (var address in addressConnections) {
        offsets[address] = {
            'from' : offset,
            'fromIp' : offset,
            'fromName' : offset,
            'to' : offset + 2
        };
        ++offset;
        redisCommands.push(['hgetall', config.coin + ':workers:' + address]);
        ++offset;
        redisCommands.push(['zrevrange', config.coin + ':payments:' + address, 0, config.api.payments - 1, 'WITHSCORES']);
        offsets[address].fromIp = offset;
        offsets[address].fromName = offset;

        if (address in minerDetailedStats) {
            // By rig IP
            for (var i = 0; i != minerDetailedStats[address].ip.length; ++i) {
                var worker = minerDetailedStats[address].ip[i];
                ++offset;
                redisCommands.push(['hgetall', config.coin + ':workersByIp:' + address + ':' + worker.ip]);
            }

            // By rig name
            offsets[address].fromName = offset;
            
            for (var i = 0; i != minerDetailedStats[address].name.length; ++i) {
                var worker = minerDetailedStats[address].name[i];
                ++offset;
                redisCommands.push(['hgetall', config.coin + ':workersByN:' + address + ':' + worker.name]);
            }
        }

        offsets[address].to = offset;
    }

    if (redisCommands.length === 0) {
        return;
    }

    redisClient.multi(redisCommands).exec(function(error, replies) {
        if (error) {
            log('error', logSystem, 'Failed to obtain live statistics %j \n %j', [error, redisCommands]);
            return;
        }

        var addresses = Object.keys(addressConnections);

        for (var i = 0; i < addresses.length; i++){
            var address = addresses[i];
            var res = addressConnections[address];
            var queryIndex = offsets[address];

            if (!queryIndex) {
                res.end(JSON.stringify({error: "not found"}));
                return;
            }

            var stats = replies[queryIndex.from];

            if (!stats) {
                res.end(JSON.stringify({error: "not found"}));
                return;
            }

            var payments = replies[queryIndex.from + 1];
            stats.hashrate = minerStats[address];

            // By rig IP
            for (var ri = 0; ri != queryIndex.fromName - queryIndex.fromIp; ++ri) {
                var rig = replies[queryIndex.fromIp + ri];

                if (rig !== undefined && rig !== null) {
                    minerDetailedStats[address].ip[ri].difficulty = (rig.difficulty || "");
                    //minerDetailedStats[address].ip[ri].hashes = rig.hashes;
                    minerDetailedStats[address].ip[ri].lastSeen = rig.lastShare;
                    minerDetailedStats[address].ip[ri].accepted = (rig.accepted || 0);
                    minerDetailedStats[address].ip[ri].expired = (rig.expired || 0);
                    minerDetailedStats[address].ip[ri].bad = (rig.bad || 0);
                    
                    if (minerDetailedStats[address].ip[ri].hashrate !== ""
                        && minerDetailedStats[address].ip[ri].hashrate !== "0.00 H"
                        && minerDetailedStats[address].ip[ri].hashrate !== "0 H") {
                            
                        minerDetailedStats[address].ip[ri].status = "OK";
                    }
                    else {
                        minerDetailedStats[address].ip[ri].status = "Error";
                    }
                }
            }

            // By rig name
            for (var rn = 0; rn != queryIndex.to - queryIndex.fromName; ++rn) {
                var rig = replies[queryIndex.fromName + rn];

                if (rig !== undefined && rig !== null) {
                    minerDetailedStats[address].name[rn].difficulty = (rig.difficulty || "");
                    //minerDetailedStats[address].name[rn].hashes = rig.hashes;
                    minerDetailedStats[address].name[rn].lastSeen = rig.lastShare;
                    minerDetailedStats[address].name[rn].accepted = (rig.accepted || 0);
                    minerDetailedStats[address].name[rn].expired = (rig.expired || 0);
                    minerDetailedStats[address].name[rn].bad = (rig.bad || 0);

                    if (minerDetailedStats[address].name[rn].hashrate !== ""
                        && minerDetailedStats[address].name[rn].hashrate !== "0.00 H"
                        && minerDetailedStats[address].name[rn].hashrate !== "0 H") {
                            
                        minerDetailedStats[address].name[rn].status = "OK";
                    }
                    else {
                        minerDetailedStats[address].name[rn].status = "Error";
                    }
                }
            }

            res.end(JSON.stringify({stats: stats, workersDetailed: minerDetailedStats[address], payments: payments}));
        }
    });
}

function handleMinerStats(urlParts, response){
    response.writeHead(200, {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'Connection': 'keep-alive'
    });
    response.write('\n');
    var address = urlParts.query.address;

    if (urlParts.query.longpoll === 'true'){
        redisClient.exists(config.coin + ':workers:' + address, function(error, result){
            if (!result){
                response.end(JSON.stringify({error: 'not found'}));
                return;
            }
            addressConnections[address] = response;
            response.on('finish', function(){
                delete addressConnections[address];
            })
        });
    }
    else{
        redisClient.multi([
            ['hgetall', config.coin + ':workers:' + address],
            ['zrevrange', config.coin + ':payments:' + address, 0, config.api.payments - 1, 'WITHSCORES'],
            ['keys', config.coin + ':workersByIp:' + address + ':*'],
            ['keys', config.coin + ':workersByN:' + address + ':*']
        ]).exec(function(error, replies){
            if (error || !replies[0]){
                response.end(JSON.stringify({error: 'not found'}));
                return;
            }

            var stats = replies[0];
            stats.hashrate = minerStats[address];

            // Detailed statistics
            var workersDetailed = { 'ip' : [], 'name' : []};
            
            // Prepare statistics by rig IP.
            for (var i = 0; i != replies[2].length; ++i) {
                var addressWithIp = replies[2][i];
                var parts = addressWithIp.split(':');
                var worker = { 
                    'ip' : '',
                    'difficulty' : '',
                    'hashrate' : '',
                    'hashes' : '',
                    'accepted' : '',
                    'expired' : '',
                    'bad' : '',
                    'lastSeen' : '',
                    'status' : ''
                };

                if (parts.length > 0) {
                    worker.ip = parts[parts.length - 1];
                }

                workersDetailed.ip.push(worker);
            }

            // Prepare statistics by rig name.
            for (var i = 0; i != replies[3].length; ++i) {
                var addressWithIp = replies[3][i];
                var parts = addressWithIp.split(':');
                var worker = { 
                    'name' : '',
                    'difficulty' : '',
                    'hashrate' : '',
                    'hashes' : '',
                    'accepted' : '',
                    'expired' : '',
                    'bad' : '',
                    'lastSeen' : '',
                    'status' : ''
                };

                if (parts.length > 0) {
                    worker.name = parts[parts.length - 1];
                }

                workersDetailed.name.push(worker);
            }

            var payments = replies[1];

            charts.getUserChartsData(address, payments, function(error, chartsData) {
                response.end(JSON.stringify({
                    stats: stats,
                    payments: payments,
                    workersDetailed: workersDetailed,
                    charts: chartsData
                }));
            });
        });
    }
}

function handleMinerWorkersAddress(urlParts, response){
    response.writeHead(200, {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'Connection': 'keep-alive'
    });
    response.write('\n');
    var address = urlParts.query.address;

    redisClient.multi([
        ['keys', config.coin + ':workersByIp:' + address + ':*'],
        ['keys', config.coin + ':workersByN:' + address + ':*']
    ]).exec(function(error, replies) {
        if (error || !replies[0]) {
            response.end(JSON.stringify({error: 'not found'}));
            return;
        }

        var workers = { 'ip' : [], 'name' : [] };

        for (var i = 0; i != replies[0].length; ++i) {
            var addressWithIp = replies[0][i];
            var parts = addressWithIp.split(':');

            if (parts.length > 0) {
                workers.ip.push(parts[parts.length - 1]);
            }
        }

        for (var i = 0; i != replies[1].length; ++i) {
            var addressWithIp = replies[1][i];
            var parts = addressWithIp.split(':');

            if (parts.length > 0) {
                workers.name.push(parts[parts.length - 1]);
            }
        }

        response.end(JSON.stringify({
            workers: workers
        }));
    });
}

function handleMinerWorkersStats(urlParts, postData, response) {
    if (!postData || !postData.address) {
        response.end(JSON.stringify({error: 'not found'}));
        return;
    }

    response.writeHead(200, {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'Connection': 'keep-alive'
    });
    response.write('\n');

    var address = postData.address;
    var redisCommands = [];

    var offset = 0;
    var offsets = {
        'fromIp' : 0,
        'fromName' : 0,
        'to' : 0
    };

    // By rig IP
    if (postData.workers && postData.workers.ip) {
        for (var i = 0; i != postData.workers.ip.length; ++i) {
            var workerIp = postData.workers.ip[i].trim().substring(0, 256).replace(/[^0-9\.]/g, '0');
            ++offset;
            redisCommands.push(['hgetall', config.coin + ':workersByIp:' + address + ':' + workerIp]);
        }
    }

    // By rig name
    offsets.fromName = offset;

    if (postData.workers && postData.workers.name) {
        for (var i = 0; i != postData.workers.name.length; ++i) {
            var workerName = postData.workers.name[i].trim().substring(0, 32).replace(/[^a-zA-Z0-9_-]/g, '_');
            ++offset;
            redisCommands.push(['hgetall', config.coin + ':workersByN:' + address + ':' + workerName]);
        }
    }

    offsets.to = offset;

    if (redisCommands.length === 0) {
        response.end(JSON.stringify({error: 'not found'}));
        return;
    }

    redisClient.multi(redisCommands).exec(function(error, replies) {
        if (error) {
            response.end(JSON.stringify({error: 'Redis error'}));
            return;
        }

        var minerWorkersStats = { 'ip' : [], 'name' : [] };

        // By rig IP
        for (var ri = 0; ri != offsets.fromName - offsets.fromIp; ++ri) {
            var rig = replies[offsets.fromIp + ri];
            var stat = {
                'ip' : postData.workers.ip[ri]
                , 'difficulty' : ''
                , 'hashes' : 0
                , 'lastSeen' : ''
                , 'accepted' : 0
                , 'expired' : 0
                , 'bad' : 0
                , 'status' : 'Error'
            };

            if (rig !== undefined && rig !== null) {
                stat.difficulty = (rig.difficulty || "");
                stat.hashes = rig.hashes;
                stat.lastSeen = rig.lastShare;
                stat.accepted = (rig.accepted || 0);
                stat.expired = (rig.expired || 0);
                stat.bad = (rig.bad || 0);
                stat.status = 'Mining';
            }

            minerWorkersStats.ip.push(stat);
        }

        // By rig name
        for (var rn = 0; rn != offsets.to - offsets.fromName; ++rn) {
            var rig = replies[offsets.fromName + rn];
            var stat = {
                'name' : postData.workers.name[rn]
                , 'difficulty' : ''
                , 'hashes' : 0
                , 'lastSeen' : ''
                , 'accepted' : 0
                , 'expired' : 0
                , 'bad' : 0
                , 'status' : 'Error'
            };

            if (rig !== undefined && rig !== null) {
                stat.difficulty = (rig.difficulty || "");
                stat.hashes = rig.hashes;
                stat.lastSeen = rig.lastShare;
                stat.accepted = (rig.accepted || 0);
                stat.expired = (rig.expired || 0);
                stat.bad = (rig.bad || 0);
                stat.status = 'Mining';
            }

            minerWorkersStats.name.push(stat);
        }

        response.end(JSON.stringify({workersDetailed: minerWorkersStats}));
    });
}

function handleGetPayments(urlParts, response){
    var paymentKey = ':payments:all';

    if (urlParts.query.address)
        paymentKey = ':payments:' + urlParts.query.address;

    redisClient.zrevrangebyscore(
            config.coin + paymentKey,
            '(' + urlParts.query.time,
            '-inf',
            'WITHSCORES',
            'LIMIT',
            0,
            config.api.payments,
        function(err, result){

            var reply;

            if (err)
                reply = JSON.stringify({error: 'query failed'});
            else
                reply = JSON.stringify(result);

            response.writeHead("200", {
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/json',
                'Content-Length': reply.length
            });
            response.end(reply);

        }
    )
}

function handleGetBlocks(urlParts, response){
    redisClient.zrevrangebyscore(
            config.coin + ':blocks:matured',
            '(' + urlParts.query.height,
            '-inf',
            'WITHSCORES',
            'LIMIT',
            0,
            config.api.blocks,
        function(err, result){

        var reply;

        if (err)
            reply = JSON.stringify({error: 'query failed'});
        else
            reply = JSON.stringify(result);

        response.writeHead("200", {
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json',
            'Content-Length': reply.length
        });
        response.end(reply);

    });
}

function handleGetMinersHashrate(response) {
    var reply = JSON.stringify({
        minersHashrate: minersHashrate
    });
    response.writeHead("200", {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json',
        'Content-Length': reply.length
    });
    response.end(reply);
}

function parseCookies(request) {
    var list = {},
        rc = request.headers.cookie;
    rc && rc.split(';').forEach(function(cookie) {
        var parts = cookie.split('=');
        list[parts.shift().trim()] = unescape(parts.join('='));
    });
    return list;
}

function authorize(request, response){
    if(request.connection.remoteAddress == '127.0.0.1') {
        return true;
    }

    response.setHeader('Access-Control-Allow-Origin', '*');

    var cookies = parseCookies(request);
    if(cookies.sid && cookies.sid == authSid) {
        return true;
    }

    var sentPass = url.parse(request.url, true).query.password;


    if (sentPass !== config.api.password){
        response.statusCode = 401;
        response.end('invalid password');
        return;
    }

    log('warn', logSystem, 'Admin authorized');
    response.statusCode = 200;

    var cookieExpire = new Date( new Date().getTime() + 60*60*24*1000);
    response.setHeader('Set-Cookie', 'sid=' + authSid + '; path=/; expires=' + cookieExpire.toUTCString());
    response.setHeader('Cache-Control', 'no-cache');
    response.setHeader('Content-Type', 'application/json');


    return true;
}

function handleAdminStats(response){

    async.waterfall([

        //Get worker keys & unlocked blocks
        function(callback){
            redisClient.multi([
                ['keys', config.coin + ':workers:*'],
                ['zrange', config.coin + ':blocks:matured', 0, -1]
            ]).exec(function(error, replies) {
                if (error) {
                    log('error', logSystem, 'Error trying to get admin data from redis %j', [error]);
                    callback(true);
                    return;
                }
                callback(null, replies[0], replies[1]);
            });
        },

        //Get worker balances
        function(workerKeys, blocks, callback){
            var redisCommands = workerKeys.map(function(k){
                return ['hmget', k, 'balance', 'paid'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting balances from redis %j', [error]);
                    callback(true);
                    return;
                }

                callback(null, replies, blocks);
            });
        },
        function(workerData, blocks, callback){
            var stats = {
                totalOwed: 0,
                totalPaid: 0,
                totalRevenue: 0,
                totalDiff: 0,
                totalShares: 0,
                blocksOrphaned: 0,
                blocksUnlocked: 0,
                totalWorkers: 0
            };

            for (var i = 0; i < workerData.length; i++){
                stats.totalOwed += parseInt(workerData[i][0]) || 0;
                stats.totalPaid += parseInt(workerData[i][1]) || 0;
                stats.totalWorkers++;
            }

            for (var i = 0; i < blocks.length; i++){
                var block = blocks[i].split(':');
                if (block[5]) {
                    stats.blocksUnlocked++;
                    stats.totalDiff += parseInt(block[2]);
                    stats.totalShares += parseInt(block[3]);
                    stats.totalRevenue += parseInt(block[5]);
                }
                else{
                    stats.blocksOrphaned++;
                }
            }
            callback(null, stats);
        }
    ], function(error, stats){
            if (error){
                response.end(JSON.stringify({error: 'error collecting stats'}));
                return;
            }
            response.end(JSON.stringify(stats));
        }
    );

}


function handleAdminUsers(response){
    async.waterfall([
        // get workers Redis keys
        function(callback) {
            redisClient.keys(config.coin + ':workers:*', callback);
        },
        // get workers data
        function(workerKeys, callback) {
            var redisCommands = workerKeys.map(function(k) {
                return ['hmget', k, 'balance', 'paid', 'lastShare', 'hashes'];
            });
            redisClient.multi(redisCommands).exec(function(error, redisData) {
                var workersData = {};
                var addressLength = config.poolServer.poolAddress.length;
                for(var i in redisData) {
                    var address = workerKeys[i].substr(-addressLength);
                    var data = redisData[i];
                    workersData[address] = {
                        pending: data[0],
                        paid: data[1],
                        lastShare: data[2],
                        hashes: data[3],
                        hashrate: minersHashrate[address] ? minersHashrate[address] : 0
                    };
                }
                callback(null, workersData);
            });
        }
    ], function(error, workersData) {
            if(error) {
                response.end(JSON.stringify({error: 'error collecting users stats'}));
                return;
            }
            response.end(JSON.stringify(workersData));
        }
    );
}


function handleAdminMonitoring(response) {
    response.writeHead("200", {
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-cache',
        'Content-Type': 'application/json'
    });
    async.parallel({
        monitoring: getMonitoringData,
        logs: getLogFiles
    }, function(error, result) {
        response.end(JSON.stringify(result));
    });
}

function handleAdminLog(urlParts, response){
    var file = urlParts.query.file;
    var filePath = config.logging.files.directory + '/' + file;
    if(!file.match(/^\w+\.log$/)) {
        response.end('wrong log file');
    }
    response.writeHead(200, {
        'Content-Type': 'text/plain',
        'Cache-Control': 'no-cache',
        'Content-Length': fs.statSync(filePath).size
    });
    fs.createReadStream(filePath).pipe(response)
}


function startRpcMonitoring(rpc, module, method, interval) {
    setInterval(function() {
        rpc(method, {}, function(error, response) {
            var stat = {
                lastCheck: new Date() / 1000 | 0,
                lastStatus: error ? 'fail' : 'ok',
                lastResponse: JSON.stringify(error ? error : response)
            };
            if(error) {
                stat.lastFail = stat.lastCheck;
                stat.lastFailResponse = stat.lastResponse;
            }
            var key = getMonitoringDataKey(module);
            var redisCommands = [];
            for(var property in stat) {
                redisCommands.push(['hset', key, property, stat[property]]);
            }
            redisClient.multi(redisCommands).exec();
        });
    }, interval * 1000);
}

function getMonitoringDataKey(module) {
    return config.coin + ':status:' + module;
}

function initMonitoring() {
    var modulesRpc = {
        daemon: apiInterfaces.rpcDaemon,
        wallet: apiInterfaces.rpcWallet
    };
    for(var module in config.monitoring) {
        var settings = config.monitoring[module];
        if(settings.checkInterval) {
            startRpcMonitoring(modulesRpc[module], module, settings.rpcMethod, settings.checkInterval);
        }
    }
}



function getMonitoringData(callback) {
    var modules = Object.keys(config.monitoring);
    var redisCommands = [];
    for(var i in modules) {
        redisCommands.push(['hgetall', getMonitoringDataKey(modules[i])])
    }
    redisClient.multi(redisCommands).exec(function(error, results) {
        var stats = {};
        for(var i in modules) {
            if(results[i]) {
                stats[modules[i]] = results[i];
            }
        }
        callback(error, stats);
    });
}

function getLogFiles(callback) {
    var dir = config.logging.files.directory;
    fs.readdir(dir, function(error, files) {
        var logs = {};
        for(var i in files) {
            var file = files[i];
            var stats = fs.statSync(dir + '/' + file);
            logs[file] = {
                size: stats.size,
                changed: Date.parse(stats.mtime) / 1000 | 0
            }
        }
        callback(error, logs);
    });
}

var server = http.createServer(function(request, response){

    if (request.method.toUpperCase() === "OPTIONS"){

        response.writeHead("204", "No Content", {
            "access-control-allow-origin": '*',
            "access-control-allow-methods": "GET, POST, PUT, DELETE, OPTIONS",
            "access-control-allow-headers": "content-type, accept",
            "access-control-max-age": 10, // Seconds.
            "content-length": 0
        });

        return(response.end());
    }

    var urlParts = url.parse(request.url, true);

    if (request.method.toUpperCase() === 'POST') {
        var jsonString = '';

        request.on('data', function (data) {
            jsonString += data;
        });

        request.on('end', function () {
            switch (urlParts.pathname) {
                case '/stats_workers':
                    var postData = JSON.parse(jsonString);
                    handleMinerWorkersStats(urlParts, postData, response);
                    break;
                default:
                    return(response.end());
            }
        });

        return;
    }

    switch(urlParts.pathname){
        case '/stats':
            var deflate = request.headers['accept-encoding'] && request.headers['accept-encoding'].indexOf('deflate') != -1;
            var reply = deflate ? currentStatsCompressed : currentStats;
            response.writeHead("200", {
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/json',
                'Content-Encoding': deflate ? 'deflate' : '',
                'Content-Length': reply.length
            });
            response.end(reply);
            break;
        case '/live_stats':
            response.writeHead(200, {
                'Access-Control-Allow-Origin': '*',
                'Cache-Control': 'no-cache',
                'Content-Type': 'application/json',
                'Content-Encoding': 'deflate',
                'Connection': 'keep-alive'
            });
            var uid = Math.random().toString();
            liveConnections[uid] = response;
            response.on("finish", function() {
                delete liveConnections[uid];
            });
            break;
        case '/stats_address':
            handleMinerStats(urlParts, response);
            break;
        case '/workers_address':
            handleMinerWorkersAddress(urlParts, response);
            break;
        case '/get_payments':
            handleGetPayments(urlParts, response);
            break;
        case '/get_blocks':
            handleGetBlocks(urlParts, response);
            break;
        case '/admin_stats':
            if (!authorize(request, response))
                return;
            handleAdminStats(response);
            break;
        case '/admin_monitoring':
            if(!authorize(request, response)) {
                return;
            }
            handleAdminMonitoring(response);
            break;
        case '/admin_log':
            if(!authorize(request, response)) {
                return;
            }
            handleAdminLog(urlParts, response);
            break;
        case '/admin_users':
            if(!authorize(request, response)) {
                return;
            }
            handleAdminUsers(response);
            break;

        case '/miners_hashrate':
            if (!authorize(request, response))
                return;
            handleGetMinersHashrate(response);
            break;

        default:
            response.writeHead(404, {
                'Access-Control-Allow-Origin': '*'
            });
            response.end('Invalid API call');
            break;
    }
});

collectStats();
initMonitoring();

server.listen(config.api.port, function(){
    log('info', logSystem, 'API started & listening on port %d', [config.api.port]);
});
