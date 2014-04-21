// todo сделать функциональный тест, который бы показывал, что без очереди сервер отдает 503, а с ней коррекно обрабатывает все запросы
// todo сделать функциональный тест, в котором количество запросов, которые держит сервер, превышало бы размер очереди.

var BUSY_QUEUE_SIZE = 10000;

// количество милисекунд, после истечения которых запрос, находящийся в очереди считается "протухшим"
// проверка происходит по самому "молодому" (те. последнему добавленному запросу). В случае, если он "протух",
// возвращаем 503 ошибку для всей группы запросов и запускаем обработчик очереди повторно.
var BUSY_EXPIRES = 50000;
var BUSY_OUTPUT_SUITE_SIZE = 500;
var BUSY_SCHEDULE_DELAY = 300;

// При выводе из очереди раз в intervalPerCheck ответов выполняется проверка toobusy()
var intervalPerCheck = 50; // Math.floor(BUSY_OUTPUT_SUITE_SIZE / 10);

var queue = new Array(BUSY_QUEUE_SIZE);
var inputIndex = 0;
var count = 0;
var outputIndex = 0;
var responseScheduleLock = false;
var busyQueueDebug = true;
var bqDebugIndex_I = 0;
var bqDebugIndex_J = 0;
var bqDebugIndex_K = 0;

function startScheduler() {

    setInterval(handler, BUSY_SCHEDULE_DELAY);

    function handler(force) {
        if (responseScheduleLock && !force) return ;
        responseScheduleLock = true;

        var item, i, curCount = Math.min(BUSY_OUTPUT_SUITE_SIZE, count);
        // если очередь пуста, то выходим отсюда
        if (curCount <= 0) {
            responseScheduleLock = false;
            return;
        }

        // если время последнего добавленного запроса из набора больше максимального, то для всех возвращаем 503
        item = queue[(outputIndex + curCount - 1) % BUSY_QUEUE_SIZE];
         // Если время истечения ожидания "верхнего" запроса в очереди истекло, то очищаем очередь
        var now = (new Date()).getTime();
        if ((now - item.time) > BUSY_EXPIRES) {

            if (busyQueueDebug) {
                console.log('BQ (pid=' + process.pid + ') [' + new Date + '] - clear time expires items (current count: ' + curCount + '; queue length: ' + count + ')');
            }

            for (i = 0; i < curCount; i++) {
                item = queue[outputIndex];
                item.res.send(503, {'Content-Type': 'application/json; charset=utf-8'});
                item.res.end('{"message":"I\'m busy right now, sorry."}');

                outputIndex += 1;
                outputIndex = outputIndex % BUSY_QUEUE_SIZE;
            }

            count -= curCount;

            bqDebugIndex_J += curCount;
            if (busyQueueDebug && bqDebugIndex_J > 100) {
                console.log('BQ (pid=' + process.pid + ') [' + new Date + '] - time expires next (current count: ' + curCount + '; queue length: ' + count + ')');
                bqDebugIndex_J = 0;
            }

            handler(true);
        } else {
            if (toobusy()) {
                responseScheduleLock = false;
                return ;
            }

            if (busyQueueDebug) {
                console.log('BQ (pid=' + process.pid + ') [' + new Date + '] - request execution (current count: ' + curCount + '; queue length: ' + count + ')');
            }

            var releaseCount = 0;
            for (i = 0; i < curCount; i++) {
                if ((i % intervalPerCheck) === 0 && toobusy()) break;

                item = queue[outputIndex];
                item.next();

                releaseCount += 1;
                outputIndex += 1;
                outputIndex = outputIndex % BUSY_QUEUE_SIZE;
            }

            count -= releaseCount;

            bqDebugIndex_K += releaseCount;
            if (busyQueueDebug && bqDebugIndex_K > 1000) {
                console.log('BQ (pid=' + process.pid + ') [' + new Date + '] - release next responses (current count: ' +
                                    curCount + '; release count: ' + releaseCount + '; queue length: ' + count + ')');
                bqDebugIndex_K = 0;
            }
        }

        responseScheduleLock = false;
    }
}

var toobusy = require('toobusy');

exports.startScheduler = startScheduler;

exports.establish = function(req, res, next) {
    res.connection.setNoDelay(true);
    if (!toobusy()) {
        next();
    } else {
        // res.send(503, {'Content-Type': 'application/json; charset=utf-8'});
        // res.end('{"message":"I\'m busy right now, sorry."}');

        // если текущее количество запросов в очереди достигло максимального, то отдаем пользователю 503
        if (count >= BUSY_QUEUE_SIZE) {
            res.send(503, {'Content-Type': 'application/json; charset=utf-8'});
            res.end('{"message":"I\'m busy right now, sorry."}');
        } else {
            queue[inputIndex] = {time:(new Date()).getTime(), req:req, res:res, next:next};
            inputIndex += 1;
            inputIndex = inputIndex % BUSY_QUEUE_SIZE;
            count += 1;

            if (busyQueueDebug && ++bqDebugIndex_I > 1000) {
                console.log('BQ (pid=' + process.pid + ') [' + new Date + '] - established next connections (queue length: ', count, ')');
                bqDebugIndex_I = 0;
            }
        }

    }
};