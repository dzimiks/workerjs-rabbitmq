let amqp = require('amqplib/callback_api');

let EventEmitter = require('events');

let messaging = {
    _listening: {}, // List of channels I subscribed to, to avoid duplicate subscribe

    _client: undefined,

    _eventEmitter: new EventEmitter(),

    _channel: {},

    on: function (channel, listener) {
        return new Promise(function (resolve, reject) {
            if (messaging._listening[channel] == undefined) {
                messaging._listening[channel] = true;

                messaging._client.createChannel((err, ch) => {
                    if (err) {
                        reject(err);
                    } else {
                        messaging._channel[channel] = ch;

                        ch.assertQueue(channel, {durable: true});
                        ch.prefetch(1);

                        ch.consume(channel, function (message) {
                            messaging._eventEmitter.emit(channel, JSON.parse(message.content));

                            let secs = message.content.toString().split('.').length - 1;

                            setTimeout(function () {
                                ch.ack(message);
                            }, secs * 1000);
                        });

                        resolve(messaging._eventEmitter.on(channel, listener));
                    }
                });
            } else {
                resolve(messaging._eventEmitter.on(channel, listener));
            }
        });
    },
    emit: function (channel, message) {
        return new Promise(function (resolve, reject) {
            if (messaging._channel[channel]) {
                let ch = messaging._channel[channel];

                ch.assertQueue(channel, {durable: true});
                ch.sendToQueue(channel, new Buffer(message + ""), {persistent: true}, () => {
                    resolve();
                });
            } else {
                messaging._client.createChannel(function (err, ch) {
                    if (err) {
                        reject(err);
                    }

                    ch.assertQueue(channel, {durable: true});
                    // ch.sendToQueue(channel, new Buffer(message + ""), {persistent: true}, () => {
                    //     resolve();
                    // });
                });
            }
        });
    }
};

module.exports = function () {
    return new Promise((resolve, reject) => {
        if (messaging._client == undefined) {
            amqp.connect('amqp://localhost', function (err, conn) {
                messaging._client = conn;
                resolve(messaging);
            });
        } else {
            resolve(messaging);
        }
    });
};