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

                        ch.assertExchange(channel, 'fanout', {durable: false});

                        ch.assertQueue('', {exclusive: true}, function (err, q) {
                            if (err) {
                                reject(err);
                            } else {
                                ch.bindQueue(q.queue, channel, '');

                                ch.consume(q.queue, function (message) {
                                    messaging._eventEmitter.emit(channel, JSON.parse(message.content));
                                });

                                resolve(messaging._eventEmitter.on(channel, listener));
                            }
                        });
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

                ch.publish(channel, '', Buffer.from(message + ""), () => {
                    resolve();
                });
            } else {
                messaging._client.createChannel(function (err, ch) {
                    if (err) {
                        reject(err);
                    }

                    ch.assertExchange(channel, 'fanout', {durable: false});

                    ch.publish(channel, '', Buffer.from(message + ""), () => {
                        resolve();
                    });
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

