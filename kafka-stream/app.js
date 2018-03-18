var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var Client = kafka.Client;
var client = new Client('localhost:2181');
var argv = require('optimist').argv;
var topic = argv.topic || 'twitter_stream_test1';
var p = argv.p || 0;
var a = argv.a || 0;
var producer = new Producer(client, {
  requireAcks: 1
});


var Twitter = require('twitter');
require('dotenv').config()

var client = new Twitter({
  consumer_key: process.env.CONSUMER_KEY,
  consumer_secret: process.env.CONSUMER_SECRET,
  access_token_key: process.env.ACCESS_KEY,
  access_token_secret: process.env.ACCESS_SECRET
});

var stream = client.stream('statuses/filter', {
  track: 'javascript'
});
stream.on('data', function(event) {
  console.log(event);
  producer.send([{
    topic: topic,
    partition: p,
    messages: [event.text],
    attributes: a
  }], function(err, result) {
    console.log(err || result);
  });
  console.log(event && event.text);
});

stream.on('error', function(error) {
  throw error;
});

producer.on('error', function(err) {
  console.log('error', err);
});
