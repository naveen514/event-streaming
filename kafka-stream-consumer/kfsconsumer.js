var kafka = require('kafka-node');
//var Consumer = kafka.Consumer;
var client = new kafka.Client("localhost:2181");
//console.log(client)
var consumer = new kafka.Consumer(
        client,
        [
            { topic: 'twitter_stream_test1', partition: 0 }
        ],
        {
            autoCommit: false
        }
    );
consumer.on('message', function (message) {
  console.log(message);
});

consumer.on('error', function (err) {
  console.log('error', err);
});
