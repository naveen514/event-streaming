var kafka = require('kafka-node');
//var Consumer = kafka.Consumer;
var client = new kafka.Client("localhost:2181");

var url = "mongodb://localhost:27017/event_stream";
var dbclient = null;
const MongoClient = require('mongodb').MongoClient

//var db = new Db('test', new Server('localhost', 27017));

function trimSpecialChars(str) {
  return str.replace(/^[\s|\[|\]|\"|*]+|[\s|\[|\]|\"|*]+$/g,'')
}

MongoClient.connect(url, (err, db) => {
  // ... start the server
  if (err) throw err;
  dbclient = db.db("event_stream");
  console.log("Database created!");
  console.log(dbclient)
  dbclient.createCollection("devices", function(err, res) {
      if (err) throw err;
      console.log("devices Collection created!");
    });

  dbclient.createCollection("events", function(err, res) {
      if (err) throw err;
      console.log("events Collection created!");
    });

    var consumer = new kafka.Consumer(
            client,
            [
                { topic: 'event_stream', partition: 0 }
            ],
            {
                autoCommit: true
            }
        );
    consumer.on('message', function (message) {
      var data = null;
      //console.log(message.value)
      try {
        data = JSON.parse(message.value);
      } catch (e) {
        return console.error(e);
      }

      var msgsplit = data.message.split(' ');
      console.log(msgsplit)

      var event = {}
      //console.log(trimSpecialChars(msgsplit[14]))
      event["device_id"] = trimSpecialChars(msgsplit[14])
      event["device_type"] = trimSpecialChars(msgsplit[15])
      event["tracer_id"] = trimSpecialChars(msgsplit[17])
      event["api_name"] = trimSpecialChars(msgsplit[7]).split('/').pop()
      event["status"] = parseInt(trimSpecialChars(msgsplit[9]), 10)
      event["request_length"] = parseInt(trimSpecialChars(msgsplit[10]), 10)
      event["response_length"] = parseInt(trimSpecialChars(msgsplit[11]), 10)

      //var event_time_arr = [];
      var event_time_arr = trimSpecialChars(msgsplit[4])
      event_time_arr = event_time_arr.split(':');
      var date_slice = event_time_arr[0]
      var time_slice = event_time_arr.slice(1,event_time_arr.length).join(':')

      var event_time = [date_slice, time_slice].join(' ')
      event["event_timestamp"] = new Date(event_time)
      //console.log(event_time)

      dbclient.collection("events").insert(event, function(err, res) {
        if (err) throw err;
        //console.log("message:"+message+" inserted")
        console.log("Event Inserted!")
      });
     });

    consumer.on('error', function (err) {
      console.log('error', err);
    });

})

//console.log(client)
