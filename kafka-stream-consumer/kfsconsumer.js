var kafka = require('kafka-node');
//var Consumer = kafka.Consumer;
var client = new kafka.Client("localhost:2181");

var url = "mongodb://localhost:27017/event_stream";
var dbclient = null;
const MongoClient = require('mongodb').MongoClient

//var db = new Db('test', new Server('localhost', 27017));

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
                { topic: 'eventlog', partition: 0 }
            ],
            {
                autoCommit: true
            }
        );
    consumer.on('message', function (message) {
      var data = null;
      console.log(message.value)
      try {
        data = JSON.parse(message.value);
      } catch (e) {
        return console.error(e);
      }

      var event = {}
      event["device_id"] = data.device_id
      event["tracer_id"] = data.tracer_id
      event["api_name"] = data.api_name
      event["status"] = data.status
      console.log(event)

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
