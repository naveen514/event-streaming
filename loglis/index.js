const io = require('socket.io-client')('http://localhost:27888');

socket.on('connect', function() {
  console.log("Connected!");
});
socket.on('event', function(data) {
  console.log("event :: " + event);
});
socket.on('disconnect', function() {});
