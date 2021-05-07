### UDP

- Server will only listen for incoming messages. A UDP service needs to connect to it temporarily
- Can clients send heartbeats periodically to tell the server they're still alive? 
- A UDP connection doesn't mean they have a port open to listen for a reply??
- Since it's connectionless, needs to extract the client <host:port> to send a response back to

### TCP




### Things to consider

How to handle clients closing connections - preferably, the broker will track this
Should you publish to your own terminal if you're also subscribed to the topic you're publishing to?