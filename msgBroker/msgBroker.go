package msgBroker

import (
	"fmt"
	"messageBroker/logging"
)

/*
1. Publish requrest comes through from either server
2. Listener processes publish requests, writing the message to:
	- An existing topic
	- A new topic if it doesn't exist
	* Once a new msg is published, it needs to be sent to all active subs
3. Listener processs subscribe requests, tracking:
	- Channels established for all UDP and TCP subs
*/

type request struct {
	method, topic string
	msg           []byte // only for publishing
	res           chan Response
}

type Response struct {
	Value []byte
	Err   error
}

// [string] represents the topic name
var subs = make(map[string][]chan Response)
var topics = make(map[string][]byte)
var reqs = make(chan request, 100)
var done = make(chan struct{})

func Listen() {
	for r := range reqs {
		switch r.method {
		case "PUB":
			r.res <- publish(r) // On publish, trigger the message to be sent to all subscribers
			close(r.res)
		case "SUB":
			subscribe(r) // On subscribe, track the channel that will be used to communicate to subs to a particular topic
		}
	}
	done <- struct{}{}
}

func Stop() {
	close(reqs)
	<-done
	logging.Write(0, "Listener Stopped.")
}

func Publish(topic string, msg []byte) ([]byte, error) {
	req := request{method: "PUB", topic: topic, msg: msg, res: make(chan Response)}
	reqs <- req
	res := <-req.res
	return res.Value, res.Err
}

func publish(r request) Response {
	topics[r.topic] = r.msg // Creates a new topic and msg if none existed, or overwrites previous msg if already exists

	// Should only write to channels whose client connections haven't closed
	go func() {
		for _, ch := range subs[r.topic] {
			ch <- Response{Value: r.msg, Err: nil}
		}
	}()

	return Response{Value: []byte(fmt.Sprintf("Published message to: %s\n", r.topic)), Err: nil}
}

func Subscribe(topic string) chan Response {
	req := request{method: "SUB", topic: topic, res: make(chan Response)}
	reqs <- req
	return req.res
}

func subscribe(r request) {
	// Track all subs publishing to a topi needs to write to
	subs[r.topic] = append(subs[r.topic], r.res)
	r.res <- Response{Value: []byte(fmt.Sprintf("Subscribed to: %s\n", r.topic)), Err: nil}
	return
}
