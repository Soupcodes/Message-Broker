package tcp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"messageBroker/logging"
	"messageBroker/msgBroker"
	"net"
	"strings"
	"time"
)

var li net.Listener
var reqs = make(chan request)
var srvDone = make(chan struct{})
var connCount = make(chan int)
var totalConns int

type request struct {
	conn          net.Conn
	method, topic string
	msg           []byte
}

func Start() (err error) {
	li, err = net.Listen("tcp", "localhost:8000") // Listening for incoming Syn. Assign values so global li is not nil
	if err != nil {
		return errors.New(fmt.Sprintf("Error starting listener:%+s\n", err))
	}
	logging.Write(0, "TCP Listening on port 8000...")
	go handleReq()
	go trackConns()

	for {
		conn, err := li.Accept()
		if err != nil {
			if conn == nil {
				return nil
			}
			logging.Write(0, err.Error())
			continue
		}
		logging.Write(0, ">>> Socket established with Client")

		go handleConn(conn)
		connCount <- 1
	}
}

func trackConns() {
	for i := range connCount {
		totalConns += i
	}
}

func handleConn(c net.Conn) {
	buf := bufio.NewReader(c)
	clientDone := make(chan struct{})

	go func() {
		select {
		case <-srvDone:
		case <-clientDone:
		}
		c.Close()
		connCount <- -1
		return
	}()

	for {
		ln, err := buf.ReadString('\n')

		if err == io.EOF {
			logging.Writef(0, "Closing Connection with Client: %s", c.RemoteAddr())
			clientDone <- struct{}{}
			return
		}

		if err != nil {
			_, err := fmt.Fprintf(c, err.Error())
			logErrSentToClient(0, err)
			return
		}

		parseReq(c, ln)
	}
}

func parseReq(c net.Conn, ln string) {
	var topic string
	var msg []byte
	req := strings.Split(ln, ":") // pub:topic:message for publishing, sub:topic for subscribing
	if len(req) < 2 || len(req) > 3 {
		_, err := fmt.Fprintf(c, "Please provide request in the format 'Pub:Topic:Message' || Sub:Topic\n")
		logErrSentToClient(0, err)
		return
	}
	method := strings.ToUpper(req[0])

	if len(req) == 2 {
		topic = strings.TrimSuffix(req[1], "\n") // Causes issues creating a valid topic key if newline isn't trimmed
	}

	if len(req) == 3 {
		topic = req[1]
		strMsg := req[2]
		msg = []byte(strMsg)
	}
	reqs <- request{conn: c, method: method, topic: topic, msg: msg}
}

func Stop() {
	err := li.Close()
	logging.Write(0, "TCP listener stopped.")
	if err != nil {
		logging.Writef(0, "TCP Server Shutdown Failed:%+s", err)
	}
	close(srvDone)

	for totalConns > 0 {
		time.Sleep(1 * time.Second)
	}
}

func handleReq() {
	for r := range reqs {
		switch r.method {
		case "PUB":
			publishTopic(r.conn, r.topic, r.msg)
		case "SUB":
			go subscribeToTopic(r.conn, r.topic)
		default:
			fmt.Fprintf(r.conn, "Method: %q not alowed. Use PUB | SUB\n", r.method)
		}
	}
}

func publishTopic(c net.Conn, topic string, msg []byte) {
	value, err := msgBroker.Publish(topic, msg)
	if errExists(0, err) {
		_, err := fmt.Fprintf(c, err.Error())
		logErrSentToClient(0, err)
		return
	}

	_, err = c.Write(value)
	if errExists(0, err) {
		return
	}
	return
}

func subscribeToTopic(c net.Conn, topic string) {
	msgs := msgBroker.Subscribe(topic)
	for {
		select {
		case m := <-msgs:
			if m.Err != nil {
				if errExists(0, m.Err) {
					_, err := c.Write([]byte(m.Err.Error()))
					logErrSentToClient(0, err)
					return
				}
			}

			// Write to active subscriber
			_, err := c.Write(m.Value)
			if errExists(0, err) {
				return
			}
		case <-srvDone:
			close(msgs) // Does this need to be explicitly closed once the connection to the client is severed?
			return

		}
	}
}

func errExists(logLevel int, err error) bool {
	if err != nil {
		logging.Write(logLevel, err.Error())
		return true
	}
	return false
}

func logErrSentToClient(logLevel int, err error) {
	if err != nil {
		logging.Write(logLevel, err.Error())
	}
}
