package udp

import (
	"errors"
	"fmt"
	"messageBroker/logging"
	"messageBroker/msgBroker"
	"net"
	"strings"
)

var reqs = make(chan request)

type request struct {
	conn          *net.UDPConn
	addr          *net.UDPAddr
	method, topic string
	msg           []byte
}

func Start() (err error) {
	protocol := "udp"
	addr := net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 8080,
	}

	// Create UDP Listener
	conn, err := net.ListenUDP(protocol, &addr)
	defer conn.Close()

	go handleReqs()

	if err != nil {
		return errors.New(fmt.Sprintf("Error starting listener: %+s\n", err))
	}
	logging.Write(0, "UDP Listening on port 8080...")

	for {
		handleConn(conn)
	}
}

func handleConn(c *net.UDPConn) {
	buf := make([]byte, 1024)

	// Read in UDP client request. n = no. bytes in req, addr = UDPAddr struct
	n, addr, err := c.ReadFromUDP(buf)
	req := string(buf[0 : n-1])
	if err != nil {
		_, err := fmt.Fprint(c, err.Error())
		logging.WriteErrSentToClient(0, err)
		return
	}
	if strings.TrimSpace(string(buf[0:n])) == "STOP" {
		logging.Write(0, "Exiting UDP Server")
		return
	}
	parseReq(c, addr, req)
}

func parseReq(c *net.UDPConn, addr *net.UDPAddr, req string) {
	var topic string
	var msg []byte
	r := strings.Split(req, ":") // pub:topic:message for publishing, sub:topic for subscribing
	method := strings.ToUpper(r[0])

	if len(r) == 2 {
		topic = r[1]
	}

	if len(r) == 3 {
		topic = r[1]
		strMsg := r[2]
		msg = []byte(strMsg)
	}
	reqs <- request{conn: c, addr: addr, method: method, topic: topic, msg: msg}
}

func handleReqs() {
	for r := range reqs {
		switch r.method {
		case "PUB":
			value, err := msgBroker.Publish(r.topic, r.msg)
			if errExists(0, err) {
				fmt.Fprintf(r.conn, err.Error())
				logging.WriteErrSentToClient(0, err)
				return
			}
			_, err = r.conn.WriteToUDP(value, r.addr)
			if errExists(0, err) {
				return
			}
		case "SUB":
			msgs := msgBroker.Subscribe(r.topic)
			go writeToSub(r.conn, r.addr, msgs)
		default:
			err := errors.New("Please provide request in the format 'Pub:Topic:Message' || Sub:Topic\n")
			fmt.Fprintf(r.conn, err.Error())
			logging.WriteErrSentToClient(0, err)
			return
		}
	}
}

func writeToSub(c *net.UDPConn, addr *net.UDPAddr, msgs chan msgBroker.Response) {
	for {
		select {
		case m := <-msgs:
			if m.Err != nil {
				return
			}

			_, err := c.WriteToUDP(m.Value, addr)
			if errExists(0, err) {
				return
			}
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
