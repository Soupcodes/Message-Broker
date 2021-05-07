package logging

import (
	"fmt"
	"log"
	"os"
)

const verbosity = 0

type message struct {
	level int
	msg   string
}

var logs = make(chan message, 100)
var done = make(chan struct{})

var l *log.Logger = log.New(os.Stdout, "", log.Ldate+log.Ltime)

func Write(level int, msg string) {
	if verbosity >= level {
		logs <- message{level: level, msg: msg}
	}
}

func Writef(level int, format string, value ...interface{}) {
	Write(level, fmt.Sprintf(format, value...))
}

func WriteErrSentToClient(logLevel int, err error) {
	if err != nil {
		Write(logLevel, err.Error())
	}
}

func Start() {
	// Can't add metrics about uptime of this service because of circular imports...
	for m := range logs {
		l.Println(m.msg)
	}
	done <- struct{}{} // Only breaks out of for ... range to reach this point when the channel is closed
}

func Stop() {
	close(logs)
	<-done
	fmt.Println("Logger stopped.")
}
