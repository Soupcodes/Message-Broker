package main

import (
	"log"
	"messageBroker/logging"
	"messageBroker/msgBroker"
	"messageBroker/tcp"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	done := make(chan os.Signal)
	errs := make(chan error)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	go logging.Start()
	go msgBroker.Listen()

	go func() {
		err := tcp.Start()
		if err != nil {
			log.Println(err)
			errs <- err
		}
	}()

	go func() {
		err := udp.Start()
		if err != nil {
			log.Println(err)
			errs <- err
		}
	}()

	select {
	case <-done:
	case <-errs:
	}
	tcp.Stop()
	msgBroker.Stop()
	logging.Stop()
}
