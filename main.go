package main

import (
	"os/signal"
	"time"

	//	_ "net/http/pprof"

	"os"

	log "github.com/sirupsen/logrus"
)

var (
	listenPort       = getEnv("LISTEN_PORT", "9099")
	loglevel, _      = log.ParseLevel(getEnv("LOG_LEVEL", "info"))
	channel          = make(chan int, 500)
	refchan, valchan = make(chan *smth, 5), make(chan smth, 5)
)

type smth struct {
	item  string
	value int
}

func getEnv(envName, defaultValue string) string {
	if val, ok := os.LookupEnv(envName); ok {
		return val
	}
	return defaultValue
}

func checkErrorFatal(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func printInit() {
	log.Debugf("listenPort=%s", listenPort)
	log.Debugf("logLevel=%s", loglevel)

}

func main() {
	log.SetLevel(loglevel)
	printInit()

	osignals := make(chan os.Signal, 1)
	signal.Notify(osignals, os.Interrupt)
	// signal.Notify(osignals)

	prodDrop, consDrop := make(chan string, 1), make(chan string, 1)
	go consume(consDrop)
	go produce(prodDrop)

	handleDrop(osignals, []chan string{prodDrop, consDrop})

}

func consume(dropChan <-chan string) {
	for {
		select {
		case message := <-dropChan:
			log.Infof("SHutting down !!! %v", message)
		case v := <-refchan:
			time.Sleep(1 * time.Second)
			log.Infof("ref value !!! %v", v)
		case v := <-valchan:
			time.Sleep(1 * time.Second)
			log.Infof("value !!! %v", v)
		}

	}
}

func produce(dropChan <-chan string) {
	for {
		example := &smth{item: "ampersant -> *", value: 13}
		select {
		case message := <-dropChan:
			log.Infof("SHutting down !!! %v", message)
		case <-time.After(3 * time.Second):
			log.Infof("send first value %v", example)
			refchan <- example
			valchan <- *example
			example.item = "dropped ampersant -> *"
			example.value = 100500
			log.Infof("changed first value %v", example)
		}

		example1 := smth{item: "just value", value: 22}
		select {
		case message := <-dropChan:
			log.Infof("SHutting down !!! %v", message)
		case <-time.After(3 * time.Second):
			log.Infof("send second value %v", example1)
			refchan <- &example1
			valchan <- example1
			example1.item = "dropped just value"
			example1.value = 100500
			log.Infof("changed second value %v", example1)
		}

		log.Info("\n\n\n")
	}
}

func delayedStart(readyToStart chan<- bool) {
	select {
	case <-time.After(3 * time.Second):
		log.Info("READY !!!")
		readyToStart <- true
	}
}

func handleDrop(signal chan os.Signal, all []chan string) {
	for {
		select {
		case signToStop := <-signal:
			log.Infof("%f[%T]%v\nGraceful shutdown started...", signToStop, signToStop, signToStop)
			for i := range all {
				all[i] <- signToStop.String()
			}
			time.Sleep(1500 * time.Millisecond)
			return
		}
	}
}
