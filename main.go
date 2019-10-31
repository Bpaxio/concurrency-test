package main

import (
	"net/http"
	"os/signal"
	"time"

	//	_ "net/http/pprof"

	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
)

var (
	//bottstrap broker list separated by ,
	metricTotal = promauto.NewGauge(prometheus.GaugeOpts{Subsystem: "idp", Name: "metric_total", Help: "some value"})
	sleeps      = promauto.NewCounter(prometheus.CounterOpts{Subsystem: "idp", Name: "sleeped_total", Help: "The total ms sleeping"})
	listenPort  = getEnv("LISTEN_PORT", "9099")
	loglevel, _ = log.ParseLevel(getEnv("LOG_LEVEL", "info"))
	channel     = make(chan int, 500)
)

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

	// consumerSleeps.Add(100500)
	log.SetLevel(loglevel)
	printInit()
	readyToStart := make(chan bool)
	go httpServer(readyToStart)

	// go httpServer()
	// trap SIGINT to trigger a shutdown.
	osignals := make(chan os.Signal, 1)
	signal.Notify(osignals, os.Interrupt)
	// signal.Notify(osignals)

	prodDrop, consDrop := make(chan string, 1), make(chan string, 1)
	go produce(prodDrop)
	go consume(consDrop)

	go delayedStart(readyToStart)
	handleErr(osignals, []chan string{prodDrop, consDrop})

}

func delayedStart(readyToStart chan<- bool) {
	select {
	case <-time.After(3 * time.Second):
		log.Info("READY !!!")
		readyToStart <- true
	}
}

func handleErr(signal chan os.Signal, all []chan string) {
	for {
		select {
		case smth := <-signal:
			log.Infof("%f[%T]%v\nGraceful shutdown started...", smth, smth, smth)
			for i := range all {
				all[i] <- smth.String()
			}
			time.Sleep(1500 * time.Millisecond)
			return
		}
	}
}

func produce(er chan string) {
	i := 0
	for {
		select {
		case smth := <-er:
			log.Infof("Exit code: %v\nGraceful shutdown producing...", smth)
			return
		default:
			i++
			// log.Infof("peuwh new value %v", i)
			channel <- i
			inc(metricTotal)
			if gaugeValue(metricTotal).GetGauge().GetValue() > 350 {
				incCounter(sleeps)
				time.Sleep(3550 * time.Millisecond)
			}
			time.Sleep(300 * time.Millisecond)

		}
	}
}

func consume(er chan string) {
	for {
		select {
		case smth := <-er:
			log.Infof("exit code: %v\nGraceful shutdown consuming...", smth)
			for dropped := range channel {
				log.Infof("dropped  %v", dropped)
			}
			return
		case smth := <-channel:
			// log.Infof("Recieved : %v", smth)
			log.Debugf("Recieved : %v", smth)
			time.Sleep(600 * time.Millisecond)
			dec(metricTotal)
		default:
			log.Info("sleep consuming")
			incCounter(sleeps)
			time.Sleep(500 * time.Millisecond)

		}
	}

}

func httpServer(readyToStart <-chan bool) {
	//wait for cunsumer to read all messages on start up
	<-readyToStart
	http.Handle("/metrics", promhttp.Handler())
	log.Infof("Metrics on path /metrics port %s", listenPort)
	log.Fatal(http.ListenAndServe(":"+listenPort, nil))
}

func incCounter(g prometheus.Counter) {
	g.Inc()
	metricPrint(g)
}

func inc(g prometheus.Gauge) {
	// log.Info("inc")
	g.Inc()
	metricPrint(g)
}

func dec(g prometheus.Gauge) {
	// log.Info("dec")
	g.Dec()
	metricPrint(g)
}

func metricPrint(m prometheus.Metric) {
	// time.Sleep(250 * time.Millisecond)
	// log.Infof("metric is now: stored")
	// switch value := m.(type) {
	// case prometheus.Counter:
	// 	log.Warnf("counter of %t is: %v", value, counterValue(value))
	// case prometheus.Gauge:
	// 	log.Warnf("gauge is: %v", gaugeValue(value))

	// }
}

func counterValue(m prometheus.Metric) *dto.Metric {
	stored := new(dto.Metric)
	m.Write(stored)
	return stored
}

func gaugeValue(m prometheus.Metric) *dto.Metric {
	stored := new(dto.Metric)
	m.Write(stored)
	return stored
}
