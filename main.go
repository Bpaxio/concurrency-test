package main

import (
	"context"
	"math/rand"
	"os/signal"
	"strings"
	"time"

	//	_ "net/http/pprof"

	"os"

	kafka "github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

var (
	brokers      = getEnv("IDP_USER_METRICS_BROKERS", "localhost:9092,localhost:9093")
	listenPort   = getEnv("LISTEN_PORT", "9099")
	loglevel, _  = log.ParseLevel(getEnv("LOG_LEVEL", "info"))
	channel      = make(chan int, 500)
	readyToStart = make(chan bool)
	// refchan, valchan = make(chan *smth, 5), make(chan smth, 5)

	metricTopic        = getEnv("METRIC_CHANGELOG_TOPIC", "metric-changelog")
	userChangelogTopic = getEnv("USER_CHANGELOG_TOPIC", "user-changelog")
	consumerClientID   = getEnv("IDP_USER_METRICS_CLIENT_ID", "idp-users-metrics-test")
	consumerGroupID    = getEnv("IDP_USER_METRICS_GROUP_ID", "idp-users-metrics-local1")
	producer           kafka.AsyncProducer

	stateChannel = make(chan map[string]*idpUserStats, 150)
)

type idpUserStats struct {
	//key = client_id
	ConsentsEnabled map[string]bool `json:"consents"`
	TfaEnabled      bool            `json:"tfaEnabled"`
	EmailVerified   bool            `json:"emailVerified"`
	PhoneVerified   bool            `json:"phoneVerified"`
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
	rand.Seed(time.Now().UnixNano())

	log.SetLevel(loglevel)
	printInit()
	resultChan := make(chan bool, 1)

	// go func(c <-chan bool) {
	// 	for result := range resultChan {
	// 		log.Infof("Done : %v", result)
	// 	}
	// }(resultChan)
	osignals := make(chan os.Signal, 1)
	signal.Notify(osignals, os.Interrupt)

	// initProducer(osignals)
	// go func(delay time.Duration) {
	// 	makeMsgEvery(delay)
	// }(1)

	go delayedStart(readyToStart)
	<-readyToStart

	go func(resultChan chan bool) {
		// for {
		err := kafkaUserChangelogConsumer(resultChan)
		if err != nil {
			//exit and make docker manage lifecycle
			log.Fatalln(err)
		}
		log.Infof("Group.Consume() returned. Recreating client and group")
		// }

	}(resultChan)
	<-osignals
	log.Info("Full exit")
	// signal.Notify(osignals)

	// prodDrop, consDrop := make(chan string, 1), make(chan string, 1)

	// handleDrop(osignals, []chan string{prodDrop, consDrop})

}
func RandomString(n int) string {
	var letter = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func delayedStart(readyToStart chan<- bool) {
	select {
	case <-time.After(5 * time.Second):
		log.Info("READY !!!")
		readyToStart <- true
	}
}

func makeMsgEvery(delay time.Duration) {
	for {
		select {
		case <-time.After(delay * time.Second):
			singleMsg := &idpUserStats{ConsentsEnabled: map[string]bool{"asd": false, "qwerty": true}, TfaEnabled: true, PhoneVerified: true, EmailVerified: false}
			stateChannel <- map[string]*idpUserStats{RandomString(8): singleMsg}
		}
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

func newKafkaConfig() (*kafka.Config, error) {
	config := kafka.NewConfig()
	config.Version = kafka.V2_0_0_0
	config.ClientID = consumerClientID
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = kafka.OffsetOldest
	return config, nil
}

func kafkaUserChangelogConsumer(resultChan chan bool) error {
	config, err := newKafkaConfig()
	if err != nil {
		return err
	}

	// Start a new consumer group
	group, err := kafka.NewConsumerGroup(strings.Split(brokers, ","), consumerGroupID, config)
	if err != nil {
		return err
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			log.Errorf("%v", err)
		}
	}()

	ctx := context.Background()

	errChan := make(chan error, 1)
	log.Infof("Consuming topic %v on cluster %v", userChangelogTopic, strings.Split(brokers, ","))
	go func(errChan chan<- error) {
		err = group.Consume(ctx, []string{userChangelogTopic}, NewHandler(resultChan, userChangelogTopic))
		if err != nil {
			errChan <- err
		}
	}(errChan)
	select {
	case err := <-errChan:
		log.Infof("err: %+v", err)
		close(errChan)
		break
	case done := <-resultChan:
		log.Infof("done: %v", done)
		close(errChan)
		break
	}
	group.Close()
	log.Info("was exit)))")
	return err
}
