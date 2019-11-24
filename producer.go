package main

import (
	"errors"
	"os"
	"strings"
	"time"

	json "encoding/json"

	kafka "github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

func initProducer(osignals chan os.Signal) (err error) {
	config := kafka.NewConfig()
	config.Version = kafka.V2_0_0_0
	config.ClientID = consumerClientID

	producer, err = kafka.NewAsyncProducer(strings.Split(brokers, ","), config)
	// producer, err = kafka.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Infof("creating producer failed with error: %v", err)
		return err
	}
	// if useTLS == "true" {
	// 	//TLS config
	// 	rootCAFile, err := ioutil.ReadFile(rootCaFileName)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	rootCAPool := x509.NewCertPool()
	// 	if ok := rootCAPool.AppendCertsFromPEM(rootCAFile); !ok {
	// 		return nil, fmt.Errorf("Cannot parse rootCA %s", rootCaFileName)
	// 	}
	// 	cert, err := tls.LoadX509KeyPair(certFileName, keyFileName)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	config.Net.TLS.Enable = true
	// 	config.Net.TLS.Config = &tls.Config{RootCAs: rootCAPool, Certificates: []tls.Certificate{cert}}
	// }
	log.Info("Successfully initialized new producer")
	go startProducer(osignals)
	return nil
}

func startProducer(osignals chan os.Signal) (err error) {
	defer func() { _ = producer.Close() }()
	for {
		select {
		case state := <-stateChannel:
			sendUserMsg(state)
		case err := <-producer.Errors():
			log.Errorf("Failed to produce message %v", err)
		case success := <-producer.Successes():
			log.Infof("Sent message value='%s' at partition = %d, offset = %d\n", success.Value, success.Partition, success.Offset)
		case signal := <-osignals:
			log.Infof("Closing by %v[%T]\nStarting graceful shutdown...", signal, signal)
			osignals <- signal
			err = errors.New("closed by system")
			return err
		default:
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

func sendUserMsg(input map[string]*idpUserStats) {
	for id, state := range input {
		sendMsg(userChangelogTopic, id, state)
	}
}

func sendMsg(topic string, key string, value interface{}) {
	message, err := json.Marshal(value)
	if err != nil {
		log.Errorln("Failed to parse message:", err)
		return
	}

	// way to unmarshal it
	// d, err := json.Marshal(newUser)
	// if err == nil {
	// 	var n *idpUserStats
	// 	json.Unmarshal(d, &n)
	// 	log.Infof("Got state.it was:%v, converted to data: %v, parsed %v", newUser, d, n)
	// }

	log.Infof("producer: %v, key: %v, body: %v", producer, key, string(message))
	msg := kafka.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Key:       kafka.StringEncoder(key),
		Value:     kafka.ByteEncoder(message),
	}
	producer.Input() <- &msg
}
