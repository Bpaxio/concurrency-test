package main

import (
	json "encoding/json"
	"strings"

	kafka "github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

var (
	mustResetOffset = true
)

type userStateHandler struct {
	Topic           string
	latestOffsets   map[int32]int64
	ResultChan      chan<- bool
	readingFinished chan int32
}

func NewHandler(resultChan chan<- bool, topic string) *userStateHandler {
	return &userStateHandler{Topic: topic, ResultChan: resultChan, latestOffsets: make(map[int32]int64), readingFinished: make(chan int32, 100)}
}

//Before processing. 	Only once for all claims (partitions). Or in rejoin events.
func (h userStateHandler) Setup(session kafka.ConsumerGroupSession) error {
	//read state from metrics
	//read state from users
	//notify, that app is ready to go

	parititions := session.Claims()[h.Topic]
	//in sesson.Claim = map[h.topic][]partition, i.e. partitions list for h.topic
	getOffsetsForClaims(h.latestOffsets, strings.Split(brokers, ","), h.Topic, parititions)

	go func(h userStateHandler) {
		waitForSuccess(h)
	}(h)
	//в sesson.Claim = map[h.topic][]partition, i.e. partitions list for h.topic
	for _, p := range parititions {
		session.ResetOffset(h.Topic, p, kafka.OffsetOldest, "")
	}
	// mustResetOffset = false
	log.Infof("handler has another & %#v", h)

	return nil
}
func (h userStateHandler) done() {
	close(h.ResultChan)
	close(h.readingFinished)
}

func waitForSuccess(handler userStateHandler) {

	pNumber := len(handler.latestOffsets)
	for partition := range handler.readingFinished {
		log.Infof("finished partition %d", partition)
		pNumber--
		if pNumber == 0 {
			handler.ResultChan <- true
			handler.done()
		}
	}
}

//After proccessing
func (userStateHandler) Cleanup(_ kafka.ConsumerGroupSession) error {
	log.Info("Clean up called")
	return nil
}

//for each claim ... is then called in a separate goroutine!!! claim ~ partition
func (h userStateHandler) ConsumeClaim(session kafka.ConsumerGroupSession, claim kafka.ConsumerGroupClaim) error {
	// claim.Messages() - channel. loop until channel close (disconnect or reballance events).
	for msg := range claim.Messages() {
		log.Infof("Got message. Topic %v, partition:%d offset:%d", msg.Topic, msg.Partition, msg.Offset)
		// way to unmarshal it
		var userState *idpUserStats
		if err := json.Unmarshal(msg.Value, &userState); err != nil {
			log.Errorln("Failed to parse message:", err)
			continue
		}
		log.Debugf("Got state. Key:%v, data: %v, parsed to %v", string(msg.Key), string(msg.Value), userState)
		if h.latestOffsets[msg.Partition] == msg.Offset {
			log.Infof("It's end of %v[%d] - offset:%d(should be %v)", msg.Topic, msg.Partition, msg.Offset, h.latestOffsets[msg.Partition])
			h.readingFinished <- msg.Partition
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func getOffsetsForClaims(latestOffsets map[int32]int64, urls []string, topic string, partitions []int32) map[int32]int64 {
	for i := range urls {
		broker := kafka.NewBroker(urls[i])
		config, err := newKafkaConfig()
		if err != nil {
			panic(err)
		}
		err = broker.Open(config)
		if err != nil {
			panic(err)
		}
		// OffsetRequest

		request := &kafka.OffsetRequest{Version: int16(1)}
		for partition := range partitions {
			request.AddBlock(topic, int32(partition), -1, 0)
		}
		response, err := broker.GetAvailableOffsets(request)
		if err != nil {
			_ = broker.Close()
			panic(err)
		}
		_ = broker.Close()

		log.Info("GetAvailableOffsets")
		for partition, offsetResponse := range response.Blocks[topic] {
			if offsetResponse.Err != 0 {
				log.Debugf("broker(%v) assignment[%v - %v]: Err: '%v'", urls[i], topic, partition, offsetResponse.Err)
				continue
			}
			log.Infof("broker(%v) assignment[%v - %v]: Offset: '%v'", urls[i], topic, partition, offsetResponse.Offset)
			if offsetResponse.Offset < 1 {
				latestOffsets[partition] = -1
			} else {
				latestOffsets[partition] = offsetResponse.Offset - 1
			}
		}

	}
	log.Infof("latest Offsets is: %+v", latestOffsets)
	return latestOffsets
}
