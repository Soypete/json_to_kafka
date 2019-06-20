package consumer

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	sarama "github.com/Shopify/sarama"
)

// ConsumeMessage takes the arguments necessary for connecting to kafka brokers. Connects and pulls down message.
func ConsumeMessage(brokers string, topic string, offset int64, partition int32) {
	consumer, err := sarama.NewConsumer([]string{brokers}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		panic(err)
	}
	log.Println("consumer setup")
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			encryptedMsg := msg.Value
			var objmap map[string]interface{}
			str := json.Unmarshal(encryptedMsg, &objmap)
			log.Printf("Consumed message offset %d\n message:%v", msg.Offset, str)
			consumed++
			time.Sleep(3 * time.Second)
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("Consumed: %d\n", consumed)
}
