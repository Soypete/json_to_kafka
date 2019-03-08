package producer

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"

	sarama "github.com/Shopify/sarama"
)

// PublishMessage sends messages to kafka.
func PublishMessage(brokers string, topic string, jsonMessage string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{brokers}, config)
	if err != nil {
		panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                          sync.WaitGroup
		enqueued, successes, errors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	msg, err := json.Marshal(jsonMessage)
	if err != nil {
		log.Print(err)
	}

ProducerLoop:
	for {
		message := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(msg)}
		select {
		case producer.Input() <- message:
			enqueued++
		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}
