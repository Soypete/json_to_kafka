package main

import (
	"flag"

	"github.com/Soypete/json_to_kafka/consumer"
	"github.com/Soypete/json_to_kafka/producer"
)

var (
	jsonMessage = `{"data":{"name":"pete","job":"data munger", "death":"cloudevents"}}`
)

func main() {
	producerToggle := flag.Bool("producer", true, "if true publishes to stream, otherwise it consumes. Default is true")
	brokers := flag.String("brokers", "localhost:9092", "A comma separated list of kafka brokers")
	partition := flag.Int("partition", 0, "That partition to consume from/publish to")
	offset := flag.Int("offset", 0, "the offset that you want to publish to")
	topic := flag.String("topic", "", "The topic to consume from/publish to")

	flag.Parse()

	if !*producerToggle {
		consumer.ConsumeMessage(*brokers, *topic, int64(*offset), int32(*partition))
	} else {
		producer.PublishMessage(*brokers, *topic, jsonMessage)
	}
}
