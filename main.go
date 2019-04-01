package main

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"flag"
	"log"

	"github.com/Soypete/json_to_kafka/consumer"
	"github.com/Soypete/json_to_kafka/producer"
	jose "gopkg.in/square/go-jose.v2"
)

var (
	jsonMessage = `{"data":{"name":"pete","job":"data munger", "death":"cloudevents"}}`
)

func main() {
	producerToggle := flag.Bool("producer", true, "if true publishes to stream, otherwise it consumes. Default is true")
	brokers := flag.String("brokers", "localhost:9092", "A comma separated list of kafka brokers")
	partition := flag.Int("partition", 0, "That partition to consume from/publish to")
	offset := flag.Int("offset", 2030572, "the offset that you want to publish to")
	topic := flag.String("topic", "", "The topic to consume from/publish to")

	flag.Parse()

	if !*producerToggle {
		log.Println("consuming from kafka")
		consumer.ConsumeMessage(*brokers, *topic, int64(*offset), int32(*partition))
	} else {
		// ctx := context.Background()
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		pubKey := &privateKey.PublicKey
		// key := jose.JSONWebKey{
		// 	Algorithm: "RSA_OAEP",
		// 	Key:       pubKey,
		// }
		encrypter, err := jose.NewEncrypter(jose.A128GCM, jose.Recipient{Algorithm: jose.RSA_OAEP, Key: pubKey}, nil)
		if err != nil {
			panic(err)
		}
		msg, err := json.Marshal(jsonMessage)
		encrMsg, err := encrypter.Encrypt(msg)
		// encrMsg, err := encrypt.JWE(ctx, key, msg)
		// if err != nil {
		// 	panic(err)
		// }
		serilazedMsg := encrMsg.FullSerialize()
		str := &serilazedMsg
		producer.PublishMessage(*brokers, *topic, *str)
	}
}
