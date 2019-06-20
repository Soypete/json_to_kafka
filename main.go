package main

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/Soypete/json_to_kafka/consumer"
	"github.com/Soypete/json_to_kafka/producer"
	jose "gopkg.in/square/go-jose.v2"
)

var (
	jsonMessage = map[string]interface{}{"name": "pete", "job": "data munger", "death": "json encoding"}
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
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		pubKey := &privateKey.PublicKey
		encrypter, err := jose.NewEncrypter(jose.A128GCM, jose.Recipient{Algorithm: jose.RSA_OAEP, Key: pubKey}, nil)
		if err != nil {
			panic(err)
		}
		for k, v := range jsonMessage {
			switch vv := v.(type) {
			case string:
				fmt.Println(k, "is string", vv)
				encr, err := encrypter.Encrypt([]byte(vv))
				if err != nil {
					panic(err)
				}
				jsonMessage[k] = encr
			case []interface{}:
				fmt.Println(k, "is an array:")
				for i, u := range vv {
					fmt.Println(i, u)
				}
			default:
				fmt.Println(k, "is of a type I don't know how to handle")
			}
		}
		msg, err := json.Marshal(jsonMessage)
		producer.PublishMessage(*brokers, *topic, msg)
	}
}
