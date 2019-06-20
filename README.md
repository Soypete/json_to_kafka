# json_to_kafka
publishes a json body to kafka

## To Run

```bash 
go run main.go -brokers "localhost:9092" -topic "json_test"
go run main.go -brokers "35.184.0.254:9092" -topic "json_test" -producer=false
```
