package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"context"
	"./lib"

	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"go.opencensus.io/trace"

)

func callapi(raws string, ctx context.Context) string {
	_, span := trace.StartSpan(ctx, "callapi")
        defer span.End()
	raw := raws
	fmt.Println(raw)
	url := "https://api.hashify.net/hash/highway-128/hex"
	method := "POST"
	payload := strings.NewReader(raw)
	client := &http.Client {
	}
	req, err := http.NewRequest(method, url, payload)
  
	if err != nil {
	  fmt.Println(err)
	}
	req.Header.Add("Content-Type", "text/plain; charset=utf-8")
  	req.Header.Add("X-Hashify-Key", "random")
	res, err := client.Do(req)
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	rstring := string(body)
	return rstring
}
func convertstr(strin string) string {
	ctx, span := trace.StartSpan(context.Background(), "convertstr")
        traceId := fmt.Sprintf("", span.SpanContext().TraceID)
        spanId := fmt.Sprintf("", span.SpanContext().SpanID)
	id := traceId+" : "+spanId
	id = strings.Replace(id, "%!(EXTRA", "", -1)
        fmt.Println(id)
        defer span.End()
	

	str := strin
	fmt.Printf("convert : " + str + "\n")
	str = strings.Replace(str, "\":[", "\":{", -1)
	str = strings.Replace(str, "]}", "}}", -1)
	str = strings.Replace(str, "TransactionTS\":", "TransactionTS\":\"", -1)
	str = strings.Replace(str, ",\"UserID", "\",\"UserID", -1)
	str = strings.Replace(str, "Operation\":\"I", "Operation\":\"Insert", -1)
	str = strings.Replace(str, "Operation\":\"U", "Operation\":\"Update", -1)
	str = strings.Replace(str, "Operation\":\"D", "Operation\":\"Delete", -1)
	str = strings.Replace(str, "Operation\":\"C", "Operation\":\"Clear", -1)
	str = callapi(str, ctx)
	return str
}

func main() {
	lib.RegisterZipkin()

	viper.SetConfigName("default") // config file name without extension
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config/") // config file path
	viper.AutomaticEnv()             // read value ENV variable
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	err := viper.ReadInConfig()
	if err != nil {
		//panic(fmt.Error("fatal error config file: %s \n", err))
		lib.Linenotify("fatal error config file: ")
		os.Exit(1)
	}

	viper.SetDefault("app.broker", "127.0.0.1:9092")
	viper.SetDefault("app.sessiontimeout", "6000")
	viper.SetDefault("app.consumerbroker", "localhost:9092")
	viper.SetDefault("app.producerbroker", "localhost:9092")

	broker := viper.GetString("app.broker")
	group := viper.GetString("app.group")
	topics := viper.GetString("app.consumertopic")
	topic := viper.GetString("app.producertopic")
	sessiontimeout := viper.GetString("app.sessiontimeout")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	p, errp := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    sessiontimeout,
		"auto.offset.reset":     "earliest"})
	if errp != nil {
		fmt.Printf("Failed to create producer: %s\n", errp)
		lib.Linenotify("Failed to create producer: %s\n")
		os.Exit(1)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    sessiontimeout,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		lib.Linenotify("Failed to create consumer: %s\n")
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	deliveryChan := make(chan kafka.Event)
	err = c.Subscribe(topics, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			lib.Linenotify("Caught signal %v: terminating\n")
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:

				str := string(e.Value)
				str = convertstr(str)

				err := p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          []byte(str),
					Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
				}, deliveryChan)
				ep := <-deliveryChan
				mp := ep.(*kafka.Message)
				if mp.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n %s", mp.TopicPartition.Error, err)
					lib.Linenotify("Delivery failed: %v\n")
				} else {
					fmt.Printf("Producer : " + str + "\n")
				}

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("")
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
	fmt.Printf("Closing producer\n")
	line := lib.Linenotify("ExitConvert")
	fmt.Printf("\n" + line + "\n")
	close(deliveryChan)
}