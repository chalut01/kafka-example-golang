package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	//	"encoding/json"
	"bytes"
	"io/ioutil"
	"net/http"

	"./lib"

	"github.com/spf13/viper"
)

func callapi(raws string) string {
	raw := raws
	url := "http://api.hashify.net/hash/md5/hex"
	//    fmt.Println("URL:>", url)
	//    fmt.Println("check raw :>", raw)
	var jsonStr = []byte(raw)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
		lib.Linenotify("test")
	}
	defer resp.Body.Close()
	//    fmt.Println("response Status:", resp.Status)
	//    fmt.Println("response Headers:", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	//    fmt.Println("response Body:", string(body))
	rstring := string(body)
	return rstring
}
func convertstr(strin string) string {
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
	str = callapi(str)
	return str
}

func main() {
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
