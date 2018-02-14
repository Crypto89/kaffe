package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	log "github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	bulkSize          = kingpin.Flag("bulk.size", "Bulk size").Short('b').Default("5000").Int()
	bulkFlushInterval = kingpin.Flag("bulk.flush.interval", "Bulk flush interval in seconds").Short('i').Default("1").Int()
	bulkProcessors    = kingpin.Flag("bulk.processors", "Concurrent bulk processors").Short('c').Default("1").Int()
	kafkaBroker       = kingpin.Flag("kafka.broker", "Kafka Broker").Short('s').Default("localhost:9092").Strings()
	kafkaGroup        = kingpin.Flag("kafka.group", "Kafka consumer group").Short('g').Default("dev").String()
	kafkaTopic        = kingpin.Flag("kafka.topic", "Kafka Topic").Short('t').Strings()
	kafkaPartition    = kingpin.Flag("kafka.partition", "Kafka partition").Default("0").Int32()
	kafkaVersion      = kingpin.Flag("kafka.version", "Kafka Version").Default("0.10.0.1").String()
	esHost            = kingpin.Flag("es.host", "Elasticsearch host").Short('d').Default("http://localhost:9200/test/logs/_bulk").String()
	esUsername        = kingpin.Flag("es.username", "Elasticsearch username").Short('u').Default("elastic").String()
	esPassword        = kingpin.Flag("es.password", "Elasticsearch password").Short('p').Envar("ES_PASSWORD").Default("changeme").String()
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func main() {
	kingpin.Parse()

	version, err := sarama.ParseKafkaVersion(*kafkaVersion)
	if err != nil {
		log.Fatalf("failed to parse kafka version: %s", err)
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Version = version

	consumer, err := cluster.NewConsumer(*kafkaBroker, *kafkaGroup, *kafkaTopic, config)
	if err != nil {
		log.Fatalf("failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// setup all the channels
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Warnf("error: %s", err)
		}
	}()

	// consume notifications
	go func() {
		for notif := range consumer.Notifications() {
			log.Infof("notification: %s", notif)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < *bulkProcessors; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bulkProcessor(ctx, i, consumer)
		}(i)
	}

	select {
	case <-signals:
		log.Debug("got signal")
		cancel()
		wg.Wait()
		time.Sleep(1 * time.Second)
		return
	}
}

func bulkProcessor(ctx context.Context, worker int, consumer *cluster.Consumer) {
	log.WithField("worker", worker).Debug("Starting processor")
	client := &http.Client{}

	for {
		var buffer bytes.Buffer
		interval := *bulkFlushInterval

		stash := cluster.NewOffsetStash()
		bctx, cancel := context.WithTimeout(ctx, time.Duration(interval)*time.Second)
		makeBulk(bctx, &buffer, consumer.Messages(), stash)
		cancel()

		if buffer.Len() != 0 {
			req, err := http.NewRequest("POST", *esHost, &buffer)
			req.Header.Add("Content-Type", "application/x-ndjson")
			if err != nil {
				log.Fatalf("bulk: %s", err)
			}

			if *esUsername != "" && *esPassword != "" {
				req.SetBasicAuth(*esUsername, *esPassword)
			}

			resp, err := client.Do(req)

			if err != nil || resp.StatusCode != 200 {
				log.Warnf("bulk request failed: %s", resp.StatusCode)
				fmt.Println(err)

				if resp != nil {
					io.Copy(os.Stdout, resp.Body)
					panic(err)
				}
			} else {
				consumer.MarkOffsets(stash)
			}

			log.WithFields(log.Fields{"worker": worker, "status": resp.StatusCode}).Debugf("bulk flush complete")

			resp.Body.Close()
		}

		select {
		case <-ctx.Done():
			log.WithField("worker", worker).Debug("Context cancelled, stopping processor")
			return
		default:
		}
	}
}

func makeBulk(ctx context.Context, w io.Writer, c <-chan *sarama.ConsumerMessage, stash *cluster.OffsetStash) {
	for i := 0; i < *bulkSize; i++ {
		select {
		case msg := <-c:
			fmt.Fprintln(w, "{\"index\": {}}")
			w.Write(msg.Value)
			stash.MarkOffset(msg, "")
			fmt.Fprintf(w, "\n")
		case <-ctx.Done():
			return
		}
	}
}
