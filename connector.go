package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	flow "github.com/23m/protobuf-flows"
	"github.com/golang/protobuf/proto"

	"net/http"

	prometheusmetrics "github.com/deathowl/go-metrics-prometheus"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Connector handles a connection to read bwNetFlow flows from kafka.
type Connector struct {
	user             string
	pass             string
	authDisable      bool
	tlsDisable       bool
	prometheusEnable bool

	consumer *Consumer

	producer         sarama.AsyncProducer
	producerChannels map[string](chan *flow.FlowMessage)
	producerWg       *sync.WaitGroup
}

// DisableAuth disables authentification
func (connector *Connector) DisableAuth() {
	connector.authDisable = true
}

// DisableTLS disables ssl/tls connection
func (connector *Connector) DisableTLS() {
	connector.tlsDisable = true
}

// EnablePrometheus enables metric exporter for both, Consumer and Producer
func (connector *Connector) EnablePrometheus(listen string) {
	connector.prometheusEnable = true
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(listen, nil)
}

// SetAuth explicitly set which login to use in SASL/PLAIN auth via TLS
func (connector *Connector) SetAuth(user string, pass string) {
	connector.user = user
	connector.pass = pass
}

// Set anonymous credentials as login method.
func (connector *Connector) SetAuthAnon() {
	connector.user = "anon"
	connector.pass = "anon"
}

// Check environment to infer which login to use in SASL/PLAIN auth via TLS
// Requires KAFKA_SASL_USER and KAFKA_SASL_PASS to be set for this process.
func (connector *Connector) SetAuthFromEnv() error {
	connector.user = os.Getenv("KAFKA_SASL_USER")
	connector.pass = os.Getenv("KAFKA_SASL_PASS")
	if connector.user == "" || connector.pass == "" {
		return errors.New("Setting Kafka SASL info from Environment was unsuccessful.")
	}
	return nil
}

// EnablePrometheus enables metric exporter for both, Consumer and Producer
func (connector *Connector) NewBaseConfig() *sarama.Config {
	config := sarama.NewConfig()

	// NOTE: This version enables Sarama support for everything we need and
	// more. However, a lower version might suffice to.
	// Actual, higher cluster versions are still supported with this.
	// Consider making this configurable anyways
	version, err := sarama.ParseKafkaVersion("2.4.0")
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	config.Version = version

	if !connector.tlsDisable {
		// Enable TLS
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			log.Panicf("TLS Error: %v", err)
		}
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}
	}

	if !connector.authDisable {
		config.Net.SASL.Enable = true

		if connector.user == "" && connector.pass == "" {
			log.Println("No Auth information is set. Assuming anonymous auth...")
			connector.SetAuthAnon()
		}
		config.Net.SASL.User = connector.user
		config.Net.SASL.Password = connector.pass
	}

	return config
}

// Start a Kafka Consumer with the specified parameters. Its output will be
// available in the channel returned by ConsumerChannel.
func (connector *Connector) StartConsumer(brokers string, topics []string, group string, offset int64) error {
	var err error
	config := connector.NewBaseConfig()

	if connector.prometheusEnable {
		prometheusClient := prometheusmetrics.NewPrometheusProvider(config.MetricRegistry, "sarama", "consumer", prometheus.DefaultRegisterer, 10*time.Second)
		go prometheusClient.UpdatePrometheusMetrics()
	}

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = offset

	// everything declared and configured, lets go
	log.Printf("Kafka Consumer: Connecting to %s", brokers)
	connector.consumer = &Consumer{
		ready: make(chan bool),
		flows: make(chan *flow.FlowMessage),
	}

	ctx, cancel := context.WithCancel(context.Background())
	connector.consumer.cancel = cancel
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Printf("Kafka Consumer: Error creating consumer group client: %v", err)
		return errors.New("ConnectionError")
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() { // this is the goroutine doing the work
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, topics, connector.consumer); err != nil {
				log.Panicf("Kafka Consumer: Error consuming: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			connector.consumer.ready = make(chan bool)
		}
	}()

	<-connector.consumer.ready // Await till the consumer has been set up
	log.Println("Kafka Consumer: Connection established.")

	go func() { // this is the goroutine that sticks around to close stuff
		<-ctx.Done()
		log.Println("Kafka Consumer: Terminating, waiting for partition threads...")
		wg.Wait()
		if err = client.Close(); err != nil {
			log.Panicf("Kafka Consumer: Error closing client: %v", err)
		}
		close(connector.consumer.flows) // signal kafkaconnector users that we're done
	}()
	return nil
}

// Start a Kafka Producer with the specified parameters. The channel returned
// by ProducerChannel will be accepting your input.
func (connector *Connector) StartProducer(broker string) error {
	var err error
	brokers := strings.Split(broker, ",")
	config := connector.NewBaseConfig()

	if connector.prometheusEnable {
		prometheusClient := prometheusmetrics.NewPrometheusProvider(config.MetricRegistry, "sarama", "producer", prometheus.DefaultRegisterer, 10*time.Second)
		go prometheusClient.UpdatePrometheusMetrics()
	}

	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
	config.Producer.Return.Successes = false                 // this would block until we've read the ACK, just don't
	config.Producer.Return.Errors = false                    // TODO: make configurable as logging feature

	connector.producerChannels = make(map[string](chan *flow.FlowMessage))
	connector.producerWg = &sync.WaitGroup{}
	// everything declared and configured, lets go
	connector.producer, err = sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Panicf("Kafka Producer: Error creating producer client: %v", err)
	}
	log.Println("Kafka Producer: Connection established.")
	return nil
}

// Return the channel used for receiving Flows from the Kafka Consumer.
// If this channel closes, it means the upstream Kafka Consumer has closed its
// channel previously of the last decoding step. You can restart the Consumer
// by using .StartConsumer() on the same Connector object.
func (connector *Connector) ConsumerChannel() <-chan *flow.FlowMessage {
	return connector.consumer.flows
}

// Return the channel used for handing over Flows to the Kafka Producer.
// If writing to this channel blocks, check the log.
func (connector *Connector) ProducerChannel(topic string) chan *flow.FlowMessage {
	if _, initialized := connector.producerChannels[topic]; !initialized {
		connector.producerChannels[topic] = make(chan *flow.FlowMessage)
		connector.producerWg.Add(1)
		go func() {
			for message := range connector.producerChannels[topic] {
				binary, err := proto.Marshal(message)
				if err != nil {
					log.Printf("Kafka Producer: Could not encode message to topic %s with error '%v'", topic, err)
					continue
				}
				connector.producer.Input() <- &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(binary),
				}
			}
			log.Printf("Kafka Producer: Terminating topic %s, channel has closed", topic)
			connector.producerWg.Done()
		}()
	}
	return connector.producerChannels[topic]
}

func (connector *Connector) Close() {
	log.Println("Kafka Connector closed.")
	if connector.consumer != nil {
		log.Println("Kafka Consumer: Closing...")
		connector.consumer.Close()
	}
	if connector.producer != nil {
		log.Println("Kafka Producer: Closing...")
		for _, producerChannel := range connector.producerChannels {
			close(producerChannel)
		}
		connector.producerWg.Wait()
		connector.producer.Close()
	}
}
