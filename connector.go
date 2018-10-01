package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	flow "omi-gitlab.e-technik.uni-ulm.de/bwnetflow/bwnetflow_api/go"
)

// Connector handles a connection to read bwNetFlow flows from kafka.
type Connector struct {
	user            string
	pass            string
	consumer        *cluster.Consumer
	producer        sarama.AsyncProducer
	consumerChannel chan *flow.FlowMessage
	producerChannel chan *flow.FlowMessage
	manualErrFlag   bool
	manualErrSignal chan bool
	channelLength   uint
}

// Explicitly set which login to use in SASL/PLAIN auth via TLS
func (connector *Connector) SetAuth(user string, pass string) {
	connector.user = user
	connector.pass = pass
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

// Set anonymous credentials as login method.
func (connector *Connector) SetAuthAnon() {
	connector.user = "anon"
	connector.pass = "anon"
}

// Enable manual error handling by setting the internal flags.
// Any application calling this will have to read all messages provided by the
// channels returned from the ConsumerErrors, ConsumerNotifications and
// ProducerErrors methods. Else there will be deadlocks.
//
// If this is called before any `.Start*` method was called, no go routines
// will be spawned for logging any messages. This is the recommended case.
// If this is called after any `.Start*` method was called, spawned go routines
// will be terminated.
func (connector *Connector) EnableManualErrorHandling() {
	connector.manualErrFlag = true
	if connector.manualErrSignal != nil {
		close(connector.manualErrSignal)
	}
}

// Set the channel length to something >0. Maybe read the source before using it.
func (connector *Connector) SetChannelLength(l uint) {
	connector.channelLength = l
}

// Start a Kafka Consumer with the specified parameters. Its output will be
// available in the channel returned by ConsumerChannel.
func (connector *Connector) StartConsumer(broker string, topics []string, consumergroup string, offset int64) error {
	var err error
	if !connector.manualErrFlag && connector.manualErrSignal == nil {
		connector.manualErrSignal = make(chan bool)
	}
	brokers := strings.Split(broker, ",")
	consConf := cluster.NewConfig()
	// Enable TLS
	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		log.Println("TLS Error:", err)
		return err
	}
	consConf.Net.TLS.Enable = true
	consConf.Net.TLS.Config = &tls.Config{RootCAs: rootCAs}

	consConf.Net.SASL.Enable = true
	consConf.Net.SASL.User = connector.user
	consConf.Net.SASL.Password = connector.pass

	// Enable these unconditionally.
	consConf.Consumer.Return.Errors = true
	consConf.Group.Return.Notifications = true
	// The offset only works initially. When reusing a Consumer Group, it's
	// last state will be resumed automatcally (grep MarkOffset)
	consConf.Consumer.Offsets.Initial = offset

	// everything declared and configured, lets go
	log.Printf("Trying to connect to Kafka as SASL/PLAIN user '%s'...", consConf.Net.SASL.User)
	connector.consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, consConf)
	if err != nil {
		return err
	}
	log.Println("Kafka TLS connection established.")

	// start message handling in background
	connector.consumerChannel = make(chan *flow.FlowMessage, connector.channelLength)
	go decodeMessages(connector.consumer, connector.consumerChannel)
	if !connector.manualErrFlag {
		go func() {
			log.Println("Spawned a Consumer Logger, no manual error handling.")
			running := true
			for running {
				select {
				case msg, ok := <-connector.consumer.Errors():
					if !ok {
						running = false
						continue
					}
					log.Printf("Kafka Consumer Error: %s\n", msg.Error())
				case msg, ok := <-connector.consumer.Notifications():
					if !ok {
						running = false
						continue
					}
					log.Printf("Kafka Consumer Notification: %+v\n", msg)
				case _, ok := <-connector.manualErrSignal:
					running = ok
				}
			}
			log.Println("Consumer Logger terminated.")
		}()
	}
	return nil
}

// Start a Kafka Producer with the specified parameters. The channel returned
// by ProducerChannel will be accepting your input.
func (connector *Connector) StartProducer(broker string, topic string) error {
	if !connector.manualErrFlag && connector.manualErrSignal == nil {
		connector.manualErrSignal = make(chan bool)
	}
	brokers := strings.Split(broker, ",")
	prodConf := sarama.NewConfig()
	prodConf.Producer.Return.Successes = false // this would block until we've read the ACK
	prodConf.Producer.Return.Errors = true

	// everything declared and configured, lets go
	var err error
	connector.producer, err = sarama.NewAsyncProducer(brokers, prodConf)
	if err != nil {
		return err
	}

	// start message handling in background
	connector.producerChannel = make(chan *flow.FlowMessage, connector.channelLength)
	go encodeMessages(connector.producer, topic, connector.producerChannel)
	if !connector.manualErrFlag {
		go func() {
			log.Println("Spawned a Producer Logger, no manual error handling.")
			running := true
			for running {
				select {
				case msg, ok := <-connector.producer.Errors():
					if !ok {
						running = false
						continue
					}
					log.Printf("Kafka Producer Error: %s\n", msg.Error())
				case _, ok := <-connector.manualErrSignal:
					running = ok
				}
			}
			log.Println("Producer Logger terminated.")
		}()
	}
	return nil
}

// Close closes the connection to kafka, i.e. Consumer and Producer
func (connector *Connector) Close() {
	if connector.consumer != nil {
		connector.consumer.Close()
		log.Println("Kafka Consumer connection closed.")
	}
	if connector.producer != nil {
		connector.producer.Close()
		log.Println("Kafka Producer connection closed.")
	}
}

// Close the Kafka Consumer specifically.
func (connector *Connector) CloseConsumer() {
	if connector.consumer != nil {
		connector.consumer.Close()
		log.Println("Kafka Consumer connection closed.")
	} else {
		log.Println("WARNING: CloseConsumer called, but no Consumer was initialized.")
	}
}

// Close the Kafka Producer specifically.
func (connector *Connector) CloseProducer() {
	if connector.producer != nil {
		connector.producer.Close()
		log.Println("Kafka Producer connection closed.")
	} else {
		log.Println("WARNING: CloseProducer called, but no Producer was initialized.")
	}
}

// Return the channel used for receiving Flows from the Kafka Consumer.
// If this channel closes, it means the upstream Kafka Consumer has closed its
// channel previously of the last decoding step. You can restart the Consumer
// by using .StartConsumer() on the same Connector object.
func (connector *Connector) ConsumerChannel() <-chan *flow.FlowMessage {
	return connector.consumerChannel
}

// Return the channel used for handing over Flows to the Kafka Producer.
// If writing to this channel blocks, check the log.
func (connector *Connector) ProducerChannel() chan *flow.FlowMessage {
	return connector.producerChannel
}

// Consumer Errors relayed directly from the Kafka Cluster.
//
// This will become an exclusive reference only after EnableManualErrorHandling
// has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ConsumerErrors() <-chan error {
	return connector.consumer.Errors()
}

// Consumer Notifications are relayed directly from the Kafka Cluster.
// These include which topics and partitions are read by this instance
// and are sent on every Rebalancing Event.
//
// This will become an exclusive reference only after EnableManualErrorHandling
// has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ConsumerNotifications() <-chan *cluster.Notification {
	return connector.consumer.Notifications()
}

// Producer Errors are relayed directly from the Kafka Cluster.
//
// This will become an exclusive reference only after EnableManualErrorHandling
// has been called.
// IMPORTANT: read EnableManualErrorHandling docs carefully
func (connector *Connector) ProducerErrors() <-chan *sarama.ProducerError {
	return connector.producer.Errors()
}
