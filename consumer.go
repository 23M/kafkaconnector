package kafka

import (
	"context"
	flow "github.com/23m/protobuf-flows"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"log"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready  chan bool
	flows  chan *flow.FlowMessage
	cancel context.CancelFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Close() {
	consumer.cancel()
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		session.MarkMessage(message, "")
		flowMsg := new(flow.FlowMessage)
		err := proto.Unmarshal(message.Value, flowMsg)
		if err != nil {
			log.Printf("decodeMessages: Received broken message: %v", err)
			continue
		}
		consumer.flows <- flowMsg
	}
	return nil
}
