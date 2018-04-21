// Our consumer is pushed messages from RabbitMQ, so unlike the publisher which
// publishes a single message, we'll keep it running to listen for messages and
// print them out.

package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	// Setting up is the same as the publisher; we open a connection and a channel,
	// and declare the queue from which we're going to consume. Note this matches up
	// with the queue that send publishes to.
	conn, err := amqp.Dial("amqp://lori:lori@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Note that we declare the queue here, as well. Because we might start the consumer
	// before the publisher, we want to make sure the queue exists before we try to consume
	// messages from it.

	// We're about to tell the server to deliver us the messages from the queue. Since it will
	// push us messages asynchronously, we will read the messages from a channel
	// (returned by amqp::Consume) in a goroutine.
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
