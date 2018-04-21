package main

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// We also need an helper function to check the return value for each amqp call:

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	// connect to RabbitMQ server
	conn, err := amqp.Dial("amqp://lori:lori@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// The connection abstracts the socket connection, and takes care of
	// protocol version negotiation and authentication and so on for us.
	// Next we create a channel, which is where most of the API for getting
	// things done resides:
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// To send, we must declare a queue for us to send to; then we can
	// publish a message to the queue:

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := "hello"
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	// Declaring a queue is idempotent - it will only be created if it
	// doesn't exist already. The message content is a byte array, so you
	// can encode whatever you like there.
}
