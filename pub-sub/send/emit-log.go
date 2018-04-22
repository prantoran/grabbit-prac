package main

/*
we'll deliver a message to multiple consumers.
This pattern is known as "publish/subscribe".

To illustrate the pattern, we're going to build a simple
logging system. It will consist of two programs --
the first will emit log messages and the second will
receive and print them.

the producer can only send messages to an exchange.
An exchange is a very simple thing. On one side it receives
messages from producers and the other side it pushes
them to queues. The exchange must know exactly what to do with
a message it receives. Should it be appended to a particular queue?
Should it be appended to many queues? Or should it get discarded.
The rules for that are defined by the exchange type.
*/

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://lori:lori@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// There are a few exchange types available: direct, topic,
	// headers and fanout. We'll focus on the last one --
	// the fanout. Let's create an exchange of this type,
	// and call it logs:
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	// fanout broadcasts all the messages it receives to all the queues it knows.

	// In previous parts of the tutorial we knew nothing about exchanges, but still
	// were able to send messages to queues. That was possible because we were using
	// a default exchange, which is identified by the empty string ("").

	body := bodyFrom(os.Args)
	err = ch.Publish(
		"logs", // exchange
		"",     // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

/*
go run receive_logs.go > logs_from_rabbit.log
go run receive_logs.go
go run emit_log.go



*/
