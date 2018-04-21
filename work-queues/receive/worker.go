// Our consumer is pushed messages from RabbitMQ, so unlike the publisher which
// publishes a single message, we'll keep it running to listen for messages and
// print them out.

package main

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	/*
		Using message acknowledgments and prefetch count you can set up a work queue.
		The durability options let the tasks survive even if RabbitMQ is restarted.
	*/
	// Setting up is the same as the publisher; we open a connection and a channel,
	// and declare the queue from which we're going to consume. Note this matches up
	// with the queue that send publishes to.
	conn, err := amqp.Dial("amqp://lori:lori@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*
		message durability
		When RabbitMQ quits or crashes it will forget the queues and messages unless you tell it
		not to. Two things are required to make sure that messages aren't lost: we need to mark
		both the queue and messages as durable.

		 RabbitMQ doesn't allow you to redefine an existing queue with different parameters and
		 will return an error to any program that tries to do that.
	*/
	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when usused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// round robin dispatching
	// One of the advantages of using a Task Queue is the ability to easily parallelise work.
	// If we are building up a backlog of work, we can just add more workers and that way,
	// scale easily.

	// First, let's try to run two worker.go scripts at the same time. They will both get messages
	//from the queue, but how exactly? Let's see.

	// You need three consoles open. Two will run the worker.go script. These consoles will be our
	// two consumers - C1 and C2.

	/*
		# shell 1
		go run worker.go
		# => [*] Waiting for messages. To exit press CTRL+C
		# shell 2
		go run worker.go
		# => [*] Waiting for messages. To exit press CTRL+C

		# shell 3
		go run new_task.go First message.
		go run new_task.go Second message..
	*/
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	/*
		prefetch count tells the rabbitmq not to give more than one message to a worker at a time.
		Or, in other words, don't dispatch a new message to a worker until it has processed and
		acknowledged the previous one. Instead, it will dispatch it to the next worker
		that is not still busy.
	*/

	// Note that we declare the queue here, as well. Because we might start the consumer
	// before the publisher, we want to make sure the queue exists before we try to consume
	// messages from it.

	// We're about to tell the server to deliver us the messages from the queue. Since it will
	// push us messages asynchronously, we will read the messages from a channel
	// (returned by amqp::Consume) in a goroutine.
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	// msgs is a channel of type <- Delivery
	failOnError(err, "Failed to register a consumer")

	// If a worker dies, we'd like the task to be delivered to another worker.
	/*
		In order to make sure a message is never lost, RabbitMQ supports message acknowledgments.
		An ack(nowledgement) is sent back by the consumer to tell RabbitMQ that a particular message
		has been received, processed and that RabbitMQ is free to delete it.

		There aren't any message timeouts; RabbitMQ will redeliver the message when the consumer dies.
		It's fine even if processing a message takes a very, very long time.

		we will use manual message acknowledgements by passing a false for the "auto-ack" argument
		and then send a proper acknowledgment from the worker with d.Ack(false) (this acknowledges
		a single delivery), once we're done with a task.
	*/

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			fmt.Printf("dotCount: %v t: %v\n", dotCount, t)
			time.Sleep(t * time.Second)
			log.Printf("Done")
			d.Ack(false) // acknowledging a single delivery
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
