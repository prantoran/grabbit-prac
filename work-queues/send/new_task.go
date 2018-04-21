// create a Work Queue that will be used to distribute time-consuming
// tasks among multiple workers.

// The main idea behind Work Queues (aka: Task Queues) is to avoid doing
// a resource-intensive task immediately and having to wait for it to
// complete. Instead we schedule the task to be done later.
// We encapsulate a task as a message and send it to a queue.
// A worker process running in the background will pop the tasks and
// eventually execute the job. When you run many workers the tasks will
// be shared between them.
package main

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
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Now we'll be sending strings that stand for complex tasks.
	// let's fake it by just pretending we're busy - by using the time.Sleep function.

	// We will slightly modify the send.go code from our previous example,
	// to allow arbitrary messages to be sent from the command line.
	// This program will schedule tasks to our work queue,
	// so let's name it new_task.go:
	/*
		At this point we're sure that the task_queue queue won't be lost even if RabbitMQ restarts.
		Now we need to mark our messages as persistent - by using the amqp.Persistent option
		amqp.Publishing takes.

		Marking messages as persistent doesn't fully guarantee that a message won't be lost.
		Although it tells RabbitMQ to save the message to disk, there is still a short time window
		when RabbitMQ has accepted a message and hasn't saved it yet. Also, RabbitMQ doesn't do
		fsync(2) for every message -- it may be just saved to cache
		and not really written to the disk. The persistence guarantees aren't strong,
		but it's more than enough for our simple task queue.
	*/
	body := bodyFrom(os.Args)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	// Declaring a queue is idempotent - it will only be created if it
	// doesn't exist already. The message content is a byte array, so you
	// can encode whatever you like there.
	log.Printf(" [x] Sent %s", body)

}

func bodyFrom(args []string) string {
	fmt.Println("bodyFrom()")
	for i, j := range args {
		fmt.Println("idx:", i, " args:", j)
	}
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	fmt.Printf("ret s: %v\n", s)
	return s
}
