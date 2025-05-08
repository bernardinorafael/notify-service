package queue

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Queue struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func New(url string) (*Queue, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create channel: %w", err)
	}

	err = ch.Qos(1, 0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &Queue{conn, ch}, nil
}

func (q *Queue) Close() {
	q.channel.Close()
	q.conn.Close()
}

func (q *Queue) CreateQueue(name string, durable, autoDel bool) (amqp.Queue, error) {
	queue, err := q.channel.QueueDeclare(
		name,    // name
		durable, // durable
		autoDel, // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("failed to declare queue: %w", err)
	}

	return queue, nil
}

func (q *Queue) CreateExchange(name, kind string, durable, autoDel bool) error {
	err := q.channel.ExchangeDeclare(
		name,    // name
		kind,    // kind
		durable, // durable
		autoDel, // delete when unused
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	return nil
}

func (q *Queue) BindQueue(exchange, queueName, routingKey string) error {
	err := q.channel.QueueBind(
		queueName,  // queue name
		routingKey, // routing key
		exchange,   // exchange
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	return nil
}

func (q *Queue) Publish(ctx context.Context, exchange, key string, message []byte) error {
	err := q.channel.PublishWithContext(
		ctx,
		exchange, // exchange
		key,      // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func (q *Queue) Consume(ctx context.Context, name, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return q.channel.ConsumeWithContext(
		ctx,
		name,     // queue name
		consumer, // consumer
		autoAck,  // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
}
