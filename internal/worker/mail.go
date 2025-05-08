package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/bernardinorafael/email-service/internal/config"
	"github.com/bernardinorafael/email-service/internal/pkg/mail"
	rmq "github.com/bernardinorafael/email-service/internal/pkg/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMessage struct {
	To   string `json:"to"`
	Data any    `json:"data"`
}

type Worker struct {
	queue    *rmq.Queue
	mailer   *mail.Mail
	quitChan chan struct{}
	wg       sync.WaitGroup
}

func NewEmailWorker(ctx context.Context, mailer *mail.Mail, cfg *config.Config) (*Worker, error) {
	rmqConn, err := rmq.New(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create rabbitmq connection: %w", err)
	}

	// set QoS to 1 message at a time
	// this is to prevent the worker from receiving more than one message at a time
	err = rmqConn.SetQoS(1, 0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	worker := Worker{
		queue:    rmqConn,
		mailer:   mailer,
		quitChan: make(chan struct{}),
	}

	return &worker, nil
}

func (w *Worker) Start(ctx context.Context) {
	queues := []string{
		rmq.UserCreatedQueue,
	}

	for _, queue := range queues {
		w.wg.Add(1)

		go func() {
			defer w.wg.Done()

			for {
				select {
				case <-ctx.Done():
					slog.Info("context canceled, closing consumer", "queue", queue)
					return
				case <-w.quitChan:
					slog.Info("shutdown signal received, closing cosnumer", "queue", queue)
					return
				default:
				}

				messages, err := w.queue.Consume(ctx, queue, "notify-service", false)
				if err != nil {
					slog.Error("error consuming msgs",
						"error", err,
						"queue", queue,
					)
					// Wait a bit before trying again
					time.Sleep(time.Second * 3)
					continue
				}

				for msg := range messages {
					w.sendEmail(msg)
				}

				// if we get here, probably a connection error
				slog.Warn("channel closed, trying to reconnect", "queue", queue)
				time.Sleep(time.Second * 3)
			}
		}()
	}

}

// sendEmail processes an individual message
func (w *Worker) sendEmail(msg amqp.Delivery) {
	var payload QueueMessage

	_ = json.Unmarshal(msg.Body, &payload)
	// Apply a fixed delay of 600ms between each email send
	// This limits the rate to 1.66 emails per second, below the 2/s limit of Resend
	time.Sleep(600 * time.Millisecond)

	err := w.mailer.Send(mail.SendParams{
		From:    mail.NotificationSender,
		To:      "rafaelferreirab2@gmail.com",
		Subject: "Seja bem-vindo(a) ao Gulg!",
		File:    "activate.html",
		Data:    payload.Data,
	})
	if err != nil {
		slog.Error("failed to send email", "error", err, "to", payload.To)
		// if the email fails, reject the message and requeue it
		_ = msg.Reject(true)
		return
	}

	slog.Info("email sent successfully",
		"to", payload.To,
	)

	_ = msg.Ack(false)
}

func (w *Worker) Stop() {
	close(w.quitChan)
	w.queue.Close()

	// Wait until all goroutines finish
	w.wg.Wait()
}
