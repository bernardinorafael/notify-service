package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/bernardinorafael/email-service/internal/pkg/mail"
	"github.com/bernardinorafael/email-service/internal/pkg/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMessage struct {
	To   string         `json:"to"`
	Data map[string]any `json:"data"`
}

type Worker struct {
	wg sync.WaitGroup

	queue  *queue.Queue
	mailer *mail.Mail
	quit   chan struct{}
}

func New(ctx context.Context, queue *queue.Queue, mailer *mail.Mail) (*Worker, error) {
	worker := Worker{
		queue:  queue,
		mailer: mailer,
		quit:   make(chan struct{}),
	}

	return &worker, nil
}

func (w *Worker) Start(ctx context.Context) {
	queues := []string{
		queue.UserCreatedQueue,
	}

	for _, queue := range queues {
		w.wg.Add(1)

		go func() {
			defer w.wg.Done()

			for {
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
					w.processQueueMessage(msg)
				}

				select {
				case <-ctx.Done():
					slog.Info("context canceled", "queue", queue)
					return
				case <-w.quit:
					slog.Info("shutdown signal received", "queue", queue)
					return
				default:
				}
			}
		}()
	}

}

// processQueueMessage processes an individual message
func (w *Worker) processQueueMessage(msg amqp.Delivery) {
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
	close(w.quit)
	w.queue.Close()
	w.wg.Wait()
}
