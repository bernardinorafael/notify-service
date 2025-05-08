package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bernardinorafael/email-service/internal/config"
	"github.com/bernardinorafael/email-service/internal/pkg/mail"
	"github.com/bernardinorafael/email-service/internal/pkg/queue"
	"github.com/bernardinorafael/email-service/internal/worker"
)

func main() {
	cfg := config.GetConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mailer := mail.New(ctx, mail.Config{
		MaxRetries: 3,
		APIKey:     cfg.ResendKey,
		RetryDelay: 5 * time.Second,
		Timeout:    10 * time.Second,
	})

	rmq, err := queue.New(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("failed to create rabbitmq connection: %v", err)
	}
	defer rmq.Close()

	w, err := worker.New(ctx, rmq, mailer)
	if err != nil {
		log.Fatalf("error initializing email worker: %v", err)
	}

	w.Start(ctx)
	slog.Info("email service started successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("shutdown signal received, shutting down service...")

	// Give a little time to process pending messages
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Stop the worker
	w.Stop()

	select {
	case <-shutdownCtx.Done():
		slog.Info("shutdown timeout exceeded, forcing shutdown")
	default:
		slog.Info("service closed successfully")
	}
}
