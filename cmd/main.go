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

	emailWorker, err := worker.NewEmailWorker(ctx, mailer, cfg)
	if err != nil {
		log.Fatalf("Error initializing email worker: %v", err)
	}

	emailWorker.Start(ctx)
	slog.Info("Email consumption service started successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutdown signal received, shutting down service...")

	// Give a little time to process pending messages
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Stop the worker
	emailWorker.Stop()

	select {
	case <-shutdownCtx.Done():
		slog.Info("Shutdown timeout exceeded, forcing shutdown")
	default:
		slog.Info("Service closed successfully")
	}
}
