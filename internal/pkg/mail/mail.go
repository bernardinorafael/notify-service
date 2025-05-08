package mail

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"log/slog"
	"time"

	"html/template"

	"github.com/resend/resend-go/v2"
)

//go:embed "templates"
var templateFS embed.FS

type SendParams struct {
	From    string
	To      string
	Subject string
	File    string
	Data    any
}

type Config struct {
	APIKey     string
	MaxRetries int
	RetryDelay time.Duration
	Timeout    time.Duration
}

type Mail struct {
	ctx    context.Context
	client *resend.Client
	config Config
}

func New(ctx context.Context, config Config) *Mail {
	return &Mail{
		ctx:    ctx,
		client: resend.NewClient(config.APIKey),
		config: config,
	}
}

func (m *Mail) Send(p SendParams) error {
	tmplLocation := fmt.Sprintf("templates/%s", p.File)

	tmpl, err := template.New("email").ParseFS(templateFS, tmplLocation)
	if err != nil {
		slog.Error("failed to parse template", "error", err)
		return fmt.Errorf("failed to parse template: %w", err)
	}

	var body bytes.Buffer
	if err := tmpl.Execute(&body, p.Data); err != nil {
		slog.Error("failed to execute template", "error", err)
		return fmt.Errorf("failed to execute template: %w", err)
	}

	params := &resend.SendEmailRequest{
		From:    p.From,
		To:      []string{p.To},
		Html:    body.String(),
		Subject: p.Subject,
	}

	return m.send(params, m.config.MaxRetries)
}

func (m *Mail) send(p *resend.SendEmailRequest, maxRetries int) error {
	var mailerErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(m.ctx, m.config.Timeout)
		defer cancel()

		_, err := m.client.Emails.SendWithContext(ctx, p)
		if err == nil {
			return nil
		}

		mailerErr = err
		time.Sleep(m.config.RetryDelay)
	}

	return fmt.Errorf("error on send email after %d attemps: %w", maxRetries, mailerErr)
}
