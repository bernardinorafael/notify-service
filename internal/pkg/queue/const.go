package rmq

const (
	// Exchanges
	EmailsExchange = "notifications.emails.topic"

	// Queues
	UserCreatedQueue = "email.activation.account"

	// Routing keys
	UserCreatedKey = "user.created"
)
