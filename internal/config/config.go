package config

import (
	"log"
	"sync"

	"github.com/spf13/viper"
)

var (
	config *Config
	once   sync.Once
)

type Config struct {
	Port        string `mapstructure:"PORT"`
	Environment string `mapstructure:"ENVIRONMENT"`
	AppName     string `mapstructure:"APP_NAME"`
	DebugMode   bool   `mapstructure:"DEBUG"`
	ResendKey   string `mapstructure:"RESEND_API_KEY"`
	RabbitMQURL string `mapstructure:"RABBITMQ_URL"`
}

func GetConfig() *Config {
	once.Do(func() {
		viper.SetConfigName(".env")
		viper.SetConfigType("env")
		viper.AddConfigPath(".")
		viper.AutomaticEnv()

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("error reading config file, %s", err)
		}

		if err := viper.Unmarshal(&config); err != nil {
			log.Fatalf("error unmarshalling config, %s", err)
		}
	})

	return config
}
