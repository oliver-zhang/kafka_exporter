package exporter

import "time"

type KafkaExporterConfig struct {
	ServerPort              int           `yml:"server_port"`
	Interval                time.Duration `yml:"interval"`
	Brokers                 string        `yml:"brokers"`
	RefreshMetadataInterval time.Duration `yml:"refresh_metadata_interval"`
	KafkaTopics             []KafkaTopic  `yml:"kafka_topics"`
}

type KafkaTopic struct {
	Topic         string `yml:"topic"`
	KafkaConsumer string `yml:"kafka_consumer"`
}
