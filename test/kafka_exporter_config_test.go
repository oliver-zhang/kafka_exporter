package test

import (
	"encoding/json"
	"github.com/prometheus/common/log"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"testing"
)

type KafkaExportConfig struct {
	ServerPort  int          `yaml:"server_port"`
	Interval    int          `yaml:"interval"`
	Brokers     string       `yaml:"brokers"`
	KafkaTopics []KafkaTopic `yaml:"kafka_topics"`
}

type KafkaTopic struct {
	Topic         string `yaml:"topic"`
	KafkaConsumer string `yaml:"kafka_consumer"`
}

func TestKafkaExporterConfig(t *testing.T) {
	var kafkaExportConfig KafkaExportConfig
	yamlFile, err := ioutil.ReadFile("/Users/oliver/go/src/github.com/oliver-zhang/kafka_exporter/kafka_exporter.yml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(yamlFile, &kafkaExportConfig)
	if err != nil {
		panic(err)
	}

	data, err := json.Marshal(kafkaExportConfig)

	if err != nil {
		log.Info("err:\t", err.Error())
		return
	}

	t.Log(string(data))
	t.Logf("serverPort: %d, interval: %d, brokers: %s, topic: %s",
		kafkaExportConfig.ServerPort,
		kafkaExportConfig.Interval,
		kafkaExportConfig.Brokers,
		kafkaExportConfig.KafkaTopics[0].Topic)
	for _, kafkaTopic := range kafkaExportConfig.KafkaTopics {
		log.Infof("kafka %s", kafkaTopic)
	}

}
