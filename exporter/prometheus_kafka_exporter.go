package exporter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"strings"
)

var (
	KafkaGroupLag           prometheus.GaugeVec
	KafkaGroupConsumerRatio prometheus.GaugeVec
	KafkaGroupProducerRatio prometheus.GaugeVec
)

func RegisterMetrics() {
	KafkaGroupProducerRatio = *promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_producer_ratio",
		Help: "kafka producer ratio"},
		[]string{"topic", "partition"})
	KafkaGroupConsumerRatio = *promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_ratio",
		Help: "kafka consumer ratio"},
		[]string{"consumer", "topic", "partition"})
	KafkaGroupLag = *promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_group_lag",
		Help: "kafka group lag"}, []string{"topic", "partition"})
}

func UpdateMetricData(metricName string, labels []string, value float64) {
	switch metricName {
	case "kafka_producer_ratio":
		KafkaGroupProducerRatio.WithLabelValues(strings.Join(labels, ",")).Set(value)
	case "kafka_consumer_ratio":
		KafkaGroupConsumerRatio.WithLabelValues(strings.Join(labels, ",")).Set(value)
	case "kafka_group_lag":
		KafkaGroupLag.WithLabelValues(strings.Join(labels, ",")).Set(value)

	}
}
