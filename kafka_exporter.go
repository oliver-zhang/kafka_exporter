package main

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/oliver-zhang/kafka_exporter/collect"
	"github.com/oliver-zhang/kafka_exporter/exporter"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

const (
	endpoint = "metrics"
)

func main() {
	var configFile = *kingpin.Flag("configFile", "config file").File()
	setUpLogger("info")

	kingpin.Parse()
	//init config file
	kafkaExporterConfig := initKafkaExporterConfig(configFile.Name())
	//register prometheus metrics
	exporter.RegisterMetrics()
	// init kafka client
	client, _ := initKafkaClient(strings.Split(kafkaExporterConfig.Brokers, ","))

	//start prometheus web server and exporter collector
	enforceGracefulShutdown(func(wg *sync.WaitGroup, shutdown chan struct{}) {
		collect.StartCollectKafkaMetrics(wg, shutdown, client, kafkaExporterConfig)
		exporter.StartServer(wg, shutdown, kafkaExporterConfig.ServerPort, endpoint)
	})

}

func initKafkaClient(brokers []string) (client sarama.Client, err error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	kafkaClient, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	var addrs []string
	for _, broker := range client.Brokers() {
		addrs = append(addrs, broker.Addr())
	}
	log.WithField("brokers", addrs).Info("connected to cluster")
	return kafkaClient, nil
}

func enforceGracefulShutdown(f func(wg *sync.WaitGroup, shutdown chan struct{})) {
	wg := &sync.WaitGroup{}
	shutdown := make(chan struct{})
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	go func() {
		<-signals
		close(shutdown)
	}()
	log.Info("Graceful shutdown enabled")
	f(wg, shutdown)
	<-shutdown
	wg.Wait()
}

func initKafkaExporterConfig(fileName string) exporter.KafkaExporterConfig {
	var kafkaExporterConfig exporter.KafkaExporterConfig
	yamlFile, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
	}
	err = yaml.Unmarshal(yamlFile, &kafkaExporterConfig)
	if err != nil {
		log.Fatal(err)
	}
	return kafkaExporterConfig
}

func setUpLogger(level string) {
	log.SetOutput(os.Stdout)
	logLevel, err := log.ParseLevel(level)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(logLevel)
	log.SetFormatter(&log.JSONFormatter{})
}
