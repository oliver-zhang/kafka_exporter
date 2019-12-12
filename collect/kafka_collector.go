package collect

import (
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/oliver-zhang/kafka_exporter/exporter"
	"strconv"
	"sync"
	"time"
)

func StartCollectKafkaMetrics(wg *sync.WaitGroup, shutdown chan struct{}, client sarama.Client, kafkaExporterConfig exporter.KafkaExporterConfig) {
	go refreshKafkaMetadataPeriodically(wg, shutdown, client, kafkaExporterConfig)
	for _, broker := range client.Brokers() {
		go collectorBrokerMetrics(wg, shutdown, broker, client, kafkaExporterConfig)
	}
}

func collectorBrokerMetrics(wg *sync.WaitGroup, shutdown chan struct{}, broker *sarama.Broker, client sarama.Client, cfg exporter.KafkaExporterConfig) {
	wg.Add(1)
	defer wg.Done()
	log.WithField("broker", broker.Addr()).Info("collector broker metrics")
	wait := time.After(0)
	for {
		select {
		case <-wait:
			log.WithField("broker", broker.Addr()).Info("Updating metrics")
			if err := connect(broker); err != nil {
				log.WithField("broker", broker.Addr()).WithField("error", err).Error("Failed to connect to broker")
				break
			}
			var groups []string
			groupResponse, err := broker.ListGroups(&sarama.ListGroupsRequest{})
			if err != nil {
				log.WithField("broker", broker.Addr()).WithField("error", err).Error("Failed to retrieve consumer groups")
			} else if groupResponse.Err != sarama.ErrNoError {
				log.WithField("broker", broker.Addr()).WithField("error", groupResponse.Err).Error("Failed to retrieve consumer groups")
			} else {
				for group := range groupResponse.Groups {
					for _, kafkaTopic := range cfg.KafkaTopics {
						if group == kafkaTopic.KafkaConsumer {
							groupCoordinator, err := client.Coordinator(group)
							if err != nil {
								log.WithField("broker", broker.Addr()).WithField("group", group).WithField("error", err).
									Info("Failed to identify broker for consumer group")

							}
							if broker == groupCoordinator {
								groups = append(groups, group)
							}
							break
						}
					}
				}
			}
			if len(groups) == 0 {
				log.WithField("broker", broker.Addr()).Debug("No consumer groups to fetch offsets for")
			}

			partitionCount := 0
			oldestRequest := sarama.OffsetRequest{}
			newestRequest := sarama.OffsetRequest{}
			var groupRequests []sarama.OffsetFetchRequest
			for _, group := range groups {
				groupRequests = append(groupRequests, sarama.OffsetFetchRequest{ConsumerGroup: group, Version: 1})
			}
			topics, err := client.Topics()
			if err != nil {
				log.WithField("broker", broker.Addr()).WithField("error", err).Error("Failed to get topics")
				break
			}
			for _, topic := range topics {
				for _, kafkaTopic := range cfg.KafkaTopics {
					if topic == kafkaTopic.Topic {
						partitions, err := client.Partitions(topic)
						if err != nil {
							log.WithField("broker", broker.Addr()).WithField("topic", topic).WithField("error", err).
								Error("Failed to get partitions for topic")
							break
						}
						for _, partition := range partitions {
							for i := range groupRequests {
								groupRequests[i].AddPartition(topic, partition)
							}
							partitionLeader, err := client.Leader(topic, partition)
							if err != nil {
								log.WithField("broker", broker.Addr()).WithField("topic", topic).WithField("partition", partition).
									WithField("error", err).Error("Failed to identify broker for partition")
								break
							}
							if broker == partitionLeader {
								oldestRequest.AddBlock(topic, partition, sarama.OffsetOldest, 1)
								newestRequest.AddBlock(topic, partition, sarama.OffsetNewest, 1)
								partitionCount++
							}
						}
						break
					}
				}
			}
			if partitionCount == 0 {
				log.WithField("broker", broker.Addr()).Debug("No partitions for broker to fetch")
			}
			log.WithField("broker", broker.Addr()).WithField("partition.count", partitionCount).WithField("group.count", len(groupRequests)).
				Info("Sending requests")
			requestWG := &sync.WaitGroup{}
			requestWG.Add(2 + len(groupRequests))
			go func() {
				defer requestWG.Done()
				handleTopicOffsetRequest(broker, &oldestRequest, "oldest")
			}()
			go func() {
				defer requestWG.Done()
				handleTopicOffsetRequest(broker, &newestRequest, "newest")
			}()
			for i := range groupRequests {
				go func(request *sarama.OffsetFetchRequest) {
					defer requestWG.Done()
					handleGroupOffsetRequest(broker, request)
				}(&groupRequests[i])
			}
			requestWG.Wait()
		case <-shutdown:
			log.WithField("broker", broker.Addr()).Info("Shutting down handler for broker")
			return
		}
		wait = time.After(cfg.Interval)
	}
}

func handleGroupOffsetRequest(broker *sarama.Broker, request *sarama.OffsetFetchRequest) {
	response, err := broker.FetchOffset(request)
	if err != nil {
		log.WithField("broker", broker.Addr()).
			WithField("group", request.ConsumerGroup).
			WithField("error", err).
			Error("Failed to request group offsets")
		return
	}

	for topic, partitions := range response.Blocks {
		for partition, block := range partitions {
			if block == nil {
				log.WithField("broker", broker.Addr()).
					WithField("group", request.ConsumerGroup).
					WithField("topic", topic).
					WithField("partition", partition).
					Warn("Failed to get data for group")
				continue
			} else if block.Err != sarama.ErrNoError {
				log.WithField("broker", broker.Addr()).
					WithField("group", request.ConsumerGroup).
					WithField("topic", topic).
					WithField("partition", partition).
					WithField("error", block.Err).
					Warn("Failed getting data for group")
				continue
			} else if block.Offset < 0 {
				continue
			}
			exporter.UpdateMetricData("kafka_consumer_ratio", []string{request.ConsumerGroup, topic, string(partition)}, float64(block.Offset))
		}
	}
}

func handleTopicOffsetRequest(broker *sarama.Broker, request *sarama.OffsetRequest, metricName string) {
	response, err := broker.GetAvailableOffsets(request)
	if err != nil {
		log.WithField("broker", broker.Addr()).
			WithField("metricName", metricName).
			WithField("error", err).
			Error("Failed to request topic offsets")
		return
	}

	for topic, partitions := range response.Blocks {
		for partition, block := range partitions {
			if block == nil {
				log.WithField("broker", broker.Addr()).
					WithField("topic", topic).
					WithField("partition", partition).
					Warn("Failed to get data for partition")
				continue
			} else if block.Err != sarama.ErrNoError {
				log.WithField("broker", broker.Addr()).
					WithField("topic", topic).
					WithField("partition", partition).
					WithField("error", block.Err).
					Warn("Failed getting offsets for partition")
				continue
			} else if len(block.Offsets) != 1 {
				log.WithField("broker", broker.Addr()).
					WithField("topic", topic).
					WithField("partition", partition).
					Warn("Got unexpected offset data for partition")
				continue
			}
			exporter.UpdateMetricData(metricName, []string{topic, strconv.Itoa(int(partition))}, float64(block.Offsets[0]))
		}
	}
}

func refreshKafkaMetadataPeriodically(wg *sync.WaitGroup, shutdown chan struct{}, client sarama.Client, kafkaExporterConfig exporter.KafkaExporterConfig) {
	wg.Add(1)
	defer wg.Done()
	log.WithField("interval", 1*time.Minute).Info("Starting metadata refresh thread")
	wait := time.After(0)
	for {
		select {
		case <-wait:
			log.Debug("Refreshing cluster metadata")
			if err := client.RefreshMetadata(); err != nil {
				log.WithField("error", err).Warn("Failed to update cluster metadata")
			}
		case <-shutdown:
			log.Info("shutdown refresh kafka metadata thread")
			return
		}
		wait = time.After(kafkaExporterConfig.RefreshMetadataInterval * time.Second)
	}
}

func connect(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0
	if err := broker.Open(cfg); err != nil {
		return err
	}

	if connected, err := broker.Connected(); err != nil {
		return err
	} else if !connected {
		return errors.New("Unknown failure")
	}

	return nil
}
