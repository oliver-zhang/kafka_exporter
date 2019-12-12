package exporter

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

func StartServer(wg *sync.WaitGroup, shutdown chan struct{}, port int, endpoint string) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		mux := http.NewServeMux()
		mux.Handle(endpoint, promhttp.Handler())
		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		}
		go func() {
			log.WithField("port", port).
				WithField("path", endpoint).
				Info("Starting metrics HTTP server")
			_ = server.ListenAndServe()
		}()
		<-shutdown
		log.Info("Shutting down metrics HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
}
