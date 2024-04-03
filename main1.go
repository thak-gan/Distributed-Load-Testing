package main

import (
	"net/http"
	"sync"

	ginMonitor "github.com/bancodobrasil/gin-monitor"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Custom metrics struct to store the number of requests and responses
type CustomMetrics struct {
	Requests  int
	Responses int
	sync.Mutex
}

var customMetrics CustomMetrics

func main() {
	// Initialize Monitoring
	monitor, err := ginMonitor.New(
		"v1.0.0",
		ginMonitor.DefaultErrorMessageKey,
		ginMonitor.DefaultBuckets,
	)
	if err != nil {
		panic(err)
	}

	r := gin.Default()

	// Register mux-monitor middleware
	r.Use(monitor.Prometheus())
	// Register metrics endpoint
	r.GET("/metrics1", gin.WrapH(promhttp.Handler()))

	// Ping Endpoint
	r.GET("/ping", func(c *gin.Context) {
		// Increment requests count
		customMetrics.Lock()
		customMetrics.Requests++
		customMetrics.Unlock()

		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
		customMetrics.Lock()
		customMetrics.Responses++
		customMetrics.Unlock()
	})

	// Custom Metrics Endpoint
	r.GET("/metrics", func(c *gin.Context) {
		customMetrics.Lock()
		defer customMetrics.Unlock()

		// Return the number of requests and responses as JSON response
		c.JSON(http.StatusOK, gin.H{"requests": customMetrics.Requests, "responses": customMetrics.Responses})
	})

	r.Run() // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
}
