package kaspastratum

import (
	"fmt"
	"net/http"
	"sync"
	"time"
	"strconv"

	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/onemorebsmith/kaspastratum/src/gostratum"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"github.com/kaspanet/kaspad/infrastructure/network/rpcclient"
	"strings"
)

var workerLabels = []string{
	"worker", "miner", "wallet", "ip",
}

var shareCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_valid_share_counter",
	Help: "Number of shares found by worker over time",
}, workerLabels)

var shareDiffCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_valid_share_diff_counter",
	Help: "Total difficulty of shares found by worker over time",
}, workerLabels)

var invalidCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_invalid_share_counter",
	Help: "Number of stale shares found by worker over time",
}, append(workerLabels, "type"))

var blockCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_blocks_mined",
	Help: "Number of blocks mined over time",
}, workerLabels)

var blockGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "ks_mined_blocks_gauge",
	Help: "Gauge containing 1 unique instance per block mined",
}, append(workerLabels, "nonce", "bluescore", "hash"))

var disconnectCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_worker_disconnect_counter",
	Help: "Number of disconnects by worker",
}, workerLabels)

var jobCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_worker_job_counter",
	Help: "Number of jobs sent to the miner by worker over time",
}, workerLabels)

var balanceGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "ks_balance_by_wallet_gauge",
	Help: "Gauge representing the wallet balance for connected workers",
}, []string{"wallet"})

var errorByWallet = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "ks_worker_errors",
	Help: "Gauge representing errors by worker",
}, []string{"wallet", "error"})

var estimatedNetworkHashrate = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "ks_estimated_network_hashrate_gauge",
	Help: "Gauge representing the estimated network hashrate",
})

var networkDifficulty = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "ks_network_difficulty_gauge",
	Help: "Gauge representing the network difficulty",
})

var networkBlockCount = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "ks_network_block_count",
	Help: "Gauge representing the network block count",
})

// RPC Pool metrics
var rpcRequestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "kaspa_stratum_rpc_requests_total",
	Help: "Total number of RPC requests made per client",
}, []string{"client_index"})

var rpcErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "kaspa_stratum_rpc_errors_total",
	Help: "Total number of RPC errors per client and type",
}, []string{"client_index", "error_type"})

var rpcReconnectCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "kaspa_stratum_rpc_reconnects_total",
	Help: "Total number of RPC client reconnections",
}, []string{"client_index"})

var rpcHealthyConnections = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "kaspa_stratum_rpc_healthy_connections",
	Help: "Number of healthy RPC connections in the pool",
})

// Block submission metrics
var blockSubmissionMetrics = struct {
	Latency    *prometheus.HistogramVec
	Success    *prometheus.CounterVec
	Failure    *prometheus.CounterVec
	Retries    *prometheus.CounterVec
	Timeouts   *prometheus.CounterVec
}{
	Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kaspa_stratum_block_submission_latency_seconds",
		Help:    "Block submission latency",
		Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 30.0, 60.0},
	}, []string{"status"}),

	Success: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaspa_stratum_block_submissions_total",
		Help: "Total successful block submissions",
	}, []string{"client_index"}),

	Failure: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaspa_stratum_block_submission_failures_total",
		Help: "Total failed block submissions",
	}, []string{"client_index", "error_type"}),

	Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaspa_stratum_block_submission_retries_total",
		Help: "Total block submission retries",
	}, []string{"client_index"}),

	Timeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kaspa_stratum_block_submission_timeouts_total",
		Help: "Total block submission timeouts",
	}, []string{"client_index"}),
}

func commonLabels(worker *gostratum.StratumContext) prometheus.Labels {
	return prometheus.Labels{
		"worker": worker.WorkerName,
		"miner":  worker.RemoteApp,
		"wallet": worker.WalletAddr,
		"ip":     worker.RemoteAddr,
	}
}

func RecordShareFound(worker *gostratum.StratumContext, shareDiff float64) {
	shareCounter.With(commonLabels(worker)).Inc()
	shareDiffCounter.With(commonLabels(worker)).Add(shareDiff)
}

func RecordStaleShare(worker *gostratum.StratumContext) {
	labels := commonLabels(worker)
	labels["type"] = "stale"
	invalidCounter.With(labels).Inc()
}

func RecordDupeShare(worker *gostratum.StratumContext) {
	labels := commonLabels(worker)
	labels["type"] = "duplicate"
	invalidCounter.With(labels).Inc()
}

func RecordInvalidShare(worker *gostratum.StratumContext) {
	labels := commonLabels(worker)
	labels["type"] = "invalid"
	invalidCounter.With(labels).Inc()
}

func RecordWeakShare(worker *gostratum.StratumContext) {
	labels := commonLabels(worker)
	labels["type"] = "weak"
	invalidCounter.With(labels).Inc()
}

func RecordBlockFound(worker *gostratum.StratumContext, nonce, bluescore uint64, hash string) {
	blockCounter.With(commonLabels(worker)).Inc()
	labels := commonLabels(worker)
	labels["nonce"] = fmt.Sprintf("%d", nonce)
	labels["bluescore"] = fmt.Sprintf("%d", bluescore)
	labels["hash"] = hash
	blockGauge.With(labels).Set(1)
}

func RecordDisconnect(worker *gostratum.StratumContext) {
	disconnectCounter.With(commonLabels(worker)).Inc()
}

func RecordNewJob(worker *gostratum.StratumContext) {
	jobCounter.With(commonLabels(worker)).Inc()
}

func RecordNetworkStats(hashrate uint64, blockCount uint64, difficulty float64) {
	estimatedNetworkHashrate.Set(float64(hashrate))
	networkDifficulty.Set(difficulty)
	networkBlockCount.Set(float64(blockCount))
}

func RecordWorkerError(address string, shortError ErrorShortCodeT) {
	errorByWallet.With(prometheus.Labels{
		"wallet": address,
		"error":  string(shortError),
	}).Inc()
}

func InitInvalidCounter(worker *gostratum.StratumContext, errorType string) {
	labels := commonLabels(worker)
	labels["type"] = errorType
	invalidCounter.With(labels).Add(0)
}

func InitWorkerCounters(worker *gostratum.StratumContext) {
	labels := commonLabels(worker)

	shareCounter.With(labels).Add(0)
	shareDiffCounter.With(labels).Add(0)

	errTypes := []string{"stale", "duplicate", "invalid", "weak"}
	for _, e := range errTypes {
		InitInvalidCounter(worker, e)
	}

	blockCounter.With(labels).Add(0)

	disconnectCounter.With(labels).Add(0)

	jobCounter.With(labels).Add(0)
}

func RecordBalances(response *appmessage.GetBalancesByAddressesResponseMessage) {
	unique := map[string]struct{}{}
	for _, v := range response.Entries {
		// only set once per run
		if _, exists := unique[v.Address]; !exists {
			balanceGauge.With(prometheus.Labels{
				"wallet": v.Address,
			}).Set(float64(v.Balance) / 100000000)
			unique[v.Address] = struct{}{}
		}
	}
}

var promInit sync.Once

func StartPromServer(log *zap.SugaredLogger, port string) {
	go func() { // prom http handler, separate from the main router
		promInit.Do(func() {
			logger := log.With(zap.String("server", "prometheus"))
			http.Handle("/metrics", promhttp.Handler())
			logger.Info("hosting prom stats on ", port, "/metrics")
			if err := http.ListenAndServe(port, nil); err != nil {
				logger.Error("error serving prom metrics", zap.Error(err))
			}
		})
	}()
}

func init() {
	// Register RPC Pool metrics
	prometheus.MustRegister(rpcRequestCounter)
	prometheus.MustRegister(rpcErrorCounter)
	prometheus.MustRegister(rpcReconnectCounter)
	prometheus.MustRegister(rpcHealthyConnections)

	// Register new metrics
	prometheus.MustRegister(blockSubmissionMetrics.Latency)
	prometheus.MustRegister(blockSubmissionMetrics.Success)
	prometheus.MustRegister(blockSubmissionMetrics.Failure)
	prometheus.MustRegister(blockSubmissionMetrics.Retries)
	prometheus.MustRegister(blockSubmissionMetrics.Timeouts)
}

// RPC Pool metric recording functions
func RecordRPCRequest(clientIndex int) {
	rpcRequestCounter.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", clientIndex),
	}).Inc()
}

func RecordRPCError(clientIndex int, errorType string) {
	rpcErrorCounter.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", clientIndex),
		"error_type":   errorType,
	}).Inc()
}

func RecordRPCReconnect(clientIndex int) {
	rpcReconnectCounter.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", clientIndex),
	}).Inc()
}

func UpdateRPCHealthyConnections(count int) {
	rpcHealthyConnections.Set(float64(count))
}

// Block submission metric recording functions
func RecordBlockSubmissionSuccess(client *rpcclient.RPCClient, duration time.Duration) {
	clientIndex := getClientIndex(client)
	blockSubmissionMetrics.Success.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", clientIndex),
	}).Inc()

	blockSubmissionMetrics.Latency.With(prometheus.Labels{
		"status": "success",
	}).Observe(duration.Seconds())
}

func RecordBlockSubmissionFailure(client *rpcclient.RPCClient, err error) {
	clientIndex := getClientIndex(client)
	errorType := "unknown"
	if strings.Contains(err.Error(), "ErrDuplicateBlock") {
		errorType = "duplicate"
	} else if strings.Contains(err.Error(), "timeout") {
		errorType = "timeout"
	}

	blockSubmissionMetrics.Failure.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", clientIndex),
		"error_type":   errorType,
	}).Inc()
}

func RecordBlockSubmissionRetry(attempt int) {
	blockSubmissionMetrics.Retries.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", attempt),
	}).Inc()
}

func RecordBlockSubmissionTimeout(client *rpcclient.RPCClient) {
	clientIndex := getClientIndex(client)
	blockSubmissionMetrics.Timeouts.With(prometheus.Labels{
		"client_index": fmt.Sprintf("%d", clientIndex),
	}).Inc()
}

// Helper function to get client index
func getClientIndex(client *rpcclient.RPCClient) int {
	// Get the client's address which should be unique
	addr := client.Address()

	// Extract the port number from the address
	// Format is typically "host:port"
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return 0 // Default to 0 if we can't parse the address
	}

	// Use the port number as the client index
	// This assumes each client has a unique port
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0 // Default to 0 if we can't parse the port
	}

	return port
}
