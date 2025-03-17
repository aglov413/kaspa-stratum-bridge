package kaspastratum

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kaspanet/kaspad/infrastructure/network/rpcclient"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// RPCConfig holds configuration for the RPC connection pool
type RPCConfig struct {
	PoolSize            int           `yaml:"pool_size"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	ReconnectDelay     time.Duration `yaml:"reconnect_delay"`
}

// RPCClientPool manages a pool of RPC connections to the Kaspa node
type RPCClientPool struct {
	clients      []*rpcclient.RPCClient
	healthStatus []bool
	maxClients   int
	currentIdx   atomic.Int32
	address      string
	logger       *zap.SugaredLogger
	mu          sync.RWMutex
	config      RPCConfig
}

// NewRPCClientPool creates a new pool of RPC clients
func NewRPCClientPool(address string, config RPCConfig, logger *zap.SugaredLogger) (*RPCClientPool, error) {
	if config.PoolSize < 1 {
		config.PoolSize = 1
	}

	logger.Info("Creating RPC pool",
		zap.String("address", address),
		zap.Int("pool_size", config.PoolSize),
		zap.Duration("health_check_interval", config.HealthCheckInterval),
		zap.Duration("reconnect_delay", config.ReconnectDelay))

	pool := &RPCClientPool{
		clients:      make([]*rpcclient.RPCClient, config.PoolSize),
		healthStatus: make([]bool, config.PoolSize),
		maxClients:   config.PoolSize,
		address:      address,
		logger:       logger.With(zap.String("component", "rpc_pool")),
		config:       config,
	}

	// Initialize all clients
	for i := 0; i < config.PoolSize; i++ {
		pool.logger.Info("Initializing RPC client", zap.Int("client_index", i))
		if err := pool.initializeClient(i); err != nil {
			pool.logger.Error("failed to create RPC client",
				zap.Int("client_index", i),
				zap.Error(err))
			// Continue to next client, health check will attempt to reconnect
		}
	}

	// Check if at least one client was created successfully
	healthyCount := pool.getHealthyClientCount()
	if healthyCount == 0 {
		return nil, errors.New("failed to create any RPC clients")
	}

	pool.logger.Info("RPC pool initialized",
		zap.Int("total_clients", config.PoolSize),
		zap.Int("healthy_clients", healthyCount))

	return pool, nil
}

// initializeClient creates and verifies a new RPC client
func (p *RPCClientPool) initializeClient(index int) error {
	client, err := rpcclient.NewRPCClient(p.address)
	if err != nil {
		p.healthStatus[index] = false
		return err
	}

	// Verify the connection works
	if err := p.verifyConnection(client); err != nil {
		p.healthStatus[index] = false
		return err
	}

	p.clients[index] = client
	p.healthStatus[index] = true
	return nil
}

// verifyConnection checks if the client is responsive
func (p *RPCClientPool) verifyConnection(client *rpcclient.RPCClient) error {
	_, err := client.GetInfo()
	return err
}

// GetClient returns a healthy client from the pool using round-robin
func (p *RPCClientPool) GetClient() *rpcclient.RPCClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	attempts := 0
	maxAttempts := p.maxClients

	for attempts < maxAttempts {
		idx := int(p.currentIdx.Add(1)) % p.maxClients
		if p.clients[idx] != nil && p.healthStatus[idx] {
			RecordRPCRequest(idx)
			return p.clients[idx]
		}
		attempts++
	}

	return nil
}

// getHealthyClientCount returns the number of healthy clients
func (p *RPCClientPool) getHealthyClientCount() int {
	count := 0
	for i := 0; i < p.maxClients; i++ {
		if p.clients[i] != nil && p.healthStatus[i] {
			count++
		}
	}
	UpdateRPCHealthyConnections(count)
	return count
}

// StartHealthCheck starts the health check routine
func (p *RPCClientPool) StartHealthCheck(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(p.config.HealthCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.checkConnections()
			}
		}
	}()
}

// checkConnections verifies all connections and attempts to reconnect failed ones
func (p *RPCClientPool) checkConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.maxClients; i++ {
		if p.clients[i] == nil || !p.healthStatus[i] {
			// Attempt to reconnect
			if err := p.initializeClient(i); err != nil {
				RecordRPCError(i, "connection_failed")
				p.healthStatus[i] = false
			} else {
				RecordRPCReconnect(i)
				p.healthStatus[i] = true
			}
		} else {
			// Check existing connection
			if err := p.verifyConnection(p.clients[i]); err != nil {
				RecordRPCError(i, "health_check_failed")
				p.healthStatus[i] = false
			}
		}
	}

	// Update metrics
	p.getHealthyClientCount()
}

// Close closes all clients in the pool
func (p *RPCClientPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, client := range p.clients {
		if client != nil {
			client.Close()
			p.clients[i] = nil
		}
	}
}
