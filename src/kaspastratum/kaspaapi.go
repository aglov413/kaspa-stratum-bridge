package kaspastratum

import (
	"context"
	"fmt"
	"time"

	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/onemorebsmith/kaspastratum/src/gostratum"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type KaspaApi struct {
	address       string
	blockWaitTime time.Duration
	logger        *zap.SugaredLogger
	rpcPool       *RPCClientPool
	connected     bool
}

func NewKaspaAPI(address string, blockWaitTime time.Duration, logger *zap.SugaredLogger, rpcConfig RPCConfig) (*KaspaApi, error) {
	// Create RPC pool with configured settings
	rpcPool, err := NewRPCClientPool(address, rpcConfig, logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create RPC connection pool")
	}

	api := &KaspaApi{
		address:       address,
		blockWaitTime: blockWaitTime,
		logger:        logger.With(zap.String("component", "kaspaapi:"+address)),
		rpcPool:       rpcPool,
		connected:     true,
	}

	return api, nil
}

func (ks *KaspaApi) Start(ctx context.Context, blockCb func()) {
	// Start the health check routine for the RPC pool
	ks.rpcPool.StartHealthCheck(ctx)
	ks.waitForSync(true)
	go ks.startBlockTemplateListener(ctx, blockCb)
	go ks.startStatsThread(ctx)
}

func (ks *KaspaApi) startStatsThread(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ctx.Done():
			ks.logger.Warn("context cancelled, stopping stats thread")
			return
		case <-ticker.C:
			client := ks.rpcPool.GetClient()
			if client == nil {
				ks.logger.Warn("no healthy RPC clients available")
				continue
			}

			dagResponse, err := client.GetBlockDAGInfo()
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from kaspa, prom stats will be out of date", zap.Error(err))
				continue
			}
			response, err := client.EstimateNetworkHashesPerSecond(dagResponse.TipHashes[0], 1000)
			if err != nil {
				ks.logger.Warn("failed to get network hashrate from kaspa, prom stats will be out of date", zap.Error(err))
				continue
			}
			RecordNetworkStats(response.NetworkHashesPerSecond, dagResponse.BlockCount, dagResponse.Difficulty)
		}
	}
}

func (s *KaspaApi) waitForSync(verbose bool) error {
	if verbose {
		s.logger.Info("checking kaspad sync state")
	}
	for {
		client := s.rpcPool.GetClient()
		if client == nil {
			s.logger.Error("no healthy RPC clients available")
			time.Sleep(5 * time.Second)
			continue
		}

		clientInfo, err := client.GetInfo()
		if err != nil {
			return errors.Wrapf(err, "error fetching server info from kaspad @ %s", s.address)
		}
		if clientInfo.IsSynced {
			break
		}
		s.logger.Warn("Kaspa is not synced, waiting for sync before starting bridge")
		time.Sleep(5 * time.Second)
	}
	if verbose {
		s.logger.Info("kaspad synced, starting server")
	}
	return nil
}

func (s *KaspaApi) startBlockTemplateListener(ctx context.Context, blockReadyCb func()) {
	var blockReadyChan chan bool
	restartChannel := true
	ticker := time.NewTicker(s.blockWaitTime)
	for {
		client := s.rpcPool.GetClient()
		if client == nil {
			s.logger.Error("no healthy RPC clients available")
			time.Sleep(5 * time.Second)
			restartChannel = true
			continue
		}

		if err := s.waitForSync(false); err != nil {
			s.logger.Error("error checking kaspad sync state: ", err)
			time.Sleep(5 * time.Second)
			restartChannel = true
			continue
		}

		if restartChannel {
			blockReadyChan = make(chan bool)
			err := client.RegisterForNewBlockTemplateNotifications(func(_ *appmessage.NewBlockTemplateNotificationMessage) {
				blockReadyChan <- true
			})
			if err != nil {
				s.logger.Error("fatal: failed to register for block notifications from kaspa")
			} else {
				restartChannel = false
			}
		}

		select {
		case <-ctx.Done():
			s.logger.Warn("context cancelled, stopping block update listener")
			return
		case <-blockReadyChan:
			blockReadyCb()
			ticker.Reset(s.blockWaitTime)
		case <-ticker.C: // timeout, manually check for new blocks
			blockReadyCb()
		}
	}
}

func (ks *KaspaApi) GetBlockTemplate(
	client *gostratum.StratumContext) (*appmessage.GetBlockTemplateResponseMessage, error) {
	rpcClient := ks.rpcPool.GetClient()
	if rpcClient == nil {
		return nil, errors.New("no healthy RPC clients available")
	}

	template, err := rpcClient.GetBlockTemplate(client.WalletAddr,
		fmt.Sprintf(`%s/kaspa-stratum-bridge/%s`, client.RemoteApp, client.CanxiumAddr))
	if err != nil {
		return nil, errors.Wrap(err, "failed fetching new block template from kaspa")
	}
	return template, nil
}

// GetBalancesByAddresses gets the balances for the provided addresses using the RPC pool
func (ks *KaspaApi) GetBalancesByAddresses(addresses []string) (*appmessage.GetBalancesByAddressesResponseMessage, error) {
	rpcClient := ks.rpcPool.GetClient()
	if rpcClient == nil {
		return nil, errors.New("no healthy RPC clients available")
	}

	response, err := rpcClient.GetBalancesByAddresses(addresses)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get balances from kaspa")
	}

	// Create a new response message with the entries
	return &appmessage.GetBalancesByAddressesResponseMessage{
		Entries: response.Entries,
	}, nil
}

// Close closes the RPC pool and all its connections
func (ks *KaspaApi) Close() {
	if ks.rpcPool != nil {
		ks.rpcPool.Close()
	}
}
