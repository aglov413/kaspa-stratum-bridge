package gostratum

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/onemorebsmith/kaspastratum/src/gostratum/stratumrpc"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type DisconnectChannel chan *StratumContext
type StateGenerator func() any
type EventHandler func(ctx *StratumContext, event stratumrpc.JsonRpcEvent) error
type StratumHandlerMap map[string]EventHandler

type StratumStats struct {
	Disconnects int64
}

type StratumListenerConfig struct {
	Logger         *zap.Logger
	HandlerMap     StratumHandlerMap
	StateGenerator StateGenerator
	Port           string
}

type StratumListener struct {
	StratumListenerConfig

	clients           sync.Map
	shuttingDown      bool
	disconnectChannel DisconnectChannel
	stats             StratumStats
	workerGroup       sync.WaitGroup
}

func NewListener(cfg StratumListenerConfig) *StratumListener {
	listener := &StratumListener{
		StratumListenerConfig: cfg,
		clients:               sync.Map{},
		workerGroup:           sync.WaitGroup{},
	}

	listener.Logger = listener.Logger.With(
		zap.String("component", "stratum"),
		zap.String("address", listener.Port),
	)

	if listener.StateGenerator == nil {
		listener.Logger.Warn("no state generator provided, using default")
		listener.StateGenerator = func() any { return nil }
	}

	return listener
}

func (s *StratumListener) Listen(ctx context.Context) error {
	s.shuttingDown = false

	serverContext, cancel := context.WithCancel(context.Background())
	defer cancel()

	lc := net.ListenConfig{}
	server, err := lc.Listen(ctx, "tcp", s.Port)
	if err != nil {
		return errors.Wrapf(err, "failed listening to socket %s", s.Port)
	}
	defer server.Close()

	go s.disconnectListener(serverContext)
	go s.tcpListener(serverContext, server)

	// block here until the context is killed
	<-ctx.Done() // context cancelled, so kill the server
	s.shuttingDown = true
	server.Close()
	s.workerGroup.Wait()
	return context.Canceled
}

func (s *StratumListener) newClient(ctx context.Context, connection net.Conn) {
	addr := connection.RemoteAddr().String()
	clientContext := &StratumContext{
		ctx:        ctx,
		RemoteAddr: addr,
		Logger:     s.Logger.With(zap.String("client", addr)),
		connection: connection,
		State:      s.StateGenerator(),
	}

	s.Logger.Info("new client connecting", zap.String("client", addr))
	s.clients.Store(addr, &clientContext)
	go spawnClientListener(clientContext, connection, s)
}

func (s *StratumListener) HandleEvent(ctx *StratumContext, event stratumrpc.JsonRpcEvent) error {
	if handler, exists := s.HandlerMap[string(event.Method)]; exists {
		return handler(ctx, event)
	}
	s.Logger.Warn(fmt.Sprintf("unhandled event '%+v'", event))
	return nil
}

func (s *StratumListener) disconnectListener(ctx context.Context) {
	s.workerGroup.Add(1)
	defer s.workerGroup.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case client := <-s.disconnectChannel:
			_, exists := s.clients.LoadAndDelete(client)
			if exists {
				s.Logger.Info("client disconnecting", zap.Any("client", client))
				s.stats.Disconnects++
			}
		}
	}
}

func (s *StratumListener) tcpListener(ctx context.Context, server net.Listener) {
	s.workerGroup.Add(1)
	defer s.workerGroup.Done()
	for { // listen and spin forever
		connection, err := server.Accept()
		if err != nil {
			if s.shuttingDown {
				s.Logger.Error("stopping listening due to server shutdown")
				return
			}
			s.Logger.Error("failed to accept incoming connection", zap.Error(err))
			continue
		}
		s.newClient(ctx, connection)
	}
}
