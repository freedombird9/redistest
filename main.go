package main

import (
	"bytes"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cyberdelia/go-metrics-graphite"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/rcrowley/go-metrics"

	"go-jasperlib/jlog"
)

const (
	numThreads = 20
	expireTime = 300 // 5 min
)

var doneChan chan bool
var keys []string
var redisOpts metrics.Timer
var wg sync.WaitGroup
var connPool *pool.Pool

func initialize() error {
	doneChan = make(chan bool, numThreads)
	keys = []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	redisOpts = metrics.NewRegisteredTimer("redisOpts", metrics.DefaultRegistry)

	// initialize graphite metrics recording
	addr, err := net.ResolveTCPAddr("tcp", "qa-scl008-005:2003")
	if err != nil {
		jlog.Warn("Cannot resolve graphite server")
		return err
	}
	go graphite.Graphite(metrics.DefaultRegistry, 1*time.Minute, "rediscluster.testcase1", addr)

	return nil
}

func stop() {
	for i := 0; i < numThreads; i++ {
		doneChan <- true
	}
}

func writeRedis() {
	defer wg.Done()
	conn, err := connPool.Get()
	if err != nil {
		jlog.Warn(err.Error())
		return
	}
	defer connPool.Put(conn)

	for {
		// Check if someone told us to stop working
		select {
		case <-doneChan:
			jlog.Info("Stop writing...")
			return
		default:
			// continue working
		}
		var buffer bytes.Buffer
		buffer.WriteString(keys[rand.Intn(8)])
		buffer.WriteString(keys[rand.Intn(8)])
		buffer.WriteString(keys[rand.Intn(8)])

		start := time.Now()
		err := conn.Cmd("incrby", buffer.String(), 88).Err
		if err != nil {
			jlog.Warn(err.Error())
		}
		redisOpts.UpdateSince(start)
		err = conn.Cmd("EXPIRE", buffer.String(), expireTime).Err
		if err != nil {
			jlog.Warn(err.Error())
		}
	}
}

func wait() {
	// wait for a signal to shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		stop()
	}()

	wg.Wait()
}

func main() {
	err := initialize()
	if err != nil {
		jlog.Warn("initialize failed")
		return
	}
	p, err := pool.New("tcp", "qa-scl007-009:6380", numThreads)
	connPool = p
	if err != nil {
		jlog.Warn(err.Error())
		return
	}

	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go writeRedis()
	}

	jlog.Info("program started")
	wait()
}
