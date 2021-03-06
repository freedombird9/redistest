package main

import (
	"bytes"
	"flag"
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

var doneChan chan bool
var keys []string
var redisOpts metrics.Timer
var wg sync.WaitGroup
var connPool *pool.Pool
var numThreads *int
var keySize *int
var expireTime *int

func initialize() error {
	doneChan = make(chan bool, *numThreads)
	keys = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
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
	for i := 0; i < *numThreads; i++ {
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
			return
		default:
			// continue working
		}
		var buffer bytes.Buffer

		keyDigit := rand.Intn(*keySize)
		for i := 0; i < keyDigit; i++ {
			buffer.WriteString(keys[rand.Intn(len(keys))])
		}

		start := time.Now()
		err := conn.Cmd("incrby", buffer.String(), 88).Err
		if err != nil {
			jlog.Warn(err.Error())
		}
		redisOpts.UpdateSince(start)
		err = conn.Cmd("EXPIRE", buffer.String(), *expireTime).Err
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
	numThreads = flag.Int("numThreads", 100, "Set the number of threads, default: 100")
	keySize = flag.Int("keySize", 3, "Set the key size, default: 3")
	expireTime = flag.Int("expiretTime", 300, "Set the key expire time, default: 300 (5 minutes)")

	flag.Parse()
	err := initialize()
	if err != nil {
		jlog.Warn("initialize failed")
		return
	}
	p, err := pool.New("tcp", "qa-scl007-009:6380", *numThreads)
	connPool = p
	if err != nil {
		jlog.Warn(err.Error())
		return
	}
	wg.Add(*numThreads)
	for i := 0; i < *numThreads; i++ {
		go writeRedis()
	}

	jlog.Info("program started")
	wait()
}
