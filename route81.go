package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rwynn/gtm"
	"github.com/rwynn/route81/encoding"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	version = "1.0.2"
)

var (
	infoLog  = log.New(os.Stderr, "INFO ", log.Flags())
	warnLog  = log.New(os.Stdout, "WARN ", log.Flags())
	statsLog = log.New(os.Stdout, "STATS ", log.Flags())
	traceLog = log.New(os.Stdout, "TRACE ", log.Flags())
	errorLog = log.New(os.Stderr, "ERROR ", log.Flags())
)

type stringargs []string

type producerError struct {
	err error
	op  *gtm.Op
}

type kafkaMeta struct {
	Id        string                 `json:"_id,omitempty"`
	Timestamp primitive.Timestamp    `json:"ts"`
	Namespace string                 `json:"ns"`
	Operation string                 `json:"op,omitempty"`
	Updates   map[string]interface{} `json:"updates,omitempty"`
}

type kafkaMessage struct {
	Meta kafkaMeta              `json:"meta"`
	Data map[string]interface{} `json:"data,omitempty"`
}

type kafkaMessageMeta struct {
	Meta kafkaMeta `json:"meta"`
}

type kafkaSettings struct {
	EnableIdempotence     bool   `toml:"enable-idempotence" json:"enable-idempotence"`
	RequestTimeoutMs      int    `toml:"request-timeout-ms" json:"request-timeout-ms"`
	MessageTimeoutMs      int    `toml:"message-timeout-ms" json:"message-timeout-ms"`
	MessageMaxRetries     int    `toml:"message-max-retries" json:"message-max-retries"`
	RetryBackoffMs        int    `toml:"retry-backoff-ms" json:"retry-backoff-ms"`
	SecurityProto         string `toml:"security-protocol" json:"security-protocol"`
	SSLCAFile             string `toml:"ssl-ca-location" json:"ssl-ca-location"`
	SSLCertFile           string `toml:"ssl-certificate-location" json:"ssl-certificate-location"`
	SSLKeyFile            string `toml:"ssl-key-location" json:"ssl-key-location"`
	SSLKeyPass            string `toml:"ssl-key-password" json:"ssl-key-password"`
	BrokerVersionFallback string `toml:"broker-version-fallback" json:"broker-version-fallback"`
	APIVersionFallback    bool   `toml:"api-version-fallback" json:"api-version-fallback"`
	APIVersionFallbackMs  int    `toml:"api-version-fallback-ms" json:"api-version-fallback-ms"`
	SASLMechanisms        string `toml:"sasl-mechanisms" json:"sasl-mechanisms"`
	SASLUser              string `toml:"sasl-username" json:"sasl-username"`
	SASLPass              string `toml:"sasl-password" json:"sasl-password"`
}

type config struct {
	ConfigFile           string        `json:"config-file"`
	MongoURI             string        `toml:"mongo" json:"mongo"`
	KafkaServers         string        `toml:"kafka" json:"kafka"`
	TopicPrefix          string        `toml:"topic-name-prefix" json:"topic-name-prefix"`
	StatsDuration        string        `toml:"stats-duration" json:"stats-duration"`
	ChangeStreamNs       stringargs    `toml:"change-stream-namespaces" json:"change-stream-namespaces"`
	DirectReadNs         stringargs    `toml:"direct-read-namespaces" json:"direct-read-namespaces"`
	DirectReadSplitMax   int           `toml:"direct-read-split-max" json:"direct-read-split-max"`
	DirectReadConcur     int           `toml:"direct-read-concur" json:"direct-read-concur"`
	FailFast             bool          `toml:"fail-fast" json:"fail-fast"`
	ExitAfterDirectReads bool          `toml:"exit-after-direct-reads" json:"exit-after-direct-reads"`
	DisableStats         bool          `toml:"disable-stats" json:"disable-stats"`
	DisableStatsLog      bool          `toml:"disable-stats-log" json:"disable-stats-log"`
	Resume               bool          `toml:"resume" json:"resume"`
	ResumeName           string        `toml:"resume-name" json:"resume-name"`
	MetadataDB           string        `toml:"metadata-db" json:"metadata-db"`
	KafkaSettings        kafkaSettings `toml:"kafka-settings" json:"kafka-settings"`
	EnableHTTPServer     bool          `toml:"http-server" json:"http-server"`
	HTTPServerAddr       string        `toml:"http-server-addr" json:"http-server-addr"`
	Pprof                bool          `toml:"pprof" json:"pprof"`
	viewConfig           bool
}

func (pe *producerError) Error() string {
	op := pe.op
	var sb strings.Builder
	sb.WriteString(pe.err.Error())
	if op != nil {
		sb.WriteString(": Document ID (")
		sb.WriteString(toDocID(op, true))
		sb.WriteString(") Operation (")
		sb.WriteString(op.Operation)
		sb.WriteString(")")
	}
	return sb.String()
}

func (c *config) nsFilter() gtm.OpFilter {
	db := c.MetadataDB
	return func(op *gtm.Op) bool {
		return op.GetDatabase() != db
	}
}

func (c *config) resumeFunc() gtm.TimestampGenerator {
	if !c.Resume {
		return nil
	}
	return func(client *mongo.Client, opts *gtm.Options) (primitive.Timestamp, error) {
		var ts primitive.Timestamp
		col := client.Database(c.MetadataDB).Collection("resume")
		result := col.FindOne(context.Background(), bson.M{
			"_id": c.ResumeName,
		})
		if err := result.Err(); err == nil {
			doc := make(map[string]interface{})
			if err = result.Decode(&doc); err == nil {
				if doc["ts"] != nil {
					ts = doc["ts"].(primitive.Timestamp)
					ts.I += 1
				}
			}
		}
		if ts.T == 0 {
			ts, _ = gtm.LastOpTimestamp(client, opts)
		}
		infoLog.Printf("Resuming from timestamp %+v", ts)
		return ts, nil
	}
}

func (c *config) hasFlag(name string) bool {
	passed := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			passed = true
		}
	})
	return passed
}

func (c *config) log(out io.Writer) {
	if b, err := json.MarshalIndent(c, "", "  "); err == nil {
		out.Write(b)
		out.Write([]byte("\n"))
	}
}

func (c *config) setDefaults() *config {
	if len(c.ChangeStreamNs) == 0 {
		c.ChangeStreamNs = []string{""}
	}
	return c
}

func (c *config) validate() error {
	if c.StatsDuration != "" {
		if _, err := time.ParseDuration(c.StatsDuration); err != nil {
			return fmt.Errorf("Invalid StatsDuration: %s", err)
		}
	} else {
		return fmt.Errorf("StatsDuration cannot be empty")
	}
	return nil
}

func (c *config) override(fc *config) {
	if !c.hasFlag("mongo") && fc.MongoURI != "" {
		c.MongoURI = fc.MongoURI
	}
	if !c.hasFlag("kakfa") && fc.KafkaServers != "" {
		c.KafkaServers = fc.KafkaServers
	}
	if fc.Pprof {
		c.Pprof = true
	}
	if fc.EnableHTTPServer {
		c.EnableHTTPServer = true
	}
	if !c.hasFlag("http-server-addr") && fc.HTTPServerAddr != "" {
		c.HTTPServerAddr = fc.HTTPServerAddr
	}
	if !c.hasFlag("topic-name-prefix") && fc.TopicPrefix != "" {
		c.TopicPrefix = fc.TopicPrefix
	}
	if fc.FailFast {
		c.FailFast = true
	}
	if fc.ExitAfterDirectReads {
		c.ExitAfterDirectReads = true
	}
	if fc.DisableStats {
		c.DisableStats = true
	}
	if fc.DisableStatsLog {
		c.DisableStatsLog = true
	}
	if fc.Resume {
		c.Resume = true
	}
	if !c.hasFlag("metadata-db") && fc.MetadataDB != "" {
		c.MetadataDB = fc.MetadataDB
	}
	if !c.hasFlag("resume-name") && fc.ResumeName != "" {
		c.ResumeName = fc.ResumeName
	}
	if !c.hasFlag("stats-duration") && fc.StatsDuration != "" {
		c.StatsDuration = fc.StatsDuration
	}
	if len(c.ChangeStreamNs) == 0 {
		c.ChangeStreamNs = fc.ChangeStreamNs
	}
	if len(c.DirectReadNs) == 0 {
		c.DirectReadNs = fc.DirectReadNs
	}
	if !c.hasFlag("direct-read-split-max") && fc.DirectReadSplitMax != 0 {
		c.DirectReadSplitMax = fc.DirectReadSplitMax
	}
	if !c.hasFlag("direct-read-concur") && fc.DirectReadConcur != 0 {
		c.DirectReadConcur = fc.DirectReadConcur
	}
	c.KafkaSettings = fc.KafkaSettings
}

func mustConfig() *config {
	conf, err := loadConfig()
	if err != nil {
		errorLog.Fatalf("Configuration failed: %s", err)
		return nil
	}
	return conf
}

func parseFlags() *config {
	var c config
	var v bool
	flag.BoolVar(&v, "version", false, "Print the version number and exit")
	flag.BoolVar(&v, "v", false, "Print the version number and exit")
	flag.BoolVar(&c.viewConfig, "view-config", false, "Print the configuration and exit")
	flag.StringVar(&c.ConfigFile, "f", "", "Path to a TOML formatted config file")
	flag.StringVar(&c.MongoURI, "mongo", "mongodb://localhost:27017",
		"MongoDB connection string URI")
	flag.StringVar(&c.KafkaServers, "kafka", "localhost:9092",
		"Address of Kafka broker")
	flag.BoolVar(&c.FailFast, "fail-fast", false,
		"True to exit the process after the first connection failure")
	flag.BoolVar(&c.ExitAfterDirectReads, "exit-after-direct-reads", false,
		"True to exit the process after direct reads have completed")
	flag.BoolVar(&c.DisableStats, "disable-stats", false,
		"True to disable stats")
	flag.BoolVar(&c.DisableStatsLog, "disable-stats-log", false,
		"True to disable periodic logging of indexing stats")
	flag.BoolVar(&c.Resume, "resume", false,
		"True to resume indexing from last saved timestamp")
	flag.StringVar(&c.ResumeName, "resume-name", "default",
		"Key to store and load saved timestamps from")
	flag.StringVar(&c.MetadataDB, "metadata-db", "route81",
		"Name of the MongoDB database to store route81 metadata")
	flag.StringVar(&c.StatsDuration, "stats-duration", "10s",
		"The max duration to wait before logging indexing stats")
	flag.Var(&c.ChangeStreamNs, "change-stream-namespace", "MongoDB namespace to watch for changes")
	flag.Var(&c.DirectReadNs, "direct-read-namespace", "MongoDB namespace to read and sync")
	flag.IntVar(&c.DirectReadSplitMax, "direct-read-split-max", 9,
		"The max number of times to split each collection for concurrent reads")
	flag.IntVar(&c.DirectReadConcur, "direct-read-concur", 4,
		"The max number collections to read concurrently")
	flag.BoolVar(&c.EnableHTTPServer, "http-server", false,
		"True to enable a HTTP server")
	flag.StringVar(&c.HTTPServerAddr, "http-server-addr", ":8080",
		"The address the HTTP server will bind to")
	flag.StringVar(&c.TopicPrefix, "topic-name-prefix", "",
		"A string to prepend to the MongoDB change event namespace to form the Kafka topic name")
	flag.BoolVar(&c.Pprof, "pprof", false,
		"True to enable profiling support")
	flag.Parse()
	if v {
		fmt.Println(version)
		os.Exit(0)
	}
	return &c
}

func loadConfig() (*config, error) {
	c := parseFlags()
	if c.ConfigFile != "" {
		var fc config
		if md, err := toml.DecodeFile(c.ConfigFile, &fc); err != nil {
			return nil, err
		} else if ud := md.Undecoded(); len(ud) != 0 {
			return nil, fmt.Errorf("Config file contains undecoded keys: %q", ud)
		}
		c.override(&fc)
	}
	if err := c.setDefaults().validate(); err != nil {
		return nil, err
	}
	return c, nil
}

type indexStats struct {
	Success    int64 `json:"success"`
	Failed     int64 `json:"failed"`
	Inserted   int64 `json:"inserted"`
	Updated    int64 `json:"updated"`
	Removed    int64 `json:"removed"`
	Dropped    int64 `json:"dropped"`
	Queued     int64 `json:"queued"`
	sync.Mutex `json:"-"`
}

type loggers struct {
	infoLog  *log.Logger
	warnLog  *log.Logger
	statsLog *log.Logger
	traceLog *log.Logger
	errorLog *log.Logger
}

type msgClient struct {
	mongoClient   *mongo.Client
	producer      *kafka.Producer
	config        *config
	readC         chan bool
	readContext   *gtm.OpCtx
	kafkaServers  string
	stopC         chan bool
	allWg         *sync.WaitGroup
	logs          *loggers
	stats         *indexStats
	statsDuration time.Duration
	timestamp     primitive.Timestamp
	deliveryC     chan kafka.Event
	started       time.Time
	httpServer    *http.Server
	respHandlers  int
	sync.Mutex
}

func (args *stringargs) String() string {
	return fmt.Sprintf("%s", *args)
}

func (args *stringargs) Set(value string) error {
	*args = append(*args, value)
	return nil
}

func toDocID(op *gtm.Op, ns bool) string {
	var id strings.Builder
	if ns {
		id.WriteString(op.Namespace)
		if op.Id != nil {
			id.WriteString(".")
		}
	}
	if op.Id != nil {
		switch val := op.Id.(type) {
		case primitive.ObjectID:
			id.WriteString(val.Hex())
		case float64:
			intID := int(val)
			if op.Id.(float64) == float64(intID) {
				fmt.Fprintf(&id, "%v", intID)
			} else {
				fmt.Fprintf(&id, "%v", op.Id)
			}
		case float32:
			intID := int(val)
			if op.Id.(float32) == float32(intID) {
				fmt.Fprintf(&id, "%v", intID)
			} else {
				fmt.Fprintf(&id, "%v", op.Id)
			}
		default:
			fmt.Fprintf(&id, "%v", op.Id)
		}
	}
	return id.String()
}

func (is *indexStats) dup() *indexStats {
	is.Lock()
	defer is.Unlock()
	return &indexStats{
		Success:  is.Success,
		Failed:   is.Failed,
		Inserted: is.Inserted,
		Updated:  is.Updated,
		Removed:  is.Removed,
		Dropped:  is.Dropped,
		Queued:   is.Queued,
	}
}

func (is *indexStats) addSuccess(count int) {
	is.Lock()
	is.Success += int64(count)
	is.Unlock()
}

func (is *indexStats) addFailed(count int) {
	is.Lock()
	is.Failed += int64(count)
	is.Unlock()
}

func (is *indexStats) addInserted(count int) {
	is.Lock()
	is.Inserted += int64(count)
	is.Unlock()
}

func (is *indexStats) addUpdated(count int) {
	is.Lock()
	is.Updated += int64(count)
	is.Unlock()
}

func (is *indexStats) addRemoved(count int) {
	is.Lock()
	is.Removed += int64(count)
	is.Unlock()
}

func (is *indexStats) addDropped(count int) {
	is.Lock()
	is.Dropped += int64(count)
	is.Unlock()
}

func (is *indexStats) addQueued(count int) {
	is.Lock()
	is.Queued += int64(count)
	is.Unlock()
}

func isNetError(err error) bool {
	if err == nil {
		return false
	}
	switch t := err.(type) {
	case net.Error:
		return true
	case *net.OpError:
		return true
	case syscall.Errno:
		if t == syscall.ECONNREFUSED {
			return true
		}
	}
	return false
}

func newLoggers() *loggers {
	return &loggers{
		infoLog:  infoLog,
		warnLog:  warnLog,
		statsLog: statsLog,
		traceLog: traceLog,
		errorLog: errorLog,
	}
}

func newMsgClient(client *mongo.Client, producer *kafka.Producer, ctx *gtm.OpCtx, conf *config) *msgClient {
	statsDuration, _ := time.ParseDuration(conf.StatsDuration)
	return &msgClient{
		mongoClient:   client,
		producer:      producer,
		config:        conf,
		readC:         make(chan bool),
		readContext:   ctx,
		allWg:         &sync.WaitGroup{},
		stopC:         make(chan bool),
		kafkaServers:  conf.KafkaServers,
		logs:          newLoggers(),
		stats:         &indexStats{},
		statsDuration: statsDuration,
		deliveryC:     make(chan (kafka.Event), 1000),
		respHandlers:  4,
	}
}

func (mc *msgClient) sigListen() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer signal.Stop(sigs)
	<-sigs
	go func() {
		<-sigs
		mc.logs.infoLog.Println("Forcing shutdown, bye bye...")
		os.Exit(1)
	}()
	mc.logs.infoLog.Println("Shutting down")
	mc.stop()
	mc.logs.infoLog.Println("Ready to exit, bye bye...")
	os.Exit(0)
}

func (mc *msgClient) logStats() {
	stats := mc.stats.dup()
	if b, err := json.Marshal(stats); err == nil {
		mc.logs.statsLog.Println(string(b))
	}
}

func (mc *msgClient) statsLoop() {
	if mc.config.DisableStats || mc.config.DisableStatsLog {
		return
	}
	heartBeat := time.NewTicker(mc.statsDuration)
	defer heartBeat.Stop()
	done := false
	for !done {
		select {
		case <-heartBeat.C:
			mc.logStats()
		case <-mc.stopC:
			mc.logStats()
			done = true
		}
	}
}

func (mc *msgClient) readListen() {
	conf := mc.config
	if len(conf.DirectReadNs) > 0 {
		mc.readContext.DirectReadWg.Wait()
		infoLog.Println("Direct reads completed")
		if conf.ExitAfterDirectReads {
			mc.logs.infoLog.Println("Shutting down")
			mc.stop()
			os.Exit(0)
		}
	}
}

func (mc *msgClient) getTimestamp() primitive.Timestamp {
	mc.Lock()
	defer mc.Unlock()
	return mc.timestamp
}

func (mc *msgClient) setTimestamp(ts primitive.Timestamp) {
	mc.Lock()
	if mc.stats.Failed == 0 {
		// if we have a single failure do not progress the timestamp
		mc.timestamp = ts
	}
	mc.Unlock()
}

func (mc *msgClient) saveTimestamp(ts primitive.Timestamp) error {
	config := mc.config
	mclient := mc.mongoClient
	if ts.T == 0 {
		return nil
	}
	col := mclient.Database(config.MetadataDB).Collection("resume")
	doc := bson.M{
		"ts": ts,
	}
	opts := options.Update()
	opts.SetUpsert(true)
	_, err := col.UpdateOne(context.Background(), bson.M{
		"_id": config.ResumeName,
	}, bson.M{
		"$set": doc,
	}, opts)
	return err
}

func (mc *msgClient) timestampLoop() {
	ticker := time.NewTicker(time.Duration(10) * time.Second)
	defer ticker.Stop()
	running := true
	lastTime := primitive.Timestamp{}
	for running {
		select {
		case <-ticker.C:
			ts := mc.getTimestamp()
			if ts.T > lastTime.T || (ts.T == lastTime.T && ts.I > lastTime.I) {
				lastTime = ts
				if err := mc.saveTimestamp(lastTime); err != nil {
					errorLog.Printf("Error saving timestamp: %s", err)
				}
			}
			break
		case <-mc.stopC:
			running = false
			break
		}
	}
}

func (mc *msgClient) handleFatalError() {
	mc.logs.errorLog.Println("Shutting down after fatal error")
	mc.stop()
	os.Exit(1)
}

func (mc *msgClient) startResponseHandlers() {
	for i := 0; i < mc.respHandlers; i++ {
		go mc.responseLoop()
	}
}

func (mc *msgClient) responseLoop() {
	running := true
	for running {
		select {
		case e := <-mc.deliveryC:
			if err := mc.onEvent(e); err != nil {
				mc.logs.errorLog.Printf("Broker response error: %s", err)
				ke, ok := err.(kafka.Error)
				if ok && ke.IsFatal() {
					go mc.handleFatalError()
				}
			}
			break
		case <-mc.stopC:
			running = false
			break
		}
	}
}

func (mc *msgClient) metaLoop() {
	config := mc.config
	if config.Resume {
		go mc.timestampLoop()
	}
}

func (mc *msgClient) serveHttp() {
	config := mc.config
	if !config.EnableHTTPServer {
		return
	}
	mc.buildServer()
	s := mc.httpServer
	infoLog.Printf("Starting http server at %s", s.Addr)
	mc.started = time.Now()
	err := s.ListenAndServe()
	select {
	case <-mc.stopC:
		return
	default:
		errorLog.Fatalf("Unable to serve http at address %s: %s", s.Addr, err)
	}
}

func (mc *msgClient) buildServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/started", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data := (time.Now().Sub(mc.started)).String()
		w.Write([]byte(data))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	if !mc.config.DisableStats {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, req *http.Request) {
			stats, err := json.MarshalIndent(mc.stats.dup(), "", "    ")
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write(stats)
			} else {
				w.WriteHeader(500)
				fmt.Fprintf(w, "Unable to print statistics: %s", err)
			}
		})
	}
	if mc.config.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	s := &http.Server{
		Addr:     mc.config.HTTPServerAddr,
		Handler:  mux,
		ErrorLog: errorLog,
	}
	mc.httpServer = s
}

func (mc *msgClient) eventLoop() {
	go mc.serveHttp()
	go mc.sigListen()
	go mc.readListen()
	go mc.statsLoop()
	mc.startResponseHandlers()
	mc.metaLoop()
	ctx := mc.readContext
	drained := false
	for {
		select {
		case err := <-ctx.ErrC:
			if err == nil {
				break
			}
			mc.logs.errorLog.Println(err)
		case op, open := <-ctx.OpC:
			if op == nil {
				if !open && !drained {
					drained = true
					close(mc.readC)
				}
				break
			}
			if err := mc.send(op); err != nil {
				mc.logs.errorLog.Printf("Producer error: %s", err)
			}
		}
	}
}

func (mc *msgClient) setKafkaServers(s string) *msgClient {
	mc.kafkaServers = s
	return mc
}

func (mc *msgClient) setStatsDuration(max time.Duration) *msgClient {
	mc.statsDuration = max
	return mc
}

func (mc *msgClient) stop() {
	mc.readContext.Stop()
	<-mc.readC
	mc.producer.Flush(2000)
	close(mc.stopC)
	mc.allWg.Wait()
}

func (mc *msgClient) recordSuccessTs(ev *kafka.Message) error {
	var mess kafkaMessageMeta
	if err := json.Unmarshal(ev.Value, &mess); err != nil {
		return err
	}
	mc.setTimestamp(mess.Meta.Timestamp)
	return nil
}

func (mc *msgClient) enrichError(ev *kafka.Message) error {
	var op *gtm.Op
	var mess kafkaMessageMeta
	if err := json.Unmarshal(ev.Value, &mess); err == nil {
		op = &gtm.Op{
			Id:        mess.Meta.Id,
			Namespace: mess.Meta.Namespace,
			Operation: mess.Meta.Operation,
		}
	}
	return &producerError{op: op, err: ev.TopicPartition.Error}
}

func (mc *msgClient) onEvent(e kafka.Event) error {
	if e == nil {
		return nil
	}
	mc.stats.addQueued(-1)
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			mc.stats.addFailed(1)
			return mc.enrichError(ev)
		}
		if err := mc.recordSuccessTs(ev); err != nil {
			return err
		}
	case kafka.Error:
		return ev
	}
	mc.stats.addSuccess(1)
	return nil
}

func (mc *msgClient) addEventType(op *gtm.Op) {
	if op.IsInsert() {
		mc.stats.addInserted(1)
	} else if op.IsUpdate() {
		mc.stats.addUpdated(1)
	} else if op.IsDelete() {
		mc.stats.addRemoved(1)
	} else if op.IsDrop() {
		mc.stats.addDropped(1)
	}
}

func (mc *msgClient) getMsgData(op *gtm.Op) map[string]interface{} {
	var data map[string]interface{}
	if op.Data != nil {
		data = encoding.ConvertMapForJSON(op.Data)
	}
	return data
}

func (mc *msgClient) getMsgUpdates(op *gtm.Op) map[string]interface{} {
	var updates map[string]interface{}
	if op.UpdateDescription != nil {
		ud := op.UpdateDescription
		updates = map[string]interface{}{
			"removed": ud["removedFields"],
			"changed": ud["updatedFields"],
		}
	}
	return updates
}

func (mc *msgClient) getMsgTopic(op *gtm.Op) string {
	var sb strings.Builder
	conf := mc.config
	if conf.TopicPrefix != "" {
		sb.WriteString(conf.TopicPrefix)
		sb.WriteString(".")
	}
	if _, drop := op.IsDropDatabase(); drop {
		sb.WriteString(op.GetDatabase())
	} else {
		sb.WriteString(op.Namespace)
	}
	return sb.String()
}

func (mc *msgClient) getMsgId(op *gtm.Op) string {
	if op.IsCommand() {
		return ""
	}
	return toDocID(op, false)
}

func (mc *msgClient) getMsgKey(op *gtm.Op) string {
	if op.IsCommand() {
		if _, drop := op.IsDropDatabase(); drop {
			return op.GetDatabase()
		}
		return op.Namespace
	}
	return mc.getMsgId(op)
}

func (mc *msgClient) getMsgOperation(op *gtm.Op) string {
	if op.IsSourceDirect() {
		return ""
	}
	return op.Operation
}

func (mc *msgClient) getMsgHeaders(op *gtm.Op) ([]kafka.Header, error) {
	ts, err := json.Marshal(op.Timestamp)
	if err != nil {
		return nil, err
	}
	headers := []kafka.Header{
		{"ts", []byte(ts)},
		{"ns", []byte(op.Namespace)},
	}
	if op.IsCommand() == false {
		id := mc.getMsgId(op)
		headers = append(headers, kafka.Header{"_id", []byte(id)})
	}
	return headers, nil
}

func (mc *msgClient) send(op *gtm.Op) error {
	mc.addEventType(op)
	topic := mc.getMsgTopic(op)
	id := mc.getMsgId(op)
	key := mc.getMsgKey(op)
	operation := mc.getMsgOperation(op)
	data := mc.getMsgData(op)
	updates := mc.getMsgUpdates(op)
	km := kafkaMessage{
		Meta: kafkaMeta{
			Id:        id,
			Timestamp: op.Timestamp,
			Namespace: op.Namespace,
			Operation: operation,
			Updates:   updates,
		},
		Data: data,
	}
	value, err := json.Marshal(km)
	if err != nil {
		mc.stats.addFailed(1)
		return &producerError{op: op, err: err}
	}
	headers, err := mc.getMsgHeaders(op)
	if err != nil {
		mc.stats.addFailed(1)
		return &producerError{op: op, err: err}
	}
	err = mc.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
		Headers:        headers,
	}, mc.deliveryC)
	if err != nil {
		mc.stats.addFailed(1)
		return &producerError{op: op, err: err}
	}
	mc.stats.addQueued(1)
	return nil
}

func buildRegistry() *bsoncodec.Registry {
	rb := bson.NewRegistryBuilder()
	rb.RegisterTypeMapEntry(bsontype.DateTime, reflect.TypeOf(time.Time{}))
	return rb.Build()
}

func dialMongo(URI string) (*mongo.Client, error) {
	clientOptions := options.Client()
	clientOptions.SetRegistry(buildRegistry())
	clientOptions.ApplyURI(URI)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %s", err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()
	if err = client.Connect(ctx1); err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %s", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if err = client.Ping(ctx2, nil); err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %s", err)
	}
	return client, nil
}

func withProducerSettings(conf *config) *kafka.ConfigMap {
	ksets := conf.KafkaSettings
	cm := &kafka.ConfigMap{
		"bootstrap.servers": conf.KafkaServers,
	}
	if ksets.RequestTimeoutMs != 0 {
		cm.SetKey("request.timeout.ms", ksets.RequestTimeoutMs)
	}
	if ksets.MessageTimeoutMs != 0 {
		cm.SetKey("message.timeout.ms", ksets.MessageTimeoutMs)
	}
	if ksets.EnableIdempotence {
		cm.SetKey("enable.idempotence", true)
	}
	if ksets.MessageMaxRetries != 0 {
		cm.SetKey("message.send.max.retries", ksets.MessageMaxRetries)
	}
	if ksets.RetryBackoffMs != 0 {
		cm.SetKey("retry.backoff.ms", ksets.RetryBackoffMs)
	}
	if ksets.SecurityProto != "" {
		cm.SetKey("security.protocol", ksets.SecurityProto)
	}
	if ksets.SSLCAFile != "" {
		cm.SetKey("ssl.ca.location", ksets.SSLCAFile)
	}
	if ksets.SSLCertFile != "" {
		cm.SetKey("ssl.certificate.location", ksets.SSLCertFile)
	}
	if ksets.SSLKeyFile != "" {
		cm.SetKey("ssl.key.location", ksets.SSLKeyFile)
	}
	if ksets.SSLKeyPass != "" {
		cm.SetKey("ssl.key.password", ksets.SSLKeyPass)
	}
	if ksets.BrokerVersionFallback != "" {
		cm.SetKey("broker.version.fallback", ksets.BrokerVersionFallback)
	}
	if ksets.APIVersionFallback {
		cm.SetKey("api.version.fallback.ms", ksets.APIVersionFallbackMs)
	}
	if ksets.SASLMechanisms != "" {
		cm.SetKey("sasl.mechanisms", ksets.SASLMechanisms)
	}
	if ksets.SASLUser != "" {
		cm.SetKey("sasl.username", ksets.SASLUser)
	}
	if ksets.SASLPass != "" {
		cm.SetKey("sasl.password", ksets.SASLPass)
	}
	return cm
}

func mustProducer(conf *config) *kafka.Producer {
	var (
		producer  *kafka.Producer
		connected bool
		err       error
	)
	cm := withProducerSettings(conf)
	for !connected {
		producer, err = kafka.NewProducer(cm)
		if err == nil {
			err = producer.GetFatalError()
		}
		if err == nil {
			connected = true
		} else {
			if conf.FailFast {
				errorLog.Fatalf("Kafka connection failed: %s", err)
				break
			} else {
				errorLog.Printf("Kafka connection failed: %s", err)
			}
			ke, ok := err.(kafka.Error)
			if ok {
				if ke.IsFatal() {
					errorLog.Fatalln("Shutting down after fatal error")
				}
				switch ke.Code() {
				case -198, -196, -186, -159:
					errorLog.Fatalln("Shutting down after fatal error")
				}
			}
		}
	}
	return producer
}

func mustConnect(conf *config) *mongo.Client {
	var (
		client    *mongo.Client
		connected bool
		err       error
	)
	for !connected {
		client, err = dialMongo(conf.MongoURI)
		if err == nil {
			connected = true
		} else {
			if conf.FailFast {
				errorLog.Fatalf("MongoDB connection failed: %s", err)
				break
			} else {
				errorLog.Printf("MongoDB connection failed: %s", err)
			}
		}
	}
	return client
}

func startReads(client *mongo.Client, conf *config) *gtm.OpCtx {
	ctx := gtm.Start(client, &gtm.Options{
		After:              conf.resumeFunc(),
		NamespaceFilter:    conf.nsFilter(),
		ChangeStreamNs:     conf.ChangeStreamNs,
		DirectReadNs:       conf.DirectReadNs,
		DirectReadConcur:   conf.DirectReadConcur,
		DirectReadSplitMax: int32(conf.DirectReadSplitMax),
		OpLogDisabled:      true,
	})
	return ctx
}

func main() {
	var (
		client   *mongo.Client
		producer *kafka.Producer
		conf     *config
	)
	conf = mustConfig()
	if conf.viewConfig {
		conf.log(os.Stdout)
		return
	}
	infoLog.Println("Establishing connection to MongoDB")
	client = mustConnect(conf)
	infoLog.Println("Connected to MongoDB")
	defer client.Disconnect(context.Background())
	producer = mustProducer(conf)
	defer producer.Close()
	ctx := startReads(client, conf)
	mc := newMsgClient(client, producer, ctx, conf)
	mc.eventLoop()
}
