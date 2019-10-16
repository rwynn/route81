package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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

	"github.com/BurntSushi/toml"
	"github.com/PaesslerAG/gval"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rwynn/gtm"
	"github.com/rwynn/route81/decoding"
	"github.com/rwynn/route81/encoding"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	version = "1.2.0"
)

var supportedConsumerFormats = []string{"json-ext", "json", "avro"}

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
	ID        interface{}            `bson:"_id,omitempty" json:"_id,omitempty"`
	Timestamp primitive.Timestamp    `bson:"ts" json:"ts"`
	Namespace string                 `bson:"ns" json:"ns"`
	Operation string                 `bson:"op,omitempty" json:"op,omitempty"`
	Updates   map[string]interface{} `bson:"updates,omitempty" json:"updates,omitempty"`
}

type kafkaMessage struct {
	Meta kafkaMeta              `bson:"meta" json:"meta"`
	Data map[string]interface{} `bson:"data,omitempty" json:"data,omitempty"`
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

type pipeline struct {
	Namespace string `toml:"namespace" json:"namespace"`
	Direct    bool   `toml:"direct" json:"direct"`
	Stages    string `toml:"stages" json:"stages"`
	stagesVal []interface{}
}

type consumer struct {
	GroupID           string   `toml:"group-id" json:"group-id"`
	Namespace         string   `toml:"namespace" json:"namespace"`
	Topics            []string `toml:"topics" json:"topics"`
	Format            string   `toml:"message-format" json:"message-format"`
	BulkSize          int      `toml:"bulk-size" json:"bulk-size"`
	BulkFlushDuration string   `toml:"bulk-flush-duration" json:"bulk-flush-duration"`
	Workers           int      `toml:"workers" json:"workers"`
	AvroSchemaSpec    string   `toml:"avro-schema-spec" json:"avro-schema-spec"`
	AvroBinary        bool     `toml:"avro-binary" json:"avro-binary"`
	DocRootPath       string   `toml:"document-root-path" json:"document-root-path"`
	DeleteIDPath      string   `toml:"delete-id-path" json:"delete-id-path"`
	rootEval          gval.Evaluable
	deleteEval        gval.Evaluable
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
	DisableChangeStream  bool          `toml:"disable-change-stream" json:"disable-change-stream"`
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
	Pipelines            []pipeline    `toml:"pipeline" json:"pipelines"`
	Consumers            []consumer    `toml:"consumer" json:"consumers"`
	pipe                 map[string][]pipeline
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
	cons := c.Consumers
	return func(op *gtm.Op) bool {
		if op.GetDatabase() == db {
			return false
		}
		if len(cons) > 0 {
			for _, c := range cons {
				if op.Namespace == c.Namespace {
					return false
				}
			}
		}
		return true
	}
}

func (c *config) makePipe() gtm.PipelineBuilder {
	if len(c.pipe) == 0 {
		return nil
	}
	return func(ns string, stream bool) ([]interface{}, error) {
		lines := c.pipe[ns]
		if lines == nil {
			return nil, nil
		}
		for _, line := range lines {
			if stream == !line.Direct {
				return line.stagesVal, nil
			}
		}
		return nil, nil
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
					ts.I++
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
	if c.DisableChangeStream {
		c.ChangeStreamNs = []string{}
	} else if len(c.ChangeStreamNs) == 0 {
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

func (c *config) loadPipelines(fc *config) {
	if len(fc.Pipelines) > 0 {
		c.Pipelines = fc.Pipelines
		c.pipe = make(map[string][]pipeline)
		for _, p := range fc.Pipelines {
			err := json.Unmarshal([]byte(p.Stages), &p.stagesVal)
			if err != nil {
				errorLog.Fatalf("Configuration failed: invalid pipeline stages: %s: %s",
					p.Stages, err)
			}
			pipes := c.pipe[p.Namespace]
			c.pipe[p.Namespace] = append(pipes, p)
		}
	}
}

func (c *config) loadConsumers(fc *config) {
	var err error
	if len(fc.Consumers) > 0 {
		for _, con := range fc.Consumers {
			if con.GroupID == "" {
				con.GroupID = "route81"
			}
			if con.Format == "" {
				con.Format = supportedConsumerFormats[0]
			} else {
				supportedFormat := false
				for _, fmt := range supportedConsumerFormats {
					if con.Format == fmt {
						supportedFormat = true
						break
					}
				}
				if !supportedFormat {
					errorLog.Fatalf("Configuration failed: unsupported consumer format: %s",
						con.Format)
				}
			}
			if con.Format == "avro" && con.AvroSchemaSpec == "" {
				errorLog.Fatalln("Configuration failed: avro-schema-spec is required for the avro format")
			}
			if con.BulkSize == 0 {
				con.BulkSize = 100
			}
			if con.BulkFlushDuration == "" {
				con.BulkFlushDuration = "5s"
			}
			if con.Workers == 0 {
				con.Workers = 4
			}
			ns := strings.SplitN(con.Namespace, ".", 2)
			if len(ns) != 2 {
				errorLog.Fatalf("Configuration failed: invalid consumer namespace: %s",
					con.Namespace)
			}
			if con.DocRootPath != "" {
				con.rootEval, err = gval.Full().NewEvaluable(con.DocRootPath)
				if err != nil {
					errorLog.Fatalf("Configuration failed: invalid doc root path: %s: %s",
						con.DocRootPath, err)
				}
			}
			if con.DeleteIDPath != "" {
				con.deleteEval, err = gval.Full().NewEvaluable(con.DeleteIDPath)
				if err != nil {
					errorLog.Fatalf("Configuration failed: invalid delete ID path: %s: %s",
						con.DeleteIDPath, err)
				}
			}
			c.Consumers = append(c.Consumers, con)
		}
	}
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
	if fc.DisableChangeStream {
		c.DisableChangeStream = true
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
	c.loadPipelines(fc)
	c.loadConsumers(fc)
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
	flag.BoolVar(&c.DisableChangeStream, "disable-change-stream", false,
		"True to disable change events")
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

type clientStats struct {
	Producer *producerStats `json:"producer"`
	Consumer *consumerStats `json:"consumer"`
}

type consumerStats struct {
	Success    int64 `json:"success"`
	Failed     int64 `json:"failed"`
	Queued     int64 `json:"queued"`
	sync.Mutex `json:"-"`
}

type producerStats struct {
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
	msgEncoder    encoding.MessageEncoder
	metaEncoder   encoding.MessageEncoder
	sinks         []*sinkClient
	stopC         chan bool
	allWg         *sync.WaitGroup
	logs          *loggers
	stats         *producerStats
	statsCons     *consumerStats
	statsDuration time.Duration
	timestamp     primitive.Timestamp
	deliveryC     chan kafka.Event
	started       time.Time
	httpServer    *http.Server
	respHandlers  int
	sync.Mutex
}

type sinkClient struct {
	msgClient *msgClient
	consumer  *consumer
	events    chan kafka.Event
	decoder   decoding.MessageDecoder
}

type sinkWorker struct {
	sinkClient *sinkClient
	docs       []interface{}
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

func (is *producerStats) dup() *producerStats {
	is.Lock()
	defer is.Unlock()
	return &producerStats{
		Success:  is.Success,
		Failed:   is.Failed,
		Inserted: is.Inserted,
		Updated:  is.Updated,
		Removed:  is.Removed,
		Dropped:  is.Dropped,
		Queued:   is.Queued,
	}
}

func (cs *consumerStats) dup() *consumerStats {
	cs.Lock()
	defer cs.Unlock()
	return &consumerStats{
		Success: cs.Success,
		Failed:  cs.Failed,
		Queued:  cs.Queued,
	}
}

func (cs *consumerStats) addSuccess(count int) {
	cs.Lock()
	cs.Success += int64(count)
	cs.Unlock()
}

func (cs *consumerStats) addFailed(count int) {
	cs.Lock()
	cs.Failed += int64(count)
	cs.Unlock()
}

func (cs *consumerStats) addQueued(count int) {
	cs.Lock()
	cs.Queued += int64(count)
	cs.Unlock()
}

func (is *producerStats) addSuccess(count int) {
	is.Lock()
	is.Success += int64(count)
	is.Unlock()
}

func (is *producerStats) addFailed(count int) {
	is.Lock()
	is.Failed += int64(count)
	is.Unlock()
}

func (is *producerStats) addInserted(count int) {
	is.Lock()
	is.Inserted += int64(count)
	is.Unlock()
}

func (is *producerStats) addUpdated(count int) {
	is.Lock()
	is.Updated += int64(count)
	is.Unlock()
}

func (is *producerStats) addRemoved(count int) {
	is.Lock()
	is.Removed += int64(count)
	is.Unlock()
}

func (is *producerStats) addDropped(count int) {
	is.Lock()
	is.Dropped += int64(count)
	is.Unlock()
}

func (is *producerStats) addQueued(count int) {
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

func (mc *msgClient) setSinks(conf *config) *msgClient {
	var sinks []*sinkClient
	if len(conf.Consumers) > 0 {
		for _, c := range conf.Consumers {
			consumer := &c
			sink := &sinkClient{
				msgClient: mc,
				consumer:  consumer,
			}
			switch consumer.Format {
			case "avro":
				sink.decoder = &decoding.AvroMessageDecoder{
					SchemaSpec: consumer.AvroSchemaSpec,
					Binary:     consumer.AvroBinary,
				}
			case "json-ext":
				sink.decoder = &decoding.JSONExtMessageDecoder{}
			default:
				sink.decoder = &decoding.JSONMessageDecoder{}
			}
			sinks = append(sinks, sink)
		}
	}
	mc.sinks = sinks
	return mc
}

func newMsgClient(client *mongo.Client, producer *kafka.Producer, ctx *gtm.OpCtx, conf *config) *msgClient {
	statsDuration, _ := time.ParseDuration(conf.StatsDuration)
	mc := &msgClient{
		mongoClient:   client,
		producer:      producer,
		config:        conf,
		readC:         make(chan bool),
		readContext:   ctx,
		allWg:         &sync.WaitGroup{},
		stopC:         make(chan bool),
		kafkaServers:  conf.KafkaServers,
		msgEncoder:    &encoding.JSONExtMessageEncoder{},
		metaEncoder:   &encoding.JSONMessageEncoder{},
		logs:          newLoggers(),
		stats:         &producerStats{},
		statsCons:     &consumerStats{},
		statsDuration: statsDuration,
		deliveryC:     make(chan (kafka.Event), 1000),
		respHandlers:  4,
	}
	mc.setSinks(conf)
	return mc
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
	producerStats := mc.stats.dup()
	consumerStats := mc.statsCons.dup()
	stats := &clientStats{
		Producer: producerStats,
		Consumer: consumerStats,
	}
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
	defer mc.allWg.Done()
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
		mc.allWg.Add(1)
		go mc.responseLoop()
	}
}

func (mc *msgClient) responseLoop() {
	defer mc.allWg.Done()
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
		mc.allWg.Add(1)
		go mc.timestampLoop()
	}
}

func (mc *msgClient) serveHTTP() {
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
			producerStats := mc.stats.dup()
			consumerStats := mc.statsCons.dup()
			stats := &clientStats{
				Producer: producerStats,
				Consumer: consumerStats,
			}
			statsJSON, err := json.MarshalIndent(stats, "", "    ")
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write(statsJSON)
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

func (sc *sinkClient) messageDoc(m *kafka.Message) (interface{}, error) {
	var err error
	var doc interface{}
	if doc, err = sc.decoder.Decode(m.Value); err != nil {
		return nil, err
	}
	return doc, nil
}

func (sw *sinkWorker) queueMessage(e kafka.Event) {
	sc := sw.sinkClient
	mc := sc.msgClient
	stats := mc.statsCons
	logs := mc.logs
	switch ev := e.(type) {
	case *kafka.Message:
		var err error
		if err = ev.TopicPartition.Error; err != nil {
			logs.errorLog.Println(err)
			return
		}
		var doc interface{}
		if doc, err = sc.messageDoc(ev); err != nil {
			logs.errorLog.Println(err)
			stats.addFailed(1)
			return
		}
		sw.docs = append(sw.docs, doc)
		stats.addQueued(1)
		if sw.full() {
			sw.flush()
		}
	case kafka.Error:
		logs.errorLog.Println(ev)
	}
}

func (sw *sinkWorker) isUpsert(doc interface{}) bool {
	t, _ := sw.transform(doc)
	if t == nil {
		return false
	}
	if m, ok := t.(map[string]interface{}); ok {
		if len(m) == 1 && m["_id"] != nil {
			return false
		}
	}
	return true
}

func (sw *sinkWorker) transform(doc interface{}) (result interface{}, err error) {
	sc := sw.sinkClient
	c := sc.consumer
	result = doc
	if c.rootEval != nil {
		if result, err = c.rootEval(context.Background(), doc); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (sw *sinkWorker) deleteFilter(doc interface{}) (bson.M, error) {
	sc := sw.sinkClient
	c := sc.consumer
	if c.deleteEval != nil {
		id, err := c.deleteEval(context.Background(), doc)
		if err != nil {
			return nil, err
		}
		return bson.M{"_id": id}, nil
	}
	if m, ok := doc.(map[string]interface{}); ok {
		if id := m["_id"]; id != nil {
			return bson.M{"_id": m["_id"]}, nil
		}
	}
	return nil, fmt.Errorf("Unable to extract _id from %v", doc)
}

func (sw *sinkWorker) replaceFilter(doc interface{}) (result interface{}, err error) {
	if m, ok := doc.(map[string]interface{}); ok {
		if id := m["_id"]; id != nil {
			return bson.M{"_id": m["_id"]}, nil
		}
	}
	return nil, fmt.Errorf("Unable to extract _id from %v", doc)
}

func (sw *sinkWorker) flush() {
	if sw.empty() {
		return
	}
	defer sw.drain()
	sc := sw.sinkClient
	mc := sc.msgClient
	stats := mc.statsCons
	defer stats.addQueued(-1 * len(sw.docs))
	logs := mc.logs
	c := sc.consumer
	ns := strings.SplitN(c.Namespace, ".", 2)
	db, collection := ns[0], ns[1]
	client := mc.mongoClient
	col := client.Database(db).Collection(collection)
	bwo := options.BulkWrite()
	bwo.SetOrdered(false)
	var models []mongo.WriteModel
	for _, doc := range sw.docs {
		if sw.isUpsert(doc) {
			if replacement, err := sw.transform(doc); err == nil {
				filter, err := sw.replaceFilter(replacement)
				if err == nil {
					model := mongo.NewReplaceOneModel()
					model.SetUpsert(true)
					model.SetFilter(filter)
					model.SetReplacement(replacement)
					models = append(models, model)
				} else {
					logs.errorLog.Printf("Message transform failed: %s", err)
					stats.addFailed(1)
				}
			} else {
				logs.errorLog.Printf("Message transform failed: %s", err)
				stats.addFailed(1)
			}
		} else {
			filter, err := sw.deleteFilter(doc)
			if err == nil {
				model := mongo.NewDeleteOneModel()
				model.SetFilter(filter)
				models = append(models, model)
			} else {
				logs.errorLog.Printf("Message transform failed: %s", err)
				stats.addFailed(1)
			}
		}
	}
	if _, err := col.BulkWrite(context.Background(), models, bwo); err != nil {
		logs.errorLog.Println(err)
		if bwe, ok := err.(mongo.BulkWriteException); ok {
			werrs := len(bwe.WriteErrors)
			stats.addFailed(werrs)
			stats.addSuccess(len(sw.docs) - werrs)
		} else {
			stats.addFailed(len(sw.docs))
		}
	} else {
		stats.addSuccess(len(sw.docs))
	}
}

func (sw *sinkWorker) drain() {
	sw.docs = nil
}

func (sw *sinkWorker) empty() bool {
	return len(sw.docs) == 0
}

func (sw *sinkWorker) full() bool {
	c := sw.sinkClient.consumer
	return len(sw.docs) >= c.BulkSize
}

func (sc *sinkClient) consumerSettings() *kafka.ConfigMap {
	mc := sc.msgClient
	config := mc.config
	c := sc.consumer
	cm := &kafka.ConfigMap{
		"bootstrap.servers":        config.KafkaServers,
		"group.id":                 c.GroupID,
		"auto.offset.reset":        "earliest",
		"go.events.channel.enable": true,
	}
	withKafkaSettings(cm, config)
	return cm
}

func (sc *sinkClient) startWorkers() {
	mc := sc.msgClient
	c := sc.consumer
	cons, err := kafka.NewConsumer(sc.consumerSettings())
	if err != nil {
		mc.logs.errorLog.Println(err)
		return
	}
	cons.SubscribeTopics(c.Topics, nil)
	sc.events = cons.Events()
	for i := 0; i < c.Workers; i++ {
		sw := &sinkWorker{sinkClient: sc}
		mc.allWg.Add(1)
		go sw.readMessages()
	}
}

func (sw *sinkWorker) readMessages() {
	sc := sw.sinkClient
	mc := sc.msgClient
	defer mc.allWg.Done()
	c := sc.consumer
	flushDur, _ := time.ParseDuration(c.BulkFlushDuration)
	flushT := time.NewTicker(flushDur)
	defer flushT.Stop()
	defer sw.drain()
	done := false
	for !done {
		select {
		case <-flushT.C:
			sw.flush()
		case e := <-sc.events:
			sw.queueMessage(e)
		case <-mc.stopC:
			done = true
		}
	}
}

func (mc *msgClient) startSinks() {
	if len(mc.sinks) > 0 {
		for _, s := range mc.sinks {
			go s.startWorkers()
		}
	}
}

func (mc *msgClient) eventLoop() {
	go mc.serveHTTP()
	go mc.startSinks()
	go mc.readListen()
	go mc.statsLoop()
	mc.startResponseHandlers()
	mc.metaLoop()
	go mc.sigListen()
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
	close(mc.stopC)
	mc.allWg.Wait()
	mc.producer.Flush(2000)
	mc.producer.Close()
	mc.mongoClient.Disconnect(context.Background())
}

func (mc *msgClient) recordSuccessTs(ev *kafka.Message) error {
	var mess kafkaMessageMeta
	if err := bson.UnmarshalExtJSON(ev.Value, true, &mess); err != nil {
		return err
	}
	mc.setTimestamp(mess.Meta.Timestamp)
	return nil
}

func (mc *msgClient) enrichError(ev *kafka.Message) error {
	var op *gtm.Op
	var mess kafkaMessageMeta
	if err := bson.UnmarshalExtJSON(ev.Value, true, &mess); err == nil {
		op = &gtm.Op{
			Id:        mess.Meta.ID,
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
	return op.Data
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

func (mc *msgClient) getMsgID(op *gtm.Op) []byte {
	enc := mc.metaEncoder
	if op.IsCommand() {
		value, _ := enc.Encode("")
		return value
	}
	value, _ := enc.Encode(op.Id)
	return value
}

func (mc *msgClient) getMsgKey(op *gtm.Op) []byte {
	enc := mc.metaEncoder
	if op.IsCommand() {
		if _, drop := op.IsDropDatabase(); drop {
			value, _ := enc.Encode(op.GetDatabase())
			return value
		}
		value, _ := enc.Encode(op.Namespace)
		return value
	}
	return mc.getMsgID(op)
}

func (mc *msgClient) getMsgOperation(op *gtm.Op) string {
	if op.IsSourceDirect() {
		return ""
	}
	return op.Operation
}

func (mc *msgClient) getMsgHeaders(op *gtm.Op) ([]kafka.Header, error) {
	enc := mc.metaEncoder
	ts, err := enc.Encode(op.Timestamp)
	if err != nil {
		return nil, err
	}
	headers := []kafka.Header{
		{Key: "ts", Value: ts},
		{Key: "ns", Value: []byte(op.Namespace)},
	}
	if op.IsCommand() == false {
		id := mc.getMsgID(op)
		headers = append(headers, kafka.Header{Key: "_id", Value: id})
	}
	return headers, nil
}

func (mc *msgClient) send(op *gtm.Op) error {
	mc.addEventType(op)
	topic := mc.getMsgTopic(op)
	key := mc.getMsgKey(op)
	operation := mc.getMsgOperation(op)
	data := mc.getMsgData(op)
	updates := mc.getMsgUpdates(op)
	km := kafkaMessage{
		Meta: kafkaMeta{
			ID:        op.Id,
			Timestamp: op.Timestamp,
			Namespace: op.Namespace,
			Operation: operation,
			Updates:   updates,
		},
		Data: data,
	}
	enc := mc.msgEncoder
	value, err := enc.Encode(km)
	if err != nil {
		mc.stats.addFailed(1)
		return &producerError{op: op, err: err}
	}
	headers, err := mc.getMsgHeaders(op)
	if err != nil {
		mc.stats.addFailed(1)
		return &producerError{op: op, err: err}
	}
	const partAny int32 = kafka.PartitionAny
	err = mc.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partAny},
		Key:            key,
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

func withKafkaSettings(cm *kafka.ConfigMap, conf *config) {
	ksets := conf.KafkaSettings
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
}

func withProducerSettings(conf *config) *kafka.ConfigMap {
	cm := &kafka.ConfigMap{
		"bootstrap.servers": conf.KafkaServers,
	}
	withKafkaSettings(cm, conf)
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
		Pipe:               conf.makePipe(),
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
	producer = mustProducer(conf)
	ctx := startReads(client, conf)
	mc := newMsgClient(client, producer, ctx, conf)
	mc.eventLoop()
}
