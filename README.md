# route81

Continuously send your MongoDB data stream to Kafka

### running route81

#### setup MongoDB

Before running route81 you will need to ensure that MongoDB is setup as a replica set.

Following the instructions for [deploying a replica set](http://docs.mongodb.org/manual/tutorial/deploy-replica-set/).

If you haven't already done so, 
follow the 5 step [procedure](https://docs.mongodb.com/manual/tutorial/deploy-replica-set/#procedure) 
to initiate and validate your replica set.

For local testing your replica set may contain a 
[single member](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/).

#### setup Kafka

If you are new to Kafka you can use the following steps to get a simple server running to test with. Run the
following commands in separate terminals:

Start Zookeeper

```
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka

```
cd kafka
bin/kafka-server-start.sh config/server.properties
```

Start a console consumer to view data producer by route81

```
cd kafka
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test.test --from-beginning
```

#### setup route81

route81 uses the [confluent go driver](https://github.com/confluentinc/confluent-kafka-go) for Kafka. This 
driver wraps the C client [librdkafka](https://github.com/edenhill/librdkafka).  You will need to follow the instructions
for installing `libdrdkakfa 1.0` on your system as a prerequisite of running route81.

You may also need the `pkg-config` package to be installed on your system.

After installing the dependencies you can build and run route81 using the latest go binary.

```
# git clone route81 to somewhere outside your $GOPATH dir
cd route81
go run route81.go
```

If you have trouble installing `librdkafka 1.0` and getting route81 to run you can alternatively run route81
via Docker. The Docker build handles building and installing route81 with librdkafka for you.

```
cd route81/docker/release/
./build.sh
docker run --rm --net=host rwynn/route81:1.0.0
```

#### publish MongoDB data to Kafka

At this point route81 should have connected to a MongoDB replica set on localhost. You can then use route81
to publish data into the `test.test` topic by interacting with the `test` collection of the `test` database in 
MongoDB.

```
rs1:PRIMARY> use test;
switched to db test
rs1:PRIMARY> for (var i=0; i<100; ++i) db.test.insert({i: i});
WriteResult({ "nInserted" : 1 })
rs1:PRIMARY> db.test.update({}, {$set: {j: 1}}, {multi:true});
WriteResult({ "nMatched" : 100, "nUpserted" : 0, "nModified" : 100 })
rs1:PRIMARY> db.test.remove({});
WriteResult({ "nRemoved" : 100 })
rs1:PRIMARY> 
``` 

As you perform these operations you should see a log of them being produced to Kafka in your 
kafka-console-consumer.sh terminal window.

### configure route81

You can configure route81 via flags and/or a TOML config file.  The most important options are the 
connection settings.  By default route81 will look to connect to MongoDB and Kafka on localhost
at the default ports.  You can change this via

```
route81 -mongo mongodb://user:pass@hostname:port -kafka hostname:9092
```

The MongoDB URL can be customized with 
[advanced settings](https://github.com/mongodb/mongo-go-driver/blob/v1.0.2/x/network/connstring/connstring.go)
for security, etc.

For Kafka you can provide a comma separated list of host:port,host:port... for the bootstrap servers.

### configure what gets read and sent from MongoDB to Kafka

By default route81 will open a change stream against the entire MongoDB deployment and send change data as JSON to
Kafka.  You can target specific databases or collections in MongoDB.

```
route81 -change-stream-namespace mydb -change-stream-namespace anotherdb.mycol
```

These can be put into the config file as 

```
change-stream-namespaces = [ "mydb", "anotherdb.mycol" ]
```

### sending entire collections or views to Kafka

In addition to watching and sending changes in MongoDB to Kafka, you can send entire collections or views

```
direct-read-namespaces = [ "mydb.mycol", "mydb.myview" ]
```

### save and resume your progress across restarts

Turn on resume mode to save timestamps in MongoDB periodically of data that has been sent to Kafka.  When started
in this mode route81 will resume progress at the last save point. This requires route81 to be able to save documents
in the collection `route81.resume` in MongoDB.

```
route81 -resume
```

### configure advanced kafka producer settings

For the advanced kafka settings you will need a config file.  For example,

```
[kafa-settings]
enable-idempotence = true
request-timeout-ms = 10000
message-timeout-ms = 10000
message-max-retries = 100
retry-backoff-ms = 50
```

If you have build librdkafka with SSL support, you can also add

```
[kafa-settings]
security-protocol = "ssl"
ssl-ca-location = "ca-cert"
ssl-certificate-location = "client_?????_client.pem"
ssl-key-location = "client_?????_client.key"
ssl-key-password = "abcdefgh"
```

```
route81 -f /path/to/above.toml
```
