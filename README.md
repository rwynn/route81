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

#### run a release version of route81

The easiest way to run route81 is via docker.

```
docker run --rm --net=host rwynn/route81:latest
```

#### build yourself and run route81 (optional)

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
docker run --rm --net=host rwynn/route81:1.0.2
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

### example messages

#### insert operation

When you insert a document into MongoDB

```
rs1:PRIMARY> db.test.insert({foo: 1});
```

You can expect a message like the following in Kafka in topic `test.test`

```json
{"meta":{"_id":"5ced395f6ad5d6d2233c6dc4","ts":{"T":1559050591,"I":2},"ns":"test.test","op":"i"},"data":{"_id":"5ced395f6ad5d6d2233c6dc4","foo":1}}
```

#### update operation

When you update a document into MongoDB

```
rs1:PRIMARY> db.test.update({}, {$unset: {foo:1}, $set: {bar:1}}, {multi:true});
```

You can expect a message like the following in Kafka in topic `test.test`

```json
{"meta":{"_id":"5ced395f6ad5d6d2233c6dc4","ts":{"T":1559050762,"I":1},"ns":"test.test","op":"u","updates":{"changed":{"bar":1},"removed":["foo"]}},"data":{"_id":"5ced395f6ad5d6d2233c6dc4","bar":1}}
```

#### remove operation

When you remove a document into MongoDB

```
rs1:PRIMARY> db.test.remove({});
```

You can expect a message like the following in Kafka in topic `test.test`

```json
{"meta":{"_id":"5ced395f6ad5d6d2233c6dc4","ts":{"T":1559050865,"I":1},"ns":"test.test","op":"d"}}
```

#### GridFS operation

When you upload a file using GridFS

```
$ echo 'hello world' > test.txt
$ base64 test.txt
aGVsbG8gd29ybGQK
$ mongofiles -d test put test.txt
2019-05-28T13:52:30.807+0000    connected to: localhost
2019-05-28T13:52:30.807+0000    added file: test.txt
```

You can expect 1 message Kafka for each file in topic `test.fs.files`

```json
{"meta":{"_id":"5ced3d1e6362390fb7743860","ts":{"T":1559051550,"I":4},"ns":"test.fs.files","op":"i"},"data":{"_id":"5ced3d1e6362390fb7743860","chunkSize":261120,"filename":"test.txt","length":12,"md5":"6f5902ac237024bdd0c176cb93063dc4","uploadDate":"2019-05-28T13:52:30.832Z"}}
```

You can also expect 1 or more messages in Kafka for the chunks in each file in topic `test.fs.chunks`

```json
{"meta":{"_id":"5ced3d1e6362390fb7743861","ts":{"T":1559051550,"I":2},"ns":"test.fs.chunks","op":"i"},"data":{"_id":"5ced3d1e6362390fb7743861","data":{"Subtype":0,"Data":"aGVsbG8gd29ybGQK"},"files_id":"5ced3d1e6362390fb7743860","n":0}}
```

Notice that the chunk data is sent to Kafka base64 encoded. Since only 1 chunk was sent the value matches the input base64.

The chunk parent file `_id` is captured in the field `files_id`.

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

### pipelines

You can run aggregation pipelines on your change stream or direct reads.  Aggregation pipelines let you
filter or modify the data that is sent to Kafka.  Pipelines are configured in your TOML file.  For example,
the following config sets up an aggregation pipeline on the `test.test` collection change streams such that
only insert events are sent to Kafka.

```toml
change-stream-namespaces = ["test.test"]

[[pipeline]]
namespace = "test.test"
direct = false
stages = """
[ { "$match" : { "operationType": "insert" } } ]
"""
```

You can have up to 2 pipelines per MongoDB namespace.  If `direct` is false the pipeline will be applied to 
the change stream on the namespace.  If `direct` is true the pipeline with be applied to direct reads of the
namespace.

The stages you configure will differ between direct and indirect.  Direct stages will be run directly
against the docs from the MongoDB namespace.  Indirect stages will be run against the stream of change events
for the namespace.  The format of a stream of change events are documented
[here](https://docs.mongodb.com/manual/reference/change-events/#change-stream-output).

By default, route81 runs with `change-stream-namespaces` set to empty string.  This means that all changes from
your MongoDB deployment will be sent to Kafka.  However, when using pipelines you will want to override
the default behavior and enumerate the list of `change-stream-namespaces` in the config as shown above.
This allows you to assign a pipeline to individual collections.

All change streams are run the lookup option set. This means that for inserts and update you can rely on the
`fullDocument` field to be present in the change event.

If you want to run a pipeline over all change events then you would configure as follows:

```toml
[[pipeline]]
stages = """
[ { "$match" : { "operationType": "insert" } } ]
"""
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

### confluent cloud

Auto-creation of topics is disabled in Confluent Cloud.  You need to pre-create topics for each MongoDB
namespace that route81 sends messages for.

```
$ ccloud topic create mydb.mycol
```

If you set a `topic-prefix`. e.g. `route81` then you would need to include that prefix in each topic name

```
$ ccloud topic create route81.mydb.mycol
```

You also need to provide kafka settings specific to your Confluent Cloud account.

```
[kafa-settings]
broker-version-fallback = "0.10.0.0"
api-version-fallback = true
api-version-fallback-ms = 0
sasl-mechanisms = "PLAIN"
security-protocol = "SASL_SSL"
sasl-username = "<ccloud key>"
sasl-password = "<ccloud secret>"
```
