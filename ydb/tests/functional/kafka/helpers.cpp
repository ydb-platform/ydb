#include "helpers.h"

#include <util/stream/output.h>

namespace NKafkaRdkafkaTests {

namespace {

void SetConf(RdKafka::Conf* conf, const TString& key, const TString& value) {
    std::string err;
    AssertConfOk(conf->set(std::string(key), std::string(value), err), err);
}

void ApplyExtra(RdKafka::Conf* conf, const THashMap<TString, TString>& extra) {
    for (const auto& [key, value] : extra) {
        SetConf(conf, key, value);
    }
}

std::unique_ptr<RdKafka::Conf> MakeGlobalConf(const THashMap<TString, TString>& extra) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    UNIT_ASSERT(conf);
    SetConf(conf.get(), "bootstrap.servers", BootstrapServers());
    SetConf(conf.get(), "client.id", "ydb-kafka-func-test");
    SetConf(conf.get(), "socket.timeout.ms", "15000");
    SetConf(conf.get(), "allow.auto.create.topics", "false");
    ApplyExtra(conf.get(), extra);
    return conf;
}

void ApplySasl(THashMap<TString, TString>& extra) {
    extra["security.protocol"] = "SASL_PLAINTEXT";
    extra["sasl.mechanisms"] = "PLAIN";
    extra["sasl.username"] = "root@" + DatabasePath();
    extra["sasl.password"] = "1234";
}

} // namespace

void TDeliveryReport::dr_cb(RdKafka::Message& message) {
    if (message.err() == RdKafka::ERR_NO_ERROR) {
        ++Ok;
        return;
    }
    ++Fail;
    LastError = TString(message.errstr());
}

void TLogEventCb::event_cb(RdKafka::Event& event) {
    if (event.type() == RdKafka::Event::EVENT_ERROR) {
        Cerr << "librdkafka error: " << RdKafka::err2str(event.err()) << " " << event.str() << Endl;
        return;
    }
    if (event.type() == RdKafka::Event::EVENT_LOG && event.severity() <= RdKafka::Event::EVENT_SEVERITY_WARNING) {
        Cerr << "librdkafka log: " << event.str() << Endl;
    }
}

TConsumer::~TConsumer() {
    if (Handle) {
        Handle->close();
    }
}

TString BootstrapServers() {
    const TString port = GetEnv("YDB_KAFKA_PROXY_PORT");
    UNIT_ASSERT_C(port, "YDB_KAFKA_PROXY_PORT is not set");
    return "localhost:" + port;
}

TString DatabasePath() {
    TString database = GetEnv("YDB_DATABASE");
    UNIT_ASSERT_C(database, "YDB_DATABASE is not set");
    if (database.empty() || database[0] != '/') {
        database.prepend('/');
    }
    return database;
}

TString UniqueName(TStringBuf prefix) {
    static std::atomic<ui64> seq{0};
    return TStringBuilder() << prefix << "-" << TInstant::Now().MicroSeconds() << "-" << seq.fetch_add(1);
}

NYdb::TDriver MakeYdbDriver() {
    TString connectionString = GetEnv("YDB_CONNECTION_STRING");
    if (!connectionString) {
        connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
    }
    return NYdb::TDriver(NYdb::TDriverConfig(connectionString));
}

void CreateYdbTopic(const TString& name, ui32 partitions) {
    auto driver = MakeYdbDriver();
    NYdb::NTopic::TTopicClient client(driver);
    auto settings = NYdb::NTopic::TCreateTopicSettings()
        .PartitioningSettings(partitions, partitions);
    auto result = client.CreateTopic(std::string(name), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

std::unique_ptr<TProducer> MakeProducer(const THashMap<TString, TString>& extra) {
    auto producer = std::make_unique<TProducer>();
    auto conf = MakeGlobalConf(extra);
    std::string err;
    AssertConfOk(conf->set("dr_cb", &producer->Dr, err), err);
    AssertConfOk(conf->set("event_cb", &producer->Log, err), err);
    producer->Handle.reset(RdKafka::Producer::create(conf.get(), err));
    UNIT_ASSERT_C(producer->Handle, err);
    return producer;
}

std::unique_ptr<TConsumer> MakeConsumer(const TString& groupId, const THashMap<TString, TString>& extra) {
    THashMap<TString, TString> confValues = extra;
    confValues["group.id"] = groupId;
    if (!confValues.contains("enable.auto.commit")) {
        confValues["enable.auto.commit"] = "false";
    }
    if (!confValues.contains("auto.offset.reset")) {
        confValues["auto.offset.reset"] = "earliest";
    }
    if (!confValues.contains("check.crcs")) {
        confValues["check.crcs"] = "false";
    }
    if (!confValues.contains("partition.assignment.strategy")) {
        confValues["partition.assignment.strategy"] = "roundrobin";
    }
    if (!confValues.contains("session.timeout.ms")) {
        confValues["session.timeout.ms"] = "10000";
    }
    if (!confValues.contains("heartbeat.interval.ms")) {
        confValues["heartbeat.interval.ms"] = "2000";
    }
    if (!confValues.contains("enable.partition.eof")) {
        confValues["enable.partition.eof"] = "false";
    }

    auto consumer = std::make_unique<TConsumer>();
    auto conf = MakeGlobalConf(confValues);
    std::string err;
    AssertConfOk(conf->set("event_cb", &consumer->Log, err), err);
    consumer->Handle.reset(RdKafka::KafkaConsumer::create(conf.get(), err));
    UNIT_ASSERT_C(consumer->Handle, err);
    return consumer;
}

std::unique_ptr<TProducer> MakeSaslProducer(const THashMap<TString, TString>& extra) {
    THashMap<TString, TString> confValues = extra;
    ApplySasl(confValues);
    return MakeProducer(confValues);
}

std::unique_ptr<TConsumer> MakeSaslConsumer(const TString& groupId, const THashMap<TString, TString>& extra) {
    THashMap<TString, TString> confValues = extra;
    ApplySasl(confValues);
    return MakeConsumer(groupId, confValues);
}

void Produce(
    RdKafka::Producer& producer,
    const TString& topic,
    const TString& payload,
    const TString& key,
    int32_t partition)
{
    const void* keyPtr = key.empty() ? nullptr : static_cast<const void*>(key.data());
    RdKafka::ErrorCode err = RdKafka::ERR_NO_ERROR;
    for (int attempt = 0; attempt < 30; ++attempt) {
        err = producer.produce(
            std::string(topic),
            partition,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(payload.data()),
            payload.size(),
            keyPtr,
            key.size(),
            /*timestamp*/ 0,
            /*msg_opaque*/ nullptr);
        if (err == RdKafka::ERR_NO_ERROR) {
            return;
        }
        if (err == RdKafka::ERR__QUEUE_FULL
            || err == RdKafka::ERR__UNKNOWN_TOPIC
            || err == RdKafka::ERR__UNKNOWN_PARTITION)
        {
            producer.poll(200);
            Sleep(TDuration::MilliSeconds(200));
            continue;
        }
        UNIT_FAIL("produce failed: " << RdKafka::err2str(err));
    }
    UNIT_FAIL("produce failed after retries: " << RdKafka::err2str(err));
}

void Flush(TProducer& producer) {
    AssertRdKafkaOk(producer.Handle->flush(30000), "flush");
    UNIT_ASSERT_VALUES_EQUAL_C(producer.Dr.Fail.load(), 0, producer.Dr.LastError);
    UNIT_ASSERT_GT(producer.Dr.Ok.load(), 0);
}

void ProduceAndFlush(
    TProducer& producer,
    const TString& topic,
    const TVector<TString>& payloads,
    const TVector<TString>& keys)
{
    UNIT_ASSERT(!payloads.empty());
    UNIT_ASSERT(keys.empty() || keys.size() == payloads.size());
    for (size_t i = 0; i < payloads.size(); ++i) {
        Produce(*producer.Handle, topic, payloads[i], keys.empty() ? TString() : keys[i]);
    }
    Flush(producer);
}

void WaitTopicPartitions(RdKafka::Handle& handle, const TString& topic, size_t partitions, TDuration timeout) {
    const TInstant deadline = TInstant::Now() + timeout;
    TString lastError = "no metadata";
    while (TInstant::Now() < deadline) {
        RdKafka::Metadata* metadata = nullptr;
        const auto err = handle.metadata(true, nullptr, &metadata, 5000);
        std::unique_ptr<RdKafka::Metadata> holder(metadata);
        if (err != RdKafka::ERR_NO_ERROR) {
            lastError = RdKafka::err2str(err);
            Sleep(TDuration::MilliSeconds(200));
            continue;
        }
        UNIT_ASSERT(metadata);
        UNIT_ASSERT(metadata->brokers());
        UNIT_ASSERT(!metadata->brokers()->empty());
        for (const auto* topicMeta : *metadata->topics()) {
            if (topicMeta->topic() == std::string(topic)
                && topicMeta->err() == RdKafka::ERR_NO_ERROR
                && topicMeta->partitions()->size() == partitions)
            {
                return;
            }
            if (topicMeta->topic() == std::string(topic)) {
                lastError = TStringBuilder()
                    << "topic err=" << RdKafka::err2str(topicMeta->err())
                    << " partitions=" << topicMeta->partitions()->size();
            }
        }
        Sleep(TDuration::MilliSeconds(200));
    }
    UNIT_FAIL("topic " << topic << " is not visible with " << partitions << " partitions: " << lastError);
}

TString MessagePayload(const RdKafka::Message& message) {
    if (!message.payload()) {
        return {};
    }
    return TString(static_cast<const char*>(message.payload()), message.len());
}

TString MessageKey(const RdKafka::Message& message) {
    if (!message.key_pointer()) {
        return {};
    }
    return TString(static_cast<const char*>(message.key_pointer()), message.key_len());
}

TVector<TString> ConsumePayloads(RdKafka::KafkaConsumer& consumer, size_t count, TDuration timeout) {
    TVector<TString> payloads;
    const TInstant deadline = TInstant::Now() + timeout;
    while (payloads.size() < count && TInstant::Now() < deadline) {
        std::unique_ptr<RdKafka::Message> message(consumer.consume(500));
        UNIT_ASSERT(message);
        if (message->err() == RdKafka::ERR_NO_ERROR) {
            payloads.push_back(MessagePayload(*message));
            continue;
        }
        if (message->err() == RdKafka::ERR__TIMED_OUT) {
            continue;
        }
        UNIT_FAIL("consume failed: " << RdKafka::err2str(message->err()) << " " << message->errstr());
    }
    UNIT_ASSERT_VALUES_EQUAL_C(payloads.size(), count, "timed out waiting for messages");
    return payloads;
}

void ConsumeUntilEmpty(RdKafka::KafkaConsumer& consumer, TDuration timeout) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        std::unique_ptr<RdKafka::Message> message(consumer.consume(500));
        UNIT_ASSERT(message);
        if (message->err() == RdKafka::ERR__TIMED_OUT) {
            continue;
        }
        UNIT_FAIL("unexpected consume result on empty topic: "
            << RdKafka::err2str(message->err()) << " payload=" << MessagePayload(*message));
    }
}

TVector<int32_t> AssignmentPartitions(RdKafka::KafkaConsumer& consumer) {
    std::vector<RdKafka::TopicPartition*> assignment;
    AssertRdKafkaOk(consumer.assignment(assignment), "assignment");
    TVector<int32_t> partitions;
    partitions.reserve(assignment.size());
    for (auto* tp : assignment) {
        partitions.push_back(tp->partition());
    }
    RdKafka::TopicPartition::destroy(assignment);
    return partitions;
}

void WaitAssignment(RdKafka::KafkaConsumer& consumer, size_t minPartitions, TDuration timeout) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        delete consumer.consume(500);
        if (AssignmentPartitions(consumer).size() >= minPartitions) {
            return;
        }
    }
    UNIT_FAIL("timed out waiting for assignment of " << minPartitions << " partitions");
}

void WaitBalanced(
    RdKafka::KafkaConsumer& first,
    RdKafka::KafkaConsumer& second,
    size_t totalPartitions,
    TDuration timeout)
{
    const TInstant deadline = TInstant::Now() + timeout;
    TString last;
    while (TInstant::Now() < deadline) {
        delete first.consume(200);
        delete second.consume(200);
        const auto firstParts = AssignmentPartitions(first);
        const auto secondParts = AssignmentPartitions(second);
        THashSet<int32_t> all(firstParts.begin(), firstParts.end());
        bool disjoint = true;
        for (int32_t partition : secondParts) {
            if (!all.insert(partition).second) {
                disjoint = false;
            }
        }
        last = TStringBuilder()
            << "first=" << firstParts.size()
            << " second=" << secondParts.size()
            << " unique=" << all.size()
            << " disjoint=" << disjoint;
        if (disjoint && all.size() == totalPartitions && !firstParts.empty() && !secondParts.empty()) {
            return;
        }
    }
    UNIT_FAIL("timed out waiting for balanced assignment: " << last);
}

void AssertTxnOk(RdKafka::Error* error, const TString& what) {
    if (!error) {
        return;
    }
    const TString message = TStringBuilder() << what << ": " << error->str() << " (" << error->name() << ")";
    delete error;
    UNIT_FAIL(message);
}

TRdEvent WaitAdminEvent(rd_kafka_queue_t* queue, int timeoutMs) {
    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, timeoutMs);
    UNIT_ASSERT_C(event, "admin request timed out");
    TRdEvent holder(event);
    if (rd_kafka_event_error(event)) {
        UNIT_FAIL(rd_kafka_event_error_string(event));
    }
    return holder;
}

void CreateKafkaTopic(RdKafka::Handle& handle, const TString& topic, int partitions) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    char errstr[512];
    rd_kafka_NewTopic_t* newTopic = rd_kafka_NewTopic_new(topic.c_str(), partitions, 1, errstr, sizeof(errstr));
    UNIT_ASSERT_C(newTopic, errstr);
    rd_kafka_CreateTopics(rk, &newTopic, 1, nullptr, queue.get());
    rd_kafka_NewTopic_destroy(newTopic);
    auto event = WaitAdminEvent(queue.get());
    const rd_kafka_CreateTopics_result_t* result = rd_kafka_event_CreateTopics_result(event.get());
    UNIT_ASSERT(result);
    size_t count = 0;
    const rd_kafka_topic_result_t** topics = rd_kafka_CreateTopics_result_topics(result, &count);
    UNIT_ASSERT_VALUES_EQUAL(count, 1);
    AssertCOk(rd_kafka_topic_result_error(topics[0]), rd_kafka_topic_result_error_string(topics[0]));
    WaitTopicPartitions(handle, topic, static_cast<size_t>(partitions));
}

void CreateKafkaPartitions(RdKafka::Handle& handle, const TString& topic, size_t totalPartitions) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    char errstr[512];
    rd_kafka_NewPartitions_t* newParts = rd_kafka_NewPartitions_new(topic.c_str(), totalPartitions, errstr, sizeof(errstr));
    UNIT_ASSERT_C(newParts, errstr);
    rd_kafka_CreatePartitions(rk, &newParts, 1, nullptr, queue.get());
    rd_kafka_NewPartitions_destroy(newParts);
    auto event = WaitAdminEvent(queue.get());
    const rd_kafka_CreatePartitions_result_t* result = rd_kafka_event_CreatePartitions_result(event.get());
    UNIT_ASSERT(result);
    size_t count = 0;
    const rd_kafka_topic_result_t** topics = rd_kafka_CreatePartitions_result_topics(result, &count);
    UNIT_ASSERT_VALUES_EQUAL(count, 1);
    AssertCOk(rd_kafka_topic_result_error(topics[0]), rd_kafka_topic_result_error_string(topics[0]));
    WaitTopicPartitions(handle, topic, totalPartitions);
}

THashMap<TString, TString> DescribeTopicConfigs(RdKafka::Handle& handle, const TString& topic) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    rd_kafka_ConfigResource_t* resource = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, topic.c_str());
    UNIT_ASSERT(resource);
    rd_kafka_DescribeConfigs(rk, &resource, 1, nullptr, queue.get());
    rd_kafka_ConfigResource_destroy(resource);
    auto event = WaitAdminEvent(queue.get());
    const rd_kafka_DescribeConfigs_result_t* result = rd_kafka_event_DescribeConfigs_result(event.get());
    UNIT_ASSERT(result);
    size_t count = 0;
    const rd_kafka_ConfigResource_t** resources = rd_kafka_DescribeConfigs_result_resources(result, &count);
    UNIT_ASSERT_VALUES_EQUAL(count, 1);
    AssertCOk(rd_kafka_ConfigResource_error(resources[0]), rd_kafka_ConfigResource_error_string(resources[0]));
    size_t entryCount = 0;
    const rd_kafka_ConfigEntry_t** entries = rd_kafka_ConfigResource_configs(resources[0], &entryCount);
    THashMap<TString, TString> configs;
    for (size_t i = 0; i < entryCount; ++i) {
        const char* name = rd_kafka_ConfigEntry_name(entries[i]);
        const char* value = rd_kafka_ConfigEntry_value(entries[i]);
        configs[name ? TString(name) : TString()] = value ? TString(value) : TString();
    }
    return configs;
}

TVector<TString> ListConsumerGroups(RdKafka::Handle& handle) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    rd_kafka_ListConsumerGroups(rk, nullptr, queue.get());
    auto event = WaitAdminEvent(queue.get());
    const rd_kafka_ListConsumerGroups_result_t* result = rd_kafka_event_ListConsumerGroups_result(event.get());
    UNIT_ASSERT(result);
    size_t count = 0;
    const rd_kafka_ConsumerGroupListing_t** groups = rd_kafka_ListConsumerGroups_result_valid(result, &count);
    TVector<TString> names;
    names.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        names.emplace_back(rd_kafka_ConsumerGroupListing_group_id(groups[i]));
    }
    return names;
}

void DescribeConsumerGroup(RdKafka::Handle& handle, const TString& groupId) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    const char* groups[] = {groupId.c_str()};
    rd_kafka_DescribeConsumerGroups(rk, groups, 1, nullptr, queue.get());
    auto event = WaitAdminEvent(queue.get());
    const rd_kafka_DescribeConsumerGroups_result_t* result = rd_kafka_event_DescribeConsumerGroups_result(event.get());
    UNIT_ASSERT(result);
    size_t count = 0;
    const rd_kafka_ConsumerGroupDescription_t** descriptions = rd_kafka_DescribeConsumerGroups_result_groups(result, &count);
    UNIT_ASSERT_VALUES_EQUAL(count, 1);
    UNIT_ASSERT_VALUES_EQUAL(rd_kafka_ConsumerGroupDescription_group_id(descriptions[0]), groupId);
    const rd_kafka_error_t* error = rd_kafka_ConsumerGroupDescription_error(descriptions[0]);
    if (error && rd_kafka_error_code(error) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        UNIT_FAIL(rd_kafka_error_string(error));
    }
}

} // namespace NKafkaRdkafkaTests
