#include "helpers.h"

#include <cstring>

#include <util/network/sock.h>
#include <util/network/socket.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/byteorder.h>
#include <util/system/error.h>

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

void ApplySaslDefaults(THashMap<TString, TString>& extra) {
    if (!extra.contains("security.protocol")) {
        extra["security.protocol"] = "SASL_PLAINTEXT";
    }
    if (!extra.contains("sasl.mechanisms")) {
        extra["sasl.mechanisms"] = "PLAIN";
    }
    if (!extra.contains("sasl.username")) {
        extra["sasl.username"] = "root@" + DatabasePath();
    }
    if (!extra.contains("sasl.password")) {
        extra["sasl.password"] = "1234";
    }
}

void PutI16(TString& buf, i16 value) {
    const ui16 net = HostToInet(static_cast<ui16>(value));
    buf.append(reinterpret_cast<const char*>(&net), sizeof(net));
}

void PutI32(TString& buf, i32 value) {
    const ui32 net = HostToInet(static_cast<ui32>(value));
    buf.append(reinterpret_cast<const char*>(&net), sizeof(net));
}

i16 ReadI16(const char*& ptr, const char* end) {
    UNIT_ASSERT_C(ptr + 2 <= end, "truncated int16");
    ui16 net = 0;
    memcpy(&net, ptr, 2);
    ptr += 2;
    return static_cast<i16>(InetToHost(net));
}

i32 ReadI32(const char*& ptr, const char* end) {
    UNIT_ASSERT_C(ptr + 4 <= end, "truncated int32");
    ui32 net = 0;
    memcpy(&net, ptr, 4);
    ptr += 4;
    return static_cast<i32>(InetToHost(net));
}

void RecvExact(TInetStreamSocket& socket, void* buf, size_t size) {
    char* ptr = static_cast<char*>(buf);
    size_t got = 0;
    while (got < size) {
        const ssize_t n = socket.Recv(ptr + got, size - got);
        UNIT_ASSERT_C(n > 0, "kafka socket closed while reading");
        got += static_cast<size_t>(n);
    }
}

bool RecvExactAllowClose(TInetStreamSocket& socket, void* buf, size_t size) {
    char* ptr = static_cast<char*>(buf);
    size_t got = 0;
    while (got < size) {
        const ssize_t n = socket.Recv(ptr + got, size - got);
        if (n <= 0) {
            return false;
        }
        got += static_cast<size_t>(n);
    }
    return true;
}

void SendAll(TInetStreamSocket& socket, const TString& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t n = socket.Send(data.data() + sent, data.size() - sent);
        UNIT_ASSERT_C(n > 0, "kafka socket send failed");
        sent += static_cast<size_t>(n);
    }
}

TString EncodeApiVersionsRequest(i16 apiVersion, i32 correlationId) {
    TString body;
    PutI16(body, KafkaApiApiVersions);
    PutI16(body, apiVersion);
    PutI32(body, correlationId);
    PutI16(body, 0); // empty client id
    TString framed;
    PutI32(framed, static_cast<i32>(body.size()));
    framed += body;
    return framed;
}

TApiVersionsReply ParseApiVersionsBody(const TString& payload) {
    UNIT_ASSERT_C(payload.size() >= 4, "api versions response too short");
    const char* ptr = payload.data();
    const char* end = payload.data() + payload.size();
    const i32 correlation = ReadI32(ptr, end);
    Y_UNUSED(correlation);
    TApiVersionsReply reply;
    reply.ErrorCode = ReadI16(ptr, end);
    const i32 count = ReadI32(ptr, end);
    UNIT_ASSERT_GE(count, 0);
    reply.ApiKeys.reserve(count);
    for (i32 i = 0; i < count; ++i) {
        TApiVersionInfo key;
        key.ApiKey = ReadI16(ptr, end);
        key.MinVersion = ReadI16(ptr, end);
        key.MaxVersion = ReadI16(ptr, end);
        reply.ApiKeys.push_back(key);
    }
    return reply;
}

TApiVersionsReply RecvApiVersions(TInetStreamSocket& socket) {
    char sizeBuf[4];
    RecvExact(socket, sizeBuf, 4);
    const char* sizePtr = sizeBuf;
    const i32 size = ReadI32(sizePtr, sizeBuf + 4);
    UNIT_ASSERT_GT(size, 0);
    UNIT_ASSERT_LT(size, 1 << 20);
    TString payload;
    payload.resize(size);
    RecvExact(socket, payload.Detach(), size);
    return ParseApiVersionsBody(payload);
}

SOCKET ConnectKafkaFd() {
    TInetStreamSocket socket;
    UNIT_ASSERT_C(SOCKET(socket) != INVALID_SOCKET, "failed to create kafka socket");
    SetSocketTimeout(SOCKET(socket), 15);
    TSockAddrInet addr("127.0.0.1", KafkaProxyPort());
    const int rc = socket.Connect(&addr);
    UNIT_ASSERT_C(rc == 0, "failed to connect kafka proxy: " << LastSystemErrorText(-rc));
    return socket.Release();
}

TApiVersionsReply ExchangeApiVersions(TInetStreamSocket& socket, i16 apiVersion, i32 correlationId) {
    SendAll(socket, EncodeApiVersionsRequest(apiVersion, correlationId));
    return RecvApiVersions(socket);
}

} // namespace

void TDeliveryReport::dr_cb(RdKafka::Message& message) {
    if (message.err() == RdKafka::ERR_NO_ERROR) {
        ++Ok;
        return;
    }
    ++Fail;
    LastErr = message.err();
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

ui16 KafkaProxyPort() {
    return FromString<ui16>(GetEnv("YDB_KAFKA_PROXY_PORT"));
}

TString DatabasePath() {
    TString database = GetEnv("YDB_DATABASE");
    UNIT_ASSERT_C(database, "YDB_DATABASE is not set");
    if (database.empty() || database[0] != '/') {
        database.prepend('/');
    }
    return database;
}

TString TopicFullPath(const TString& name) {
    if (name.StartsWith('/')) {
        return name;
    }
    return DatabasePath() + "/" + name;
}

TString UniqueName(TStringBuf prefix) {
    static std::atomic<ui64> seq{0};
    return TStringBuilder() << prefix << "-" << TInstant::Now().MicroSeconds() << "-" << seq.fetch_add(1);
}

bool TopicMessagesBatchingEnabled() {
    return GetEnv("YDB_FEATURE_FLAGS").Contains("enable_topic_messages_batching");
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

void CreateAutopartitionedYdbTopic(const TString& name) {
    auto driver = MakeYdbDriver();
    NYdb::NTopic::TTopicClient client(driver);
    auto settings = NYdb::NTopic::TCreateTopicSettings();
    settings.BeginConfigurePartitioningSettings()
        .MinActivePartitions(1)
        .MaxActivePartitions(100)
        .BeginConfigureAutoPartitioningSettings()
        .Strategy(NYdb::NTopic::EAutoPartitioningStrategy::ScaleUp)
        .StabilizationWindow(TDuration::Seconds(30))
        .EndConfigureAutoPartitioningSettings()
        .EndConfigurePartitioningSettings();
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
    ApplySaslDefaults(confValues);
    return MakeProducer(confValues);
}

std::unique_ptr<TConsumer> MakeSaslConsumer(const TString& groupId, const THashMap<TString, TString>& extra) {
    THashMap<TString, TString> confValues = extra;
    ApplySaslDefaults(confValues);
    return MakeConsumer(groupId, confValues);
}

void Produce(
    RdKafka::Producer& producer,
    const TString& topic,
    const TString& payload,
    const TString& key,
    int32_t partition,
    int64_t timestamp)
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
            timestamp,
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

void ProduceWithHeaders(
    RdKafka::Producer& producer,
    const TString& topic,
    const TString& payload,
    const TString& key,
    THashMap<TString, TString> headers,
    int32_t partition,
    int64_t timestamp)
{
    std::unique_ptr<RdKafka::Headers> hdrs(RdKafka::Headers::create());
    UNIT_ASSERT(hdrs);
    for (const auto& [name, value] : headers) {
        AssertRdKafkaOk(hdrs->add(std::string(name), std::string(value)), "header add");
    }
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
            timestamp,
            hdrs.get(),
            /*msg_opaque*/ nullptr);
        if (err == RdKafka::ERR_NO_ERROR) {
            hdrs.release();
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
        UNIT_FAIL("produce with headers failed: " << RdKafka::err2str(err));
    }
    UNIT_FAIL("produce with headers failed after retries: " << RdKafka::err2str(err));
}

RdKafka::ErrorCode ProduceOnce(
    RdKafka::Producer& producer,
    const TString& topic,
    const TString& payload,
    const TString& key,
    int32_t partition,
    bool nullKey)
{
    const void* actualKey = nullptr;
    size_t keyLen = 0;
    if (!nullKey) {
        actualKey = key.empty() ? static_cast<const void*>("") : static_cast<const void*>(key.data());
        keyLen = key.size();
    }
    return producer.produce(
        std::string(topic),
        partition,
        RdKafka::Producer::RK_MSG_COPY,
        payload.empty() ? nullptr : const_cast<char*>(payload.data()),
        payload.size(),
        actualKey,
        keyLen,
        /*timestamp*/ 0,
        /*msg_opaque*/ nullptr);
}

void Flush(TProducer& producer) {
    AssertRdKafkaOk(producer.Handle->flush(30000), "flush");
    UNIT_ASSERT_VALUES_EQUAL_C(producer.Dr.Fail.load(), 0, producer.Dr.LastError);
    UNIT_ASSERT_GT(producer.Dr.Ok.load(), 0);
}

void FlushAllowErrors(TProducer& producer, int timeoutMs) {
    producer.Handle->flush(timeoutMs);
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

bool TopicVisible(RdKafka::Handle& handle, const TString& topic) {
    RdKafka::Metadata* metadata = nullptr;
    const auto err = handle.metadata(true, nullptr, &metadata, 5000);
    std::unique_ptr<RdKafka::Metadata> holder(metadata);
    if (err != RdKafka::ERR_NO_ERROR || !metadata) {
        return false;
    }
    for (const auto* topicMeta : *metadata->topics()) {
        if (topicMeta->topic() == std::string(topic) && topicMeta->err() == RdKafka::ERR_NO_ERROR) {
            return true;
        }
    }
    return false;
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

TConsumedMessage MakeConsumed(RdKafka::Message& message) {
    TConsumedMessage consumed;
    consumed.Topic = TString(message.topic_name());
    consumed.Partition = message.partition();
    consumed.Offset = message.offset();
    consumed.Key = MessageKey(message);
    consumed.Payload = MessagePayload(message);
    consumed.Timestamp = message.timestamp().timestamp;
    if (RdKafka::Headers* headers = message.headers()) {
        for (const auto& header : headers->get_all()) {
            if (header.err() != RdKafka::ERR_NO_ERROR || !header.value()) {
                continue;
            }
            consumed.Headers[TString(header.key())] = TString(
                static_cast<const char*>(header.value()),
                header.value_size());
        }
    }
    return consumed;
}

TVector<TString> ConsumePayloads(RdKafka::KafkaConsumer& consumer, size_t count, TDuration timeout) {
    TVector<TString> payloads;
    for (const auto& message : ConsumeMessages(consumer, count, timeout)) {
        payloads.push_back(message.Payload);
    }
    return payloads;
}

TVector<TConsumedMessage> ConsumeMessages(RdKafka::KafkaConsumer& consumer, size_t count, TDuration timeout) {
    TVector<TConsumedMessage> messages;
    const TInstant deadline = TInstant::Now() + timeout;
    while (messages.size() < count && TInstant::Now() < deadline) {
        std::unique_ptr<RdKafka::Message> message(consumer.consume(500));
        UNIT_ASSERT(message);
        if (message->err() == RdKafka::ERR_NO_ERROR) {
            messages.push_back(MakeConsumed(*message));
            continue;
        }
        if (message->err() == RdKafka::ERR__TIMED_OUT) {
            continue;
        }
        UNIT_FAIL("consume failed: " << RdKafka::err2str(message->err()) << " " << message->errstr());
    }
    UNIT_ASSERT_VALUES_EQUAL_C(messages.size(), count, "timed out waiting for messages");
    return messages;
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

TVector<TString> AssignmentKeys(RdKafka::KafkaConsumer& consumer) {
    std::vector<RdKafka::TopicPartition*> assignment;
    AssertRdKafkaOk(consumer.assignment(assignment), "assignment");
    TVector<TString> keys;
    keys.reserve(assignment.size());
    for (auto* tp : assignment) {
        keys.push_back(TStringBuilder() << tp->topic() << ":" << tp->partition());
    }
    RdKafka::TopicPartition::destroy(assignment);
    return keys;
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
    WaitGroupCovers({&first, &second}, totalPartitions, timeout);
}

void WaitGroupCovers(
    const TVector<RdKafka::KafkaConsumer*>& consumers,
    size_t totalPartitions,
    TDuration timeout)
{
    UNIT_ASSERT(!consumers.empty());
    const TInstant deadline = TInstant::Now() + timeout;
    TString last;
    while (TInstant::Now() < deadline) {
        THashSet<TString> all;
        bool disjoint = true;
        bool allNonEmpty = true;
        for (auto* consumer : consumers) {
            delete consumer->consume(200);
            const auto keys = AssignmentKeys(*consumer);
            if (keys.empty()) {
                allNonEmpty = false;
            }
            for (const auto& key : keys) {
                if (!all.insert(key).second) {
                    disjoint = false;
                }
            }
        }
        last = TStringBuilder()
            << "unique=" << all.size()
            << " disjoint=" << disjoint
            << " allNonEmpty=" << allNonEmpty;
        if (disjoint && all.size() == totalPartitions && allNonEmpty) {
            return;
        }
    }
    UNIT_FAIL("timed out waiting for group assignment: " << last);
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
    auto holder = WaitAdminEventRaw(queue, timeoutMs);
    if (rd_kafka_event_error(holder.get())) {
        UNIT_FAIL(rd_kafka_event_error_string(holder.get()));
    }
    return holder;
}

TRdEvent WaitAdminEventRaw(rd_kafka_queue_t* queue, int timeoutMs) {
    rd_kafka_event_t* event = rd_kafka_queue_poll(queue, timeoutMs);
    UNIT_ASSERT_C(event, "admin request timed out");
    return TRdEvent(event);
}

void CreateKafkaTopic(RdKafka::Handle& handle, const TString& topic, int partitions) {
    AssertCOk(CreateKafkaTopicEx(handle, topic, {.Partitions = partitions}), "CreateTopics");
}

rd_kafka_resp_err_t CreateKafkaTopicEx(RdKafka::Handle& handle, const TString& topic, const TCreateKafkaTopicOptions& options) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    char errstr[512];
    rd_kafka_NewTopic_t* newTopic = rd_kafka_NewTopic_new(topic.c_str(), options.Partitions, 1, errstr, sizeof(errstr));
    UNIT_ASSERT_C(newTopic, errstr);
    for (const auto& [name, value] : options.Configs) {
        AssertCOk(rd_kafka_NewTopic_set_config(newTopic, name.c_str(), value.c_str()), "NewTopic_set_config");
    }
    TRdAdminOptions adminOptions;
    if (options.ValidateOnly) {
        adminOptions.reset(rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS));
        UNIT_ASSERT(adminOptions);
        AssertCOk(rd_kafka_AdminOptions_set_validate_only(adminOptions.get(), 1, errstr, sizeof(errstr)), errstr);
    }
    rd_kafka_CreateTopics(rk, &newTopic, 1, adminOptions.get(), queue.get());
    rd_kafka_NewTopic_destroy(newTopic);
    auto event = WaitAdminEvent(queue.get());
    const rd_kafka_CreateTopics_result_t* result = rd_kafka_event_CreateTopics_result(event.get());
    UNIT_ASSERT(result);
    size_t count = 0;
    const rd_kafka_topic_result_t** topics = rd_kafka_CreateTopics_result_topics(result, &count);
    UNIT_ASSERT_VALUES_EQUAL(count, 1);
    const auto err = rd_kafka_topic_result_error(topics[0]);
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR && options.WaitReady && !options.ValidateOnly) {
        WaitTopicPartitions(handle, topic, static_cast<size_t>(options.Partitions));
    }
    return err;
}

void CreateKafkaPartitions(RdKafka::Handle& handle, const TString& topic, size_t totalPartitions) {
    AssertCOk(CreateKafkaPartitionsResult(handle, topic, totalPartitions), "CreatePartitions");
    WaitTopicPartitions(handle, topic, totalPartitions);
}

rd_kafka_resp_err_t CreateKafkaPartitionsResult(RdKafka::Handle& handle, const TString& topic, size_t totalPartitions) {
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
    return rd_kafka_topic_result_error(topics[0]);
}

rd_kafka_resp_err_t DeleteKafkaTopic(RdKafka::Handle& handle, const TString& topic) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    rd_kafka_DeleteTopic_t* del = rd_kafka_DeleteTopic_new(topic.c_str());
    UNIT_ASSERT(del);
    rd_kafka_DeleteTopics(rk, &del, 1, nullptr, queue.get());
    rd_kafka_DeleteTopic_destroy(del);
    auto event = WaitAdminEventRaw(queue.get());
    if (rd_kafka_event_error(event.get())) {
        return rd_kafka_event_error(event.get());
    }
    const rd_kafka_DeleteTopics_result_t* result = rd_kafka_event_DeleteTopics_result(event.get());
    if (!result) {
        return RD_KAFKA_RESP_ERR__BAD_MSG;
    }
    size_t count = 0;
    const rd_kafka_topic_result_t** topics = rd_kafka_DeleteTopics_result_topics(result, &count);
    if (count == 0) {
        return RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION;
    }
    return rd_kafka_topic_result_error(topics[0]);
}

TString AlterTopicConfigs(RdKafka::Handle& handle, const TString& topic, const THashMap<TString, TString>& configs) {
    rd_kafka_t* rk = handle.c_ptr();
    TRdQueue queue(rd_kafka_queue_new(rk));
    rd_kafka_ConfigResource_t* resource = rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC, topic.c_str());
    UNIT_ASSERT(resource);
    for (const auto& [name, value] : configs) {
        AssertCOk(rd_kafka_ConfigResource_set_config(resource, name.c_str(), value.c_str()), "ConfigResource_set_config");
    }
    rd_kafka_AlterConfigs(rk, &resource, 1, nullptr, queue.get());
    rd_kafka_ConfigResource_destroy(resource);
    auto event = WaitAdminEventRaw(queue.get());
    if (rd_kafka_event_error(event.get())) {
        return TString(rd_kafka_event_error_string(event.get()));
    }
    const rd_kafka_AlterConfigs_result_t* result = rd_kafka_event_AlterConfigs_result(event.get());
    if (!result) {
        return "AlterConfigs result is missing";
    }
    size_t count = 0;
    const rd_kafka_ConfigResource_t** resources = rd_kafka_AlterConfigs_result_resources(result, &count);
    if (count == 0) {
        return "AlterConfigs returned no resources";
    }
    if (rd_kafka_ConfigResource_error(resources[0]) != RD_KAFKA_RESP_ERR_NO_ERROR) {
        return TString(rd_kafka_ConfigResource_error_string(resources[0]));
    }
    return {};
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

TApiVersionsReply RequestApiVersions(i16 apiVersion) {
    TInetStreamSocket socket(ConnectKafkaFd());
    return ExchangeApiVersions(socket, apiVersion, 1);
}

bool TryRecvApiVersions(TInetStreamSocket& socket, TApiVersionsReply& reply) {
    char sizeBuf[4];
    if (!RecvExactAllowClose(socket, sizeBuf, 4)) {
        return false;
    }
    const char* sizePtr = sizeBuf;
    const i32 size = ReadI32(sizePtr, sizeBuf + 4);
    if (size <= 0 || size >= (1 << 20)) {
        return false;
    }
    TString payload;
    payload.resize(size);
    if (!RecvExactAllowClose(socket, payload.Detach(), size)) {
        return false;
    }
    reply = ParseApiVersionsBody(payload);
    return true;
}

bool RequestApiVersionsMaybe(i16 apiVersion, TApiVersionsReply& reply) {
    TInetStreamSocket socket(ConnectKafkaFd());
    SendAll(socket, EncodeApiVersionsRequest(apiVersion, 1));
    return TryRecvApiVersions(socket, reply);
}

bool RequestApiVersionsKeepConnection(
    i16 firstVersion,
    i16 secondVersion,
    TApiVersionsReply* first,
    TApiVersionsReply* second)
{
    TInetStreamSocket socket(ConnectKafkaFd());
    SendAll(socket, EncodeApiVersionsRequest(firstVersion, 1));
    TApiVersionsReply firstReply;
    if (!TryRecvApiVersions(socket, firstReply)) {
        return false;
    }
    if (first) {
        *first = firstReply;
    }
    SendAll(socket, EncodeApiVersionsRequest(secondVersion, 2));
    TApiVersionsReply secondReply;
    if (!TryRecvApiVersions(socket, secondReply)) {
        return false;
    }
    if (second) {
        *second = secondReply;
    }
    return true;
}

} // namespace NKafkaRdkafkaTests
