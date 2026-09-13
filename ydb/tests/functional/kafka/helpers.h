#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <librdkafka/rdkafka.h>
#include <rdkafkacpp.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/env.h>

#include <atomic>
#include <memory>
#include <string>

namespace NKafkaRdkafkaTests {

struct TDeliveryReport: public RdKafka::DeliveryReportCb {
    std::atomic<int> Ok{0};
    std::atomic<int> Fail{0};
    TString LastError;

    void dr_cb(RdKafka::Message& message) override;
};

struct TLogEventCb: public RdKafka::EventCb {
    void event_cb(RdKafka::Event& event) override;
};

struct TProducer {
    TDeliveryReport Dr;
    TLogEventCb Log;
    std::unique_ptr<RdKafka::Producer> Handle;

    TProducer() = default;
    TProducer(const TProducer&) = delete;
    TProducer& operator=(const TProducer&) = delete;
};

struct TConsumer {
    TLogEventCb Log;
    std::unique_ptr<RdKafka::KafkaConsumer> Handle;

    TConsumer() = default;
    TConsumer(const TConsumer&) = delete;
    TConsumer& operator=(const TConsumer&) = delete;
    ~TConsumer();
};

struct TRdEventDestroy {
    void operator()(rd_kafka_event_t* ev) const noexcept {
        rd_kafka_event_destroy(ev);
    }
};

struct TRdQueueDestroy {
    void operator()(rd_kafka_queue_t* q) const noexcept {
        rd_kafka_queue_destroy(q);
    }
};

using TRdEvent = std::unique_ptr<rd_kafka_event_t, TRdEventDestroy>;
using TRdQueue = std::unique_ptr<rd_kafka_queue_t, TRdQueueDestroy>;

TString BootstrapServers();
TString DatabasePath();
TString UniqueName(TStringBuf prefix);

NYdb::TDriver MakeYdbDriver();
void CreateYdbTopic(const TString& name, ui32 partitions);

std::unique_ptr<TProducer> MakeProducer(const THashMap<TString, TString>& extra = {});
std::unique_ptr<TConsumer> MakeConsumer(const TString& groupId, const THashMap<TString, TString>& extra = {});
std::unique_ptr<TProducer> MakeSaslProducer(const THashMap<TString, TString>& extra = {});
std::unique_ptr<TConsumer> MakeSaslConsumer(const TString& groupId, const THashMap<TString, TString>& extra = {});

void Produce(
    RdKafka::Producer& producer,
    const TString& topic,
    const TString& payload,
    const TString& key = {},
    int32_t partition = RdKafka::Topic::PARTITION_UA);
void Flush(TProducer& producer);
void ProduceAndFlush(
    TProducer& producer,
    const TString& topic,
    const TVector<TString>& payloads,
    const TVector<TString>& keys = {});

void WaitTopicPartitions(RdKafka::Handle& handle, const TString& topic, size_t partitions, TDuration timeout = TDuration::Seconds(30));

TVector<TString> ConsumePayloads(RdKafka::KafkaConsumer& consumer, size_t count, TDuration timeout = TDuration::Seconds(30));
void ConsumeUntilEmpty(RdKafka::KafkaConsumer& consumer, TDuration timeout = TDuration::Seconds(2));
TVector<int32_t> AssignmentPartitions(RdKafka::KafkaConsumer& consumer);
void WaitAssignment(RdKafka::KafkaConsumer& consumer, size_t minPartitions, TDuration timeout = TDuration::Seconds(30));
void WaitBalanced(
    RdKafka::KafkaConsumer& first,
    RdKafka::KafkaConsumer& second,
    size_t totalPartitions,
    TDuration timeout = TDuration::Seconds(60));

void AssertTxnOk(RdKafka::Error* error, const TString& what);

TRdEvent WaitAdminEvent(rd_kafka_queue_t* queue, int timeoutMs = 30000);
void CreateKafkaTopic(RdKafka::Handle& handle, const TString& topic, int partitions);
void CreateKafkaPartitions(RdKafka::Handle& handle, const TString& topic, size_t totalPartitions);
THashMap<TString, TString> DescribeTopicConfigs(RdKafka::Handle& handle, const TString& topic);
TVector<TString> ListConsumerGroups(RdKafka::Handle& handle);
void DescribeConsumerGroup(RdKafka::Handle& handle, const TString& groupId);

TString MessagePayload(const RdKafka::Message& message);
TString MessageKey(const RdKafka::Message& message);

inline void AssertConfOk(RdKafka::Conf::ConfResult result, const std::string& err) {
    UNIT_ASSERT_C(result == RdKafka::Conf::CONF_OK, err);
}

inline void AssertRdKafkaOk(RdKafka::ErrorCode err, const TString& what = {}) {
    UNIT_ASSERT_C(err == RdKafka::ERR_NO_ERROR, what << ": " << RdKafka::err2str(err));
}

inline void AssertCOk(rd_kafka_resp_err_t err, const char* what = nullptr) {
    UNIT_ASSERT_C(err == RD_KAFKA_RESP_ERR_NO_ERROR, TString(what ? what : rd_kafka_err2str(err)));
}

} // namespace NKafkaRdkafkaTests
