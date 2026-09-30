#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaProtocol) {
    const TApiVersionInfo* FindApi(const TApiVersionsReply& reply, i16 apiKey) {
        for (const auto& key : reply.ApiKeys) {
            if (key.ApiKey == apiKey) {
                return &key;
            }
        }
        return nullptr;
    }

    Y_UNIT_TEST(ApiVersionsAdvertisesProduceMinZeroAndFetchMaxFour) {
        const auto reply = RequestApiVersions(2);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(reply.ErrorCode), 0);
        const auto* produce = FindApi(reply, KafkaApiProduce);
        UNIT_ASSERT(produce);
        UNIT_ASSERT_VALUES_EQUAL(produce->MinVersion, 0);
        const auto* fetch = FindApi(reply, KafkaApiFetch);
        UNIT_ASSERT(fetch);
        UNIT_ASSERT_VALUES_EQUAL(fetch->MaxVersion, 4);
    }

    Y_UNIT_TEST(UnsupportedApiVersionsKeepsConnection) {
        TApiVersionsReply first;
        TApiVersionsReply supported;
        UNIT_ASSERT_C(
            RequestApiVersionsKeepConnection(5, 2, &first, &supported),
            "KIP-511: unsupported ApiVersions must keep the connection so the client can retry");
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(first.ErrorCode), KafkaUnsupportedVersion);
        UNIT_ASSERT(!first.ApiKeys.empty());
        UNIT_ASSERT_VALUES_EQUAL(first.ApiKeys[0].ApiKey, KafkaApiApiVersions);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(supported.ErrorCode), 0);
        UNIT_ASSERT(FindApi(supported, KafkaApiProduce));
    }

    Y_UNIT_TEST(ProduceWorksAfterVersionNegotiation) {
        const TString topic = UniqueName("rdk-proto-produce");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer();
        WaitTopicPartitions(*producer->Handle, topic, 1);
        ProduceAndFlush(*producer, topic, {"after-api-versions"});
        auto consumer = MakeConsumer(UniqueName("group"));
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*consumer->Handle, 1)[0], "after-api-versions");
    }
}
