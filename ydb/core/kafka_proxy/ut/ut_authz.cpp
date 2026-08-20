#include "kafka_test_client.h"
#include "test_server.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/string/cast.h>

#include <map>
#include <unordered_map>
#include <vector>

using namespace NKafka;
using namespace NYdb;

namespace {

TKafkaRecordBatch MakeTestBatch(TStringBuf value) {
    TKafkaRecordBatch batch;
    batch.Magic = 2;
    batch.Records.resize(1);
    batch.Records[0].Value = TKafkaRawBytes(value.data(), value.size());
    return batch;
}

TKafkaInt16 ProduceError(TKafkaTestClient& client, const TString& topicName, TStringBuf value = "record", ui32 partition = 0) {
    auto msg = client.Produce(topicName, partition, MakeTestBatch(value));
    UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].PartitionResponses.size(), 1);
    return msg->Responses[0].PartitionResponses[0].ErrorCode;
}

std::vector<TString> FetchRecordValues(TKafkaTestClient& client, const TString& topicName) {
    auto msg = client.Fetch({{topicName, {0}}});
    UNIT_ASSERT_VALUES_EQUAL(msg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(
        msg->Responses[0].Partitions[0].ErrorCode,
        static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    UNIT_ASSERT(msg->Responses[0].Partitions[0].Records);

        TStringBuf data(
            msg->Responses[0].Partitions[0].Records->data(),
            msg->Responses[0].Partitions[0].Records->size());
    std::vector<TString> values;
    while (!data.empty()) {
        const auto header = ReadKafkaBatchHeader(data);
        UNIT_ASSERT(header);
        const size_t batchSize = sizeof(TKafkaRecordBatch::BaseOffsetMeta::Type)
            + sizeof(TKafkaRecordBatch::BatchLengthMeta::Type)
            + header->BatchLength;
        UNIT_ASSERT_LE(batchSize, data.size());
        const auto batch = ReadRecordBatch(TStringBuf(data.data(), batchSize));
        for (const auto& record : batch.Records) {
            values.emplace_back(record.Value->data(), record.Value->size());
        }
        data.Skip(batchSize);
    }
    return values;
}

void CreateTopic(NTopic::TTopicClient& pqClient, const TString& topicName, const TString& consumer = {}

} // namespace

Y_UNIT_TEST_SUITE(KafkaAuthzRecheck) {
Y_UNIT_TEST(LongSessionWithValidTokenKeepsProduceAndFetchWorking) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .TokenRecheckIntervalMs = 500,
            .LoginTokenExpireTime = "1h",
        });

        TString topicName = "/Root/topic-long-valid-token";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();

        std::vector<TString> expected;
        const int rounds = 6;
        for (int i = 0; i < rounds; ++i) {
            if (i != 0) {
                Sleep(TDuration::MilliSeconds(500));
            }
            TString value = "record-" + ToString(i);
            expected.push_back(value);
            UNIT_ASSERT_VALUES_EQUAL(
                ProduceError(client, topicName, value),
                static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        UNIT_ASSERT_VALUES_EQUAL(FetchRecordValues(client, topicName), expected);

        auto apiVersions = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersions->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }


Y_UNIT_TEST(TokenRecheckDisabledDoesNotDropConnection) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .TokenRecheckIntervalMs = 0,
            .LoginTokenExpireTime = "2s",
            .AuthRefreshTime = "1s",
        });

        TString topicName = "/Root/topic-recheck-disabled";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "before-sleep"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        Sleep(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "after-sleep"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        auto apiVersions = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersions->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }


}
