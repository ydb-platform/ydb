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

TKafkaInt16 FetchPartitionError(TKafkaTestClient& client, const TString& topicName) {
    auto msg = client.Fetch({{topicName, {0}}});
    UNIT_ASSERT_VALUES_EQUAL(msg->Responses.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Partitions.size(), 1);
    return msg->Responses[0].Partitions[0].ErrorCode;
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

TKafkaInt16 ListOffsetsPartitionError(TKafkaTestClient& client, const TString& topicName) {
    std::vector<std::pair<i32, i64>> partitions{{0, -1}};
    auto msg = client.ListOffsets(partitions, topicName);
    UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 1);
    return msg->Topics[0].Partitions[0].ErrorCode;
}

TKafkaInt16 OffsetFetchPartitionError(TKafkaTestClient& client, const TString& topicName, const TString& groupId) {
    std::map<TString, std::vector<i32>> topicsToPartitions;
    topicsToPartitions[topicName] = {0};
    auto msg = client.OffsetFetch(groupId, topicsToPartitions);
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 1);
    return msg->Groups[0].Topics[0].Partitions[0].ErrorCode;
}

void AssertOffsetFetchNoneMinusOne(TKafkaTestClient& client, const TString& topicName, const TString& groupId = "unknown-group") {
    std::map<TString, std::vector<i32>> topicsToPartitions;
    topicsToPartitions[topicName] = {0};
    auto msg = client.OffsetFetch(groupId, topicsToPartitions);
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(
        msg->Groups[0].Topics[0].Partitions[0].ErrorCode,
        static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    UNIT_ASSERT_VALUES_EQUAL(msg->Groups[0].Topics[0].Partitions[0].CommittedOffset, -1);
}

TKafkaInt16 OffsetCommitPartitionError(TKafkaTestClient& client, const TString& topicName, const TString& groupId) {
    std::unordered_map<TString, std::vector<NKafka::TEvKafka::PartitionConsumerOffset>> offsets;
    offsets[topicName] = {NKafka::TEvKafka::PartitionConsumerOffset(0, 0)};
    auto msg = client.OffsetCommit(groupId, offsets);
    UNIT_ASSERT_VALUES_EQUAL(msg->Topics.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(msg->Topics[0].Partitions.size(), 1);
    return msg->Topics[0].Partitions[0].ErrorCode;
}

void CreateTopic(NTopic::TTopicClient& pqClient, const TString& topicName, const TString& consumer = {}, ui32 partitions = 1) {
    auto settings = NTopic::TCreateTopicSettings().PartitioningSettings(partitions, 100);
    if (consumer) {
        settings.BeginAddConsumer(consumer).EndAddConsumer();
    }
    auto result = pqClient.CreateTopic(topicName, settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
}

void AlterTopicPartitions(NTopic::TTopicClient& pqClient, const TString& topicName, ui64 minActivePartitions) {
    auto result = pqClient
        .AlterTopic(topicName, NTopic::TAlterTopicSettings().AlterPartitioningSettings(minActivePartitions, 100))
        .ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
}

void DropTopic(NTopic::TTopicClient& pqClient, const TString& topicName) {
    auto result = pqClient.DropTopic(topicName).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
}

void ModifyTopicPermissions(
    TDriver& driver,
    const TString& path,
    const TString& user,
    const std::vector<std::string>& names,
    bool grant)
{
    NYdb::NScheme::TSchemeClient schemeClient(driver);
    NYdb::NScheme::TPermissions permissions(user, names);
    auto settings = grant
        ? NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
        : NYdb::NScheme::TModifyPermissionsSettings().AddRevokePermissions(permissions);
    auto result = schemeClient.ModifyPermissions(path, settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
}

template <typename TFn>
void WaitUntil(TFn&& fn, TDuration timeout = TDuration::Seconds(20)) {
    const auto deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        if (fn()) {
            return;
        }
        Sleep(TDuration::MilliSeconds(200));
    }
    UNIT_ASSERT_C(false, "timed out waiting for authorization error");
}

} // namespace

Y_UNIT_TEST_SUITE(KafkaAuthzRecheck) {
    Y_UNIT_TEST(ProduceAndFetchFailAfterTopicAclRevoke) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .ACLRetryTimeoutSec = 300,
        });

        TString topicName = "/Root/topic-acl-revoke";
        TString groupId = "authz-group";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, groupId);
        ModifyTopicPermissions(
            *testServer.Driver,
            topicName,
            "usernorights",
            {"ydb.generic.read", "ydb.generic.write"},
            true);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka("usernorights@/Root", "dummyPass");

        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "before-revoke"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(FetchPartitionError(client, topicName), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(ListOffsetsPartitionError(client, topicName), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(OffsetFetchPartitionError(client, topicName, groupId), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(OffsetCommitPartitionError(client, topicName, groupId), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        ModifyTopicPermissions(
            *testServer.Driver,
            topicName,
            "usernorights",
            {"ydb.generic.read", "ydb.generic.write"},
            false);

        const auto revokeWaitStart = TInstant::Now();
        WaitUntil([&] {
            auto error = ProduceError(client, topicName, "after-revoke");
            UNIT_ASSERT_VALUES_UNEQUAL(error, static_cast<TKafkaInt16>(EKafkaErrors::REQUEST_TIMED_OUT));
            UNIT_ASSERT_VALUES_UNEQUAL(error, static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_SERVER_ERROR));
            return error == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });
        UNIT_ASSERT_C(
            TInstant::Now() - revokeWaitStart < TDuration::Seconds(10),
            "produce after ACL revoke must fail immediately, not after the 30s cookie timeout");
        WaitUntil([&] {
            return FetchPartitionError(client, topicName) == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });
        // Scheme cache hides topics without DescribeSchema as PathErrorUnknown.
        // ListOffsets must keep UNKNOWN_TOPIC_OR_PARTITION for missing topics (mixed-version / auto-create).
        WaitUntil([&] {
            return ListOffsetsPartitionError(client, topicName) == static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION);
        });
        WaitUntil([&] {
            return OffsetFetchPartitionError(client, topicName, groupId) == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });
        WaitUntil([&] {
            return OffsetCommitPartitionError(client, topicName, groupId) == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });

        auto apiVersions = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersions->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }

    Y_UNIT_TEST(ProduceAndFetchFailAfterTokenExpires) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .TokenRecheckIntervalMs = 500,
            .LoginTokenExpireTime = "2s",
            .AuthRefreshTime = "1s",
            .ACLRetryTimeoutSec = 1,
        });

        TString topicName = "/Root/topic-token-expire";
        TString groupId = "authz-expire-group";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName, groupId);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();

        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "before-expire"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(FetchPartitionError(client, topicName), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        WaitUntil([&] {
            return ProduceError(client, topicName, "after-expire") == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        }, TDuration::Seconds(20));
        UNIT_ASSERT_VALUES_EQUAL(FetchPartitionError(client, topicName), static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(ListOffsetsPartitionError(client, topicName), static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(OffsetFetchPartitionError(client, topicName, groupId), static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(OffsetCommitPartitionError(client, topicName, groupId), static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));

        {
            auto init = client.InitProducerId();
            UNIT_ASSERT_VALUES_EQUAL(init->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto metadata = client.Metadata({topicName}, false);
            UNIT_ASSERT_VALUES_EQUAL(metadata->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(metadata->Topics[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto createTopics = client.CreateTopics({TTopicConfig("/Root/topic-after-expire", 1)});
            UNIT_ASSERT_VALUES_EQUAL(createTopics->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(createTopics->Topics[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto createPartitions = client.CreatePartitions({TTopicConfig(topicName, 2)});
            UNIT_ASSERT_VALUES_EQUAL(createPartitions->Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(createPartitions->Results[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }

        const TProducerInstanceId producerInstanceId{1, 0};
        {
            auto addPartitions = client.AddPartitionsToTxn("txn-after-expire", producerInstanceId, {{topicName, {0}}});
            UNIT_ASSERT_VALUES_EQUAL(addPartitions->Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(addPartitions->Results[0].Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(addPartitions->Results[0].Results[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto addOffsets = client.AddOffsetsToTxn("txn-after-expire", producerInstanceId, groupId);
            UNIT_ASSERT_VALUES_EQUAL(addOffsets->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto txnOffsetCommit = client.TxnOffsetCommit(
                "txn-after-expire", producerInstanceId, groupId, 0, {{topicName, {{0, 0}}}});
            UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommit->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommit->Topics[0].Partitions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(txnOffsetCommit->Topics[0].Partitions[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto endTxn = client.EndTxn("txn-after-expire", producerInstanceId, true);
            UNIT_ASSERT_VALUES_EQUAL(endTxn->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            TString joinGroupId = groupId;
            std::vector<TString> topics{topicName};
            auto join = client.JoinGroup(topics, joinGroupId, "roundrobin");
            UNIT_ASSERT_VALUES_EQUAL(join->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            TString memberId = "expired-member";
            TString protocolName = "roundrobin";
            std::vector<NKafka::TSyncGroupRequestData::TSyncGroupRequestAssignment> assignments;
            auto sync = client.SyncGroup(memberId, 0, groupId, assignments, protocolName);
            UNIT_ASSERT_VALUES_EQUAL(sync->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
            UNIT_ASSERT(sync->Assignment);
            UNIT_ASSERT_VALUES_EQUAL(
                client.Heartbeat(memberId, 0, groupId)->ErrorCode,
                static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
            UNIT_ASSERT_VALUES_EQUAL(
                client.LeaveGroup(memberId, groupId)->ErrorCode,
                static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            UNIT_ASSERT_VALUES_EQUAL(
                client.ListGroups()->ErrorCode,
                static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto describeGroups = client.DescribeGroups({groupId});
            UNIT_ASSERT_VALUES_EQUAL(describeGroups->Groups.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                describeGroups->Groups[0].ErrorCode,
                static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            UNIT_ASSERT_VALUES_EQUAL(
                client.FindCoordinator(groupId)->ErrorCode,
                static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto describe = client.DescribeConfigs({topicName});
            UNIT_ASSERT_VALUES_EQUAL(describe->Results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(describe->Results[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }
        {
            auto alter = client.AlterConfigs({TTopicConfig(topicName, 1)});
            UNIT_ASSERT_VALUES_EQUAL(alter->Responses.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(alter->Responses[0].ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        }

        auto apiVersions = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersions->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }

    Y_UNIT_TEST(ListOffsetsUnknownTopicStaysUnknownAfterSasl) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
        });

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();

        UNIT_ASSERT_VALUES_EQUAL(
            ListOffsetsPartitionError(client, "/Root/topic-does-not-exist"),
            static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));
    }

    // Matches Apache Kafka OffsetFetch v1+: missing topic → NONE + committedOffset -1,
    // no auto-create. See https://issues.apache.org/jira/browse/KAFKA-20165
    Y_UNIT_TEST(OffsetFetchUnknownTopicIsNoneMinusOneAfterSasl) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
        });

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();
        AssertOffsetFetchNoneMinusOne(client, "/Root/topic-does-not-exist");
    }

    // Describer with a token that cannot see /Root reports UNAUTHORIZED for a missing
    // path. OffsetFetch must still distinguish "does not exist" via an unauthenticated
    // describe, same as the old scheme-cache existence check.
    Y_UNIT_TEST(OffsetFetchUnknownTopicIsNoneMinusOneWithoutDescribe) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
        });

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka("usernorights@/Root", "dummyPass");
        AssertOffsetFetchNoneMinusOne(client, "/Root/topic-does-not-exist-norights");
    }

    Y_UNIT_TEST(OffsetFetchHiddenTopicWithoutPriorAclIsAuth) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
        });

        TString topicName = "/Root/topic-offsetfetch-hidden";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka("usernorights@/Root", "dummyPass");
        UNIT_ASSERT_VALUES_EQUAL(
            OffsetFetchPartitionError(client, topicName, "hidden-group"),
            static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
    }

    Y_UNIT_TEST(AclRevokeThenGrantRestoresProduceAndFetch) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .ACLRetryTimeoutSec = 300,
        });

        TString topicName = "/Root/topic-acl-regrant";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);
        ModifyTopicPermissions(
            *testServer.Driver,
            topicName,
            "usernorights",
            {"ydb.generic.read", "ydb.generic.write"},
            true);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka("usernorights@/Root", "dummyPass");
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "before-revoke"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        ModifyTopicPermissions(
            *testServer.Driver,
            topicName,
            "usernorights",
            {"ydb.generic.read", "ydb.generic.write"},
            false);
        WaitUntil([&] {
            return ProduceError(client, topicName, "revoked") == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });

        ModifyTopicPermissions(
            *testServer.Driver,
            topicName,
            "usernorights",
            {"ydb.generic.read", "ydb.generic.write"},
            true);
        WaitUntil([&] {
            return ProduceError(client, topicName, "after-grant") == static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR);
        });
        UNIT_ASSERT_VALUES_EQUAL(FetchPartitionError(client, topicName), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        auto apiVersions = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersions->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }

    Y_UNIT_TEST(AclRevokeOnOneTopicDoesNotAffectAnother) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .ACLRetryTimeoutSec = 300,
        });

        TString revokedTopic = "/Root/topic-acl-revoked";
        TString keptTopic = "/Root/topic-acl-kept";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, revokedTopic);
        CreateTopic(pqClient, keptTopic);
        for (const auto& topicName : {revokedTopic, keptTopic}) {
            ModifyTopicPermissions(
                *testServer.Driver,
                topicName,
                "usernorights",
                {"ydb.generic.read", "ydb.generic.write"},
                true);
        }

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka("usernorights@/Root", "dummyPass");
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, revokedTopic, "revoked-before"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, keptTopic, "kept-before"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        ModifyTopicPermissions(
            *testServer.Driver,
            revokedTopic,
            "usernorights",
            {"ydb.generic.read", "ydb.generic.write"},
            false);

        WaitUntil([&] {
            return ProduceError(client, revokedTopic, "revoked-after") == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, keptTopic, "kept-after"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(FetchPartitionError(client, keptTopic), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }

    Y_UNIT_TEST(ReadOnlyUserCanFetchButNotProduce) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
        });

        TString topicName = "/Root/topic-read-only";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);

        {
            TKafkaTestClient writer(testServer.Port);
            writer.PlainAuthenticateToKafka();
            UNIT_ASSERT_VALUES_EQUAL(ProduceError(writer, topicName, "readable"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        }

        TKafkaTestClient reader(testServer.Port);
        reader.PlainAuthenticateToKafka("useronlyreadrights@/Root", "AbAcAbA");
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(reader, topicName, "forbidden"), static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED));
        UNIT_ASSERT_VALUES_EQUAL(FetchPartitionError(reader, topicName), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(FetchRecordValues(reader, topicName), std::vector<TString>{"readable"});
    }

    Y_UNIT_TEST(DroppedTopicFailsProduceWithoutDroppingConnection) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .ACLRetryTimeoutSec = 300,
        });

        TString topicName = "/Root/topic-dropped";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "before-drop"), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        DropTopic(pqClient, topicName);

        WaitUntil([&] {
            auto error = ProduceError(client, topicName, "after-drop");
            UNIT_ASSERT_VALUES_UNEQUAL(error, static_cast<TKafkaInt16>(EKafkaErrors::REQUEST_TIMED_OUT));
            UNIT_ASSERT_VALUES_UNEQUAL(error, static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_SERVER_ERROR));
            return error == static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION)
                || error == static_cast<TKafkaInt16>(EKafkaErrors::TOPIC_AUTHORIZATION_FAILED);
        });

        auto apiVersions = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersions->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
    }

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

    Y_UNIT_TEST(ProduceToNewPartitionAfterAlterTopic) {
        TInsecureTestServer testServer(TTestServerSettings{
            .KafkaApiMode = "2",
            .CheckACL = true,
            .ACLRetryTimeoutSec = 300,
        });

        TString topicName = "/Root/topic-alter-partitions";
        NTopic::TTopicClient pqClient(*testServer.Driver);
        CreateTopic(pqClient, topicName);

        TKafkaTestClient client(testServer.Port);
        client.PlainAuthenticateToKafka();

        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "p0", 0), static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(ProduceError(client, topicName, "p1-before-alter", 1), static_cast<TKafkaInt16>(EKafkaErrors::UNKNOWN_TOPIC_OR_PARTITION));

        AlterTopicPartitions(pqClient, topicName, 2);

        WaitUntil([&] {
            return ProduceError(client, topicName, "p1-after-alter", 1) == static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR);
        }, TDuration::Seconds(20));
    }
}
