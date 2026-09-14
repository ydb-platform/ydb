#include "supported_codecs_fixture.h"
#include "run_ydb.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/common/env.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/env.h>

namespace NYdbCliTests {

namespace {

NYdb::NTopic::TTopicClient MakeTopicClient() {
    NYdb::TDriverConfig config;
    config.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    config.SetDatabase(GetEnv("YDB_DATABASE"));
    return NYdb::NTopic::TTopicClient(NYdb::TDriver(config));
}

void WriteMessages(const TString& topicName, ui32 count) {
    auto client = MakeTopicClient();
    auto session = client.CreateSimpleBlockingWriteSession(
        NYdb::NTopic::TWriteSessionSettings()
            .Path(topicName)
            .ProducerId("cli-reset-offset-ut")
            .MessageGroupId("cli-reset-offset-ut"));
    for (ui32 i = 0; i < count; ++i) {
        UNIT_ASSERT(session->Write(TStringBuilder() << "msg-" << i));
    }
    UNIT_ASSERT(session->Close(TDuration::Seconds(30)));
}

ui64 GetCommittedOffset(const TString& topicName, const TString& consumerName, ui32 partitionId = 0) {
    auto client = MakeTopicClient();
    auto describe = client.DescribeConsumer(
        topicName,
        consumerName,
        NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
    UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
    const auto& partitions = describe.GetConsumerDescription().GetPartitions();
    UNIT_ASSERT_LT(partitionId, partitions.size());
    const auto& stats = partitions[partitionId].GetPartitionConsumerStats();
    UNIT_ASSERT(stats);
    return stats->GetCommittedOffset();
}

TString ExecResetOffset(
    const TString& topicName,
    const TString& consumerOpt,
    const TString& consumerName,
    const TString& position,
    bool checkExitCode = true)
{
    TList<TString> cmd = {
        "topic", "consumer", "offset", "reset",
        consumerOpt, consumerName,
        "--position", position,
        topicName,
    };
    return RunYdb({}, cmd, checkExitCode);
}

void ExpectExecFails(const TList<TString>& cmd) {
    try {
        RunYdb({}, cmd);
        UNIT_ASSERT_C(false, "expected non-zero exit code");
    } catch (const yexception&) {
        // ok
    }
}

} // namespace

Y_UNIT_TEST_SUITE(YdbTopicResetOffset) {

Y_UNIT_TEST_F(ResetOffsetShortConsumerFlagEarliest, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    UNIT_ASSERT_STRING_CONTAINS(
        ExecResetOffset(topicName, "--consumer", consumerName, "latest"), "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);

    TString output = ExecResetOffset(topicName, "-c", consumerName, "earliest");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 0);
}

Y_UNIT_TEST_F(ResetOffsetShortConsumerFlagTimestampPast, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    UNIT_ASSERT_STRING_CONTAINS(
        ExecResetOffset(topicName, "-c", consumerName, "latest"), "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);

    TString output = ExecResetOffset(topicName, "-c", consumerName, "1970-01-01T00:00:00Z");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 0);
}

Y_UNIT_TEST_F(ResetOffsetEarliestPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TString output = ExecResetOffset(topicName, "--consumer", consumerName, "earliest");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(ResetOffsetLatestPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TString output = ExecResetOffset(topicName, "--consumer", consumerName, "latest");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(ResetOffsetShortConsumerFlag, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    TString output = ExecResetOffset(topicName, "-c", consumerName, "latest");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);
}

Y_UNIT_TEST_F(ResetOffsetEarliestAndLatestMoveCommitted, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 3);

    UNIT_ASSERT_STRING_CONTAINS(
        ExecResetOffset(topicName, "--consumer", consumerName, "latest"), "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 3);

    UNIT_ASSERT_STRING_CONTAINS(
        ExecResetOffset(topicName, "--consumer", consumerName, "earliest"), "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 0);
}

Y_UNIT_TEST_F(ResetOffsetTimestampIsoFutureGoesToEnd, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    TString output = ExecResetOffset(
        topicName, "--consumer", consumerName, "2099-01-01T00:00:00Z");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);
}

Y_UNIT_TEST_F(ResetOffsetTimestampUnixSecondsFutureGoesToEnd, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    // Far-future unix seconds (same semantics as ISO future).
    TString output = ExecResetOffset(topicName, "--consumer", consumerName, "4102444800");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);
}

Y_UNIT_TEST_F(ResetOffsetTimestampIsoPastGoesToStart, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    UNIT_ASSERT_STRING_CONTAINS(
        ExecResetOffset(topicName, "--consumer", consumerName, "latest"), "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);

    TString output = ExecResetOffset(
        topicName, "--consumer", consumerName, "1970-01-01T00:00:00Z");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 0);
}

Y_UNIT_TEST_F(ResetOffsetTimestampUnixSecondsPastGoesToStart, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);
    WriteMessages(topicName, 2);

    UNIT_ASSERT_STRING_CONTAINS(
        ExecResetOffset(topicName, "--consumer", consumerName, "latest"), "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 2);

    TString output = ExecResetOffset(topicName, "--consumer", consumerName, "1");
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(topicName, consumerName), 0);
}

Y_UNIT_TEST_F(ResetOffsetMissingConsumerPrintsIssues, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    YdbTopicCreate(topicName);

    try {
        ExecResetOffset(topicName, "--consumer", "missing-consumer", "earliest");
        UNIT_ASSERT_C(false, "expected non-zero exit code");
    } catch (const yexception& e) {
        const TString text = TString(e.what());
        UNIT_ASSERT_C(
            text.Contains("does not exist") || text.Contains("SCHEME_ERROR") || text.Contains("issues"),
            text);
    }
}

Y_UNIT_TEST_F(ResetOffsetMissingTopicFails, TSupportedCodecsFixture) {
    ExpectExecFails({
        "topic", "consumer", "offset", "reset",
        "--consumer", GetConsumerName(),
        "--position", "earliest",
        "no-such-topic-for-reset-offset",
    });
}

Y_UNIT_TEST_F(ResetOffsetMissingPositionFails, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();
    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    ExpectExecFails({
        "topic", "consumer", "offset", "reset",
        "--consumer", consumerName,
        topicName,
    });
}

Y_UNIT_TEST_F(ResetOffsetMissingConsumerOptionFails, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    YdbTopicCreate(topicName);

    ExpectExecFails({
        "topic", "consumer", "offset", "reset",
        "--position", "earliest",
        topicName,
    });
}

Y_UNIT_TEST_F(ResetOffsetMissingTopicArgFails, TSupportedCodecsFixture) {
    ExpectExecFails({
        "topic", "consumer", "offset", "reset",
        "--consumer", "c",
        "--position", "earliest",
    });
}

Y_UNIT_TEST_F(ResetOffsetInvalidPositionFails, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    try {
        ExecResetOffset(topicName, "--consumer", consumerName, "not-a-position");
        UNIT_ASSERT_C(false, "expected non-zero exit code");
    } catch (const yexception& e) {
        const TString text = TString(e.what());
        UNIT_ASSERT_C(
            text.Contains("timestamp") || text.Contains("position") || text.Contains("failed to parse"),
            text);
    }
}

}

} // namespace NYdbCliTests
