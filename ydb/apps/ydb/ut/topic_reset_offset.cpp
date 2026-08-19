#include "supported_codecs_fixture.h"
#include "run_ydb.h"

#include <util/generic/yexception.h>

namespace NYdbCliTests {

Y_UNIT_TEST_SUITE(YdbTopicResetOffset) {

Y_UNIT_TEST_F(ResetOffsetEarliestPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "reset",
        "--consumer", consumerName,
        "--position", "earliest",
        topicName,
    };
    TString output = ExecYdb(cmd);
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(ResetOffsetLatestPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "reset",
        "--consumer", consumerName,
        "--position", "latest",
        topicName,
    };
    TString output = ExecYdb(cmd);
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(ResetOffsetMissingConsumerPrintsIssues, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    YdbTopicCreate(topicName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "reset",
        "--consumer", "missing-consumer",
        "--position", "earliest",
        topicName,
    };
    try {
        ExecYdb(cmd);
        UNIT_ASSERT_C(false, "expected non-zero exit code");
    } catch (const yexception& e) {
        const TString text = TString(e.what());
        UNIT_ASSERT_C(
            text.Contains("does not exist") || text.Contains("SCHEME_ERROR") || text.Contains("issues"),
            text);
    }
}

Y_UNIT_TEST_F(ResetOffsetTimestampPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "reset",
        "--consumer", consumerName,
        "--position", "2099-01-01T00:00:00Z",
        topicName,
    };
    TString output = ExecYdb(cmd);
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(ResetOffsetInvalidPositionFails, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "reset",
        "--consumer", consumerName,
        "--position", "not-a-position",
        topicName,
    };
    try {
        ExecYdb(cmd);
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
