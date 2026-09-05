#include "supported_codecs_fixture.h"
#include "run_ydb.h"

#include <util/generic/yexception.h>

namespace NYdbCliTests {

Y_UNIT_TEST_SUITE(YdbTopicSetOffsets) {

Y_UNIT_TEST_F(SetOffsetsEarliestPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "set",
        "--consumer", consumerName,
        "--position", "earliest",
        topicName,
    };
    TString output = ExecYdb(cmd);
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(SetOffsetsLatestPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "set",
        "--consumer", consumerName,
        "--position", "latest",
        topicName,
    };
    TString output = ExecYdb(cmd);
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(SetOffsetsMissingConsumerPrintsIssues, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    YdbTopicCreate(topicName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "set",
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

Y_UNIT_TEST_F(SetOffsetsTimestampPrintsOk, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "set",
        "--consumer", consumerName,
        "--position", "2099-01-01T00:00:00Z",
        topicName,
    };
    TString output = ExecYdb(cmd);
    UNIT_ASSERT_STRING_CONTAINS(output, "OK");
}

Y_UNIT_TEST_F(SetOffsetsInvalidPositionFails, TSupportedCodecsFixture) {
    const TString topicName = GetTopicName();
    const TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);
    YdbTopicConsumerAdd(topicName, consumerName);

    TList<TString> cmd = {
        "topic", "consumer", "offset", "set",
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
