#include "supported_codecs_fixture.h"

namespace NYdbCliTests {
Y_UNIT_TEST_SUITE(YdbTopic) {

Y_UNIT_TEST_F(SupportedCodecs_TopicCreate_DefaultValue, TSupportedCodecsFixture)
{
    TestTopicCreate(GetTopicName(), {}, {"RAW"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicCreate_UserValue, TSupportedCodecsFixture)
{
    TestTopicCreate(GetTopicName(), {"GZIP", "ZSTD"}, {"GZIP", "ZSTD"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicAlter, TSupportedCodecsFixture)
{
    TString topicName = GetTopicName();

    YdbTopicCreate(topicName);

    TestTopicAlter(topicName, {}, {"RAW"});
    TestTopicAlter(topicName, {"GZIP", "ZSTD"}, {"GZIP", "ZSTD"});
    TestTopicAlter(topicName, {}, {"GZIP", "ZSTD"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicConsumerAdd_DefaultValue, TSupportedCodecsFixture)
{
    TString topicName = GetTopicName();
    TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);

    TestTopicConsumerAdd(topicName, consumerName, {}, {"RAW"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicConsumerAdd_UserValue, TSupportedCodecsFixture)
{
    TString topicName = GetTopicName();
    TString consumerName[2] = { GetConsumerName(0), GetConsumerName(1) };

    YdbTopicCreate(topicName, {"GZIP", "ZSTD"});

    TestTopicConsumerAdd(topicName, consumerName[0], {"GZIP", "ZSTD"}, {"GZIP", "ZSTD"});
    TestTopicConsumerAdd(topicName, consumerName[1], {"GZIP", "ZSTD", "RAW"}, {"GZIP", "ZSTD", "RAW"});
}

}
}
