#include "utils.h"

#include <library/cpp/testing/gtest/gtest.h>

using namespace NKikimr::NSqsTopic;

TEST(SqsTopicQueueUrl, ParseEmpty) {
    EXPECT_FALSE(ParseQueueUrl("").has_value());
    EXPECT_FALSE(ParseQueueUrl("http://sqs.ydb.tech/").has_value());
}

TEST(SqsTopicQueueUrl, BasicPack) {
    TRichQueueUrl qu{
        .Database = "/Root",
        .TopicPath = "topic/path",
        .Consumer = "ydb_sqs_consumer",
        .Fifo = false,
    };
    TString result = PackQueueUrlPath(qu);
    EXPECT_EQ(result, "/v1/5//Root/10/topic/path/16/ydb_sqs_consumer");
    EXPECT_EQ(qu, ParseQueueUrlPath(result));
}

TEST(SqsTopicQueueUrl, BasicPackFifo) {
    TRichQueueUrl qu{
        .Database = "/Root",
        .TopicPath = "topic/path",
        .Consumer = "ydb_sqs_consumer",
        .Fifo = true,
    };
    TString result = PackQueueUrlPath(qu);
    EXPECT_EQ(result, "/v1/5//Root/10/topic/path/16/ydb_sqs_consumer.fifo");
    EXPECT_EQ(qu, ParseQueueUrlPath(result));
}
