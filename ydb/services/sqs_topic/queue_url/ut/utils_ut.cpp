#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NSqsTopic;

Y_UNIT_TEST_SUITE(SqsTopicQueueUrl) {
    Y_UNIT_TEST(ParseEmpty) {
        UNIT_ASSERT(!ParseQueueUrl("").has_value());
        UNIT_ASSERT(!ParseQueueUrl("http://sqs.ydb.tech/").has_value());
    }

    Y_UNIT_TEST(BasicPack) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic/path",
            .Consumer = "ydb_sqs_consumer",
            .Fifo = false,
        };
        TString result = PackQueueUrlPath(qu);
        UNIT_ASSERT_VALUES_EQUAL(result, "/v1/5//Root/10/topic/path/16/ydb_sqs_consumer");
        UNIT_ASSERT(*ParseQueueUrlPath(result) == qu);
    }

    Y_UNIT_TEST(BasicPackFifo) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic/path",
            .Consumer = "ydb_sqs_consumer",
            .Fifo = true,
        };
        TString result = PackQueueUrlPath(qu);
        UNIT_ASSERT_VALUES_EQUAL(result, "/v1/5//Root/10/topic/path/16/ydb_sqs_consumer.fifo");
        UNIT_ASSERT(*ParseQueueUrlPath(result) == qu);
    }
}
