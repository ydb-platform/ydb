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

    Y_UNIT_TEST(MakeQueueUrlPrefersRequestEndpoint) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic",
            .Consumer = "consumer",
            .Fifo = false,
        };
        const TString url = MakeQueueUrl(
            qu,
            "https://lbkx.example.net:8443",
            "node.example.net",
            2135);
        UNIT_ASSERT_VALUES_EQUAL(
            url,
            "https://lbkx.example.net:8443/v1/5//Root/5/topic/8/consumer");
    }

    Y_UNIT_TEST(MakeQueueUrlStripsTrailingSlashFromRequestEndpoint) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic",
            .Consumer = "consumer",
            .Fifo = false,
        };
        const TString url = MakeQueueUrl(
            qu,
            "https://lbkx.example.net:8443/",
            "node.example.net",
            2135);
        UNIT_ASSERT_VALUES_EQUAL(
            url,
            "https://lbkx.example.net:8443/v1/5//Root/5/topic/8/consumer");
    }

    Y_UNIT_TEST(MakeQueueUrlFallsBackToFqdnAndHttpProxyPort) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic",
            .Consumer = "consumer",
            .Fifo = false,
        };
        const TString url = MakeQueueUrl(qu, "", "vla5-2135.example.net", 2135, true);
        UNIT_ASSERT_VALUES_EQUAL(
            url,
            "https://vla5-2135.example.net:2135/v1/5//Root/5/topic/8/consumer");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeQueueUrlEndpoint("", "vla5-2135.example.net", 2135, true),
            "https://vla5-2135.example.net:2135");
    }

    Y_UNIT_TEST(MakeQueueUrlFallbackUsesHttpWhenNotSecure) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic",
            .Consumer = "consumer",
            .Fifo = false,
        };
        UNIT_ASSERT_VALUES_EQUAL(
            MakeQueueUrl(qu, "", "localhost", 2135, false),
            "http://localhost:2135/v1/5//Root/5/topic/8/consumer");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeQueueUrlEndpoint("", "localhost", 2135, false),
            "http://localhost:2135");
    }

    Y_UNIT_TEST(MakeQueueUrlFallbackOmitsZeroPort) {
        TRichQueueUrl qu{
            .Database = "/Root",
            .TopicPath = "topic",
            .Consumer = "consumer",
            .Fifo = false,
        };
        UNIT_ASSERT_VALUES_EQUAL(
            MakeQueueUrl(qu, "", "vla5-2135.example.net", 0, true),
            "https://vla5-2135.example.net/v1/5//Root/5/topic/8/consumer");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeQueueUrlEndpoint("", "vla5-2135.example.net", 0, true),
            "https://vla5-2135.example.net");
        UNIT_ASSERT_VALUES_EQUAL(
            MakeQueueUrlEndpoint("", "localhost", 0, false),
            "http://localhost");
    }
}
