
#include <ydb/core/http_proxy/ut/datastreams_fixture/datastreams_fixture.h>

#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/ymq/actor/metering.h>
#include <ydb/core/ymq/base/limits.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/url/url.h>

using namespace NKikimr::NHttpProxy;
using namespace NKikimr::Tests;
using namespace NActors;


using TFixture = THttpProxyTestMockForSQSTopic;


static TString GetPathFromQueueUrlMap(const NJson::TJsonMap& json) {
    TString url =  GetByPath<TString>(json, "QueueUrl");
    auto [host, path] = NUrl::SplitUrlToHostAndPath(url);
    return ToString(path);
}

Y_UNIT_TEST_SUITE(TestSqsTopicHttpProxy) {

    Y_UNIT_TEST_F(TestGetQueueUrlEmpty, TFixture) {
        if ("X-Fail") {
            return;
        }
        auto json = GetQueueUrl({}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");
    }

   Y_UNIT_TEST_F(TestGetQueueUrl, TFixture) {
        const TString queueUrl = "/v1/5//Root/16/ExampleQueueName/16/ydb-sqs-consumer";
        auto queueName = "ExampleQueueName";
        auto json = GetQueueUrl({{"QueueName", queueName}});
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

        // ignore QueueOwnerAWSAccountId parameter.
        json = GetQueueUrl({{"QueueName", queueName}, {"QueueOwnerAWSAccountId", "some-account-id"}});
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

        json = GetQueueUrl({{"QueueName", queueName}, {"WrongParameter", "some-value"}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidArgumentException");
    }

    Y_UNIT_TEST_F(TestGetQueueUrlOfNotExistingQueue, TFixture) {
        if ("X-Fail") {
            return;
        }
        auto json = GetQueueUrl({{"QueueName", "not-existing-queue"}}, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
        TString resultMessage = GetByPath<TString>(json, "message");
        UNIT_ASSERT_VALUES_EQUAL(resultMessage, "The specified queue doesn't exist.");
    }

    Y_UNIT_TEST_F(TestGetQueueUrlWithConsumer, TFixture) {
        const TString consumer = "user_consumer";
        const TString queueName = "ExampleQueueName";
        const TString queueUrl = "/v1/5//Root/16/ExampleQueueName/13/user_consumer";
        const TString requestQueueName = queueName + "@" + consumer;
        auto json = GetQueueUrl({
            {"QueueName", requestQueueName},
        });
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));
    }

} // Y_UNIT_TEST_SUITE(TestYmqHttpProxy)
