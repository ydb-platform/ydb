#include <kikimr/serverless_proxy/lib/driver_cache_actor.h>
#include <kikimr/serverless_proxy/lib/proxy_actor_system.h>

#include <ydb/core/http_proxy/http_req.h>
#include <kikimr/yndx/security/ticket_parser.h>
#include <kikimr/yndx/testlib/service_mocks/database_service_mock.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>

#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <library/cpp/json/json_reader.h>

#include <library/cpp/testing/unittest/registar.h>
#include <nlohmann/json.hpp>

#include <ydb/core/testlib/test_client.h>


using namespace NKikimr::NHttpProxy;
using namespace NKikimr::Tests;

using TResponseField = std::pair<int, TString>;

#include "datastreams_fixture.h"


class THttpExceptionsProxyTestMock : public THttpProxyTestMock {
public:
    static std::function<NJson::TJsonValue(void)> createRequest(std::string method) {
        if (method == "CreateStream")
            return [](){ return CreateCreateStreamRequest(); };
        if (method == "DeleteStream")
            return [](){ return CreateDeleteStreamRequest(); };
        if (method == "DescribeStream")
            return [](){ return CreateDescribeStreamRequest(); };
        if (method == "DescribeStreamSummary")
            return [](){ return CreateDescribeStreamSummaryRequest(); };
        if (method == "GetRecords")
            return [](){ return CreateGetRecordsRequest(); };
        if (method == "GetShardIterator")
            return [](){ return CreateGetShardIteratorRequest(); };
        if (method == "ListShards")
            return [](){ return CreateListShardsRequest(); };
        if (method == "PutRecords")
            return [](){ return CreatePutRecordsRequest(); };
        if (method == "RegisterStreamConsumer")
            return [](){ return CreateRegisterStreamConsumerRequest(); };
        if (method == "DeregisterStreamConsumer")
            return [](){ return CreateDeregisterStreamConsumerRequest(); };
        if (method == "ListStreamConsumers")
            return [](){ return CreateListStreamConsumersRequest(); };
        return [](){ return CreateCreateStreamRequest(); };
    }


    THttpResult CreateStreamForTesting(const TString& streamName="testtopic", size_t shardCount=5) {
        auto createReq = CreateCreateStreamRequest();
        createReq["streamName"] = streamName;
        createReq["ShardCount"] = shardCount;
        return SendHttpRequest("/Root", "kinesisApi.CreateStream", createReq, FormAuthorizationStr("ru-central1"));
    }

    THttpResult DeleteStreamForTesting(const TString& streamName="testtopic") {
        auto createReq = CreateDeleteStreamRequest();
        createReq["streamName"] = streamName;
        return SendHttpRequest("/Root", "kinesisApi.DeleteStream", createReq, FormAuthorizationStr("ru-central1"));
    }

    TResponseField getStringFromResponseField(const TString& methodName, const TString& fieldName, const TString& streamName="testtopic") {
        auto req = createRequest(methodName)();
        req["StreamName"] = streamName;
        auto res = SendHttpRequest("/Root", "kinesisApi." + methodName, req, FormAuthorizationStr("ru-central1"));
        if (res.HttpCode != 200) {
            return TResponseField(res.HttpCode, "");
        }

        NJson::TJsonValue bodyJson;
        NJson::ReadJsonTree(res.Body, &bodyJson);
        return TResponseField(res.HttpCode, GetByPath<TString>(bodyJson, fieldName));
    }
};


Y_UNIT_TEST_SUITE(CommonErrors) {

    std::vector<std::string> methods =
           { "DescribeStream", "DescribeStreamSummary", "GetRecords",
           "GetShardIterator", "ListShards", "PutRecords", "DeleteStream", "CreateStream",
           "RegisterStreamConsumer", "DeregisterStreamConsumer", "ListStreamConsumers" };

    Y_UNIT_TEST_F(AccessDeniedException, THttpExceptionsProxyTestMock) {
        TString streamName = "/Root/test";

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : methods) {
            auto record = createRequest(method)();
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record,
                                    "Authorization: XXXXXX unauthorized_user@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "AccessDeniedException");
        }
    }

    Y_UNIT_TEST_F(IncompleteSignature, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : methods) {
            auto record = createRequest(method)();

            TString authStr = TStringBuilder() <<
                "Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/ru-central1" <<
                "/service/aws4_request";
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, authStr);
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "IncompleteSignature");
        }
    }

    Y_UNIT_TEST_F(InvalidAction, THttpExceptionsProxyTestMock) {
        // get (UnknownOperationException, 400) instead of InvalidAction in Kinesis
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto record = CreateDescribeStreamRequest();
        res = SendHttpRequest("/Root", std::string("kinesisApi.DesribeStream"), record,
                                    FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidAction");  // UnknownOperationException in Kinesis

    }

    Y_UNIT_TEST_F(InvalidParameterCombination, THttpExceptionsProxyTestMock) {
        std::vector<std::string> local_methods = { "ListShards", "ListStreamConsumers" };  // To add more?
        std::map<std::string, std::string> streamDeterminators = {
                { "ListShards", "StreamName" },
                { "ListStreamConsumers", "StreamArn" }
            };

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (int i = 0; i < 5; i++) {
            auto req = CreateRegisterStreamConsumerRequest();
            req["StreamArn"] = "testtopic";
            req["ConsumerName"] = "Oleg" + std::to_string(i);
            res = SendHttpRequest("/Root", std::string("kinesisApi.RegisterStreamConsumer"), req,
                                FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        for (auto method : local_methods) {
            auto record = createRequest(method)();
            record["MaxResults"] = 1;
            record[streamDeterminators[method]] = "testtopic";
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            Cerr << res.Body << Endl;
            NJson::TJsonValue bodyJson;
            NJson::ReadJsonTree(res.Body, &bodyJson);
            auto nextToken = GetByPath<TString>(bodyJson, "NextToken");
            record = createRequest(method)();
            record[streamDeterminators[method]] = "testtopic";
            record["NextToken"] = nextToken;
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            Cerr << res.Body << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidParameterCombination");
        }

    }

    Y_UNIT_TEST_F(ValidationException, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods =
           { "DescribeStream", "DescribeStreamSummary", "GetRecords",
           "GetShardIterator", "ListShards", "PutRecords", "DeleteStream", "CreateStream" };
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto [httpCode, shardIterator] = getStringFromResponseField("GetShardIterator", "ShardIterator");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);

        for (auto& method : actualMethods) {
            auto record = createRequest(method)();

            if (method == "ListShards") {
                record["MaxResults"] = -30;
            }
            else if (method == "GetRecords") {
                record["Limit"] = -30;
                record["ShardIterator"] = shardIterator;
            }
            else if (method == "CreateStream") {
                record["ShardCount"] = -30;
            }
            else
                record["StreamName"] = "";
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ValidationException");
        }

    }

    Y_UNIT_TEST_F(ValidationExceptionConsumers, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "RegisterStreamConsumer", "DeregisterStreamConsumer", "ListStreamConsumers" };

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);


        for (auto& method : actualMethods) {
            auto record = createRequest(method)();
            if (method == "ListStreamConsumers") {
                record["MaxResults"] = -30;
            }
            else {
                record["StreamArn"] = "";
            }
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ValidationException");
        }

    }

    Y_UNIT_TEST_F(MissingAction, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        {
            auto record = CreateDescribeStreamRequest();
            res = SendHttpRequest("/Root", std::string("kinesisApi."), record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
        {
            auto record = CreateDescribeStreamRequest();
            res = SendHttpRequest("/Root", std::string("kinesisApi."), record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
        {
            auto record = CreateDescribeStreamRequest();
            res = SendHttpRequest("/Root", std::string("kinesisApi."), record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
        {
            auto record = CreateDescribeStreamRequest();
            res = SendHttpRequest("/Root", std::string("kinesisApi."), record,
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
    }

    Y_UNIT_TEST_F(MissingAuthenticationToken, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : methods) {
            auto record = createRequest(method)();

            TString authStr = TStringBuilder() <<
            "Authorization: AWS4-HMAC-SHA256 Credential=/20150830/ru-central1"
            "/service/aws4_request, SignedHeaders=host;x-amz-date, Signature="
            "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b)__";
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, authStr);
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAuthenticationToken");
        }
    }

    Y_UNIT_TEST_F(MissingParameter, THttpExceptionsProxyTestMock) {
        // get ValidationException in Kinesis
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : methods) {
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, NJson::TJsonValue(),
                                        FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingParameter");
        }
    }

}



Y_UNIT_TEST_SUITE(OtherErrors) {

    Y_UNIT_TEST_F(InvalidArgumentException, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        auto record = NJson::TJsonValue();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        {
            std::vector<std::string> actualMethods = { "GetShardIterator", "RegisterStreamConsumer" };  // +, "DeregisterStreamConsumer" - how?
            std::map<std::string, std::vector<std::pair<std::string, std::string>>> invalidArguments = {
                { "GetShardIterator", { { "ShardIteratorType", "ИНВЭЛИД" } } },
                { "RegisterStreamConsumer", { { "StreamArn", "testtopic" }, { "ConsumerName", "I/N|V/A|L/I|D" } } },
                { "DeregisterStreamConsumer", { { "StreamArn", "testtopic" }, { "ConsumerName", "I/N|V/A|L/I|D" } } }
            };

            for (auto method : actualMethods) {
                record = createRequest(method)();
                for (auto& [tag, value] : invalidArguments[method]) {
                    record[tag] = value;
                }
                res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
                UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
                Cerr << method << "\n" << res.Description << Endl;
                UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
            }
        }
        {
            record = CreateListStreamConsumersRequest();
            record["NextToken"] = "$$%^&*токен";
            record["StreamArn"] = "";
            res = SendHttpRequest("/Root", std::string("kinesisApi.ListStreamConsumers"), record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            Cerr << res.Description << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
        }
        {
            record = CreateCreateStreamRequest();
            record["StreamModeDetails"] = NJson::TJsonValue(NJson::JSON_MAP);
            record["StreamModeDetails"]["StreamMode"] = "ИНВЭЛИД";
            res = SendHttpRequest("/Root", std::string("kinesisApi.CreateStream"), record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            Cerr << res.Description << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
        }
        {
            record = CreatePutRecordsRequest();
            record["StreamName"] = "/\n\t\r/|:)";

            res = SendHttpRequest("/Root", std::string("kinesisApi.PutRecords"), record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            Cerr << res.Description << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");

        }
        {
            // Get ValidationException in Kinesis
            record = CreateGetRecordsRequest();
            record["ShardIterator"] = "ИНВЭЛИД";
            res = SendHttpRequest("/Root", std::string("kinesisApi.GetRecords"), record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");  // ValidationException in Kinesis
        }
    }

    Y_UNIT_TEST_F(LimitExceededException, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "DescribeStream", "ListShards", "DescribeStreamSummary" };  // + DeleteStream ?

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : actualMethods) {
            auto req = createRequest(method)();
            // auto start = TInstant::Now();
            for (int i = 0; i < 100; i++) {
                res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, req, FormAuthorizationStr("ru-central1"));
                if (res.HttpCode != 200) {
                    break;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            // It has to lead to the error but it does not really, even in AWS Kinesis
            // UNIT_ASSERT_VALUES_EQUAL(res.Description, "LimitExceededException");
        }
    }

    Y_UNIT_TEST_F(LimitExceededExceptionConsumers, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "RegisterStreamConsumer", "ListStreamConsumers", "DeregisterStreamConsumer" };
        std::string baseConsumerName = "Oleg";

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        auto [httpCode, streamArn] = getStringFromResponseField("DescribeStream", "StreamDescription.StreamArn");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);

        for (auto method : actualMethods) {
            auto req = createRequest(method)();
            req["StreamArn"] = streamArn;
            for (int i = 0; i < 100; i++) {
                if (method != "ListStreamConsumers") {
                    req["ConsumerName"] = baseConsumerName + std::to_string(i);
                }
                res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, req, FormAuthorizationStr("ru-central1"));
                if (res.HttpCode != 200) {
                    break;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            // In AWS this leads to the error, but in YDS it is OK
            // UNIT_ASSERT_VALUES_EQUAL(res.Description, "LimitExceededException");
        }
    }

    Y_UNIT_TEST_F(LimitExceededExceptionCreateStream, THttpExceptionsProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["ShardCount"] = 2000000;

        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "LimitExceededException");
    }

    Y_UNIT_TEST_F(ResourceInUseExceptionCreate, THttpExceptionsProxyTestMock) {
        // std::vector<std::string> actualMethods = { "ListShards", "DeleteStream", "CreateStream", "RegisterStreamConsumer", "ListStreamConsumers" };
        // std::vector<std::string> actualMethods = { "DeleteStream" };
        std::vector<std::string> actualMethods = { "CreateStream" };

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : actualMethods) {
            auto record = createRequest(method)();
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceInUseException");
        }

    }

    Y_UNIT_TEST_F(ResourceInUseExceptionConsumers, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "RegisterStreamConsumer" };  // , "ListStreamConsumers" ??

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto [httpCode, streamArn] = getStringFromResponseField("DescribeStream", "StreamDescription.StreamArn");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);

        auto req = CreateRegisterStreamConsumerRequest();
        req["ConsumerName"] = "Oleg";
        req["StreamArn"] = streamArn;
        res = SendHttpRequest("/Root", std::string("kinesisApi.RegisterStreamConsumer"), req, FormAuthorizationStr("ru-central1"));

        for (auto method : actualMethods) {
            req = createRequest(method)();
            req["ConsumerName"] = "Oleg";
            req["StreamArn"] = streamArn;
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, req, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceInUseException");
        }
    }

    Y_UNIT_TEST_F(ResourceInUseExceptionDelete, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto record = CreateRegisterStreamConsumerRequest();
        auto [httpCode, streamArn] = getStringFromResponseField("DescribeStream", "StreamDescription.StreamArn");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);
        record["StreamArn"] = streamArn;
        record["ConsumerName"] = "Oleg";
        res = SendHttpRequest("/Root", std::string("kinesisApi.RegisterStreamConsumer"), record, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        record = CreateDeleteStreamRequest();
        res = SendHttpRequest("/Root", std::string("kinesisApi.DeleteStream"), record, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceInUseException");
    }

    Y_UNIT_TEST_F(ResourceNotFoundException, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "GetShardIterator", "DescribeStream", "ListShards",
                                                     "DescribeStreamSummary", "PutRecords", "DeleteStream" };
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : actualMethods) {
            auto record = createRequest(method)();
            record["StreamName"] = "Completely_new_stream_nAmE";
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceNotFoundException");
        }
    }

    Y_UNIT_TEST_F(ResourceNotFoundExceptionGetRecords, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto [httpCode, shardIterator] = getStringFromResponseField("GetShardIterator", "ShardIterator");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);

        auto record = CreateDeleteStreamRequest();
        res = SendHttpRequest("/Root", std::string("kinesisApi.DeleteStream"), record, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        record = CreateGetRecordsRequest();
        record["ShardIterator"] = shardIterator;
        res = SendHttpRequest("/Root", std::string("kinesisApi.GetRecords"), record, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceNotFoundException");
    }

    Y_UNIT_TEST_F(ResourceNotFoundExceptionConsumers, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "RegisterStreamConsumer", "ListStreamConsumers", "DeregisterStreamConsumer" };

        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto [httpCode, streamArn] = getStringFromResponseField("DescribeStream", "StreamDescription.StreamArn");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);

        res = DeleteStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto method : actualMethods) {
            auto record = createRequest(method)();
            record["StreamArn"] = streamArn;
            if (method != "ListStreamConsumers") {
                record["ConsumerName"] = "Oleg";
            }

            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceNotFoundException");
        }
    }

    Y_UNIT_TEST_F(ExpiredIteratorException, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "GetRecords" };
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto [httpCode, shardIterator] = getStringFromResponseField("GetShardIterator", "ShardIterator");
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200);

        NKikimrPQ::TYdsShardIterator protoShardIterator;
        TString decoded;
        Base64Decode(shardIterator, decoded);
        Y_ABORT_UNLESS(protoShardIterator.ParseFromString(decoded));

        protoShardIterator.SetCreationTimestampMs((TInstant::Now() -  TDuration::Seconds(500)).MilliSeconds());
        Y_ABORT_UNLESS(protoShardIterator.SerializeToString(&decoded));

        Base64Encode(decoded, shardIterator);

        for (auto method : actualMethods) {
            auto record = createRequest(method)();
            record["ShardIterator"] = shardIterator;
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ExpiredIteratorException");
        }

    }

    Y_UNIT_TEST_F(MalformedIteratorGetRecords, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting("testtopic", 10);
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto record = CreateGetRecordsRequest();
        record["ShardIterator"] = "Invalid";
        res = SendHttpRequest("/Root", "kinesisApi.GetRecords", record, FormAuthorizationStr("ru-central1"));

        Cerr << res.Body << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
    }

    Y_UNIT_TEST_F(ExpiredNextTokenException, THttpExceptionsProxyTestMock) {
        std::vector<std::string> actualMethods = { "ListShards", "ListStreamConsumers" };
        std::map<std::string, std::string> streamIdentifiers = {
            { "ListShards", "StreamName" },
            { "ListStreamConsumers", "StreamArn" }
         };
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto record = CreateListShardsRequest();
        record["StreamName"] = "testtopic";
        record["MaxResults"] = 2;
        res = SendHttpRequest("/Root", "kinesisApi.ListShards", record, FormAuthorizationStr("ru-central1"));

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue bodyJson;
        NJson::ReadJsonTree(res.Body, &bodyJson);
        auto nextToken = GetByPath<TString>(bodyJson, "NextToken");

        NKikimrPQ::TYdsNextToken protoNextToken;
        TString decoded;
        Base64Decode(nextToken, decoded);
        Cerr << "NEXT TOKEN: " << nextToken << "; DECODED: " << decoded << Endl;
        Y_ABORT_UNLESS(protoNextToken.ParseFromString(decoded));

        protoNextToken.SetCreationTimestamp((TInstant::Now() - TDuration::Seconds(500)).MilliSeconds());
        Y_ABORT_UNLESS(protoNextToken.SerializeToString(&decoded));

        Base64Encode(decoded, nextToken);

        for (auto method : actualMethods) {
            record = createRequest(method)();
            record["NextToken"] = nextToken;
            record[streamIdentifiers[method]] = "";
            res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ExpiredNextTokenException");
        }
    }

}


Y_UNIT_TEST_SUITE(AuxiliaryTests) {
    Y_UNIT_TEST_F(StreamStatus, THttpExceptionsProxyTestMock) {
        TString streamName = "testtopic";
        auto res = CreateStreamForTesting();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto record = CreateDescribeStreamSummaryRequest();
        res = SendHttpRequest("/Root", std::string("kinesisApi.DescribeStreamSummary"), record, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        // UNIT_ASSERT_VALUES_EQUAL(res.Body["StreamDescriptionSummary"]["StreamStatus"], "ACTIVE");
    }

    Y_UNIT_TEST_F(ListStreamConsumersOK, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting();
        auto record = NJson::TJsonValue();
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        {
            std::vector<std::string> actualMethods = { "RegisterStreamConsumer", "ListStreamConsumers" };
            std::map<std::string, std::vector<std::pair<std::string, std::string>>> invalidArguments = {
                { "RegisterStreamConsumer", { { "StreamArn", "testtopic" }, { "ConsumerName", "ИНВЭЛИД" } } },
                { "ListStreamConsumers", { { "StreamArn", "testtopic" } } }
            };

            for (auto method : actualMethods) {
                record = createRequest(method)();
                for (auto& [tag, value] : invalidArguments[method]) {
                    record[tag] = value;
                }
                res = SendHttpRequest("/Root", std::string("kinesisApi.") + method, record, FormAuthorizationStr("ru-central1"));
                UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
                Cerr << method << "\n" << res.Description << Endl;
            }
        }
    }

    Y_UNIT_TEST_F(ListStreamConsumersBadMaxResults, THttpExceptionsProxyTestMock) {
        auto res = CreateStreamForTesting("testtopic", 10);
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto record = CreateListShardsRequest();
        record["StreamName"] = "testtopic";
        record["MaxResults"] = 2;
        res = SendHttpRequest("/Root", "kinesisApi.ListShards", record, FormAuthorizationStr("ru-central1"));

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue bodyJson;
        NJson::ReadJsonTree(res.Body, &bodyJson);
        auto nextToken = GetByPath<TString>(bodyJson, "NextToken");

        NKikimrPQ::TYdsNextToken protoNextToken;
        TString decoded;
        Base64Decode(nextToken, decoded);
        Y_ABORT_UNLESS(protoNextToken.ParseFromString(decoded));
        Cerr << "NEXT TOKEN: " << protoNextToken << ";" << Endl;

        record = CreateListShardsRequest();
        record["NextToken"] = nextToken;
        record["StreamName"] = "testtopic";
        record["MaxResults"] = "100";
        res = SendHttpRequest("/Root", "kinesisApi.ListShards", record, FormAuthorizationStr("ru-central1"));

        Cerr << res.Body << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

}
