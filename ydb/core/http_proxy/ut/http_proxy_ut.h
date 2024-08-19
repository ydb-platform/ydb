#pragma once

#include "library/cpp/json/writer/json_value.h"
#include "library/cpp/testing/unittest/registar.h"

#include <chrono>
#include <thread>

extern TString Name_;
extern bool ForceFork_;
extern TString FormAuthorizationStr(const TString& region);
extern NJson::TJsonValue CreateCreateStreamRequest();
extern NJson::TJsonValue CreateDescribeStreamRequest();
extern NJson::TJsonValue CreateSqsGetQueueUrlRequest();
extern NJson::TJsonValue CreateSqsCreateQueueRequest();
extern struct THttpResult httpResult;

extern THttpResult SendHttpRequest(
        const TString& handler,
        const TString& target,
        NJson::TJsonValue value,
        const TString& authorizationStr,
        const TString& contentType = "application/json"
);


Y_UNIT_TEST_SUITE(TestHttpProxy) {

    Y_UNIT_TEST_F(CreateStreamInIncorrectDb, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root1", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                   FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        TString expectedBody("{\"__type\":\"ResourceNotFoundException\",\"message\":\"Database with path '/Root1' doesn't exists\"}");
        UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceNotFoundException");
    }

    Y_UNIT_TEST_F(CreateStreamWithInvalidName, THttpProxyTestMock) {
        auto record = CreateCreateStreamRequest();
        record["StreamName"] = 10;
        auto res = SendHttpRequest("/Root", std::string("kinesisApi.CreateStream"), record, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        Cerr << res.Description << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
    }

    Y_UNIT_TEST_F(MissingAction, THttpProxyTestMock) {
        auto request = CreateCreateStreamRequest();
        request["StreamName"] = "teststream";
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                    FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        {
            request = CreateDescribeStreamRequest();
            request["StreamName"] = "teststream";
            res = SendHttpRequest("/Root", "kinesisApi.", request, FormAuthorizationStr("ru-central-1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
        {
            request = CreateDescribeStreamRequest();
            request["StreamName"] = "teststream";
            res = SendHttpRequest("/Root", ".", request, FormAuthorizationStr("ru-central-1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
        {
            request = CreateDescribeStreamRequest();
            request["StreamName"] = "teststream";
            res = SendHttpRequest("/Root", "", request, FormAuthorizationStr("ru-central-1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
        {
            request = CreateDescribeStreamRequest();
            request["StreamName"] = "teststream";
            res = SendHttpRequest("/Root", ".DescribeStream", request, FormAuthorizationStr("ru-central-1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingAction");
        }
    }

    Y_UNIT_TEST_F(CreateStreamWithDifferentRetentions, THttpProxyTestMock) {
        {
            auto request = CreateCreateStreamRequest();
            request["StreamName"] = "stream-1";
            request["RetentionPeriodHours"] = 24;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
        {
            auto request = CreateCreateStreamRequest();
            request["StreamName"] = "stream-2";
            request["RetentionPeriodHours"] = 1;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
        {
            auto request = CreateCreateStreamRequest();
            request["StreamName"] = "stream-3";
            request["RetentionPeriodHours"] = 56;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            const TString expectedBody("{\"__type\":\"ValidationException\",\"message\":\"<main>: Error: retention hours and storage megabytes must fit one of: { hours : [0, 24],  storage : [0, 0]}, { hours : [0, 168],  storage : [51200, 1048576]}, provided values: hours 56, storage 0, code: 500080\\n\"}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ValidationException");
        }
        {
            auto request = CreateCreateStreamRequest();
            request["StreamName"] = "stream-4";
            request["RetentionStorageMegabytes"] = 50_GB / 1_MB;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
        {
            auto request = CreateCreateStreamRequest();
            request["StreamName"] = "stream-3";
            request["RetentionStorageMegabytes"] = 45_GB / 1_MB;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            const TString expectedBody("{\"__type\":\"ValidationException\",\"message\":\"<main>: Error: retention hours and storage megabytes must fit one of: { hours : [0, 24],  storage : [0, 0]}, { hours : [0, 168],  storage : [51200, 1048576]}, provided values: hours 168, storage 46080, code: 500080\\n\"}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ValidationException");
        }
        {
            auto request = CreateCreateStreamRequest();
            request["StreamName"] = "stream-5";
            request["RetentionPeriodHours"] = 12;
            request["RetentionStorageMegabytes"] = 50_GB / 1_MB;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", request,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(DifferentContentTypes, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req,
                                       FormAuthorizationStr("ru-central1"), "application/bson");
            const TString expectedBody("{\"__type\":\"InvalidArgumentException\",\"message\":\"Unknown ContentType\"}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            req["StreamName"] = "/Root/testtopic_json";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req,
                                       FormAuthorizationStr("ru-central1"), "application/json");
            const TString expectedBody("{}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req,
                                       FormAuthorizationStr("ru-central1"), "application/cbor");
            const TString expectedBody("\242f__typex\x18InvalidArgumentExceptiongmessagex$Can not parse request body from CBOR");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            auto cbor = nlohmann::json::to_cbor(
                "{\"ShardCount\":5,\"StreamName\":\"/Root/testtopic_cbor\"}"_json
            );
            auto res = SendHttpRequestRaw("/Root", "kinesisApi.CreateStream", {cbor.data(), cbor.size()},
                                          FormAuthorizationStr("ru-central1"), "application/cbor");
            const TString expectedBody("\xA0");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto cbor = nlohmann::json::to_cbor(
                "{\"ShardCount\":5,\"StreamName\":\"/Root/testtopic_cbor\"}"_json
            );
            auto res = SendHttpRequestRaw("/Root", "kinesisApi.CreateStream", {cbor.data(), cbor.size()},
                                          FormAuthorizationStr("ru-central1"), "application/cbor");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            TStringBuf resBody(res.Body);
            auto fromCbor = nlohmann::json::from_cbor(resBody.begin(), resBody.end(), true, false);
            UNIT_ASSERT(!fromCbor.is_discarded());
            TString body{fromCbor.dump()};
            UNIT_ASSERT_VALUES_EQUAL(body,
                TString("{\"__type\":\"ResourceInUseException\",\"message\":\"<main>: "
                        "Error: Stream with name /Root/testtopic_cbor already exists, code: "
                        "500070\\n\"}"));
        }

        {
            req["StreamName"] = "/Root/testtopic_cbor_2";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req,
                                       FormAuthorizationStr("ru-central1"), "");
            const TString expectedBody("{\"__type\":\"InvalidArgumentException\",\"message\":\"Unknown ContentType\"}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "InvalidArgumentException");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            req["StreamName"] = "/Root/testtopic_json_v10";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req,
                                       FormAuthorizationStr("ru-central1"), "application/x-amz-json-1.0");
            const TString expectedBody("{}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            req["StreamName"] = "/Root/testtopic_json_v11";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", req,
                                       FormAuthorizationStr("ru-central1"), "application/x-amz-json-1.1");
            const TString expectedBody("{}");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto cbor = nlohmann::json::to_cbor(
                "{\"ShardCount\":5,\"StreamName\":\"/Root/testtopic_cbor_v11\"}"_json
            );
            auto res = SendHttpRequestRaw("/Root", "kinesisApi.CreateStream", {cbor.data(), cbor.size()},
                                          FormAuthorizationStr("ru-central1"), "application/x-amz-cbor-1.1");
            const TString expectedBody("\xA0");
            UNIT_ASSERT_VALUES_EQUAL(res.Body, expectedBody);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(GoodRequestPutRecords, THttpProxyTestMock) {
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto req = CreatePutRecordsRequest();
            auto res = SendHttpRequest("/Root", "kinesisApi.PutRecords", req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<i64>(json, "FailedRecordCount"), 0);
        }
    }

    Y_UNIT_TEST_F(PutRecordsWithLongExplicitHashKey, THttpProxyTestMock) {
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto req = CreatePutRecordsRequest();
            req["Records"].Back()["ExplicitHashKey"] = TString(38, '1');
            auto res = SendHttpRequest("/Root", "kinesisApi.PutRecords", req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<i64>(json, "FailedRecordCount"), 0);
        }
    }

    Y_UNIT_TEST_F(PutRecordsWithIncorrectHashKey, THttpProxyTestMock) {
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto req = CreatePutRecordsRequest();
            req["Records"].Back()["ExplicitHashKey"] = TString(40, '1');
            auto res = SendHttpRequest("/Root", "kinesisApi.PutRecords", req,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }
    }

    Y_UNIT_TEST_F(CreateDeleteStream, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        res = SendHttpRequest("/Root", "kinesisApi.DeleteStream", CreateDeleteStreamRequest(), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
    }

    Y_UNIT_TEST_F(DoubleCreateStream, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                   FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                              FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(CreateDeleteStreamWithConsumer, THttpProxyTestMock) {
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto req = CreateRegisterStreamConsumerRequest();
            req["StreamArn"] = "testtopic";
            req["ConsumerName"] = "user1";
            auto res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer",
                                       req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto res = SendHttpRequest("/Root", "kinesisApi.DeleteStream", CreateDeleteStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            auto req = CreateDeregisterStreamConsumerRequest();
            req["StreamArn"] = "testtopic";
            req["ConsumerName"] = "user1";
            auto res = SendHttpRequest("/Root", "kinesisApi.DeregisterStreamConsumer",
                                       req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto res = SendHttpRequest("/Root", "kinesisApi.DeleteStream", CreateDeleteStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(CreateDeleteStreamWithConsumerWithFlag, THttpProxyTestMock) {
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto req = CreateRegisterStreamConsumerRequest();
            req["StreamArn"] = "testtopic";
            req["ConsumerName"] = "user1";
            auto res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer",
                                       req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto req = CreateDeleteStreamRequest();
            req["EnforceConsumerDeletion"] = false;
            auto res = SendHttpRequest("/Root", "kinesisApi.DeleteStream", req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            auto req = CreateDeleteStreamRequest();
            req["EnforceConsumerDeletion"] = true;
            auto res = SendHttpRequest("/Root", "kinesisApi.DeleteStream", req,
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(GoodRequestGetRecords, THttpProxyTestMock) {
        const TString streamName{"put-get-stream"};
        auto createRequest = CreateCreateStreamRequest();
        createRequest["ShardCount"] = 1;
        createRequest["StreamName"] = streamName;
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", createRequest,
                                   FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        TVector<TString> put_data;
        for (auto i : {1, 2, 3, 4, 5}) {
            Y_UNUSED(i);
            auto record = CreatePutRecordsRequest();
            auto data = Base64Encode("data" + std::to_string(i) + TString(200000, 'a'));
            put_data.push_back(data);
            record["Records"].Back()["Data"] = data;
            record["StreamName"] = streamName;
            res = SendHttpRequest("/Root", "kinesisApi.PutRecords", record, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<i64>(json, "FailedRecordCount"), 0);
        }

        auto sendShardIteratorRequest = [this](const auto& getShardIteratorRequest) {
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator",
                                       getShardIteratorRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "ShardIterator").empty(), false);
            return json;
        };

        auto sendGetRecordsRequest = [this](const auto& getRecordsRequest) {
            auto res = SendHttpRequest("/Root", "kinesisApi.GetRecords",
                                       getRecordsRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            return json;
        };

        // Read LATEST
        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "LATEST";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(json.Has("Records"), false);
        }

        // Read AT_SEQUENCE_NUMBER
        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AT_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "00000";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 5);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AT_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "00001";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 4);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AT_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "999999";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(json.Has("Records"), false);
        }

        // Read AFTER_SEQUENCE_NUMBER
        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AFTER_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "00000";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 4);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AFTER_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "00001";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 3);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AFTER_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "99999";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(json.Has("Records"), false);
        }

        // Read TRIM_HORIZON
        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "TRIM_HORIZON";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 5);
            i32 i = 0;
            for (const auto& recordData : GetByPath<TJVector>(json, "Records")) {
                UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(recordData, "Data"), put_data[i]);
                ++i;
            }
        }

        // Read AT_TIMESTAMP
        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AT_TIMESTAMP";
            getShardIteratorRequest["Timestamp"] = TInstant::Now().MilliSeconds()/1000 - 1000;
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 5);
            i32 i = 0;
            for (const auto& recordData : GetByPath<TJVector>(json, "Records")) {
                UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(recordData, "Data"), put_data[i]);
                ++i;
            }

            getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AT_TIMESTAMP";
            getShardIteratorRequest["Timestamp"] = TInstant::Now().MilliSeconds()/1000 + 1000;
            getShardIteratorRequest["StreamName"] = streamName;
            res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator",
                                  getShardIteratorRequest,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }
    }

    Y_UNIT_TEST_F(GoodRequestGetRecordsCbor, THttpProxyTestMock) {
        const TString streamName{"put-get-cbor-stream"};
        auto createRequest = CreateCreateStreamRequest();
        createRequest["ShardCount"] = 1;
        createRequest["StreamName"] = streamName;
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", createRequest,
                                   FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        std::vector<std::string> put_data;
        for (char i : {1, 2, 3, 4, 5}) {
            std::string msg = {'{','"','d','a','t','a','"',':','"','m','e','s','s','a','g','e','#',i,'"','}'};
            put_data.push_back(msg);
            nlohmann::json data;
            data["Data"] = nlohmann::json::binary({msg.begin(), msg.end()});
            data["PartitionKey"] = "x";
            nlohmann::json record;
            record["StreamName"] = streamName;
            record["Records"] = nlohmann::json::array({data});
            auto cbor = nlohmann::json::to_cbor(record);
            res = SendHttpRequestRaw("/Root", "kinesisApi.PutRecords", {cbor.data(), cbor.size()},
                                     FormAuthorizationStr("ru-central1"), "application/x-amz-cbor-1.1");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        auto sendShardIteratorRequest = [this](const auto& getShardIteratorRequest) {
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator",
                                       getShardIteratorRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "ShardIterator").empty(), false);
            return json;
        };

        auto sendGetRecordsRequest = [this](const auto& getRecordsRequest) {
            auto res = SendHttpRequest("/Root", "kinesisApi.GetRecords",
                                       getRecordsRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            return json;
        };

        // Read TRIM_HORIZON
        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "TRIM_HORIZON";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 5);
            i32 i = 0;
            for (const auto& recordData : GetByPath<TJVector>(json, "Records")) {
                TString saved{put_data[i].begin(), put_data[i].end()};
                TString received = Base64Decode(GetByPath<TString>(recordData, "Data"));
                UNIT_ASSERT_VALUES_EQUAL(saved, received);
                ++i;
            }
        }

        auto sendGetRecordsRequestCbor = [this](const auto& getRecordsRequest) {
            auto cbor =
                    nlohmann::json::to_cbor(nlohmann::json::parse(NJson::WriteJson(getRecordsRequest, false)));

            auto res = SendHttpRequestRaw("/Root", "kinesisApi.GetRecords",
                                          {cbor.data(), cbor.size()},
                                          FormAuthorizationStr("ru-central1"), "application/x-amz-cbor-1.1");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            TStringBuf requestStr = res.Body;
            auto fromCbor = nlohmann::json::from_cbor(requestStr.begin(), requestStr.end(),
                                                      true, false,
                                                      nlohmann::json::cbor_tag_handler_t::ignore);
            UNIT_ASSERT(!fromCbor.is_discarded());
            return fromCbor;
        };

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "TRIM_HORIZON";
            getShardIteratorRequest["StreamName"] = streamName;
            auto yajson = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(yajson, "ShardIterator");
            auto json = sendGetRecordsRequestCbor(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(json["Records"].size(), 5);
            i32 i = 0;
            for (const auto& recordData : json["Records"]) {
                UNIT_ASSERT(recordData["Data"].is_binary());
                TString saved{put_data[i].begin(), put_data[i].end()};
                TString received{(char*)&recordData["Data"].get_binary()[0],
                                 recordData["Data"].get_binary().size()};
                UNIT_ASSERT_VALUES_EQUAL(saved, received);
                ++i;
            }
        }

    }

    Y_UNIT_TEST_F(GoodRequestGetRecordsLongStreamName, THttpProxyTestMock) {
        const TString streamName = "/Root/whatever";
        auto createRequest = CreateCreateStreamRequest();
        createRequest["StreamName"] = streamName;
        createRequest["ShardCount"] = 1;
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", createRequest, FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        TVector<TString> put_data;
        for (auto i : {1, 2, 3, 4, 5}) {
            auto record = CreatePutRecordsRequest();
            auto data = Base64Encode("data" + std::to_string(i));
            put_data.push_back(data);
            record["Records"].Back()["Data"] = data;
            record["StreamName"] = streamName;
            res = SendHttpRequest("/", "kinesisApi.PutRecords", record, FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        NJson::TJsonValue json;
        // Read TRIM_HORIZON
        auto getShardIteratorRequest = CreateGetShardIteratorRequest();
        getShardIteratorRequest["StreamName"] = streamName;
        getShardIteratorRequest["ShardIteratorType"] = "TRIM_HORIZON";
        res = SendHttpRequest("/", "kinesisApi.GetShardIterator", getShardIteratorRequest, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(!GetByPath<TString>(json, "ShardIterator").empty());

        auto getRecordsRequest = CreateGetRecordsRequest();
        std::vector<NJson::TJsonValue> records;
        records.reserve(5);
        getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
        while (true) {
            res = SendHttpRequest("/", "kinesisApi.GetRecords", getRecordsRequest, FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            if (json.Has("Records")) {
                for (const auto& r : GetByPath<TJVector>(json, "Records")) {
                    records.push_back(r);
                }
            } else {
                break;
            }
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "NextShardIterator");
            getRecordsRequest["Limit"] = 2;
        }

        UNIT_ASSERT_VALUES_EQUAL(records.size(), 5);
        i32 i = 0;
        for (const auto& recordData : records) {
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(recordData, "Data"), put_data[i]);
            ++i;
        }
    }

    Y_UNIT_TEST_F(ErroneousRequestGetRecords, THttpProxyTestMock) {
        auto createRequest = CreateCreateStreamRequest();
        createRequest["ShardCount"] = 1;
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", createRequest,
                                   FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (auto i : {1, 2, 3, 4, 5}) {
            Y_UNUSED(i);
            auto record = CreatePutRecordsRequest();
            auto data = Base64Encode("data" + std::to_string(i));
            record["Records"].Back()["Data"] = data;
            res = SendHttpRequest("/Root", "kinesisApi.PutRecords", record,
                                  FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<i64>(json, "FailedRecordCount"), 0);
        }

        NJson::TJsonValue json;
        // Read LATEST
        auto getShardIteratorRequest = CreateGetShardIteratorRequest();
        getShardIteratorRequest["ShardIteratorType"] = "LATEST";
        res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator", getShardIteratorRequest,
                              FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(!GetByPath<TString>(json, "ShardIterator").empty());


        auto shardIterator = GetByPath<TString>(json, "ShardIterator");
        auto getRecordsRequest = CreateGetRecordsRequest();
        {
            getRecordsRequest["ShardIterator"] = shardIterator;
            getRecordsRequest["Limit"] = 0;
            res = SendHttpRequest("/Root", "kinesisApi.GetRecords", getRecordsRequest,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(json.Has("Records"), false);
        }

        {
            getRecordsRequest["Limit"] = -1;
            res = SendHttpRequest("/Root", "kinesisApi.GetRecords", getRecordsRequest,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_UNEQUAL(res.HttpCode, 200);
        }

        {
            getRecordsRequest["Limit"] = 100000;
            res = SendHttpRequest("/Root", "kinesisApi.GetRecords", getRecordsRequest,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_UNEQUAL(res.HttpCode, 200);
        }

        {
            getRecordsRequest["ShardIterator"] = shardIterator + "garbage";
            getRecordsRequest["Limit"] = 0;
            res = SendHttpRequest("/Root", "kinesisApi.GetRecords", getRecordsRequest,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_UNEQUAL(res.HttpCode, 200);
        }

        {
            getRecordsRequest["ShardIterator"] = "$YhxeANhDeX$$*()[}!!";
            getRecordsRequest["Limit"] = 0;
            res = SendHttpRequest("/Root", "kinesisApi.GetRecords", getRecordsRequest,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_UNEQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(UnauthorizedGetShardIteratorRequest, THttpProxyTestMock) {
        {
            auto createRequest = CreateCreateStreamRequest();
            createRequest["ShardCount"] = 1;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", createRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "LATEST";
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator", getShardIteratorRequest,
                                       "X-YaCloud-SubjectToken: unauthorized_user@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "LATEST";
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator", getShardIteratorRequest,
                                       "X-YaCloud-SubjectToken: sa_proxy@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "LATEST";
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator", getShardIteratorRequest,
                                       "Authorization: XXXXXX unauthorized_user@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        }

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "LATEST";
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator", getShardIteratorRequest,
                                       "Authorization: Bearer proxy_sa@builtin"); //TODO: no Bearer
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(GoodRequestCreateStream, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.DescribeStream", CreateDescribeStreamRequest(),
                                       FormAuthorizationStr("ru-central-1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            const auto& shards = GetByPath<TJVector>(json, "StreamDescription.Shards");
            UNIT_ASSERT_VALUES_EQUAL(shards[0]["SequenceNumberRange"].Has("StartingSequenceNumber"), true);
            UNIT_ASSERT_VALUES_EQUAL(shards[0]["SequenceNumberRange"].Has("EndingSequenceNumber"), false);
            UNIT_ASSERT_VALUES_EQUAL(json["StreamDescription"]["StreamModeDetails"].Has("StreamMode"), true);
            UNIT_ASSERT_VALUES_EQUAL(json["StreamDescription"]["StreamModeDetails"]["StreamMode"].GetStringSafe(), "ON_DEMAND");
        }
        {
            res = SendHttpRequest("/Root", "kinesisApi.DescribeStreamSummary", CreateDescribeStreamSummaryRequest(),
                                  FormAuthorizationStr("ru-central-1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        }
        {
            auto describeReq = CreateDescribeStreamRequest();
            auto res = SendHttpRequest("/Root", "kinesisApi.DescribeStream", std::move(describeReq), FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            auto streamArn = GetByPath<TString>(json, "StreamDescription.StreamArn");
            UNIT_ASSERT_VALUES_EQUAL(streamArn, "testtopic");
        }
    }

    Y_UNIT_TEST_F(TestRequestWithWrongRegion, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                   FormAuthorizationStr("aliaska-2"));
        Cerr << res.HttpCode << " " << res.Body << "\n";

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(TestRequestWithIAM, THttpProxyTestMock) {
        //TODO CHeck Permission Denied in ydb/core/testlib/mocks access_service.h - Permission Denied from AccessService
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(), "Authorization: Bearer proxy_sa@builtin"); // TODO no Bearer
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.DescribeStream", CreateDescribeStreamRequest(), "X-YaCloud-SubjectToken: Bearer proxy_sa@builtin"); // TODO no Bearer
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto res = SendHttpRequest("/Root", "kinesisApi.DescribeStreamSummary", CreateDescribeStreamSummaryRequest(), "X-YaCloud-SubjectToken: Bearer proxy_sa@builtin");// TODO no Bearer
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }
    }

    Y_UNIT_TEST_F(TestPing, THttpProxyTestMock) {
        auto res = SendPing();

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
    }

    Y_UNIT_TEST_F(TestRequestBadJson, THttpProxyTestMock) {
        const TString garbage{"Some garbage in json"};
        auto res = SendHttpRequestRaw("/", "kinesisApi.CreateStream", {&garbage[0], garbage.size()}, "");

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(TestRequestNoAuthorization, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(), "");

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(TestUnauthorizedPutRecords, THttpProxyTestMock) {
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", CreateCreateStreamRequest(),
                                        "X-YaCloud-SubjectToken: unauthorized_user@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

            auto putRecordsRequest = CreatePutRecordsRequest();
            res = SendHttpRequest("/Root", "kinesisApi.PutRecords", putRecordsRequest,
                                        "X-YaCloud-SubjectToken: unauthorized_user@builtin");
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "ResourceNotFoundException");
        }
    }

    Y_UNIT_TEST_F(ListShards, THttpProxyTestMock) {
        {
            auto req = CreateCreateStreamRequest();
            req["StreamName"] = "teststream";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        // FIXME: Here we should return error as there's a typo in filter type
        {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "AT_TRIM_HORIZONT";
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "AT_TRIM_HORIZON";
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), 5);
        }

        {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "AT_TRIM_HORIZON";
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), 5);
            for (const auto& shard : GetByPath<TJVector>(json, "Shards")) {
                std::cerr << GetByPath<TString>(shard, "ShardId") <<
                    " #from: " << GetByPath<TString>(shard, "SequenceNumberRange.StartingSequenceNumber") << "\n";
            }
        }

        {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "FROM_TRIM_HORIZON";
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), 5);
        }

        auto callAfterShardId = [&] (const TString& shardId, ui32 shardsCount) {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "AFTER_SHARD_ID";
            listShardsReq["ShardFilter"]["ShardId"] = shardId;
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            if (shardsCount > 0) {
                UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), shardsCount);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(json.Has("Shards"), false);
            }
        };
        callAfterShardId("zzzz", 0);
        callAfterShardId("shard-000000", 4);
        callAfterShardId("shard-000002", 2);
        callAfterShardId("shard-000004", 0);
    }

    Y_UNIT_TEST_F(ListShardsEmptyFields, THttpProxyTestMock) {
        {
            auto req = CreateCreateStreamRequest();
            req["StreamName"] = "teststream";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "AT_TRIM_HORIZON";
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), 5);
            // KCL doesn't accept empty fields:
            // either there's something, or those fields are not returned at all
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").at(0).Has("ParentShardId"), false);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").at(0).Has("AdjacentParentShardId"), false);
        }
    }

    Y_UNIT_TEST_F(ListShardsExclusiveStartShardId, THttpProxyTestMock) {
        {
            auto req = CreateCreateStreamRequest();
            req["StreamName"] = "teststream";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        auto callExclusiveShardId = [&] (const TString& shardId, ui32 shardsCount) {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ExclusiveStartShardId"] = shardId;
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            if (shardsCount > 0) {
                UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), shardsCount);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(json.Has("Shards"), false);
            }
        };
        callExclusiveShardId("zzzz", 0);
        callExclusiveShardId("shard-000000", 5);
        callExclusiveShardId("shard-000002", 3);
        callExclusiveShardId("shard-000004", 1);
    }

    Y_UNIT_TEST_F(ListShardsTimestamp, THttpProxyTestMock) {
        {
            auto req = CreateCreateStreamRequest();
            req["StreamName"] = "teststream";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        auto callAtTimestamp = [&] (double timestamp, i32 shardsCount) {
            auto listShardsReq = CreateListShardsRequest();
            listShardsReq["StreamName"] = "teststream";
            listShardsReq["ShardFilter"]["Type"] = "AT_TIMESTAMP";
            listShardsReq["ShardFilter"]["Timestamp"] = timestamp;
            auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT(!res.Body.empty());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Shards").size(), shardsCount);
        };
        callAtTimestamp(5, 5);
        callAtTimestamp(TInstant::Now().MilliSeconds() - 1000, 5);
        callAtTimestamp(TInstant::Now().MilliSeconds(), 5);
    }

    Y_UNIT_TEST_F(ListShardsToken, THttpProxyTestMock) {
        {
            auto req = CreateCreateStreamRequest();
            req["StreamName"] = "teststream";
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req),
                                       FormAuthorizationStr("ru-central1"));
            Cerr << res.HttpCode << " " << res.Body << "\n";
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        {
            std::set<TString> shardNames;
            TString nextToken = "";
            auto callTrimHorizon = [&] () {
                auto listShardsReq = CreateListShardsRequest();
                listShardsReq["ShardFilter"]["Type"] = "AT_TRIM_HORIZON";
                listShardsReq["MaxResults"] = 2;
                listShardsReq["StreamName"] = "teststream";
                auto res = SendHttpRequest("/Root", "kinesisApi.ListShards", std::move(listShardsReq),
                                           FormAuthorizationStr("ru-central1"));
                Cerr << res.HttpCode << " " << res.Body << "\n";
                UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
                UNIT_ASSERT(!res.Body.empty());
                NJson::TJsonValue json;
                UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
                UNIT_ASSERT_GT(GetByPath<TJVector>(json, "Shards").size(), 0);
                UNIT_ASSERT_LT(GetByPath<TJVector>(json, "Shards").size(), 3);
                nextToken = GetByPath<TString>(json, "NextToken");
                for (const auto& shard : GetByPath<TJVector>(json, "Shards")) {
                    shardNames.insert(GetByPath<TString>(shard, "ShardId"));
                }
            };

            callTrimHorizon();
            callTrimHorizon();
            callTrimHorizon();

            UNIT_ASSERT_VALUES_EQUAL(shardNames.size(), 2);
        }
    }

    Y_UNIT_TEST_F(TestConsumersEmptyNames, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["StreamName"] = "teststream";
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto regReq = CreateRegisterStreamConsumerRequest();
        regReq["StreamArn"] = "teststream";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(regReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT_VALUES_EQUAL(res.Description, "ValidationException");

    }

    Y_UNIT_TEST_F(TestListStreamConsumers, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["StreamName"] = "teststream";
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json.Has("Consumers"), false);

        auto consumerReq = CreateRegisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user1";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Consumers").size(), 1);

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream1";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        consumerReq = CreateRegisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user2";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Consumers").size(), 2);

        consumerReq = CreateDeregisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user2";
        res = SendHttpRequest("/Root", "kinesisApi.DeregisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Consumers").size(), 1);

        consumerReq = CreateDeregisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user1";
        res = SendHttpRequest("/Root", "kinesisApi.DeregisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json.Has("Consumers"), false);

        consumerReq = CreateDeregisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user1";
        res = SendHttpRequest("/Root", "kinesisApi.DeregisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(TestListStreamConsumersWithMaxResults, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["StreamName"] = "teststream";
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto consumerReq = CreateRegisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user1";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        consumerReq = CreateRegisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user2";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        auto listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        listReq["MaxResults"] = 1;
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Consumers").size(), 1);
        auto nextToken = json["NextToken"];

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        listReq["MaxResults"] = 0;
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        listReq["MaxResults"] = 10001;
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

    }

    Y_UNIT_TEST_F(TestListStreamConsumersWithToken, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["StreamName"] = "teststream";
        auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        auto consumerReq = CreateRegisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user1";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        consumerReq = CreateRegisterStreamConsumerRequest();
        consumerReq["StreamArn"] = "teststream";
        consumerReq["ConsumerName"] = "user2";
        res = SendHttpRequest("/Root", "kinesisApi.RegisterStreamConsumer", std::move(consumerReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());

        auto listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "teststream";
        listReq["MaxResults"] = 1;
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Consumers").size(), 1);
        auto nextToken = GetByPath<TString>(json, "NextToken");

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "";
        listReq["NextToken"] = nextToken + "garbage";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "";
        listReq["NextToken"] = "Y18AjrkckeND9kjnbEhjhkX$^[]?!";
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        listReq = CreateListStreamConsumersRequest();
        listReq["StreamArn"] = "";
        listReq["NextToken"] = nextToken;
        res = SendHttpRequest("/Root", "kinesisApi.ListStreamConsumers", std::move(listReq), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(!res.Body.empty());
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Consumers").size(), 1);
    }

    Y_UNIT_TEST_F(TestWrongStream, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["StreamName"] = "/RootNodb/stream";
        auto res = SendHttpRequest("/", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(TestWrongStream2, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["StreamName"] = "stream";
        auto res = SendHttpRequest("/RootNodb", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }


    Y_UNIT_TEST_F(TestWrongRequest, THttpProxyTestMock) {
        auto req = CreateCreateStreamRequest();
        req["WrongStreamName"] = "WrongStreamName";
        auto res = SendHttpRequest("/", "kinesisApi.CreateStream", std::move(req), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(BadRequestUnknownMethod, THttpProxyTestMock) {
        auto res = SendHttpRequest("/Root", "kinesisApi.UnknownMethodName", NJson::TJsonValue(NJson::JSON_MAP), FormAuthorizationStr("ru-central1"));
        Cerr << res.HttpCode << " " << res.Body << "\n";
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
    }

    Y_UNIT_TEST_F(TestCounters, THttpProxyTestMock) {
        const TString streamName = TStringBuilder() << "test-counters-stream";
        {
            auto createRequest = CreateCreateStreamRequest();
            createRequest["ShardCount"] = 1;
            createRequest["StreamName"] = streamName;
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", createRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        }

        TVector<TString> put_data;
        for (auto i : {1, 2, 3, 4, 5}) {
            Y_UNUSED(i);
            auto record = CreatePutRecordsRequest();
            auto data = Base64Encode("data" + std::to_string(i));
            put_data.push_back(data);
            record["Records"].Back()["Data"] = data;
            record["StreamName"] = streamName;
            auto res = SendHttpRequest("/Root", "kinesisApi.PutRecords", record,
                                  FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<i64>(json, "FailedRecordCount"), 0);
        }

        auto sendShardIteratorRequest = [this](const auto& getShardIteratorRequest) {
            auto res = SendHttpRequest("/Root", "kinesisApi.GetShardIterator",
                                       getShardIteratorRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT(!GetByPath<TString>(json, "ShardIterator").empty());
            return json;
        };

        auto sendGetRecordsRequest = [this](const auto& getRecordsRequest) {
            auto res = SendHttpRequest("/Root", "kinesisApi.GetRecords",
                                       getRecordsRequest,
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            return json;
        };

        {
            auto getShardIteratorRequest = CreateGetShardIteratorRequest();
            getShardIteratorRequest["ShardIteratorType"] = "AT_SEQUENCE_NUMBER";
            getShardIteratorRequest["StartingSequenceNumber"] = "00000";
            getShardIteratorRequest["StreamName"] = streamName;
            auto json = sendShardIteratorRequest(getShardIteratorRequest);

            auto getRecordsRequest = CreateGetRecordsRequest();
            getRecordsRequest["ShardIterator"] = GetByPath<TString>(json, "ShardIterator");
            json = sendGetRecordsRequest(getRecordsRequest);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TJVector>(json, "Records").size(), 5);
        }

        compareJsons(MonPort, "/counters/json", "proxy_counters.json");
        compareJsons(KikimrServer->Server_->GetRuntime()->GetMonPort(),
                     "/counters/counters%3Ddatastreams/json", "internal_counters.json");
    }

    Y_UNIT_TEST_F(TestEmptyHttpBody, THttpProxyTestMock) {
        const TString streamName = TStringBuilder() << "test-counters-stream";
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", {},
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingParameter");
        }
    }

    Y_UNIT_TEST_F(TestCreateQueue, THttpProxyTestMock) {
        auto req = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));
    }

    Y_UNIT_TEST_F(TestCreateQueueWithSameNameAndSameParams, THttpProxyTestMock) {
        auto req = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        req = CreateSqsCreateQueueRequest();
        res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));
    }

    Y_UNIT_TEST_F(TestCreateQueueWithSameNameAndDifferentParams, THttpProxyTestMock) {
        auto req = CreateSqsCreateQueueRequest();
        NJson::TJsonMap attributes = NJson::TJsonMap({std::pair<TString, TString>("MessageRetentionPeriod", "60")});
        req["Attributes"] = attributes;
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        req = CreateSqsCreateQueueRequest();
        attributes = NJson::TJsonMap({std::pair<TString, TString>("MessageRetentionPeriod", "61")});
        req["Attributes"] = attributes;
        res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "ValidationError");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithBadQueueName, THttpProxyTestMock) {
        auto req = CreateSqsCreateQueueRequest();
        req["QueueName"] = "B@d_queue_name";
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithEmptyName, THttpProxyTestMock) {
        NJson::TJsonValue req;
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "MissingParameter");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithWrongBody, THttpProxyTestMock) {
        NJson::TJsonValue req;
        req["wrongField"] = "foobar";
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "InvalidArgumentException");
    }

    Y_UNIT_TEST_F(TestGetQueueUrlOfNotExistingQueue, THttpProxyTestMock) {
        auto req = CreateSqsGetQueueUrlRequest();
        req["QueueName"] = "not-existing-queue";
        auto res = SendHttpRequest("/Root", "AmazonSQS.GetQueueUrl", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
        TString resultMessage = GetByPath<TString>(json, "message");
        UNIT_ASSERT_VALUES_EQUAL(resultMessage, "The specified queue doesn't exist.");
    }

    Y_UNIT_TEST_F(TestSendMessage, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;
        sendMessageReq["MessageDeduplicationId"] = "MessageDeduplicationId-0";
        sendMessageReq["MessageGroupId"] = "MessageGroupId-0";

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(!GetByPath<TString>(json, "SequenceNumber").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "Md5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MessageId").empty());
    }

    Y_UNIT_TEST_F(TestReceiveMessage, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;
        sendMessageReq["MessageBody"] = body;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(!GetByPath<TString>(json, "Md5OfMessageBody").empty());
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (int i = 0; i < 20; ++i) {
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
            if (res.Body != TString("{}")) {
                break;;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);
    }

    Y_UNIT_TEST_F(TestGetQueueAttributes, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        NJson::TJsonValue attributes;
        attributes["DelaySeconds"] = "1";
        createQueueReq["Attributes"] = attributes;

        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue getQueueAttributes;
        getQueueAttributes["QueueUrl"] = resultQueueUrl;
        NJson::TJsonArray attributeNames = {"DelaySeconds"};
        getQueueAttributes["AttributeNames"] = attributeNames;

        res = SendHttpRequest("/Root", "AmazonSQS.GetQueueAttributes", std::move(getQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue resultJson;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &resultJson));
        UNIT_ASSERT_VALUES_EQUAL(resultJson["Attributes"]["DelaySeconds"], "1");
    }

    Y_UNIT_TEST_F(TestListQueues, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue listQueuesReq;
        listQueuesReq["QueueNamePrefix"] = "Ex";
        res = SendHttpRequest("/Root", "AmazonSQS.ListQueues", std::move(listQueuesReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonArray result;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &result));
        UNIT_ASSERT_VALUES_EQUAL(result["QueueUrls"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result["QueueUrls"][0], resultQueueUrl);
    }

    Y_UNIT_TEST_F(TestDeleteMessage, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (int i = 0; i < 20; ++i) {
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
            if (res.Body != TString("{}")) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.Empty());

        NJson::TJsonValue deleteMessageReq;
        deleteMessageReq["QueueUrl"] = resultQueueUrl;
        deleteMessageReq["ReceiptHandle"] = receiptHandle;

        res = SendHttpRequest("/Root", "AmazonSQS.DeleteMessage", std::move(deleteMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);
    }

    Y_UNIT_TEST_F(TestPurgeQueue, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue purgeQueueReq;
        purgeQueueReq["QueueUrl"] = resultQueueUrl;

        res = SendHttpRequest("/Root", "AmazonSQS.PurgeQueue", std::move(purgeQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);
    }

    Y_UNIT_TEST_F(TestDeleteQueue, THttpProxyTestMock) {
        auto req = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");

        NJson::TJsonValue deleteQueueReq;
        deleteQueueReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.DeleteQueue", std::move(deleteQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);


        for (int i = 0; i < 61; ++i) {
            req = CreateSqsGetQueueUrlRequest();
            res = SendHttpRequest("/Root", "AmazonSQS.GetQueueUrl", std::move(req), FormAuthorizationStr("ru-central1"));
            if (res.HttpCode != 200) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributes, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        NJson::TJsonValue attributes;
        attributes["DelaySeconds"] = "1";
        createQueueReq["Attributes"] = attributes;
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");

        NJson::TJsonValue setQueueAttributes;
        setQueueAttributes["QueueUrl"] = resultQueueUrl;
        attributes = {};
        attributes["DelaySeconds"] = "2";
        setQueueAttributes["Attributes"] = attributes;

        res = SendHttpRequest("/Root", "AmazonSQS.SetQueueAttributes", std::move(setQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue getQueueAttributes;
        getQueueAttributes["QueueUrl"] = resultQueueUrl;
        NJson::TJsonArray attributeNames = {"DelaySeconds"};
        getQueueAttributes["AttributeNames"] = attributeNames;

        res = SendHttpRequest("/Root", "AmazonSQS.GetQueueAttributes", std::move(getQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue resultJson;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &resultJson));
        UNIT_ASSERT_VALUES_EQUAL(resultJson["Attributes"]["DelaySeconds"], "2");
    }

    Y_UNIT_TEST_F(TestSendMessageBatch, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue message0;
        message0["Id"] = "Id-0";
        message0["MessageBody"] = "MessageBody-0";
        message0["MessageDeduplicationId"] = "MessageDeduplicationId-0";

        NJson::TJsonValue message1;
        message1["Id"] = "Id-1";
        message1["MessageBody"] = "MessageBody-1";
        message1["MessageDeduplicationId"] = "MessageDeduplicationId-1";

        NJson::TJsonArray entries = {message0, message1};

        NJson::TJsonValue sendMessageBatchReq;
        sendMessageBatchReq["QueueUrl"] = resultQueueUrl;
        sendMessageBatchReq["Entries"] = entries;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessageBatch", std::move(sendMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);
        auto succesful0 = json["Successful"][0];
        UNIT_ASSERT(succesful0["Id"] == "Id-0");
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "Md5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MessageId").empty());
    }

    Y_UNIT_TEST_F(TestDeleteMessageBatch, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue message0;
        message0["Id"] = "Id-0";
        message0["MessageBody"] = "MessageBody-0";
        message0["MessageDeduplicationId"] = "MessageDeduplicationId-0";

        NJson::TJsonValue message1;
        message1["Id"] = "Id-1";
        message1["MessageBody"] = "MessageBody-1";
        message1["MessageDeduplicationId"] = "MessageDeduplicationId-1";

        NJson::TJsonArray entries = {message0, message1};

        NJson::TJsonValue sendMessageBatchReq;
        sendMessageBatchReq["QueueUrl"] = resultQueueUrl;
        sendMessageBatchReq["Entries"] = entries;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessageBatch", std::move(sendMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);

        TVector<NJson::TJsonValue> messages;
        for (int i = 0; i < 20; ++i) {
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
            if (res.Body != TString("{}")) {
                NJson::ReadJsonTree(res.Body, &json);
                if (json["Messages"].GetArray().size() == 2) {
                    messages.push_back(json["Messages"][0]);
                    messages.push_back(json["Messages"][1]);
                    break;
                }
                if (json["Messages"].GetArray().size() == 1) {
                    messages.push_back(json["Messages"][0]);
                    if (messages.size() == 2) {
                        break;
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        auto receiptHandle0 = messages[0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle0.Empty());
        auto receiptHandle1 = messages[1]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle1.Empty());

        NJson::TJsonValue deleteMessageBatchReq;
        deleteMessageBatchReq["QueueUrl"] = resultQueueUrl;

        NJson::TJsonValue entry0;
        entry0["Id"] = "Id-0";
        entry0["ReceiptHandle"] = receiptHandle0;

        NJson::TJsonValue entry1;
        entry1["Id"] = "Id-1";
        entry1["ReceiptHandle"] = receiptHandle1;

        NJson::TJsonArray deleteEntries = {entry0, entry1};
        deleteMessageBatchReq["Entries"] = deleteEntries;

        res = SendHttpRequest("/Root", "AmazonSQS.DeleteMessageBatch", std::move(deleteMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][0]["Id"], "Id-0");
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][1]["Id"], "Id-1");

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);

    }

    Y_UNIT_TEST_F(TestListDeadLetterSourceQueues, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");

        auto createDlqReq = CreateSqsCreateQueueRequest();
        createQueueReq["QueueName"] = "DlqName";
        res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString dlqUrl = GetByPath<TString>(json, "QueueUrl");

        NJson::TJsonValue getQueueAttributes;
        getQueueAttributes["QueueUrl"] = dlqUrl;
        NJson::TJsonArray attributeNames = {"QueueArn"};
        getQueueAttributes["AttributeNames"] = attributeNames;
        res = SendHttpRequest("/Root", "AmazonSQS.GetQueueAttributes", std::move(getQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString dlqArn = GetByPath<TString>(json["Attributes"], "QueueArn");

        NJson::TJsonValue setQueueAttributes;
        setQueueAttributes["QueueUrl"] = resultQueueUrl;
        NJson::TJsonValue attributes = {};
        auto redrivePolicy = TStringBuilder()
            << "{\"deadLetterTargetArn\" : \"" << dlqArn << "\", \"maxReceiveCount\" : 100}";
        attributes["RedrivePolicy"] = redrivePolicy;
        setQueueAttributes["Attributes"] = attributes;

        res = SendHttpRequest("/Root", "AmazonSQS.SetQueueAttributes", std::move(setQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue listDeadLetterSourceQueues;
        listDeadLetterSourceQueues["QueueUrl"] = dlqUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ListDeadLetterSourceQueues", std::move(listDeadLetterSourceQueues), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"][0], resultQueueUrl);
    }

} // Y_UNIT_TEST_SUITE(TestHttpProxy)
