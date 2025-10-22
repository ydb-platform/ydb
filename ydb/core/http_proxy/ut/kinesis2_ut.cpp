#include <ydb/core/http_proxy/ut/datastreams_fixture/datastreams_fixture.h>

#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>


#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <nlohmann/json.hpp>



Y_UNIT_TEST_SUITE(TestKinesis2HttpProxy) {

    Y_UNIT_TEST_F(GoodRequestPutRecords, THttpProxyTestMockForSQSExtQueueUrl) {
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

    Y_UNIT_TEST_F(PutRecordsWithLongExplicitHashKey, THttpProxyTestMockForSQSExtQueueUrl) {
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

    Y_UNIT_TEST_F(GoodRequestGetRecords, THttpProxyTestMockForSQSExtQueueUrl) {
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

    Y_UNIT_TEST_F(TestEmptyHttpBody, THttpProxyTestMockForSQSExtQueueUrl) {
        const TString streamName = TStringBuilder() << "test-counters-stream";
        {
            auto res = SendHttpRequest("/Root", "kinesisApi.CreateStream", {},
                                       FormAuthorizationStr("ru-central1"));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
            UNIT_ASSERT_VALUES_EQUAL(res.Description, "MissingParameter");
        }
    }
} // Y_UNIT_TEST_SUITE(TestKinesisHttpProxy)
