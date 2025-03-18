#include "ut/ut_utils.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/http/fetch/httpheader.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

using namespace NKikimr;
using namespace Tests;
using namespace NYdb::NPersQueue;
using namespace NHttp;
using namespace NJson;


Y_UNIT_TEST_SUITE(ViewerTopicDataTests) {
    template <typename T>
    void CheckMapValue(const NJson::TJsonValue::TMapType& map, const TString& key, const T& value) {
        auto iter = map.find(key);
        UNIT_ASSERT(iter != map.end());
        UNIT_ASSERT_VALUES_EQUAL_C(iter->second, value, key);
    }
    TString GetRequestUrl(TString topic, ui32 partition, ui64 offset = 0, ui32 limit = 10) {
        TStringBuilder url;
        CGIUnescape(topic);
        url << "/viewer/topic_data" << "?path=" << topic << "&partition=" << partition << "&offset=" << offset
            << "&limit=" << limit << "&encode_data=false";
        return url;
    }

    TKeepAliveHttpClient::THttpCode MakeRequest(TKeepAliveHttpClient& httpClient, const TString& url, TJsonValue& json) {
        json = TJsonValue{};
        TString response;
        TStringStream responseStream;

        TKeepAliveHttpClient::THeaders headers;
        headers["Accept"] = "application/json";

        NKikimr::NViewerTests::THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.HttpHeaders.AddHeader("Accept", "application/json");

        auto statusCode = httpClient.DoGet(url, &responseStream, headers);
        response = responseStream.ReadAll();
        if (statusCode != HTTP_OK) {
            Cerr << "Got response:" << statusCode << ": " << response << Endl;
            return statusCode;
        }

        NJson::TJsonReaderConfig jsonCfg;
        NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);
        UNIT_ASSERT(json.GetType() == EJsonValueType::JSON_MAP);
        const auto& map_ = json.GetMap();
        UNIT_ASSERT(map_.find("Messages") != map_.end());
        UNIT_ASSERT(map_.find("Messages")->second.GetType() == EJsonValueType::JSON_ARRAY);
        return statusCode;
    }

    Y_UNIT_TEST(TopicDataTest) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        ui16 grpcPort = tp.GetPort(2135);
        ui16 monPort = tp.GetPort(8765);

        auto settings = NKikimr::NPersQueueTests::PQSettings(port, 1);
        settings.PQConfig.MutableQuotingConfig()->SetEnableQuoting(false);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);

        settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .SetMonitoringPortOffset(monPort, true);

        auto grpcSettings = NYdbGrpc::TServerOptions().SetHost("[::1]").SetPort(grpcPort);
        TServer server{settings};
        server.EnableGRpc(grpcSettings);

        auto client = MakeHolder<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(settings, grpcPort);
        client->InitRoot();
        client->InitSourceIds();
        NYdb::TDriverConfig driverCfg;
        TString topicPath = "/Root/topic1";
        driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << grpcPort)
                 .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));

        NYdb::TDriver ydbDriver{driverCfg};
        auto topicClient = NYdb::NTopic::TTopicClient(ydbDriver);

        auto res = topicClient.CreateTopic(topicPath).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto writeData = [&](NYdb::NPersQueue::ECodec codec, ui64 count, const TString& producerId, ui64 size = 100u) {
            NYdb::NPersQueue::TWriteSessionSettings wsSettings;
            wsSettings.Path(topicPath);
            wsSettings.MessageGroupId(producerId);
            wsSettings.Codec(codec);

            auto writer = TPersQueueClient(ydbDriver).CreateSimpleBlockingWriteSession(TWriteSessionSettings(wsSettings).ClusterDiscoveryMode(EClusterDiscoveryMode::Off));
            TString dataFiller{size, 'a'};

            for (auto i = 0u; i < count; ++i) {
                writer->Write(TStringBuilder() << "Message " << i << " : " << dataFiller);
            }
            writer->Close();
        };

        writeData(ECodec::GZIP, 20, "producer1");
        writeData(ECodec::RAW, 20, "producer2");

        TKeepAliveHttpClient httpClient("localhost", monPort);
        NKikimr::NViewerTests::WaitForHttpReady(httpClient);

        TJsonValue json;
        TString producer1, producer2, producer3;

        // Test 1 - compressed data with limit
        {
            auto statusCode = MakeRequest(httpClient, GetRequestUrl(topicPath, 0, 0, 10), json);
            UNIT_ASSERT_EQUAL(statusCode, HTTP_OK);
            const auto& overalResponse = json.GetMap();
            CheckMapValue(overalResponse, "StartOffset", 0);
            CheckMapValue(overalResponse, "EndOffset", 40);

            const auto& messages = overalResponse.find("Messages")->second.GetArray();

            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 10);
            for (auto i = 0u; i < 10; ++i) {
                const auto& item = messages[i];
                UNIT_ASSERT(item.GetType() == EJsonValueType::JSON_MAP);
                const auto& jsonMap = item.GetMap();
                CheckMapValue(jsonMap, "Offset", i);
                CheckMapValue(jsonMap, "SeqNo", i + 1);
                CheckMapValue(jsonMap, "StorageSize", 35);
                CheckMapValue(jsonMap, "OriginalSize", 112);
                CheckMapValue(jsonMap, "Codec", 1);
                if (producer1.empty()) {
                    UNIT_ASSERT(jsonMap.find("ProducerId") != jsonMap.end());
                    producer1 = jsonMap.find("ProducerId")->second.GetString();
                } else {
                    CheckMapValue(jsonMap, "ProducerId", producer1);
                }
                UNIT_ASSERT(jsonMap.find("CreateTimestamp") != jsonMap.end());
                UNIT_ASSERT(jsonMap.find("WriteTimestamp") != jsonMap.end());
            }
        }
        // Test 2 - uncompressed data with limit and start offset
        {
            auto statusCode = MakeRequest(httpClient, GetRequestUrl(topicPath, 0, 20, 10), json);
            UNIT_ASSERT_EQUAL(statusCode, HTTP_OK);
            const auto& overalResponse = json.GetMap();
            CheckMapValue(overalResponse, "StartOffset", 0);
            CheckMapValue(overalResponse, "EndOffset", 40);
            const auto& messages = overalResponse.find("Messages")->second.GetArray();


            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 10);
            for (auto i = 0u; i < 10; ++i) {
                const auto& item = messages[i];
                UNIT_ASSERT(item.GetType() == EJsonValueType::JSON_MAP);
                const auto& jsonMap = item.GetMap();
                CheckMapValue(jsonMap, "Offset", 20 + i);
                CheckMapValue(jsonMap, "SeqNo", i + 1);
                CheckMapValue(jsonMap, "StorageSize", 112);
                CheckMapValue(jsonMap, "OriginalSize", 112);
                CheckMapValue(jsonMap, "Codec", 0);
                if (producer2.empty()) {
                    UNIT_ASSERT(jsonMap.find("ProducerId") != jsonMap.end());
                    producer2 = jsonMap.find("ProducerId")->second.GetString();
                    UNIT_ASSERT_VALUES_UNEQUAL(producer2, producer1);
                } else {
                    CheckMapValue(jsonMap, "ProducerId", producer2);
                }
                UNIT_ASSERT(jsonMap.find("CreateTimestamp") != jsonMap.end());
                UNIT_ASSERT(jsonMap.find("WriteTimestamp") != jsonMap.end());
            }
        }
        // Test 3 - large messages

        {
            writeData(ECodec::GZIP, 20, "producer3", 1_MB);

            auto statusCode = MakeRequest(httpClient, GetRequestUrl(topicPath, 0, 40, 20), json);
            UNIT_ASSERT_EQUAL(statusCode, HTTP_OK);
            const auto& overalResponse = json.GetMap();
            CheckMapValue(overalResponse, "EndOffset", 60);
            const auto& messages = overalResponse.find("Messages")->second.GetArray();


            UNIT_ASSERT_C(messages.size() <= 10, messages.size());
            for (auto i = 0u; i < 10; ++i) {
                const auto& item = messages[i];
                UNIT_ASSERT(item.GetType() == EJsonValueType::JSON_MAP);
                const auto& jsonMap = item.GetMap();
                CheckMapValue(jsonMap, "Offset", 40 + i);
                CheckMapValue(jsonMap, "OriginalSize", 1_MB + 12);
                CheckMapValue(jsonMap, "Codec", 1);
                UNIT_ASSERT(jsonMap.find("Message") != jsonMap.end());
                Cerr << "Size: " << jsonMap.find("Message")->second.GetString().size() << Endl;
                UNIT_ASSERT(jsonMap.find("Message")->second.GetString().size() < 1500_KB);
                UNIT_ASSERT(jsonMap.find("Message")->second.GetString().size() > 500_KB);
                CheckMapValue(jsonMap, "SeqNo", i + 1);

                if (producer3.empty()) {
                    UNIT_ASSERT(jsonMap.find("ProducerId") != jsonMap.end());
                    producer3 = jsonMap.find("ProducerId")->second.GetString();
                    UNIT_ASSERT_VALUES_UNEQUAL(producer2, producer3);
                } else {
                    CheckMapValue(jsonMap, "ProducerId", producer3);
                }
                UNIT_ASSERT(jsonMap.find("CreateTimestamp") != jsonMap.end());
                UNIT_ASSERT(jsonMap.find("WriteTimestamp") != jsonMap.end());
            }
        }
        // Test 4 - bad topic, partition, offset
        {
            auto statusCode = MakeRequest(httpClient, GetRequestUrl("/Root/bad-topic", 0, 20, 10), json);
            UNIT_ASSERT_EQUAL(statusCode, HTTP_BAD_REQUEST);
            statusCode = MakeRequest(httpClient, GetRequestUrl(topicPath, 10, 20, 10), json);
            UNIT_ASSERT_EQUAL(statusCode, HTTP_BAD_REQUEST);
            statusCode = MakeRequest(httpClient, GetRequestUrl(topicPath, 0, 10000, 10), json);
            UNIT_ASSERT_EQUAL(statusCode, HTTP_BAD_REQUEST);
        }
    }
};
