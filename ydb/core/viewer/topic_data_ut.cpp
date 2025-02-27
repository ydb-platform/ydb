#include "ut/ut_utils.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include<library/cpp/http/fetch/httpheader.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/topic/client.h>

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

    Y_UNIT_TEST(GetTopicDataTest) {
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

        NYdb::NPersQueue::TWriteSessionSettings wsSettings;
        wsSettings.Path(topicPath);
        wsSettings.MessageGroupId("12345");
        wsSettings.Codec(ECodec::GZIP);

        Cerr << "Write data\n";

        auto writer = TPersQueueClient(ydbDriver).CreateSimpleBlockingWriteSession(TWriteSessionSettings(wsSettings).ClusterDiscoveryMode(EClusterDiscoveryMode::Off));
        TString dataFiller{100u, 'a'};

        for (auto i = 0u; i < 20; ++i) {
            writer->Write(TStringBuilder() << "Message " << i << " : " << dataFiller);
        }
        Cerr << "Close writer\n";
        writer->Close();
        Cerr << "Write data - done\n";


        TKeepAliveHttpClient httpClient("localhost", monPort);
        NKikimr::NViewerTests::WaitForHttpReady(httpClient);
        TStringStream responseStream;
        TStringBuilder url;
        CGIUnescape(topicPath);
        url << "/viewer/get_topic_data" << "?topic_path=" << topicPath << "&partition=0" << "&offset=0" << "&limit=10";

        TKeepAliveHttpClient::THeaders headers;
        headers["Accept"] = "application/json";

        NKikimr::NViewerTests::THttpRequest httpReq(HTTP_METHOD_GET);
        httpReq.HttpHeaders.AddHeader("Accept", "application/json");

        const TKeepAliveHttpClient::THttpCode statusCode = httpClient.DoGet(url, &responseStream, headers);
        const TString response = responseStream.ReadAll();
        UNIT_ASSERT_EQUAL_C(statusCode, HTTP_OK, statusCode << ": " << response);
        Cerr << "Response: " << response << Endl;
        NJson::TJsonReaderConfig jsonCfg;
        NJson::TJsonValue json;
        NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);
        Cerr << "Data: " << json.GetString() << Endl;
        UNIT_ASSERT(json.GetType() == EJsonValueType::JSON_ARRAY);
        const auto& array = json.GetArray();
        UNIT_ASSERT_VALUES_EQUAL(array.size(), 10);
        for (auto i = 0u; i < 10; ++i) {
            const auto& item = array[i];
            UNIT_ASSERT(item.GetType() == EJsonValueType::JSON_MAP);
            const auto& jsonMap = item.GetMap();
            CheckMapValue(jsonMap, "Offset", i);
            CheckMapValue(jsonMap, "SeqNo", i + 1);
            CheckMapValue(jsonMap, "Size", 35);
            CheckMapValue(jsonMap, "OriginalSize", 112);
            UNIT_ASSERT(jsonMap.find("ProducerId") != jsonMap.end());
            UNIT_ASSERT(jsonMap.find("CreateTimestamp") != jsonMap.end());
            UNIT_ASSERT(jsonMap.find("WriteTimestamp") != jsonMap.end());
        }
    }
};
