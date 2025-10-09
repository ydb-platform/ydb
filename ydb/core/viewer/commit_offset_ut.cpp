#include "ut/ut_utils.h"
#include <ydb/core/mon/ut_utils/ut_utils.h>

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
using namespace NMonitoring::NTests;

Y_UNIT_TEST_SUITE(CommitOffsetTests) {
    TString GetRequestUrl(TString topic, TString consumer, ui32 partition_id, ui64 offset = 0, TString read_session_id = "", ui32 timeout = 0) {
        TStringBuilder url;
        CGIUnescape(topic);
        url << "viewer/commit_offset" << "?path=" << topic << "&consumer=" << consumer << "&partition=" << partition_id << "&offset=" << offset
            << "&read_session_id" << read_session_id << "&timeout=" << timeout;
        return url;
    }
    void GrantConnect(TClient& client) {
        client.CreateUser("/Root", "username", "password");
        client.GrantConnect("username");

        const auto alterAttrsStatus = client.AlterUserAttributes("/", "Root", {
            { "folder_id", "test_folder_id" },
            { "database_id", "test_database_id" },
        });
        UNIT_ASSERT_EQUAL(alterAttrsStatus, NMsgBusProxy::MSTATUS_OK);
    }

    //  TKeepAliveHttpClient::THttpCode MakeRequest(TKeepAliveHttpClient& httpClient, const TString& url, TJsonValue& json) {
    //     json = TJsonValue{};
    //     TString response;
    //     TStringStream responseStream;

    //     TKeepAliveHttpClient::THeaders headers;
    //     headers["Accept"] = "application/json";

    //     NKikimr::NViewerTests::THttpRequest httpReq(HTTP_METHOD_GET);
    //     httpReq.HttpHeaders.AddHeader("Accept", "application/json");

    //     auto statusCode = httpClient.DoPost(url, &responseStream, headers);
    //     response = responseStream.ReadAll();
    //     if (statusCode != HTTP_OK) {
    //         Cerr << "Got response:" << statusCode << ": " << response << Endl;
    //         return statusCode;
    //     }

    //     NJson::TJsonReaderConfig jsonCfg;
    //     NJson::ReadJsonTree(response, &jsonCfg, &json, /* throwOnError = */ true);
    //     UNIT_ASSERT(json.GetType() == EJsonValueType::JSON_MAP);
    //     const auto& map_ = json.GetMap();
    //     UNIT_ASSERT(map_.find("Messages") != map_.end());
    //     UNIT_ASSERT(map_.find("Messages")->second.GetType() == EJsonValueType::JSON_ARRAY);
    //     return statusCode;
    // }

     NJson::TJsonValue PostQuery(TKeepAliveHttpClient& httpClient) {
        NJson::TJsonValue jsonRequest;
        jsonRequest["database"] = "/Root";
        jsonRequest["path"] = "/Root/topic1";
        jsonRequest["consumer"] = "consumer1";
        jsonRequest["partition_id"] = 0;
        jsonRequest["offset"] = 0;
        TStringStream responseStream;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/json";
        headers["Authorization"] = VALID_TOKEN;
        const TKeepAliveHttpClient::THttpCode statusCode = httpClient.DoPost("/viewer/commit_offset/", NJson::WriteJson(jsonRequest, false), &responseStream, headers);
        UNIT_ASSERT_EQUAL(statusCode, HTTP_OK);
        return NJson::ReadJsonTree(&responseStream, /* throwOnError = */ true);
    }

}
