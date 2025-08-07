#include "audit.h"

#include <ydb/core/testlib/test_client.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/http/simple/http_client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/null.h>

using namespace NKikimr;
using namespace Tests;
using namespace NMonitoring::NAudit;

#ifdef NDEBUG
#define Ctest Cnull
#else
#define Ctest Cerr
#endif

Y_UNIT_TEST_SUITE(TRedirectTest) {
    struct RequestParams {
        ui32 Mask;
        TString Path;
        TString Url;
        TString Body;
        TString ContentType;
        bool XForwardedFromNode;
    };

    class TestEnv {
    public:
        TPortManager PortManager;
        ui16 Port = PortManager.GetPort(2134);
        ui16 GrpcPort = PortManager.GetPort(2135);
        ui16 MonPort = PortManager.GetPort(8765);
        TServerSettings Settings = TServerSettings(Port);
        TIntrusivePtr<TServer> Server;
        TClient Client;
        std::unique_ptr<TTenants> Tenants;
        TKeepAliveHttpClient HttpClient;

        void WaitForHttpReady(TKeepAliveHttpClient& client) {
            for (int retries = 0;; ++retries) {
                UNIT_ASSERT(retries < 100);
                TStringStream responseStream;
                const TKeepAliveHttpClient::THttpCode statusCode = client.DoGet("/viewer/simple_counter?max_counter=1&period=100", &responseStream);
                const TString response = responseStream.ReadAll();
                if (statusCode == HTTP_OK) {
                    break;
                }
            }
        }

        void SetupResourcesTenant(Ydb::Cms::CreateDatabaseRequest& request, Ydb::Cms::StorageUnits* storage, const TString& name) {
            request.set_path(name);
            storage->set_unit_kind(name);
            storage->set_count(1);
        }

        TestEnv()
            : Settings(TServerSettings(Port))
            , Client(Settings)
            , HttpClient("localhost", MonPort)
        {
            Settings.InitKikimrRunConfig()
                .SetNodeCount(1)
                .SetDynamicNodeCount(1)
                .SetUseRealThreads(true)
                .SetDomainName("Root")
                .AddStoragePoolType("/Root/db1")
                .SetUseSectorMap(true)
                .SetMonitoringPortOffset(MonPort, true);

            Server = MakeIntrusive<TServer>(Settings);
            Server->EnableGRpc(GrpcPort);

            Tenants = std::make_unique<TTenants>(Server);
            Ydb::Cms::CreateDatabaseRequest request;
            SetupResourcesTenant(request, request.mutable_resources()->add_storage_units(), "/Root/db1");
            Tenants->CreateTenant(std::move(request));

            TVector<ui32> tenantNodesIdx = Tenants->List("/Root/db1");
            for (auto nodeIdx : tenantNodesIdx) {
                Server->EnableGRpc(GrpcPort, nodeIdx, "/Root/db1");
            }

            WaitForHttpReady(HttpClient);
        }

        void EatWholeString(NHttp::THttpIncomingRequestPtr request, const TString& data) {
            request->EnsureEnoughSpaceAvailable(data.size());
            auto size = std::min(request->Avail(), data.size());
            memcpy(request->Pos(), data.data(), size);
            request->Advance(size);
        }

        bool PredictRedirect(const RequestParams& params) {
            TStringBuilder request;
            request << "POST " << params.Url << " HTTP/1.1\r\n"
                << "Host: test.ydb\r\n"
                << "Content-Type: " << params.ContentType << "\r\n";
            if (params.XForwardedFromNode) {
                request << "X-Forwarded-From-Node: 1\r\n";
            }
            request << "Content-Length: " << params.Body.size() << "\r\n"
                << "\r\n"
                << params.Body;

            NHttp::THttpIncomingRequestPtr incomingRequest = new NHttp::THttpIncomingRequest();
            EatWholeString(incomingRequest, request); // request — ваша строка из TStringBuilder
            return IsViewerRedirectRequest(incomingRequest, "/Root");
        }

        bool ViewerRedirect(const RequestParams& params) {
            TStringStream responseStream;
            TKeepAliveHttpClient::THeaders headers;
            headers["Content-Type"] = params.ContentType;
            if (params.XForwardedFromNode) {
                headers["X-Forwarded-From-Node"] = "1";
            }
            const TKeepAliveHttpClient::THttpCode statusCode = HttpClient.DoPost(params.Url, params.Body, &responseStream, headers);
            const TString response = responseStream.ReadAll();

            return statusCode == HTTP_TEMPORARY_REDIRECT;
        }

        void TestRedirect(RequestParams params) {
            bool expected = PredictRedirect(params);
            bool result = ViewerRedirect(params);

            Ctest << params.Path
                  << "(" << params.Mask
                  << "): expected: " << expected
                  << ", result: " << result << Endl;

            UNIT_ASSERT_EQUAL_C(expected, result, TStringBuilder() << "Redirect test failed [expected: " << expected
                << ", result: " << result
                << "] for path: " << params.Url
                << ", body: " << params.Body
                << ", Content-Type: " << params.ContentType
                << ", X-Forwarded-From-Node: " << (params.XForwardedFromNode ? "true" : "false"));
        }
    };

    enum ERedirectFlags : ui16 {
        DB_URL            = 1 << 0, // database in url
        DB_BODY           = 1 << 1, // database in body
        DB_ROOT_DB        = 1 << 2, // 0 - /Root, 1 - /Root/db1
        TENANT_URL        = 1 << 3, // tenant in url
        TENANT_BODY       = 1 << 4, // tenant in body
        TENANT_ROOT_DB    = 1 << 5, // 0 - /Root, 1 - /Root/db1
        DIRECT_URL        = 1 << 6, // direct in url
        DIRECT_BODY       = 1 << 7, // direct in body
        BODY_IS_JSON      = 1 << 8, // 0 - form-urlencoded, 1 - json
        XFF_FROM_NODE     = 1 << 9  // 0 - false, 1 - true
    };

    void TestRedirectHandler(TestEnv& evn, const TString& path, ui32 mask) {
        TString url = path;
        TString body;
        TString contentType = (mask & BODY_IS_JSON) ? "application/json" : "application/x-www-form-urlencoded";
        bool xForwardedFromNode = mask & XFF_FROM_NODE;

        // direct
        bool directUrl  = mask & DIRECT_URL;
        bool directBody = mask & DIRECT_BODY;

        // database
        bool dbInUrl  = mask & DB_URL;
        bool dbInBody = mask & DB_BODY;
        bool dbRootDb = mask & DB_ROOT_DB;
        TString dbValue = dbRootDb ? "/Root/db1" : "/Root";

        // tenant
        bool tenantInUrl  = mask & TENANT_URL;
        bool tenantInBody = mask & TENANT_BODY;
        bool tenantRootDb = mask & TENANT_ROOT_DB;
        TString tenantValue = tenantRootDb ? "/Root/db1" : "/Root";

        // URL
        TVector<TString> queryParams;
        if (dbInUrl) {
            queryParams.push_back("database=" + dbValue);
        }
        if (tenantInUrl) {
            queryParams.push_back("tenant=" + tenantValue);
        }
        if (directUrl) {
            queryParams.push_back("direct=true");
        }
        if (!queryParams.empty()) {
            url += "?" + JoinSeq("&", queryParams);
        }

        // BODY
        if (contentType == "application/json") {
            THashMap<TString, TString> jsonMap;
            if (dbInBody) {
                jsonMap["database"] = dbValue;
            }
            if (tenantInBody) {
                jsonMap["tenant"] = tenantValue;
            }
            if (directBody) {
                jsonMap["direct"] = "true";
            }
            TStringStream ss;
            ss << "{";
            bool first = true;
            for (auto& kv : jsonMap) {
                if (!first) ss << ",";
                ss << "\"" << kv.first << "\": \"" << kv.second << "\"";
                first = false;
            }
            ss << "}";
            body = ss.Str();
        } else {
            TVector<TString> formParams;
            if (dbInBody) {
                formParams.push_back("database=" + dbValue);
            }
            if (tenantInBody) {
                formParams.push_back("tenant=" + tenantValue);
            }
            if (directBody) {
                formParams.push_back("direct=true");
            }
            body = JoinSeq("&", formParams);
        }

        evn.TestRedirect({
            .Mask = mask,
            .Path = path,
            .Url = url,
            .Body = body,
            .ContentType = contentType,
            .XForwardedFromNode = xForwardedFromNode
        });
    }

    void TestRedirectHandler(const TString& path) {
        TestEnv evn;

        for (ui16 mask = 0; mask < (1u << 10); ++mask) {
            TestRedirectHandler(evn, path, mask);
        }
    }

    Y_UNIT_TEST(ViewerQuery) {
        TestRedirectHandler("/viewer/json/query");
        TestRedirectHandler("/viewer/query");
    }

    Y_UNIT_TEST(SchemeDirectory) {
        TestRedirectHandler("/scheme/json/directory");
        TestRedirectHandler("/scheme/directory");
    }

    Y_UNIT_TEST(ViewerAcl) {
        TestRedirectHandler("/viewer/acl");
        TestRedirectHandler("/viewer/json/acl");
    }

    Y_UNIT_TEST(OperationCancel) {
        TestRedirectHandler("/operation/cancel");
        TestRedirectHandler("/operation/json/cancel");
    }

    Y_UNIT_TEST(OperationForget) {
        TestRedirectHandler("/operation/forget");
        TestRedirectHandler("/operation/json/forget");
    }

    Y_UNIT_TEST(QueryScriptExecute) {
        TestRedirectHandler("/query/script/execute");
        TestRedirectHandler("/query/json/script/execute");
    }

    Y_UNIT_TEST(PdiskRestart) {
        TestRedirectHandler("/pdisk/restart");
        TestRedirectHandler("/pdisk/json/restart");
    }

    Y_UNIT_TEST(VDiskEvict) {
        TestRedirectHandler("/vdisk/evict");
        TestRedirectHandler("/vdisk/json/evict");
    }
}
