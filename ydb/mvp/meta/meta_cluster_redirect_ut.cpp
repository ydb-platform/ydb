#include "meta_cluster_redirect.h"
#include "ut/meta_test_runtime.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

NYdb::NTable::TDataQueryResult MakeClusterResult(TStringBuf balancer, bool found = true,
        NYdb::EStatus status = NYdb::EStatus::SUCCESS) {
    Ydb::ResultSet proto;
    auto* column = proto.add_columns();
    column->set_name("balancer");
    column->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    if (found) {
        auto* value = proto.add_rows()->add_items();
        if (balancer.empty()) {
            value->set_null_flag_value(google::protobuf::NULL_VALUE);
        } else {
            value->set_text_value(TString(balancer));
        }
    }
    return NYdb::NTable::TDataQueryResult(
        NYdb::TStatus(status, {}), {NYdb::TResultSet(std::move(proto))},
        std::nullopt, std::nullopt, false, std::nullopt);
}

class TTestRedirectActor : public NMVP::TMetaClusterRedirectActor {
    NYdb::NTable::TDataQueryResult Result;
    TString& SelectedCluster;
    bool Timeout;

public:
    TTestRedirectActor(const TYdbLocation& location, const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request, NYdb::NTable::TDataQueryResult result,
            TString& selectedCluster, bool timeout)
        : TMetaClusterRedirectActor(location, sender, request)
        , Result(std::move(result))
        , SelectedCluster(selectedCluster)
        , Timeout(timeout)
    {}

    void RequestClusterInfo() override {
        SelectedCluster = ClusterName;
        if (Timeout) {
            Send(SelfId(), new NActors::TEvents::TEvWakeup());
        } else {
            Send(SelfId(), new NMVP::THandlerActorYdb::TEvPrivate::TEvDataQueryResult(std::move(Result)));
        }
    }
};

struct TTestContext {
    TTestActorRuntime Runtime;
    const TYdbLocation Location = TYdbLocation("meta", "meta", {}, "/Root/meta");
    TString SelectedCluster;

    NHttp::THttpOutgoingResponsePtr Run(const NHttp::THttpIncomingRequestPtr& request,
            NYdb::NTable::TDataQueryResult result, bool timeout = false) {
        auto sender = Runtime.AllocateEdgeActor();
        Runtime.Register(new TTestRedirectActor(Location, sender, request, std::move(result), SelectedCluster, timeout));
        TAutoPtr<NActors::IEventHandle> handle;
        return Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle)->Response;
    }
};

constexpr TStringBuf Balancer = "https://oidc.example.net/storage.example.net:8765/viewer/json";

} // namespace

Y_UNIT_TEST_SUITE(MetaClusterRedirect) {
    Y_UNIT_TEST(RedirectsToConfiguredProxyAndPreservesQuery) {
        TTestContext context;
        const TStringBuf query = "?database=%2FRoot%2Ftest&name=another-cluster&limit=80&offset=20&x=a+b&x=a%20b&empty=";
        auto request = BuildHttpRequest(TStringBuilder() << "/cluster/testing-global/viewer/json/nodes" << query);
        auto response = context.Run(request, MakeClusterResult(Balancer));

        UNIT_ASSERT_VALUES_EQUAL(context.SelectedCluster, "testing-global");
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "307");
        const NHttp::THeaders headers(response->Headers);
        UNIT_ASSERT_VALUES_EQUAL(headers.Get("Location"), TStringBuilder()
            << "https://oidc.example.net/storage.example.net:8765/viewer/json/nodes" << query);
        UNIT_ASSERT_VALUES_EQUAL(headers.Get("Cache-Control"), "no-store");
        UNIT_ASSERT(response->Body.empty());
    }

    Y_UNIT_TEST(PostUsesMethodPreservingRedirect) {
        TTestContext context;
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        const TString body = R"({"database":"/Root/test","query":"SELECT 1"})";
        EatWholeString(request, TStringBuilder()
            << "POST /cluster/testing-global/viewer/json/query?database=%2FRoot%2Ftest HTTP/1.1\r\n"
            << "Host: localhost\r\nContent-Type: application/json\r\nContent-Length: " << body.size()
            << "\r\n\r\n" << body);
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        auto response = context.Run(request, MakeClusterResult(Balancer));

        UNIT_ASSERT_VALUES_EQUAL(response->Status, "307");
        UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(response->Headers).Get("Location"),
            "https://oidc.example.net/storage.example.net:8765/viewer/json/query?database=%2FRoot%2Ftest");
        UNIT_ASSERT_VALUES_EQUAL(request->Body, body);
    }

    Y_UNIT_TEST(SupportsBalancerFormatsAndNonViewerPaths) {
        for (TStringBuf balancer : {
                "https://oidc.example.net/https://storage.example.net:8765/viewer/json/",
                "https://oidc.example.net/https://storage.example.net:8765/viewer",
                "https://oidc.example.net/https://storage.example.net:8765/",
                "https://oidc.example.net/https://storage.example.net:8765"}) {
            TTestContext context;
            auto response = context.Run(BuildHttpRequest("/cluster/testing-global/counters/counters%20tablets/json"), MakeClusterResult(balancer));
            UNIT_ASSERT_VALUES_EQUAL(response->Status, "307");
            UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(response->Headers).Get("Location"),
                "https://oidc.example.net/https://storage.example.net:8765/counters/counters%20tablets/json");
        }
    }

    Y_UNIT_TEST(DecodesClusterNameAndSupportsRootPath) {
        TTestContext context;
        auto response = context.Run(BuildHttpRequest("/cluster/testing%2Dglobal/"), MakeClusterResult("http://storage.example.net:8765/viewer/json"));
        UNIT_ASSERT_VALUES_EQUAL(context.SelectedCluster, "testing-global");
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "307");
        UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(response->Headers).Get("Location"), "http://storage.example.net:8765/");
    }

    Y_UNIT_TEST(UnknownClusterReturnsNotFound) {
        TTestContext context;
        auto response = context.Run(BuildHttpRequest("/cluster/unknown/viewer/json/nodes"), MakeClusterResult({}, false));
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "404");
        UNIT_ASSERT(NHttp::THeaders(response->Headers).Get("Location").empty());
    }

    Y_UNIT_TEST(RejectsInvalidRoutesBeforeDatabaseLookup) {
        for (TStringBuf url : {
                "/cluster/", "/cluster//viewer/json/nodes", "/cluster/testing-global",
                "/cluster/testing-global?database=/Root/test", "/other/testing-global/viewer/json/nodes",
                "/cluster/testing%2fglobal/", "/cluster/testing%/", "/cluster/../",
                "/cluster/testing-global/../other-host/viewer/json/nodes",
                "/cluster/testing-global/%2E%2e/other-host/viewer/json/nodes",
                "/cluster/testing-global/%2e%2e%2fother-host/", "/cluster/testing-global/%5cother-host/",
                "/cluster/testing-global/viewer/json/nodes#fragment"}) {
            TTestContext context;
            auto response = context.Run(BuildHttpRequest(url), MakeClusterResult(Balancer));
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, "400", url);
            UNIT_ASSERT_C(context.SelectedCluster.empty(), url);
        }
    }

    Y_UNIT_TEST(RejectsMissingOrInvalidBalancer) {
        for (TStringBuf balancer : {
                "", "storage.example.net:8765/viewer/json", "//oidc.example.net/storage/",
                "ftp://oidc.example.net/storage/", "https:///storage/",
                "https://user:password@oidc.example.net/storage/",
                "https://oidc.example.net/storage/?query=1", "https://oidc.example.net/storage/#fragment",
                "https://oidc.example.net/storage/\r\nX-Injected: yes"}) {
            TTestContext context;
            auto response = context.Run(BuildHttpRequest("/cluster/testing-global/viewer/json/nodes"), MakeClusterResult(balancer));
            UNIT_ASSERT_VALUES_EQUAL_C(response->Status, "503", balancer);
            UNIT_ASSERT(NHttp::THeaders(response->Headers).Get("Location").empty());
        }
    }

    Y_UNIT_TEST(DatabaseFailureIsNotReportedAsUnknownCluster) {
        TTestContext context;
        auto response = context.Run(BuildHttpRequest("/cluster/testing-global/viewer/json/nodes"),
            MakeClusterResult({}, false, NYdb::EStatus::UNAVAILABLE));
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "503");
        UNIT_ASSERT(NHttp::THeaders(response->Headers).Get("Location").empty());
    }

    Y_UNIT_TEST(DatabaseTimeoutReturnsGatewayTimeout) {
        TTestContext context;
        auto response = context.Run(BuildHttpRequest("/cluster/testing-global/viewer/json/nodes"), MakeClusterResult(Balancer), true);
        UNIT_ASSERT_VALUES_EQUAL(response->Status, "504");
    }
}
