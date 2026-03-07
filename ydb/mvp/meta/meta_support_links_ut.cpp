#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/meta_support_links.h>
#include <ydb/mvp/meta/support_links/ut/mock_link_source.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

#include <google/protobuf/text_format.h>
#include <memory>

Y_UNIT_TEST_SUITE(MetaSupportLinks) {
    class TMvpGuard {
    public:
        TMvpGuard() {
            const char* argv[] = {"meta_support_links_ut"};
            Mvp = std::make_unique<NMVP::TMVP>(1, argv);
            NMVP::NTest::RegisterMockLinkSources();
        }

    private:
        std::unique_ptr<NMVP::TMVP> Mvp;
    };

    class TTestActorRuntime : public NActors::TTestActorRuntimeBase {
    public:
        TTestActorRuntime() {
            Initialize();
        }
    };

    class TSupportLinksTestActor : public NMVP::TMetaSupportLinksGetHandlerActor {
    public:
        NYdb::NTable::TDataQueryResult Result;

        TSupportLinksTestActor(
            const NActors::TActorId& httpProxyId,
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            NYdb::NTable::TDataQueryResult&& result)
            : NMVP::TMetaSupportLinksGetHandlerActor(httpProxyId, location, sender, request)
            , Result(std::move(result))
        {}

        void RequestClusterInfo() override {
            Send(SelfId(), new NMVP::THandlerActorYdb::TEvPrivate::TEvDataQueryResult(std::move(Result)));
        }
    };

    static NHttp::THttpIncomingRequestPtr BuildHttpRequest(TStringBuf url, TStringBuf method = "GET") {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, TStringBuilder() << method << " " << url << " HTTP/1.1\r\nHost: localhost\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        return request;
    }

    static NYdb::NTable::TDataQueryResult MakeClusterInfoResult(TStringBuf workspace, TStringBuf datasource) {
        const TString resultSetString = TStringBuilder()
            << R"(columns {
  name: "workspace"
  type { type_id: UTF8 }
}
columns {
  name: "grafana_ds"
  type { type_id: UTF8 }
}
rows {
  items { text_value: ")"
            << workspace
            << R"(" }
  items { text_value: ")"
            << datasource
            << R"(" }
}
)";

        Ydb::ResultSet rsProto;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(resultSetString, &rsProto));

        return NYdb::NTable::TDataQueryResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}),
            {NYdb::TResultSet(std::move(rsProto))},
            std::nullopt,
            std::nullopt,
            false,
            std::nullopt
        );
    }

    static NYdb::NTable::TDataQueryResult MakeEmptyClusterInfoResult() {
        const TString resultSetString = R"(columns {
  name: "workspace"
  type { type_id: UTF8 }
}
columns {
  name: "grafana_ds"
  type { type_id: UTF8 }
}
)";

        Ydb::ResultSet rsProto;
        UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(resultSetString, &rsProto));

        return NYdb::NTable::TDataQueryResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}),
            {NYdb::TResultSet(std::move(rsProto))},
            std::nullopt,
            std::nullopt,
            false,
            std::nullopt
        );
    }

    static NMVP::TSupportLinksConfig MakeSyncConfig() {
        NMVP::TSupportLinksConfig cfg;
        auto* cluster = cfg.AddCluster();
        cluster->SetSource("mock/sourceSync");
        cluster->SetTitle("sync");
        cluster->SetUrl("mock://sync");
        return cfg;
    }

    static NMVP::TSupportLinksConfig MakeAsyncConfig() {
        NMVP::TSupportLinksConfig cfg;
        auto* cluster = cfg.AddCluster();
        cluster->SetSource("mock/sourceAsync");
        cluster->SetTitle("async");
        cluster->SetUrl("mock://async");
        return cfg;
    }

    static NMVP::TSupportLinksConfig MakeEmptyConfig() {
        return {};
    }

    static void SetSupportLinkSources(const NMVP::TSupportLinksConfig& config) {
        NMVP::InstanceMVP->MetaSettings.ClusterLinkSources.clear();
        NMVP::InstanceMVP->MetaSettings.DatabaseLinkSources.clear();
        NMVP::InstanceMVP->MetaSettings.ClusterLinkSources.reserve(config.GetCluster().size());
        for (int i = 0; i < config.GetCluster().size(); ++i) {
            NMVP::InstanceMVP->MetaSettings.ClusterLinkSources.push_back(NMVP::MakeLinkSource(config.GetCluster(i)));
        }
        NMVP::InstanceMVP->MetaSettings.DatabaseLinkSources.reserve(config.GetDatabase().size());
        for (int i = 0; i < config.GetDatabase().size(); ++i) {
            NMVP::InstanceMVP->MetaSettings.DatabaseLinkSources.push_back(NMVP::MakeLinkSource(config.GetDatabase(i)));
        }
    }

    Y_UNIT_TEST(SupportLinksReturnsBadRequestWhenClusterMissing) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        SetSupportLinkSources(MakeSyncConfig());

        auto request = BuildHttpRequest("/meta/support_links?database=/root/test");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "400");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "meta");
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("Invalid identity parameters"));
    }

    Y_UNIT_TEST(SupportLinksReturnsMethodNotAllowedForNonGet) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");

        auto handler = runtime.Register(new NMVP::TMetaSupportLinksHandlerActor(anyHttpProxy, location));
        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global", "POST");
        runtime.Send(new NActors::IEventHandle(handler, sender, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "405");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "meta");
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["message"].GetStringRobust(), "Only GET method is supported");
    }

    static void AssertMockResponse(TStringBuf body) {
        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["title"].GetStringRobust(), "Grafana Dashboard");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["url"].GetStringRobust(), "https://grafana.example.com/d/mock-dashboard");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][1]["title"].GetStringRobust(), "Runbook");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][1]["url"].GetStringRobust(), "https://wiki.example.com/runbooks/mock-dashboard");
        UNIT_ASSERT(!json.Has("errors"));
    }

    Y_UNIT_TEST(UsesSourceMockSync) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        SetSupportLinkSources(MakeSyncConfig());

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
        AssertMockResponse(response->Response->Body);
    }

    Y_UNIT_TEST(UsesSourceMockActorAsync) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        SetSupportLinkSources(MakeAsyncConfig());

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
        AssertMockResponse(response->Response->Body);
    }

    Y_UNIT_TEST(ReturnsEmptyLinksForValidRequestWhenNoSupportLinksConfigured) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        SetSupportLinkSources(MakeEmptyConfig());

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global&database=/root/test");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 0);
        UNIT_ASSERT(!json.Has("errors"));
    }

    Y_UNIT_TEST(PreservesMetaErrorWhenClusterIsNotFoundAndSourcesRespond) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        SetSupportLinkSources(MakeSyncConfig());

        auto request = BuildHttpRequest("/meta/support_links?cluster=missing-cluster");
        auto result = MakeEmptyClusterInfoResult();
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["title"].GetStringRobust(), "Grafana Dashboard");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["url"].GetStringRobust(), "https://grafana.example.com/d/mock-dashboard");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][1]["title"].GetStringRobust(), "Runbook");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][1]["url"].GetStringRobust(), "https://wiki.example.com/runbooks/mock-dashboard");

        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "meta");
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("missing-cluster"));
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("is not found in MasterClusterExt.db"));
    }
}
