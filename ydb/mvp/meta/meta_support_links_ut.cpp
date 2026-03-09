#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/meta_support_links.h>
#include <ydb/mvp/meta/support_links/ut/mock_link_source.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>
#include <memory>
#include <mutex>

Y_UNIT_TEST_SUITE(MetaSupportLinks) {
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
            const NMVP::TMetaSettings& settings,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            NYdb::NTable::TDataQueryResult&& result)
            : NMVP::TMetaSupportLinksGetHandlerActor(httpProxyId, location, settings, sender, request)
            , Result(std::move(result))
        {}

        void RequestClusterInfo() override {
            Send(SelfId(), new NMVP::THandlerActorYdb::TEvPrivate::TEvDataQueryResult(std::move(Result)));
        }
    };

    struct TSupportLinksTestContext {
        NMVP::TMetaSettings Settings;
        const TYdbLocation Location = TYdbLocation("meta", "meta", {}, "/Root");

        explicit TSupportLinksTestContext(const NMVP::TSupportLinksConfig& config) {
            static std::once_flag once;
            std::call_once(once, [] {
                NMVP::NTest::RegisterMockLinkSources();
            });
            Settings.ClusterLinkSources.reserve(config.GetCluster().size());
            for (int i = 0; i < config.GetCluster().size(); ++i) {
                Settings.ClusterLinkSources.push_back(NMVP::MakeLinkSource(config.GetCluster(i)));
            }
            Settings.DatabaseLinkSources.reserve(config.GetDatabase().size());
            for (int i = 0; i < config.GetDatabase().size(); ++i) {
                Settings.DatabaseLinkSources.push_back(NMVP::MakeLinkSource(config.GetDatabase(i)));
            }
        }

        NActors::TActorId RegisterGet(
            TTestActorRuntime& runtime,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            NYdb::NTable::TDataQueryResult&& result) const {
            auto httpProxy = runtime.AllocateEdgeActor();
            return runtime.Register(new TSupportLinksTestActor(
                httpProxy,
                Location,
                Settings,
                sender,
                request,
                std::move(result)));
        }

        NActors::TActorId RegisterHandler(TTestActorRuntime& runtime) const {
            auto httpProxy = runtime.AllocateEdgeActor();
            return runtime.Register(new NMVP::TMetaSupportLinksHandlerActor(httpProxy, Location, Settings));
        }
    };

    static NHttp::THttpIncomingRequestPtr BuildHttpRequest(TStringBuf url, TStringBuf method = "GET") {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, TStringBuilder() << method << " " << url << " HTTP/1.1\r\nHost: localhost\r\n\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        return request;
    }

    static NYdb::NTable::TDataQueryResult MakeClusterInfoResult() {
        const TString resultSetString = R"(
columns {
  name: "k8s_namespace"
  type { type_id: UTF8 }
}
columns {
  name: "datasource"
  type { type_id: UTF8 }
}
rows {
  items { text_value: "ws" }
  items { text_value: "ds" }
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
  name: "k8s_namespace"
  type { type_id: UTF8 }
}
columns {
  name: "datasource"
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

    static NMVP::TSupportLinksConfig MakeConfig(TStringBuf source) {
        NMVP::TSupportLinksConfig cfg;
        auto* cluster = cfg.AddCluster();
        cluster->SetSource(TString(source));
        cluster->SetTitle("mock");
        cluster->SetUrl("mock://source");
        return cfg;
    }

    static void ClearConfig(NMVP::TMetaSettings& settings) {
        settings.ClusterLinkSources.clear();
        settings.DatabaseLinkSources.clear();
    }

    Y_UNIT_TEST(SupportLinksReturnsBadRequestWhenClusterMissing) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeConfig("mock/sourceSync"));

        auto request = BuildHttpRequest("/meta/support_links?database=/root/test");
        auto result = MakeClusterInfoResult();
        context.RegisterGet(runtime, sender, request, std::move(result));

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
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeEmptyConfig());

        auto handler = context.RegisterHandler(runtime);
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
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeConfig("mock/sourceSync"));

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global");
        auto result = MakeClusterInfoResult();
        context.RegisterGet(runtime, sender, request, std::move(result));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
        AssertMockResponse(response->Response->Body);
    }

    Y_UNIT_TEST(UsesSourceMockActorAsync) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeConfig("mock/sourceAsync"));

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global");
        auto result = MakeClusterInfoResult();
        context.RegisterGet(runtime, sender, request, std::move(result));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");
        AssertMockResponse(response->Response->Body);
    }

    Y_UNIT_TEST(ReturnsEmptyLinksForValidRequestWhenNoSupportLinksConfigured) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeEmptyConfig());

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global&database=/root/test");
        auto result = MakeClusterInfoResult();
        context.RegisterGet(runtime, sender, request, std::move(result));

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
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeConfig("mock/sourceSync"));

        auto request = BuildHttpRequest("/meta/support_links?cluster=missing-cluster");
        auto result = MakeEmptyClusterInfoResult();
        context.RegisterGet(runtime, sender, request, std::move(result));

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
        UNIT_ASSERT(json["errors"].GetArray().size() >= 1);
        bool hasMetaError = false;
        for (const auto& error : json["errors"].GetArray()) {
            if (error["source"].GetStringRobust() == "meta" &&
                error["message"].GetStringRobust().Contains("missing-cluster") &&
                error["message"].GetStringRobust().Contains("is not found in MasterClusterExt.db"))
            {
                hasMetaError = true;
                break;
            }
        }
        UNIT_ASSERT(hasMetaError);
    }
}
