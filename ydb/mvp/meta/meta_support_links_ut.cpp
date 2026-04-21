#include <ydb/mvp/meta/meta_support_links.h>
#include <ydb/mvp/meta/support_links/ut/mock_link_source.h>
#include <ydb/mvp/meta/ut/meta_test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>
#include <memory>

namespace {

    std::shared_ptr<NMVP::ILinkSource> MakeTestLinkSource(NMVP::TSupportLinkEntryConfig config, const NMVP::TMetaSettings& settings) {
        if (config.GetSource() == "mock/sync") {
            return NMVP::NTest::MakeMockLinkSourceSync(std::move(config));
        }
        if (config.GetSource() == "mock/async") {
            return NMVP::NTest::MakeMockLinkSourceAsync(std::move(config));
        }
        return NMVP::MakeLinkSource(std::move(config), settings);
    }

} // namespace

Y_UNIT_TEST_SUITE(MetaSupportLinks) {
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

        std::unique_ptr<NMVP::TSupportLinksResolver> CreateSupportLinksResolver() override {
            return std::make_unique<NMVP::TSupportLinksResolver>(NMVP::TSupportLinksResolver::TParams{
                .EntityType = EntityType,
                .Settings = &Settings,
                .ClusterInfo = ClusterInfo,
                .UrlParameters = Request.Parameters.UrlParameters,
                .Owner = SelfId(),
                .HttpProxyId = HttpProxyId,
                .LinkSourceFactory = MakeTestLinkSource,
            });
        }
    };

    struct TSupportLinksTestContext {
        NMVP::TMetaSettings Settings;
        const TYdbLocation Location = TYdbLocation("meta", "meta", {}, "/Root");

        explicit TSupportLinksTestContext(const NMVP::TSupportLinksConfig& config) {
            Settings.SupportLinks.ClusterLinks.reserve(config.GetCluster().size());
            for (int i = 0; i < config.GetCluster().size(); ++i) {
                Settings.SupportLinks.ClusterLinks.push_back(config.GetCluster(i));
            }
            Settings.SupportLinks.DatabaseLinks.reserve(config.GetDatabase().size());
            for (int i = 0; i < config.GetDatabase().size(); ++i) {
                Settings.SupportLinks.DatabaseLinks.push_back(config.GetDatabase(i));
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

    static NYdb::NTable::TDataQueryResult MakeClusterInfoResult() {
        const TString resultSetString = R"(
columns {
  name: "workspace"
  type { type_id: UTF8 }
}
columns {
  name: "grafana_ds"
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

    static NMVP::TSupportLinksConfig MakeConfig(TStringBuf source) {
        NMVP::TSupportLinksConfig cfg;
        auto* cluster = cfg.AddCluster();
        cluster->SetSource(TString(source));
        cluster->SetTitle("mock");
        cluster->SetUrl("mock://source");
        return cfg;
    }

    static NMVP::TSupportLinksConfig MakeEmptyConfig() {
        return {};
    }

    Y_UNIT_TEST(SupportLinksReturnsBadRequestWhenClusterMissing) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        TSupportLinksTestContext context(MakeConfig("mock/sync"));

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
        TSupportLinksTestContext context(MakeConfig("mock/sync"));

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
        TSupportLinksTestContext context(MakeConfig("mock/async"));

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
        TSupportLinksTestContext context(MakeConfig("mock/sync"));

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
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "meta");
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("missing-cluster"));
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("is not found in MasterClusterExt.db"));
    }
}
