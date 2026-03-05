#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>
#include <memory>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/meta_support_links.h>

#include <util/generic/yexception.h>

Y_UNIT_TEST_SUITE(MetaSupportLinks) {
    class TMvpGuard {
    public:
        TMvpGuard() {
            const char* argv[] = {"meta_support_links_ut"};
            Mvp = std::make_unique<NMVP::TMVP>(1, argv);
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

    class TSourceMockActor : public NActors::TActorBootstrapped<TSourceMockActor> {
    public:
        size_t Place = 0;
        NActors::TActorId Parent;
        TVector<NMVP::NSupportLinks::TResolvedLink> Links;
        TVector<NMVP::NSupportLinks::TSupportError> Errors;

        TSourceMockActor(
            size_t place,
            NActors::TActorId parent,
            TVector<NMVP::NSupportLinks::TResolvedLink>&& links,
            TVector<NMVP::NSupportLinks::TSupportError>&& errors)
            : Place(place)
            , Parent(parent)
            , Links(std::move(links))
            , Errors(std::move(errors))
        {}

        void Bootstrap(const NActors::TActorContext& ctx) {
            ctx.Send(Parent, new NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse(Place, std::move(Links), std::move(Errors)));
            Die(ctx);
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

        void RequestClusterInfo(const NActors::TActorContext& ctx) override {
            ctx.Send(ctx.SelfID, new NMVP::THandlerActorYdb::TEvPrivate::TEvDataQueryResult(std::move(Result)));
        }

        NActors::IActor* CreateSourceHandler(NMVP::NSupportLinks::TLinkResolveContext sourceContext) {
            TVector<NMVP::NSupportLinks::TResolvedLink> links;
            TVector<NMVP::NSupportLinks::TSupportError> errors;

            if (sourceContext.LinkConfig.GetSource() == "mock/cluster") {
                links.emplace_back(NMVP::NSupportLinks::TResolvedLink{
                    .Title = sourceContext.LinkConfig.GetTitle(),
                    .Url = TStringBuilder() << "mock://dashboard/" << sourceContext.LinkConfig.GetTitle()
                });
            } else if (sourceContext.LinkConfig.GetSource() == "mock/database") {
                links.emplace_back(NMVP::NSupportLinks::TResolvedLink{
                    .Title = "Search mock link",
                    .Url = "mock://search/result"
                });
                if (sourceContext.LinkConfig.GetTag() == "emit_error") {
                    errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                        .Source = "mock/database",
                        .Message = "Search mock error"
                    });
                }
            } else {
                ythrow yexception() << "Unexpected source in meta_support_links_ut: " << sourceContext.LinkConfig.GetSource();
            }

            return new TSourceMockActor(
                sourceContext.Place,
                sourceContext.Parent,
                std::move(links),
                std::move(errors)
            );
        }
    };

    static NHttp::THttpIncomingRequestPtr BuildHttpRequest(TStringBuf url) {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, TStringBuilder() << "GET " << url << " HTTP/1.1\r\nHost: localhost\r\n\r\n");
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

    static NMVP::TSupportLinksConfig MakeConfig() {
        NMVP::TSupportLinksConfig cfg;
        auto* cluster = cfg.AddCluster();
        cluster->SetSource("mock/cluster");
        cluster->SetTitle("Cluster CPU");
        cluster->SetUrl("/d/cluster-cpu");

        auto* database = cfg.AddDatabase();
        database->SetSource("mock/database");
        database->SetTitle("");
        database->SetUrl("/api/search?limit=100&type=dash-db");
        database->SetTag("emit_error");
        return cfg;
    }

    static NMVP::TSupportLinksConfig MakeEmptyConfig() {
        return {};
    }

    Y_UNIT_TEST(SupportLinksReturnsBadRequestWhenClusterMissing) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        NMVP::InstanceMVP->SupportLinksConfig = MakeConfig();

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

    Y_UNIT_TEST(UsesClusterEntityLinksWithMockFactory) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        NMVP::InstanceMVP->SupportLinksConfig = MakeConfig();

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 0);
        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "mock/cluster");
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("unsupported support_links source"));
    }

    Y_UNIT_TEST(UsesDatabaseEntityLinksAndPropagatesSourceErrorWithMockFactory) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        NMVP::InstanceMVP->SupportLinksConfig = MakeConfig();

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
        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "mock/database");
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("unsupported support_links source"));
    }

    Y_UNIT_TEST(ReturnsEmptyLinksForValidRequestWhenNoSupportLinksConfigured) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        NMVP::InstanceMVP->SupportLinksConfig = MakeEmptyConfig();

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
        NMVP::InstanceMVP->SupportLinksConfig = MakeConfig();

        auto request = BuildHttpRequest("/meta/support_links?cluster=missing-cluster");
        auto result = MakeEmptyClusterInfoResult();
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 0);

        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 2);
        bool hasMetaError = false;
        bool hasUnsupportedSourceError = false;
        for (const auto& error : json["errors"].GetArray()) {
            const auto source = error["source"].GetStringRobust();
            const auto message = error["message"].GetStringRobust();
            if (source == "meta") {
                hasMetaError = message.Contains("missing-cluster") && message.Contains("is not found in MasterClusterExt.db");
            } else if (source == "mock/cluster") {
                hasUnsupportedSourceError = message.Contains("unsupported support_links source");
            }
        }
        UNIT_ASSERT(hasMetaError);
        UNIT_ASSERT(hasUnsupportedSourceError);
    }
}
