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

        NActors::IActor* CreateSourceHandler(NMVP::NSupportLinks::TLinkResolveContext sourceContext) override {
            TVector<NMVP::NSupportLinks::TResolvedLink> links;
            TVector<NMVP::NSupportLinks::TSupportError> errors;

            if (sourceContext.LinkConfig.Source == "mock/cluster") {
                links.emplace_back(NMVP::NSupportLinks::TResolvedLink{
                    .Title = sourceContext.LinkConfig.Title,
                    .Url = TStringBuilder() << "mock://dashboard/" << sourceContext.LinkConfig.Title
                });
            } else if (sourceContext.LinkConfig.Source == "mock/database") {
                links.emplace_back(NMVP::NSupportLinks::TResolvedLink{
                    .Title = "Search mock link",
                    .Url = "mock://search/result"
                });
                if (sourceContext.LinkConfig.Tag == "emit_error") {
                    errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                        .Source = "mock/database",
                        .Message = "Search mock error"
                    });
                }
            } else {
                ythrow yexception() << "Unexpected source in meta_support_links_ut: " << sourceContext.LinkConfig.Source;
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
            << "columns {\n"
            << "  name: \"workspace\"\n"
            << "  type { type_id: UTF8 }\n"
            << "}\n"
            << "columns {\n"
            << "  name: \"grafana_ds\"\n"
            << "  type { type_id: UTF8 }\n"
            << "}\n"
            << "rows {\n"
            << "  items { text_value: \"" << workspace << "\" }\n"
            << "  items { text_value: \"" << datasource << "\" }\n"
            << "}\n";

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
        const TString resultSetString = TStringBuilder()
            << "columns {\n"
            << "  name: \"workspace\"\n"
            << "  type { type_id: UTF8 }\n"
            << "}\n"
            << "columns {\n"
            << "  name: \"grafana_ds\"\n"
            << "  type { type_id: UTF8 }\n"
            << "}\n";

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
        cfg.Cluster.push_back(NMVP::TSupportLinkEntryConfig{
            .Source = "mock/cluster",
            .Title = "Cluster CPU",
            .Url = "/d/cluster-cpu",
        });
        cfg.Database.push_back(NMVP::TSupportLinkEntryConfig{
            .Source = "mock/database",
            .Title = "",
            .Url = "/api/search?limit=100&type=dash-db",
            .Tag = "emit_error",
        });
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
        NMVP::InstanceMVP->MetaSettings.SupportLinksConfig = MakeConfig();

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
        NMVP::InstanceMVP->MetaSettings.SupportLinksConfig = MakeConfig();

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["title"].GetStringRobust(), "Cluster CPU");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["url"].GetStringRobust(), "mock://dashboard/Cluster CPU");
        UNIT_ASSERT(!json.Has("errors"));
    }

    Y_UNIT_TEST(UsesDatabaseEntityLinksAndPropagatesSourceErrorWithMockFactory) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        NMVP::InstanceMVP->MetaSettings.SupportLinksConfig = MakeConfig();

        auto request = BuildHttpRequest("/meta/support_links?cluster=testing-global&database=/root/test");
        auto result = MakeClusterInfoResult("ws", "ds");
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["title"].GetStringRobust(), "Search mock link");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["url"].GetStringRobust(), "mock://search/result");
        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "mock/database");
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["message"].GetStringRobust(), "Search mock error");
    }

    Y_UNIT_TEST(ReturnsEmptyLinksForValidRequestWhenNoSupportLinksConfigured) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        NMVP::InstanceMVP->MetaSettings.SupportLinksConfig = MakeEmptyConfig();

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
        NMVP::InstanceMVP->MetaSettings.SupportLinksConfig = MakeConfig();

        auto request = BuildHttpRequest("/meta/support_links?cluster=missing-cluster");
        auto result = MakeEmptyClusterInfoResult();
        runtime.Register(new TSupportLinksTestActor(anyHttpProxy, location, sender, request, std::move(result)));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        NJson::TJsonReaderConfig jsonReaderConfig;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response->Response->Body, &jsonReaderConfig, &json));
        UNIT_ASSERT(json.Has("links"));
        UNIT_ASSERT_VALUES_EQUAL(json["links"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["title"].GetStringRobust(), "Cluster CPU");
        UNIT_ASSERT_VALUES_EQUAL(json["links"][0]["url"].GetStringRobust(), "mock://dashboard/Cluster CPU");

        UNIT_ASSERT(json.Has("errors"));
        UNIT_ASSERT_VALUES_EQUAL(json["errors"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "meta");
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("missing-cluster"));
        UNIT_ASSERT(json["errors"][0]["message"].GetStringRobust().Contains("is not found in MasterClusterExt.db"));
    }
}
