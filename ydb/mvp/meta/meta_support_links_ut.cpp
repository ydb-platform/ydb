#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>
#include <memory>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/mvp/core/mvp_test_runtime.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/meta_support_links.h>
#include <ydb/mvp/meta/support_links/source.h>

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

    static NMVP::TSupportLinkEntry MakeSupportLinkEntry(
        TString source,
        TString title,
        TString url,
        TString tag = {},
        TString folder = {})
    {
        NMVP::TSupportLinkEntry entry;
        entry.SetSource(std::move(source));
        entry.SetTitle(std::move(title));
        entry.SetUrl(std::move(url));
        entry.SetTag(std::move(tag));
        entry.SetFolder(std::move(folder));
        return entry;
    }

    static void FillConfig(NMVP::TMetaSettings& settings) {
        settings.ClusterLinkSources.push_back(NMVP::MakeGrafanaLinkSource(
            settings.ClusterLinkSources.size(),
            MakeSupportLinkEntry("grafana/dashboard", "Cluster CPU", "/d/cluster-cpu")
        ));
        settings.DatabaseLinkSources.push_back(NMVP::MakeGrafanaLinkSource(
            settings.DatabaseLinkSources.size(),
            MakeSupportLinkEntry("grafana/dashboard/search", "", "/api/search?limit=100&type=dash-db", "emit_error")
        ));
    }

    static void ClearConfig(NMVP::TMetaSettings& settings) {
        settings.ClusterLinkSources.clear();
        settings.DatabaseLinkSources.clear();
    }

    Y_UNIT_TEST(SupportLinksReturnsBadRequestWhenClusterMissing) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        ClearConfig(NMVP::InstanceMVP->MetaSettings);
        FillConfig(NMVP::InstanceMVP->MetaSettings);

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

    Y_UNIT_TEST(UsesClusterEntityLinks) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        ClearConfig(NMVP::InstanceMVP->MetaSettings);
        FillConfig(NMVP::InstanceMVP->MetaSettings);

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
        UNIT_ASSERT_VALUES_EQUAL(
            json["links"][0]["url"].GetStringRobust(),
            "/d/cluster-cpu?var-workspace=ws&var-ds=ds&var-cluster=testing-global");
        UNIT_ASSERT(!json.Has("errors"));
    }

    Y_UNIT_TEST(UsesDatabaseEntityLinksAndReturnsSourceErrors) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        ClearConfig(NMVP::InstanceMVP->MetaSettings);
        FillConfig(NMVP::InstanceMVP->MetaSettings);

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
        UNIT_ASSERT_VALUES_EQUAL(json["errors"][0]["source"].GetStringRobust(), "grafana/dashboard/search");
        UNIT_ASSERT(!json["errors"][0]["message"].GetStringRobust().empty());
    }

    Y_UNIT_TEST(ReturnsEmptyLinksForValidRequestWhenNoSupportLinksConfigured) {
        TMvpGuard mvpGuard;
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto sender = runtime.AllocateEdgeActor();
        auto anyHttpProxy = runtime.AllocateEdgeActor();
        TYdbLocation location("meta", "meta", {}, "/Root");
        ClearConfig(NMVP::InstanceMVP->MetaSettings);

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
        ClearConfig(NMVP::InstanceMVP->MetaSettings);
        FillConfig(NMVP::InstanceMVP->MetaSettings);

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
        UNIT_ASSERT_VALUES_EQUAL(
            json["links"][0]["url"].GetStringRobust(),
            "/d/cluster-cpu?var-cluster=missing-cluster");

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
