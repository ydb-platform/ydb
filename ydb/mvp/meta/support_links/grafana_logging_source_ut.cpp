#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/json/json_reader.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/meta/support_links/grafana_logging_source.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace {

NMVP::TMetaSettings MakeMetaSettings(TStringBuf grafanaEndpoint) {
    NMVP::TMetaSettings settings;
    settings.SupportLinks.GrafanaEndpoint = TString(grafanaEndpoint);
    return settings;
}

NMVP::TSupportLinkEntryConfig MakeConfig(TStringBuf url = TStringBuf()) {
    NMVP::TSupportLinkEntryConfig config;
    config.SetSource("grafana/logging");
    config.SetTitle("Logs");
    if (!url.empty()) {
        config.SetUrl(TString(url));
    }
    return config;
}

NMVP::TSupportLinkEntryConfig MakeConfigWithBucket(TStringBuf bucket, TStringBuf url = TStringBuf()) {
    NMVP::TSupportLinkEntryConfig config = MakeConfig(url);
    config.SetBucket(TString(bucket));
    return config;
}

NHttp::TUrlParametersBuilder MakeUrlParameters(TStringBuf query) {
    NHttp::TUrlParametersBuilder builder;
    for (TStringBuf param = query.NextTok('&'); !param.empty(); param = query.NextTok('&')) {
        TStringBuf name = param.NextTok('=');
        builder.Set(name, param);
    }
    return builder;
}

NJson::TJsonValue ParseJson(TStringBuf body) {
    NJson::TJsonReaderConfig jsonReaderConfig;
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(body, &jsonReaderConfig, &json));
    return json;
}

struct TGrafanaLoggingTestContext {
    NMVP::TSupportLinkEntryConfig Config;
    NMVP::TMetaSettings Settings;
    THashMap<TString, TString> ClusterInfo;
    NHttp::TUrlParametersBuilder UrlParameters;
    NMVP::ILinkSource::TLinkResolveInput Input;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
    NMVP::ILinkSource::TResolveContext Context;

    explicit TGrafanaLoggingTestContext(TStringBuf url = TStringBuf())
        : Config(MakeConfig(url))
        , Settings(MakeMetaSettings("https://grafana.example.net"))
        , UrlParameters("")
        , Input{
            .ClusterInfo = ClusterInfo,
            .UrlParameters = UrlParameters,
        }
        , Owner(1, "ow")
        , HttpProxyId(2, "hp")
        , Context{
            .Place = 0,
            .EntityType = NMVP::ESupportLinksEntityType::Cluster,
            .Owner = Owner,
            .HttpProxyId = HttpProxyId,
        }
    {}

    std::shared_ptr<NMVP::ILinkSource> CreateSource() const {
        return NMVP::MakeGrafanaLoggingSource(Config, Settings);
    }

    NMVP::TResolveOutput Resolve() const {
        return CreateSource()->Resolve(Input, Context);
    }
};

void AssertPanesQuery(
    const TString& url,
    TStringBuf expectedBaseUrl,
    TStringBuf expectedExpr,
    TStringBuf expectedDatasource)
{
    const TStringBuf actualUrl = url;
    UNIT_ASSERT_VALUES_EQUAL(actualUrl.Before('?'), expectedBaseUrl);

    TCgiParameters query;
    query.Scan(actualUrl.After('?'));
    UNIT_ASSERT_VALUES_EQUAL(query.Get("schemaVersion"), "1");
    UNIT_ASSERT_VALUES_EQUAL(query.Get("orgId"), "1");

    const NJson::TJsonValue panesJson = ParseJson(query.Get("panes"));
    UNIT_ASSERT_VALUES_EQUAL(panesJson.GetMapSafe().size(), 1u);
    const NJson::TJsonValue& pane = panesJson["x"];
    UNIT_ASSERT_VALUES_EQUAL(pane["datasource"].GetString(), expectedDatasource);
    UNIT_ASSERT_VALUES_EQUAL(pane["range"]["from"].GetString(), "now-1h");
    UNIT_ASSERT_VALUES_EQUAL(pane["range"]["to"].GetString(), "now");
    UNIT_ASSERT(!pane.Has("panelsState"));
    UNIT_ASSERT(!pane.Has("compact"));
    UNIT_ASSERT_VALUES_EQUAL(pane["queries"][0]["expr"].GetString(), expectedExpr);
    UNIT_ASSERT_VALUES_EQUAL(pane["queries"][0]["queryType"].GetString(), "range");
    UNIT_ASSERT(!pane["queries"][0].Has("editorMode"));
    UNIT_ASSERT_VALUES_EQUAL(pane["queries"][0]["direction"].GetString(), "backward");
    UNIT_ASSERT_VALUES_EQUAL(pane["queries"][0]["datasource"]["type"].GetString(), "loki");
    UNIT_ASSERT_VALUES_EQUAL(pane["queries"][0]["datasource"]["uid"].GetString(), expectedDatasource);
}

} // namespace

Y_UNIT_TEST_SUITE(SupportLinksGrafanaLoggingSource) {
    Y_UNIT_TEST(ValidationRejectsDefaultRelativeUrlWithoutGrafanaEndpoint) {
        TGrafanaLoggingTestContext context;
        context.Settings = MakeMetaSettings("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "grafana.endpoint is required for relative url"
        );
    }

    Y_UNIT_TEST(ResolveBuildsDefaultExploreUrlFromScratch) {
        TGrafanaLoggingTestContext context;
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "cd868168-09d4-4ece-82db-e646130697e5";
        context.UrlParameters = MakeUrlParameters("database=%2Froot%2Ftenant");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Database;
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://grafana.example.net/explore",
            "{database=\"/root/tenant\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "cd868168-09d4-4ece-82db-e646130697e5"
        );
    }

    Y_UNIT_TEST(ResolveBuildsAbsoluteExploreUrlFromScratch) {
        TGrafanaLoggingTestContext context("https://external.example.net/explore");
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";
        context.UrlParameters = MakeUrlParameters("database=%2Fnew%2Fdb");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Database;
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://external.example.net/explore",
            "{database=\"/new/db\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesOnlyDatabaseRequestParameterForDatabaseEntity) {
        TGrafanaLoggingTestContext context;
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";
        context.UrlParameters = MakeUrlParameters("custom_label=new-value&database=%2Fnew%2Fdb&node=static-node-1&host=host-1.example.net");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Database;
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://grafana.example.net/explore",
            "{database=\"/new/db\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesOnlyNodeRequestParameterForNodeEntity) {
        TGrafanaLoggingTestContext context;
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";
        context.UrlParameters = MakeUrlParameters("cluster=ignored-cluster&custom_label=ignored&database=%2Fnew%2Fdb&node=static-node-1&host=host-1.example.net");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Node;
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://grafana.example.net/explore",
            "{node=\"static-node-1\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesOnlyHostRequestParameterForHostEntity) {
        TGrafanaLoggingTestContext context;
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";
        context.UrlParameters = MakeUrlParameters("cluster=ignored-cluster&custom_label=ignored&database=%2Fnew%2Fdb&node=static-node-1&host=host-1.example.net");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Host;
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://grafana.example.net/explore",
            "{host=\"host-1.example.net\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesLabelOverridesFromConfig) {
        TGrafanaLoggingTestContext context;
        context.Settings.SupportLinks.GrafanaLogging.DatabaseLabel = "db_path";
        context.Settings.SupportLinks.GrafanaLogging.NodeLabel = "instance_id";
        context.Settings.SupportLinks.GrafanaLogging.HostLabel = "node_name";
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";

        context.UrlParameters = MakeUrlParameters("database=%2Fnew%2Fdb");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Database;
        auto databaseResult = context.Resolve();
        UNIT_ASSERT_VALUES_EQUAL(databaseResult.Errors.size(), 0u);
        AssertPanesQuery(
            databaseResult.Links[0].Url,
            "https://grafana.example.net/explore",
            "{db_path=\"/new/db\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );

        context.UrlParameters = MakeUrlParameters("node=static-node-1");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Node;
        auto nodeResult = context.Resolve();
        UNIT_ASSERT_VALUES_EQUAL(nodeResult.Errors.size(), 0u);
        AssertPanesQuery(
            nodeResult.Links[0].Url,
            "https://grafana.example.net/explore",
            "{instance_id=\"static-node-1\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );

        context.UrlParameters = MakeUrlParameters("host=host-1.example.net");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Host;
        auto hostResult = context.Resolve();
        UNIT_ASSERT_VALUES_EQUAL(hostResult.Errors.size(), 0u);
        AssertPanesQuery(
            hostResult.Links[0].Url,
            "https://grafana.example.net/explore",
            "{node_name=\"host-1.example.net\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesClusterLabelOverrideForClusterEntity) {
        TGrafanaLoggingTestContext context;
        context.Settings.SupportLinks.GrafanaLogging.ClusterLabel = "cluster_id";
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";
        context.UrlParameters = MakeUrlParameters("cluster=testing-global");
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://grafana.example.net/explore",
            "{cluster_id=\"testing-global\", __workspace__=\"ydb-common\", __bucket__=\"ydb\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesBucketOverrideFromConfig) {
        TGrafanaLoggingTestContext context;
        context.Config = MakeConfigWithBucket("custom-bucket");
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        context.ClusterInfo["datasource_logging"] = "ds-42";
        context.UrlParameters = MakeUrlParameters("database=%2Fnew%2Fdb");
        context.Context.EntityType = NMVP::ESupportLinksEntityType::Database;
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);
        AssertPanesQuery(
            result.Links[0].Url,
            "https://grafana.example.net/explore",
            "{database=\"/new/db\", __workspace__=\"ydb-common\", __bucket__=\"custom-bucket\"}",
            "ds-42"
        );
    }

    Y_UNIT_TEST(ResolveReturnsErrorWhenWorkspaceMissing) {
        TGrafanaLoggingTestContext context;
        context.ClusterInfo["datasource_logging"] = "ds-42";
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Source, "grafana/logging");
        UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Message, "k8s_namespace is required in cluster info for source=grafana/logging");
    }

    Y_UNIT_TEST(ResolveReturnsErrorWhenDatasourceMissing) {
        TGrafanaLoggingTestContext context;
        context.ClusterInfo["k8s_namespace"] = "ydb-common";
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Source, "grafana/logging");
        UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Message, "datasource_logging is required in cluster info for source=grafana/logging");
    }

    Y_UNIT_TEST(ValidationRejectsUrlWithQueryParameters) {
        TGrafanaLoggingTestContext context("/explore?panes=template");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "query parameters are not supported in url for source=grafana/logging"
        );
    }
}
