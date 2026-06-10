#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/cgiparam/cgiparam.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_source.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace {

NMVP::TMetaSettings MakeMetaSettings(TStringBuf grafanaEndpoint) {
    NMVP::TMetaSettings settings;
    settings.SupportLinks.GrafanaEndpoint = TString(grafanaEndpoint);
    return settings;
}

NMVP::TSupportLinkEntryConfig ParseGrafanaDashboardConfig(TStringBuf url) {
    NMVP::TSupportLinkEntryConfig config;
    config.SetSource("grafana/dashboard");
    config.SetTitle("CPU");
    config.SetUrl(TString(url));
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

void AssertSingleResolvedLink(const NMVP::TResolveOutput& result, TStringBuf expectedUrl) {
    UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1);
    const TStringBuf actualUrl = result.Links[0].Url;
    UNIT_ASSERT_VALUES_EQUAL(actualUrl.Before('?'), expectedUrl.Before('?'));

    TCgiParameters actualQuery;
    TCgiParameters expectedQuery;
    actualQuery.Scan(actualUrl.After('?'));
    expectedQuery.Scan(expectedUrl.After('?'));

    UNIT_ASSERT_VALUES_EQUAL(actualQuery.Print(), expectedQuery.Print());
    UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0);
}

struct TGrafanaDashboardTestContext {
    NMVP::TSupportLinkEntryConfig Config;
    NMVP::TMetaSettings Settings;
    NMVP::NSupportLinks::EEntityType EntityType = NMVP::NSupportLinks::EEntityType::Database;
    THashMap<TString, TString> ClusterInfo;
    NHttp::TUrlParametersBuilder UrlParameters;
    TCgiParameters AdditionalRequestParams;
    NMVP::NSupportLinks::TEntityIdentity Identity;
    NMVP::NSupportLinks::ILinkSource::TLinkResolveInput Input;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
    NMVP::NSupportLinks::ILinkSource::TResolveContext Context;

    explicit TGrafanaDashboardTestContext(TStringBuf url = "/d/cpu")
        : Config(ParseGrafanaDashboardConfig(url))
        , Settings(MakeMetaSettings("https://grafana.example.net"))
        , UrlParameters("")
        , AdditionalRequestParams("")
        , Input{
            .ClusterInfo = ClusterInfo,
            .AdditionalRequestParams = AdditionalRequestParams,
            .Identity = Identity,
        }
        , Owner(1, "ow")
        , HttpProxyId(2, "hp")
        , Context{
            .Place = 0,
            .Owner = Owner,
            .HttpProxyId = HttpProxyId,
        }
    {}

    std::shared_ptr<NMVP::NSupportLinks::ILinkSource> CreateSource() const {
        return NMVP::NSupportLinks::MakeGrafanaDashboardSource(Config, EntityType, Settings);
    }

    void SetDefaultClusterInfo() {
        ClusterInfo["k8s_namespace"] = "ydb-workspace";
        ClusterInfo["datasource"] = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63";
    }

    NMVP::TResolveOutput Resolve() {
        Identity = NMVP::NSupportLinks::BuildEntityIdentity(EntityType, UrlParameters);
        AdditionalRequestParams = NMVP::NSupportLinks::BuildAdditionalRequestParameters(UrlParameters);
        return CreateSource()->Resolve(Input, Context);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SupportLinksGrafanaDashboardSource) {
    Y_UNIT_TEST(ValidationRejectsEmptyUrl) {
        TGrafanaDashboardTestContext context("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "url is required for source=grafana/dashboard"
        );
    }

    Y_UNIT_TEST(ValidationRejectsRelativeUrlWithoutGrafanaEndpoint) {
        TGrafanaDashboardTestContext context;
        context.Settings = MakeMetaSettings("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "grafana.endpoint is required for relative url"
        );
    }

    Y_UNIT_TEST(ResolveBuildsFullUrl) {
        TGrafanaDashboardTestContext context;
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&database=root_test");
        auto result = context.Resolve();

        UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(result.Links[0].Title, "CPU");
        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-cluster=ydb-global&var-database=root_test"
        );
    }

    Y_UNIT_TEST(ResolveSkipsMissingDatasourceInAdditionalParams) {
        TGrafanaDashboardTestContext context;
        auto* datasource = context.Config.AddLinkParameterMappings();
        datasource->SetParameter("ds");
        datasource->SetFromClusterInfo("datasource");
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&database=%2Froot%2Ftest");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-cluster=ydb-global&var-database=/root/test"
        );
    }

    Y_UNIT_TEST(ResolveSkipsEmptyDatasourceInAdditionalParams) {
        TGrafanaDashboardTestContext context;
        auto* datasource = context.Config.AddLinkParameterMappings();
        datasource->SetParameter("ds");
        datasource->SetFromClusterInfo("datasource");
        context.ClusterInfo["datasource"] = "";
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&database=%2Froot%2Ftest");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-cluster=ydb-global&var-database=/root/test"
        );
    }

    Y_UNIT_TEST(ResolveSkipsMissingWorkspaceInAdditionalParams) {
        TGrafanaDashboardTestContext context;
        auto* workspace = context.Config.AddLinkParameterMappings();
        workspace->SetParameter("workspace");
        workspace->SetFromClusterInfo("k8s_namespace");
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-cluster=ydb-global"
        );
    }

    Y_UNIT_TEST(ResolveUsesAbsoluteUrlWithoutGrafanaEndpoint) {
        TGrafanaDashboardTestContext context("https://external.example.net/d/cpu");
        context.Settings = MakeMetaSettings("");
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("database=root");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://external.example.net/d/cpu?var-database=root"
        );
    }

    Y_UNIT_TEST(ResolveUsesAbsoluteUrlWithGrafanaEndpoint) {
        TGrafanaDashboardTestContext context("https://external.example.net/d/cpu");
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("database=root");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://external.example.net/d/cpu?var-database=root"
        );
    }

    Y_UNIT_TEST(ResolveJoinsRelativeUrlWithTrailingSlashInGrafanaEndpoint) {
        TGrafanaDashboardTestContext context("/d/cpu");
        context.Settings = MakeMetaSettings("https://grafana.example.net/");
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("database=root");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-database=root"
        );
    }

    Y_UNIT_TEST(ResolveJoinsRelativeUrlWithoutLeadingSlash) {
        TGrafanaDashboardTestContext context("d/cpu");
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("database=root");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-database=root"
        );
    }

    Y_UNIT_TEST(ResolveJoinsRelativeUrlWithoutLeadingSlashWithTrailingSlashInGrafanaEndpoint) {
        TGrafanaDashboardTestContext context("d/cpu");
        context.Settings = MakeMetaSettings("https://grafana.example.net/");
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("database=root");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-database=root"
        );
    }

    Y_UNIT_TEST(ResolveEncodesQueryParameters) {
        TGrafanaDashboardTestContext context;
        context.SetDefaultClusterInfo();
        context.UrlParameters = MakeUrlParameters("database=root%26x%3Dy");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-database=root%26x%3Dy"
        );
    }

    Y_UNIT_TEST(ResolveAppliesRequestAndAdditionalParamOverrides) {
        TGrafanaDashboardTestContext context;
        context.SetDefaultClusterInfo();
        auto* database = context.Config.AddLinkParameterMappings();
        database->SetParameter("db_path");
        database->SetFromRequest("database");
        auto* workspace = context.Config.AddLinkParameterMappings();
        workspace->SetParameter("workspace");
        workspace->SetFromClusterInfo("custom_namespace");
        context.ClusterInfo["custom_namespace"] = "custom-workspace";
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&database=%2Froot%2Ftest");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://grafana.example.net/d/cpu?var-workspace=custom-workspace&var-cluster=ydb-global&var-db_path=/root/test"
        );
    }
}
