#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/meta/support_links/source.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace {

using EEntityType = NMVP::NSupportLinks::EEntityType;

NHttp::TUrlParametersBuilder MakeUrlParameters(TStringBuf query) {
    NHttp::TUrlParametersBuilder builder;
    for (TStringBuf param = query.NextTok('&'); !param.empty(); param = query.NextTok('&')) {
        TStringBuf name = param.NextTok('=');
        builder.Set(name, param);
    }
    return builder;
}

std::pair<TStringBuf, TStringBuf> SplitFragment(TStringBuf url) {
    const size_t fragmentPos = url.find('#');
    if (fragmentPos == TStringBuf::npos) {
        return {url, TStringBuf()};
    }
    return {
        url.SubStr(0, fragmentPos),
        url.SubStr(fragmentPos + 1),
    };
}

void AssertSingleResolvedLink(const NMVP::TResolveOutput& result, TStringBuf expectedUrl) {
    UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);

    const auto [actualUrl, actualFragment] = SplitFragment(result.Links[0].Url);
    const auto [expectedUrlWithoutFragment, expectedFragment] = SplitFragment(expectedUrl);
    UNIT_ASSERT_VALUES_EQUAL(actualFragment, expectedFragment);
    UNIT_ASSERT_VALUES_EQUAL(actualUrl.Before('?'), expectedUrlWithoutFragment.Before('?'));

    TCgiParameters actualQuery;
    TCgiParameters expectedQuery;
    actualQuery.Scan(actualUrl.After('?'));
    expectedQuery.Scan(expectedUrlWithoutFragment.After('?'));
    UNIT_ASSERT_VALUES_EQUAL(actualQuery.Print(), expectedQuery.Print());
}

void AssertSingleResolveErrorContains(const NMVP::TResolveOutput& result, TStringBuf expectedMessagePart) {
    UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(result.Errors[0].Source, "url");
    UNIT_ASSERT_STRING_CONTAINS(result.Errors[0].Message, expectedMessagePart);
}

struct TUrlSourceTestContext {
    NMVP::TSupportLinkEntryConfig Config;
    NMVP::TMetaSettings Settings;
    THashMap<TString, TString> ClusterInfo;
    NHttp::TUrlParametersBuilder UrlParameters;
    EEntityType EntityType;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
    NMVP::NSupportLinks::ILinkSource::TResolveContext Context;

    explicit TUrlSourceTestContext(TStringBuf url = "https://support.example.net/link")
        : UrlParameters("")
        , EntityType(EEntityType::Cluster)
        , Owner(1, "ow")
        , HttpProxyId(2, "hp")
        , Context{
            .Place = 0,
            .Owner = Owner,
            .HttpProxyId = HttpProxyId,
        }
    {
        Config.SetTitle("Support");
        Config.SetUrl(TString(url));
    }

    std::shared_ptr<NMVP::NSupportLinks::ILinkSource> CreateSource() const {
        return NMVP::NSupportLinks::MakeLinkSource(Config, Settings);
    }

    NMVP::TResolveOutput Resolve() const {
        const TCgiParameters additionalRequestParams = NMVP::NSupportLinks::BuildAdditionalRequestParameters(UrlParameters);
        const auto identity = NMVP::NSupportLinks::BuildEntityIdentity(EntityType, UrlParameters);
        return CreateSource()->Resolve(NMVP::NSupportLinks::ILinkSource::TLinkResolveInput{
            .ClusterInfo = ClusterInfo,
            .AdditionalRequestParams = additionalRequestParams,
            .Identity = identity,
        }, Context);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SupportLinksUrlSource) {
    Y_UNIT_TEST(AllowsExplicitUrlSource) {
        TUrlSourceTestContext context;
        context.Config.SetSource("url");
        UNIT_ASSERT_NO_EXCEPTION(context.CreateSource());
    }

    Y_UNIT_TEST(ValidationRejectsEmptyUrl) {
        TUrlSourceTestContext context("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "url is required for source=url"
        );
    }

    Y_UNIT_TEST(ValidationRejectsTagAndFolder) {
        TUrlSourceTestContext context;
        context.Config.AddTag("ui-cluster");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "tag is not supported for source=url"
        );

        context.Config.ClearTag();
        context.Config.AddFolder("folder-1");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "folder is not supported for source=url"
        );
    }

    Y_UNIT_TEST(ResolveReturnsStaticUrlWithoutMappings) {
        TUrlSourceTestContext context;
        auto result = context.Resolve();

        AssertSingleResolvedLink(result, "https://support.example.net/link");
        UNIT_ASSERT_VALUES_EQUAL(result.Links[0].Title, "Support");
    }

    Y_UNIT_TEST(ResolveForwardsCurrentRequestParametersWithoutMappings) {
        TUrlSourceTestContext context;
        context.EntityType = EEntityType::Host;
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net&ticket=ABC-42&node=ignored-node");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://support.example.net/link?cluster=ydb-global&host=node-1.example.net&ticket=ABC-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesIdentityAndAdditionalRequestClusterInfoAndStaticMappings) {
        TUrlSourceTestContext context;
        auto* clusterMapping = context.Config.AddLinkParameterMappings();
        clusterMapping->SetParameter("cluster_name");
        clusterMapping->SetFromRequest("cluster");
        auto* customMapping = context.Config.AddLinkParameterMappings();
        customMapping->SetParameter("ticket");
        customMapping->SetFromRequest("ticket");
        auto* dcMapping = context.Config.AddLinkParameterMappings();
        dcMapping->SetParameter("dc");
        dcMapping->SetFromClusterInfo("support_dc");
        auto* bucketMapping = context.Config.AddLinkParameterMappings();
        bucketMapping->SetParameter("bucket");
        bucketMapping->SetStaticValue("ydb");

        context.ClusterInfo["support_dc"] = "man-testing";
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&ticket=ABC-42");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://support.example.net/link?ticket=ABC-42&dc=man-testing&bucket=ydb&cluster_name=ydb-global"
        );
    }

    Y_UNIT_TEST(ResolveUsesHostIdentityAndPreservesExistingQueryAndSkipsMissingValues) {
        TUrlSourceTestContext context("https://support.example.net/link?tab=overview");
        auto* hostMapping = context.Config.AddLinkParameterMappings();
        hostMapping->SetParameter("host");
        hostMapping->SetFromRequest("host");
        auto* missingMapping = context.Config.AddLinkParameterMappings();
        missingMapping->SetParameter("dc");
        missingMapping->SetFromClusterInfo("missing_dc");

        context.EntityType = EEntityType::Host;
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net&node=ignored-node");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://support.example.net/link?tab=overview&host=node-1.example.net"
        );
    }

    Y_UNIT_TEST(ResolveUsesRequestValueForTemplateParameter) {
        TUrlSourceTestContext context("https://service.example.net/instance/{host}?dc={dc}");
        context.Config.SetSource("url");

        context.EntityType = EEntityType::Host;
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net&dc=req");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://service.example.net/instance/node-1.example.net?dc=req"
        );
    }

    Y_UNIT_TEST(ResolveEscapesTemplateValuesUsedInQueryTemplateParameters) {
        TUrlSourceTestContext context("https://service.example.net/instance/{host}?ticket={ticket}");
        context.Config.SetSource("url");

        context.EntityType = EEntityType::Host;
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net");
        context.ClusterInfo["ticket"] = "A&B?x=1";
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://service.example.net/instance/node-1.example.net?ticket=A%26B%3Fx%3D1"
        );
    }

    Y_UNIT_TEST(ResolveDoesNotUseQueryMappingsForTemplateParameters) {
        TUrlSourceTestContext context("https://service.example.net/instance/{host}?dc={dc}");
        context.Config.SetSource("url");
        auto* dcMapping = context.Config.AddLinkParameterMappings();
        dcMapping->SetParameter("dc");
        dcMapping->SetFromClusterInfo("support_dc");

        context.EntityType = EEntityType::Host;
        context.ClusterInfo["support_dc"] = "man";
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net");
        auto result = context.Resolve();

        AssertSingleResolveErrorContains(result, "failed to render URL template");
    }

    Y_UNIT_TEST(ResolvePreservesUrlFragmentAfterQueryMerge) {
        TUrlSourceTestContext context("https://service.example.net/instance/{host}?dc={dc}#details-{host}");
        context.Config.SetSource("url");

        context.EntityType = EEntityType::Host;
        context.ClusterInfo["dc"] = "man";
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net&ticket=ABC-42");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://service.example.net/instance/node-1.example.net?dc=man&ticket=ABC-42#details-node-1.example.net"
        );
    }

    Y_UNIT_TEST(ResolveReturnsErrorForMissingTemplateParameter) {
        TUrlSourceTestContext context("https://service.example.net/instance/{host}?dc={dc}");
        context.Config.SetSource("url");
        context.EntityType = EEntityType::Host;
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net");

        auto result = context.Resolve();

        AssertSingleResolveErrorContains(result, "failed to render URL template");
    }
}
