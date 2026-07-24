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

void AssertSingleResolvedLink(const NMVP::TResolveOutput& result, TStringBuf expectedUrl) {
    UNIT_ASSERT_VALUES_EQUAL(result.Links.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(result.Errors.size(), 0u);

    const TStringBuf actualUrl = result.Links[0].Url;
    UNIT_ASSERT_VALUES_EQUAL(actualUrl.Before('?'), expectedUrl.Before('?'));

    TCgiParameters actualQuery;
    TCgiParameters expectedQuery;
    actualQuery.Scan(actualUrl.After('?'));
    expectedQuery.Scan(expectedUrl.After('?'));
    UNIT_ASSERT_VALUES_EQUAL(actualQuery.Print(), expectedQuery.Print());
}

struct TSimpleLinkTestContext {
    NMVP::TSupportLinkEntryConfig Config;
    NMVP::TMetaSettings Settings;
    THashMap<TString, TString> ClusterInfo;
    NHttp::TUrlParametersBuilder UrlParameters;
    EEntityType EntityType;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
    NMVP::NSupportLinks::ILinkSource::TResolveContext Context;

    explicit TSimpleLinkTestContext(TStringBuf url = "https://support.example.net/link")
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

Y_UNIT_TEST_SUITE(SupportLinksSimpleSource) {
    Y_UNIT_TEST(ValidationRejectsEmptyUrl) {
        TSimpleLinkTestContext context("");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "url is required when source is omitted"
        );
    }

    Y_UNIT_TEST(ValidationRejectsTagAndFolder) {
        TSimpleLinkTestContext context;
        context.Config.AddTag("ui-cluster");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "tag is not supported when source is omitted"
        );

        context.Config.ClearTag();
        context.Config.AddFolder("folder-1");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            context.CreateSource(),
            yexception,
            "folder is not supported when source is omitted"
        );
    }

    Y_UNIT_TEST(ResolveReturnsStaticUrlWithoutMappings) {
        TSimpleLinkTestContext context;
        auto result = context.Resolve();

        AssertSingleResolvedLink(result, "https://support.example.net/link");
        UNIT_ASSERT_VALUES_EQUAL(result.Links[0].Title, "Support");
    }

    Y_UNIT_TEST(ResolveForwardsCurrentRequestParametersWithoutMappings) {
        TSimpleLinkTestContext context;
        context.EntityType = EEntityType::Host;
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&host=node-1.example.net&ticket=ABC-42&node=ignored-node");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://support.example.net/link?cluster=ydb-global&host=node-1.example.net&ticket=ABC-42"
        );
    }

    Y_UNIT_TEST(ResolveUsesIdentityAndAdditionalRequestClusterInfoAndStaticMappings) {
        TSimpleLinkTestContext context;
        auto* clusterMapping = context.Config.AddLinkParameterMappings();
        clusterMapping->SetParameter("cluster_name");
        clusterMapping->SetFromRequest("cluster");
        auto* customMapping = context.Config.AddLinkParameterMappings();
        customMapping->SetParameter("ticket");
        customMapping->SetFromRequest("ticket");
        auto* dcMapping = context.Config.AddLinkParameterMappings();
        dcMapping->SetParameter("dc");
        dcMapping->SetFromClusterInfo("hwaas_dc");
        auto* bucketMapping = context.Config.AddLinkParameterMappings();
        bucketMapping->SetParameter("bucket");
        bucketMapping->SetStaticValue("ydb");

        context.ClusterInfo["hwaas_dc"] = "man-testing";
        context.UrlParameters = MakeUrlParameters("cluster=ydb-global&ticket=ABC-42");
        auto result = context.Resolve();

        AssertSingleResolvedLink(
            result,
            "https://support.example.net/link?ticket=ABC-42&dc=man-testing&bucket=ydb&cluster_name=ydb-global"
        );
    }

    Y_UNIT_TEST(ResolveUsesHostIdentityAndPreservesExistingQueryAndSkipsMissingValues) {
        TSimpleLinkTestContext context("https://support.example.net/link?tab=overview");
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
}
