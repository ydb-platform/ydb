#include <ydb/mvp/meta/meta_support_links_config.h>
#include <ydb/mvp/meta/ut/meta_test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

NMVP::TSupportLinkEntryConfig MakeSupportLinkEntry(TStringBuf source) {
    NMVP::TSupportLinkEntryConfig config;
    config.SetSource(TString(source));
    return config;
}

} // namespace

Y_UNIT_TEST_SUITE(MetaSupportLinksConfig) {
    Y_UNIT_TEST(ReturnsConfiguredEntities) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        NMVP::TSupportLinksSettings settings;
        settings.ClusterLinks.push_back(MakeSupportLinkEntry("grafana/dashboard"));
        settings.NodeLinks.push_back(MakeSupportLinkEntry("grafana/logging"));

        auto sender = runtime.AllocateEdgeActor();
        auto supportLinksConfig = runtime.Register(new NMVP::THandlerActorMetaSupportLinksConfig(settings));
        auto request = BuildHttpRequest("/meta/support_links/config");

        runtime.Send(new NActors::IEventHandle(
            supportLinksConfig,
            sender,
            new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)
        ));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        static constexpr TStringBuf expectedBody = R"(
{
  "entities": {
    "cluster": {
      "configured": true
    },
    "database": {
      "configured": false
    },
    "host": {
      "configured": false
    },
    "node": {
      "configured": true
    }
  }
}
)";
        AssertJsonEquals(response->Response->Body, expectedBody);
    }
}
