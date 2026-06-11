#include <ydb/mvp/meta/meta_capabilities.h>
#include <ydb/mvp/meta/ut/meta_test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(MetaCapabilities) {
    Y_UNIT_TEST(ReturnsRegisteredCapabilitiesIncludingSelf) {
        TTestActorRuntime runtime;
        TAutoPtr<NActors::IEventHandle> handle;

        auto capabilitiesRegistry = std::make_shared<NMVP::TMetaCapabilities>();
        capabilitiesRegistry->AddCapability("/meta/support_links");
        capabilitiesRegistry->AddCapability("/meta/cloud", 7);
        capabilitiesRegistry->AddCapability("/capabilities");

        auto sender = runtime.AllocateEdgeActor();
        auto capabilities = runtime.Register(new NMVP::THandlerActorMetaCapabilities(capabilitiesRegistry));
        auto request = BuildHttpRequest("/capabilities");

        runtime.Send(new NActors::IEventHandle(
            capabilities,
            sender,
            new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)
        ));

        auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, "200");

        static constexpr TStringBuf expectedBody = R"(
{
  "Capabilities": {
    "/capabilities": 1,
    "/meta/cloud": 7,
    "/meta/support_links": 1
  }
}
)";
        AssertJsonEquals(response->Response->Body, expectedBody);
    }
}
