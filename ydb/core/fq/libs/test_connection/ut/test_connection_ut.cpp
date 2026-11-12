#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/test_connection/events/events.h>
#include <ydb/core/fq/libs/test_connection/test_connection.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/helpers.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/str.h>

namespace NFq {

namespace {

void CheckExternalServiceCredentialsInTrace(FederatedQuery::ExternalService::Protocol protocol) {
    const TString endpoint = protocol == FederatedQuery::ExternalService::HTTP ? "https://service.test" : "service.test:443";
    const TString serviceToken = "external-service-token-sentinel";
    const TString headerSecret = "external-service-header-sentinel";
    TStringStream log;
    {
        NKikimr::TTestBasicRuntime runtime;
        runtime.SetLogBackend(new TStreamLogBackend(&log));
        NKikimr::TAppPrepare app;
        runtime.Initialize(app.Unwrap());
        runtime.SetLogPriority(NKikimrServices::YQ_TEST_CONNECTION, NActors::NLog::PRI_TRACE);

        NConfig::TControlPlaneStorageConfig storageConfig;
        storageConfig.AddAvailableConnection("EXTERNAL_SERVICE");
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const auto actor = runtime.Register(CreateTestConnectionActor({}, storageConfig, {}, {}, {}, {}, {}, {}, nullptr, {}, counters));
        const auto sender = runtime.AllocateEdgeActor();
        NActors::TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(options);

        FederatedQuery::TestConnectionRequest request;
        auto* service = request.mutable_setting()->mutable_external_service();
        service->set_protocol(protocol);
        service->set_endpoint(endpoint);
        service->mutable_auth()->mutable_token()->set_token(serviceToken);
        (*service->mutable_headers())["x-api-key"] = headerSecret;
        runtime.Send(
            new NActors::IEventHandle(actor, sender, new TEvTestConnection::TEvTestConnectionRequest("yandexcloud://test-folder", request,
                                                         "test-user", "caller-token", "test-cloud", TPermissions{}, {}, {}, {})));

        const auto response = runtime.GrabEdgeEvent<TEvTestConnection::TEvTestConnectionResponse>(sender, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        const auto& issues = response->Get()->Issues;
        UNIT_ASSERT_VALUES_EQUAL(issues.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(issues.begin()->GetCode(), static_cast<ui32>(TIssuesIds::BAD_REQUEST));
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "External service connections must be tested using a service query");
    }

    // Assert the actor emitted the request trace: a disabled logger or early validation error must not pass this test.
    UNIT_ASSERT_STRING_CONTAINS(log.Str(), "TestConnectionRequest");
    UNIT_ASSERT_STRING_CONTAINS(log.Str(), endpoint);
    UNIT_ASSERT_C(!log.Str().Contains(serviceToken), "TestConnection TRACE exposes the service token");
    UNIT_ASSERT_C(!log.Str().Contains(headerSecret), "TestConnection TRACE exposes a static header secret");
}

}   // namespace

Y_UNIT_TEST_SUITE(TTestConnectionLogging) {
    Y_UNIT_TEST(HttpExternalServiceCredentialsAreHiddenInTrace) {
        CheckExternalServiceCredentialsInTrace(FederatedQuery::ExternalService::HTTP);
    }

    Y_UNIT_TEST(GrpcExternalServiceCredentialsAreHiddenInTrace) {
        CheckExternalServiceCredentialsInTrace(FederatedQuery::ExternalService::GRPC);
    }
}

}   // namespace NFq
