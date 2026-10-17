#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/service_control_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include "service_control_service.h"

Y_UNIT_TEST_SUITE(TServiceControlServiceTest) {
    using namespace NKikimr;
    using namespace Tests;

    struct TFixture {
        TPortManager PortManager;
        THolder<TServer> Server;
        TTestActorRuntime* Runtime = nullptr;
        TActorId Sender;

        TFixture() {
            ui16 kikimrPort = PortManager.GetPort(2134);
            NKikimrProto::TAuthConfig authConfig;
            auto settings = TServerSettings(kikimrPort, authConfig);
            settings.SetDomainName("Root");
            Server = MakeHolder<TServer>(settings);
            Runtime = Server->GetRuntime();
            Sender = Runtime->AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST(SetupAndRevokeDelegation) {
        TFixture f;
        TAutoPtr<IEventHandle> handle;

        ui16 servicePort = f.PortManager.GetPort(8443);
        IActor* serviceControl = NCloud::CreateServiceControlService(NCloud::TServiceControlServiceSettings("localhost:" + ToString(servicePort), "ydb-test"));
        const TActorId serviceControlId = f.Runtime->Register(serviceControl);

        TServiceControlServiceMock mock;
        mock.ExpectedAuthorization = "Bearer ssa-token";
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:" + ToString(servicePort), grpc::InsecureServerCredentials()).RegisterService(&mock);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

        {
            auto request = MakeHolder<NCloud::TEvServiceControlService::TEvSetupDelegationRequest>();
            request->Token = "ssa-token";
            request->Request.set_service_id("ydb");
            request->Request.set_microservice_id("data-plane");
            request->Request.mutable_resource()->set_id("cloud-1");
            request->Request.mutable_resource()->set_type("resource-manager.cloud");
            request->Request.set_target_service_account_id("sa-1");
            request->Request.mutable_referrer()->set_id("ref-1");
            request->Request.mutable_referrer()->set_type("ydb.secret");
            request->Request.set_on_behalf_of_subject_id("user-1");
            f.Runtime->Send(new IEventHandle(serviceControlId, f.Sender, request.Release()));
            auto result = f.Runtime->GrabEdgeEvent<NCloud::TEvServiceControlService::TEvSetupDelegationResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_C(result->Status.Ok(), result->Status.Msg);
            UNIT_ASSERT(result->Response.done());
            UNIT_ASSERT_VALUES_EQUAL(result->Response.id(), "op-1");

            const auto call = mock.LastCall();
            UNIT_ASSERT_VALUES_EQUAL(call.Method, "SetupDelegation");
            UNIT_ASSERT_VALUES_EQUAL(call.Setup.on_behalf_of_subject_id(), "user-1");
            UNIT_ASSERT_VALUES_EQUAL(call.Setup.target_service_account_id(), "sa-1");
            UNIT_ASSERT_VALUES_EQUAL(call.Setup.referrer().id(), "ref-1");
        }

        {
            // authorization header is checked by the mock
            auto request = MakeHolder<NCloud::TEvServiceControlService::TEvRevokeDelegationRequest>();
            request->Token = "wrong-token";
            request->Request.set_service_id("ydb");
            f.Runtime->Send(new IEventHandle(serviceControlId, f.Sender, request.Release()));
            auto result = f.Runtime->GrabEdgeEvent<NCloud::TEvServiceControlService::TEvRevokeDelegationResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT(!result->Status.Ok());
            UNIT_ASSERT_VALUES_EQUAL(result->Status.GRpcStatusCode, grpc::StatusCode::UNAUTHENTICATED);
        }

        {
            mock.NotDoneCount = 1;
            auto request = MakeHolder<NCloud::TEvServiceControlService::TEvRevokeDelegationRequest>();
            request->Token = "ssa-token";
            request->Request.set_service_id("ydb");
            request->Request.set_target_service_account_id("sa-1");
            request->Request.mutable_referrer()->set_id("ref-1");
            f.Runtime->Send(new IEventHandle(serviceControlId, f.Sender, request.Release()));
            auto result = f.Runtime->GrabEdgeEvent<NCloud::TEvServiceControlService::TEvRevokeDelegationResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_C(result->Status.Ok(), result->Status.Msg);
            UNIT_ASSERT(!result->Response.done());
            UNIT_ASSERT_VALUES_EQUAL(mock.RevokeCalls(), 2u);
        }
    }
}
