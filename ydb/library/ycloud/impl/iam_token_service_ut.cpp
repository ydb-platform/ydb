#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include "iam_token_service.h"

Y_UNIT_TEST_SUITE(TIamTokenServiceTest) {
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

    Y_UNIT_TEST(CreateForService) {
        TFixture f;
        TAutoPtr<IEventHandle> handle;

        ui16 servicePort = f.PortManager.GetPort(8445);
        IActor* iamTokenService = NCloud::CreateIamTokenService("localhost:" + ToString(servicePort), "ydb-test");
        const TActorId iamTokenServiceId = f.Runtime->Register(iamTokenService);

        TIamTokenServiceMock mock;
        mock.ServiceTokens[TIamTokenServiceMock::ServiceTokenKey("cloud-1", "sa-1")] = "delegated-token";
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:" + ToString(servicePort), grpc::InsecureServerCredentials()).RegisterService(&mock);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

        {
            auto request = MakeHolder<NCloud::TEvIamTokenService::TEvCreateForServiceRequest>();
            request->Token = "ssa-token";
            request->Request.set_service_id("ydb");
            request->Request.set_microservice_id("data-plane");
            request->Request.set_resource_id("cloud-1");
            request->Request.set_resource_type("resource-manager.cloud");
            request->Request.set_target_service_account_id("sa-1");
            f.Runtime->Send(new IEventHandle(iamTokenServiceId, f.Sender, request.Release()));
            // the reply must arrive as TEvCreateForServiceResponse, not as the TEvCreateResponse of
            // CreateForServiceAccount: the two requests must not share a response event type
            auto result = f.Runtime->GrabEdgeEvent<NCloud::TEvIamTokenService::TEvCreateForServiceResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(handle->GetTypeRewrite(), (ui32)NCloud::TEvIamTokenService::EvCreateForServiceResponse);
            UNIT_ASSERT_C(result->Status.Ok(), result->Status.Msg);
            UNIT_ASSERT_VALUES_EQUAL(result->Response.iam_token(), "delegated-token");
        }

        {
            // no delegation set up for this pair
            auto request = MakeHolder<NCloud::TEvIamTokenService::TEvCreateForServiceRequest>();
            request->Token = "ssa-token";
            request->Request.set_resource_id("cloud-1");
            request->Request.set_target_service_account_id("sa-2");
            f.Runtime->Send(new IEventHandle(iamTokenServiceId, f.Sender, request.Release()));
            auto result = f.Runtime->GrabEdgeEvent<NCloud::TEvIamTokenService::TEvCreateForServiceResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT(!result->Status.Ok());
            UNIT_ASSERT_VALUES_EQUAL(result->Status.GRpcStatusCode, grpc::StatusCode::PERMISSION_DENIED);
        }
    }
}
