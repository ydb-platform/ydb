#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/operation_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include "operation_service.h"

Y_UNIT_TEST_SUITE(TOperationServiceTest) {
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

    Y_UNIT_TEST(GetOperation) {
        TFixture f;
        TAutoPtr<IEventHandle> handle;

        ui16 servicePort = f.PortManager.GetPort(8444);
        IActor* operationService = NCloud::CreateOperationService(NCloud::TOperationServiceSettings("localhost:" + ToString(servicePort), "ydb-test"));
        const TActorId operationServiceId = f.Runtime->Register(operationService);

        TOperationServiceMock mock;
        mock.GetsUntilDone = 2;
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:" + ToString(servicePort), grpc::InsecureServerCredentials()).RegisterService(&mock);
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

        for (ui32 i = 1; i <= 2; ++i) {
            auto request = MakeHolder<NCloud::TEvOperationService::TEvGetOperationRequest>();
            request->Token = "ssa-token";
            request->Request.set_operation_id("op-1");
            f.Runtime->Send(new IEventHandle(operationServiceId, f.Sender, request.Release()));
            auto result = f.Runtime->GrabEdgeEvent<NCloud::TEvOperationService::TEvGetOperationResponse>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_C(result->Status.Ok(), result->Status.Msg);
            UNIT_ASSERT_VALUES_EQUAL(result->Response.id(), "op-1");
            UNIT_ASSERT_VALUES_EQUAL(result->Response.done(), i == 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(mock.GetCalls.load(), 2u);
    }
}
