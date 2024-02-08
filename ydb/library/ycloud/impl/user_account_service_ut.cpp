#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/user_account_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include "user_account_service.h"

Y_UNIT_TEST_SUITE(TUserAccountServiceTest) {
    Y_UNIT_TEST(Get) {
        using namespace NKikimr;
        using namespace Tests;

        TPortManager tp;
        // Kikimr
        ui16 kikimrPort = tp.GetPort(2134);
        NKikimrProto::TAuthConfig authConfig;
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        TServer server(settings);
        TClient client(settings);
        NClient::TKikimr kikimr(client.GetClientConfig());
        client.InitRootScheme();

        TTestActorRuntime* runtime = server.GetRuntime();
        TActorId sender = runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        // User Account Service
        ui16 servicePort = tp.GetPort(8443);
        IActor* userAccountService = NCloud::CreateUserAccountService("localhost:" + ToString(servicePort));
        runtime->Register(userAccountService);

        // User Account Service Mock
        TUserAccountServiceMock userAccountServiceMock;
        grpc::ServerBuilder builder;
        userAccountServiceMock.UserAccountData["user1"].set_id("user1");
        builder.AddListeningPort("[::]:" + ToString(servicePort), grpc::InsecureServerCredentials()).RegisterService(&userAccountServiceMock);
        std::unique_ptr<grpc::Server> userAccountServer(builder.BuildAndStart());

        // check for not found
        auto request = MakeHolder<NCloud::TEvUserAccountService::TEvGetUserAccountRequest>();
        request->Request.set_user_account_id("bad1");
        runtime->Send(new IEventHandle(userAccountService->SelfId(), sender, request.Release()));
        auto result = runtime->GrabEdgeEvent<NCloud::TEvUserAccountService::TEvGetUserAccountResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Status.Msg, "Not Found");

        // check for found
        request = MakeHolder<NCloud::TEvUserAccountService::TEvGetUserAccountRequest>();
        request->Request.set_user_account_id("user1");
        runtime->Send(new IEventHandle(userAccountService->SelfId(), sender, request.Release()));
        result = runtime->GrabEdgeEvent<NCloud::TEvUserAccountService::TEvGetUserAccountResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_EQUAL(result->Response.id(), "user1");
    }
}
