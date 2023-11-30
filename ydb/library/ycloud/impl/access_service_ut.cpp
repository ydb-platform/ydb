#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include "access_service.h"

using namespace NKikimr;
using namespace Tests;

struct TTestSetup {
    TPortManager PortManager;
    ui16 KikimrPort;
    ui16 ServicePort;

    // Kikimr
    THolder<TServer> Server;
    THolder<TClient> Client;
    THolder<NClient::TKikimr> Kikimr;
    TActorId EdgeActor;
    IActor* AccessServiceActor = nullptr;

    // Access service
    TAccessServiceMock AccessServiceMock;
    std::unique_ptr<grpc::Server> AccessServer;

    TTestSetup()
        : KikimrPort(PortManager.GetPort(2134))
        , ServicePort(PortManager.GetPort(4286))
    {
        StartKikimr();
        StartAccessService();
    }

    TTestActorRuntime* GetRuntime() {
        return Server->GetRuntime();
    }

    void StartKikimr() {
        NKikimrProto::TAuthConfig authConfig;
        auto settings = TServerSettings(KikimrPort, authConfig);
        settings.SetDomainName("Root");
        Server = MakeHolder<TServer>(settings);
        Server->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_DEBUG);
        Client = MakeHolder<TClient>(settings);
        Kikimr = MakeHolder<NClient::TKikimr>(Client->GetClientConfig());
        Client->InitRootScheme();
        EdgeActor = GetRuntime()->AllocateEdgeActor();

        //AccessServiceActor = NCloud::CreateAccessService("localhost:" + ToString(ServicePort));
        NCloud::TAccessServiceSettings sets;
        sets.Endpoint = "localhost:" + ToString(ServicePort);
        AccessServiceActor = NCloud::CreateAccessServiceWithCache(sets);
        GetRuntime()->Register(AccessServiceActor);
    }

    void StartAccessService() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:" + ToString(ServicePort), grpc::InsecureServerCredentials()).RegisterService(&AccessServiceMock);
        AccessServer = builder.BuildAndStart();
    }
};

Y_UNIT_TEST_SUITE(TAccessServiceTest) {
    Y_UNIT_TEST(Authenticate) {
        TTestSetup setup;

        TAutoPtr<IEventHandle> handle;
        setup.AccessServiceMock.AuthenticateData["good1"].Response.mutable_subject()->mutable_user_account()->set_id("1234");

        // check for not found
        auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token("bad1");
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        auto result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthenticateResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Status.Msg, "Permission Denied");

        // check for found
        request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token("good1");
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthenticateResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_VALUES_EQUAL(result->Response.subject().user_account().id(), "1234");
    }

    Y_UNIT_TEST(PassRequestId) {
        TTestSetup setup;

        TAutoPtr<IEventHandle> handle;
        auto& req = setup.AccessServiceMock.AuthenticateData["token"];
        req.Response.mutable_subject()->mutable_user_account()->set_id("1234");
        req.RequireRequestId = true;

        // check for not found
        auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token("token");
        request->RequestId = "trololo";
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        auto result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthenticateResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
    }
}
