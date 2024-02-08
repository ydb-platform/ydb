#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/service_account_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include "service_account_service.h"

using namespace NKikimr;
using namespace Tests;

namespace {
    static const TString IAM_TOKEN = "a-b-c";
}

class TServiceAccountServiceFixture : public NUnitTest::TBaseFixture {
public:
    TServiceAccountServiceFixture() {
        GrpcPort = PortManager.GetPort(2134);

        NKikimrProto::TAuthConfig authConfig;
        auto settings = TServerSettings(GrpcPort, authConfig);
        settings.SetDomainName("Root");
        Server = MakeHolder<TServer>(settings);
        Client = MakeHolder<TClient>(settings);
        Client->InitRootScheme();

        Runtime = Server->GetRuntime();
        SenderActorId = Runtime->AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        // Service Account Service
        ui16 servicePort = PortManager.GetPort(8443);
        ServiceAccountService = NCloud::CreateServiceAccountService("localhost:" + ToString(servicePort));
        Runtime->Register(ServiceAccountService);

        // Service Account Service Mock
        grpc::ServerBuilder builder;
        ServiceAccountServiceMock.ServiceAccountData["Service1"].set_id("Service1");
        ServiceAccountServiceMock.IamTokens["Service1"].set_iam_token(IAM_TOKEN);
        builder.AddListeningPort("[::]:" + ToString(servicePort), grpc::InsecureServerCredentials()).RegisterService(&ServiceAccountServiceMock);
        ServiceAccountServer = builder.BuildAndStart();
    }

public:
    ui16 GrpcPort;
    TPortManager PortManager;
    THolder<TServer> Server;
    THolder<TClient> Client;
    TTestActorRuntime* Runtime;
    TServiceAccountServiceMock ServiceAccountServiceMock;
    IActor* ServiceAccountService;
    std::unique_ptr<grpc::Server> ServiceAccountServer;
    TActorId SenderActorId;
};

namespace {
    template<class TEvRequest>
    TEvRequest* CreateRequest(const TString& accountId) {
        auto* result = new TEvRequest();
        result->Request.set_service_account_id(accountId);
        return result;
    }
}

Y_UNIT_TEST_SUITE_F(TServiceAccountServiceTest, TServiceAccountServiceFixture) {
    Y_UNIT_TEST(Get) {
        TAutoPtr<IEventHandle> handle;
        // check for not found
        Runtime->Send(new IEventHandle(ServiceAccountService->SelfId(), SenderActorId, CreateRequest<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest>("bad1")));
        auto result = Runtime->GrabEdgeEvent<NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Status.Msg, "Not Found");

        // check for found
        Runtime->Send(new IEventHandle(ServiceAccountService->SelfId(), SenderActorId, CreateRequest<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest>("Service1")));
        result = Runtime->GrabEdgeEvent<NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse>(handle);

        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_EQUAL(result->Response.id(), "Service1");
    }

    Y_UNIT_TEST(IssueToken) {
        TAutoPtr<IEventHandle> handle;
        // check for not found
        Runtime->Send(new IEventHandle(ServiceAccountService->SelfId(), SenderActorId, CreateRequest<NCloud::TEvServiceAccountService::TEvIssueTokenRequest>("bad1")));
        auto result = Runtime->GrabEdgeEvent<NCloud::TEvServiceAccountService::TEvIssueTokenResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->Status.GRpcStatusCode, grpc::StatusCode::UNAUTHENTICATED);

        // check for found
        Runtime->Send(new IEventHandle(ServiceAccountService->SelfId(), SenderActorId, CreateRequest<NCloud::TEvServiceAccountService::TEvIssueTokenRequest>("Service1")));
        result = Runtime->GrabEdgeEvent<NCloud::TEvServiceAccountService::TEvIssueTokenResponse>(handle);

        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_EQUAL(result->Response.iam_token(), IAM_TOKEN);
    }
}
