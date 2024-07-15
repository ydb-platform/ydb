#include "access_service.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/testlib/service_mocks/nebius_access_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NKikimr;

struct TTestSetup : public NUnitTest::TBaseFixture {
    TPortManager PortManager;
    ui16 ServicePort = 0;

    TTestActorRuntime Runtime;
    NActors::TActorId EdgeActor;
    NActors::IActor* AccessServiceActor = nullptr;
    NActors::TActorId AccessServiceActorId;
    TAutoPtr<IEventHandle> EventHandle; // handles last event

    TNebiusAccessServiceMock AccessServiceMock;
    std::unique_ptr<grpc::Server> AccessServer;

    TTestSetup()
        : ServicePort(PortManager.GetPort())
        , Runtime(1, true /*useRealThreads*/)
    {
        Init();
    }

    TTestActorRuntime::TEgg MakeEgg() {
        return
            { new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
    }

    void Init() {
        Runtime.SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_DEBUG);
        Runtime.Initialize(MakeEgg());
        EdgeActor = Runtime.AllocateEdgeActor();

        AccessServiceActor = NNebiusCloud::CreateAccessServiceV1(TStringBuilder() << "localhost:" << ServicePort);
        AccessServiceActorId = Runtime.Register(AccessServiceActor);

        StartAccessService();
    }

    void StartAccessService() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(TStringBuilder() << "[::]:" << ServicePort, grpc::InsecureServerCredentials()).RegisterService(&AccessServiceMock);
        AccessServer = builder.BuildAndStart();
    }

    NNebiusCloud::TEvAccessService::TEvAuthenticateResponse* Authenticate(const TString& iamToken, const TString& requestId = {}, TAutoPtr<IEventHandle>* eventHandle = nullptr) {
        auto request = MakeHolder<NNebiusCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token(iamToken);
        request->RequestId = requestId;
        Runtime.Send(new IEventHandle(AccessServiceActorId, EdgeActor, request.Release()));
        NNebiusCloud::TEvAccessService::TEvAuthenticateResponse* result = Runtime.GrabEdgeEvent<NNebiusCloud::TEvAccessService::TEvAuthenticateResponse>(eventHandle ? *eventHandle : EventHandle);
        UNIT_ASSERT_C(result, iamToken << '-' << requestId);
        return result;
    }

    NNebiusCloud::TEvAccessService::TEvAuthorizeResponse* Authorize(const TString& iamToken, const TString& permission, const TString& pathId, const TString& requestId = {}, TAutoPtr<IEventHandle>* eventHandle = nullptr) {
        auto request = MakeHolder<NNebiusCloud::TEvAccessService::TEvAuthorizeRequest>();
        auto& check = (*request->Request.mutable_checks())[0];
        check.set_iam_token(iamToken);
        check.mutable_permission()->set_name(permission);
        check.mutable_resource_path()->add_path()->set_id(pathId);
        request->RequestId = requestId;
        Runtime.Send(new IEventHandle(AccessServiceActorId, EdgeActor, request.Release()));
        NNebiusCloud::TEvAccessService::TEvAuthorizeResponse* result = Runtime.GrabEdgeEvent<NNebiusCloud::TEvAccessService::TEvAuthorizeResponse>(eventHandle ? *eventHandle : EventHandle);
        UNIT_ASSERT_C(result, iamToken << '-' << permission << '-' << pathId << '-' << requestId);
        return result;
    }
};

Y_UNIT_TEST_SUITE_F(TNebiusAccessServiceTest, TTestSetup) {
    Y_UNIT_TEST(Authenticate) {
        AccessServiceMock.AuthenticateData["good"].Response.mutable_account()->mutable_user_account()->set_id("1234");

        {
            auto* result = Authenticate("bad");
            UNIT_ASSERT_VALUES_EQUAL(result->Status.Msg, "Permission Denied");
        }

        {
            auto* result = Authenticate("good");
            UNIT_ASSERT(result->Status.Ok());
            UNIT_ASSERT_VALUES_EQUAL(result->Response.account().user_account().id(), "1234");
        }
    }

    Y_UNIT_TEST(PassRequestId) {
        auto& req = AccessServiceMock.AuthenticateData["token"];
        req.Response.mutable_account()->mutable_user_account()->set_id("1234");
        req.RequireRequestId = true;

        auto* result = Authenticate("token", "reqId");
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
    }

    Y_UNIT_TEST(Authorize) {
        auto& resp = AccessServiceMock.AuthorizeData["token-perm-path_id"];
        {
            auto& result = (*resp.Response.mutable_results())[0];
            result.set_resultcode(nebius::iam::v1::AuthorizeResult::OK);
            result.mutable_account()->mutable_user_account()->set_id("user_id");
        }


        {
            auto* result = Authorize("token", "perm", "path_id");
            UNIT_ASSERT(result->Status.Ok());
            const auto resultIt = result->Response.results().find(0);
            UNIT_ASSERT(resultIt != result->Response.results().end());
            const auto& resultRec = resultIt->second;
            UNIT_ASSERT_VALUES_EQUAL(resultRec.account().user_account().id(), "user_id");
        }

        {
            auto* result = Authorize("unknown_token", "perm", "path_id");
            UNIT_ASSERT_VALUES_EQUAL(result->Status.Msg, "Permission Denied");
        }

        {
            auto* result = Authorize("token", "denied", "path_id");
            UNIT_ASSERT_VALUES_EQUAL(result->Status.Msg, "Permission Denied");
        }

        {
            auto* result = Authorize("token", "perm", "p");
            UNIT_ASSERT_VALUES_EQUAL(result->Status.Msg, "Permission Denied");
        }
    }
}
