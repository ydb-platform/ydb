#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>
#include <util/generic/strbuf.h>
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

    TTestSetup(TStringBuf userAgent)
        : KikimrPort(PortManager.GetPort(2134))
        , ServicePort(PortManager.GetPort(4286))
    {
        StartKikimr(userAgent);
        StartAccessService();
    }

    TTestActorRuntime* GetRuntime() {
        return Server->GetRuntime();
    }

    void StartKikimr(TStringBuf userAgent) {
        NKikimrProto::TAuthConfig authConfig;
        auto settings = TServerSettings(KikimrPort, authConfig);
        settings.SetDomainName("Root");
        Server = MakeHolder<TServer>(settings);
        Server->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_DEBUG);
        Client = MakeHolder<TClient>(settings);
        Kikimr = MakeHolder<NClient::TKikimr>(Client->GetClientConfig());
        Client->InitRootScheme();
        EdgeActor = GetRuntime()->AllocateEdgeActor();

        NCloud::TAccessServiceSettings sets("localhost:" + ToString(ServicePort), userAgent);
        AccessServiceActor = NCloud::CreateAccessServiceWithCache(sets);
        GetRuntime()->Register(AccessServiceActor);
    }

    void StartAccessService() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:" + ToString(ServicePort), grpc::InsecureServerCredentials());
        builder.RegisterService(&AccessServiceMock);
        AccessServer = builder.BuildAndStart();
    }
};

Y_UNIT_TEST_SUITE(TAccessServiceTest) {
    Y_UNIT_TEST(Authenticate) {
        TTestSetup setup("ydb-as");

        TAutoPtr<IEventHandle> handle;
        setup.AccessServiceMock.AuthenticateData["good1"].Response.mutable_subject()->mutable_user_account()->set_id("1234");

        auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token("bad1");
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        auto result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthenticateResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(!result->Status.Ok());
        UNIT_ASSERT_VALUES_EQUAL(result->Status.Msg, "Permission Denied");

        request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token("good1");
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthenticateResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_VALUES_EQUAL(result->Response.subject().user_account().id(), "1234");
        with_lock (setup.AccessServiceMock.MetadataMutex) {
            UNIT_ASSERT_STRING_CONTAINS(setup.AccessServiceMock.CapturedUserAgent, "ydb-as/");
        }
    }

    Y_UNIT_TEST(Authorize) {
        TTestSetup setup("");

        TAutoPtr<IEventHandle> handle;
        setup.AccessServiceMock.AuthorizeData["user1-something.read-test_folder"].Response.mutable_subject()->mutable_user_account()->set_id("user1");

        auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthorizeRequest>();
        request->Request.set_iam_token("user1");
        request->Request.add_resource_path()->set_id("test_folder");
        request->Request.set_permission("something.read");
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        auto result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthorizeResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_VALUES_EQUAL(result->Response.subject().user_account().id(), "user1");
        with_lock (setup.AccessServiceMock.MetadataMutex) {
            UNIT_ASSERT_STRING_CONTAINS(setup.AccessServiceMock.CapturedUserAgent, "ydb/");
        }
    }

    Y_UNIT_TEST(BulkAuthorize) {
        TTestSetup setup("");

        TAutoPtr<IEventHandle> handle;
        setup.AccessServiceMock.AuthorizeData["user1-something.read-test_folder_1"].Response.mutable_subject()->mutable_user_account()->set_id("user1");

        auto request = MakeHolder<NCloud::TEvAccessService::TEvBulkAuthorizeRequest>();
        request->Request.set_iam_token("user1");
        auto* action1 = request->Request.mutable_actions()->add_items();
        action1->add_resource_path()->set_id("test_folder_1");
        action1->set_permission("something.read");
        auto* action2 = request->Request.mutable_actions()->add_items();
        action2->add_resource_path()->set_id("test_folder_2");
        action2->set_permission("something.write");
        request->Request.set_result_filter(yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest::ALL_FAILED);
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        auto result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvBulkAuthorizeResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_VALUES_EQUAL(result->Response.subject().user_account().id(), "user1");
        UNIT_ASSERT_VALUES_EQUAL(result->Response.results().items_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Response.results().items(0).permission(), "something.write");
        UNIT_ASSERT_VALUES_EQUAL(result->Response.results().items(0).resource_path_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Response.results().items(0).resource_path(0).id(), "test_folder_2");
        with_lock (setup.AccessServiceMock.MetadataMutex) {
            UNIT_ASSERT_STRING_CONTAINS(setup.AccessServiceMock.CapturedUserAgent, "ydb/");
        }
    }

    Y_UNIT_TEST(PassRequestId) {
        TTestSetup setup("");

        TAutoPtr<IEventHandle> handle;
        auto& req = setup.AccessServiceMock.AuthenticateData["token"];
        req.Response.mutable_subject()->mutable_user_account()->set_id("1234");
        req.RequireRequestId = true;

        auto request = MakeHolder<NCloud::TEvAccessService::TEvAuthenticateRequest>();
        request->Request.set_iam_token("token");
        request->RequestId = "trololo";
        setup.GetRuntime()->Send(new IEventHandle(setup.AccessServiceActor->SelfId(), setup.EdgeActor, request.Release()));
        auto result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvAccessService::TEvAuthenticateResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        with_lock (setup.AccessServiceMock.MetadataMutex) {
            UNIT_ASSERT_STRING_CONTAINS(setup.AccessServiceMock.CapturedUserAgent, "ydb/");
        }
    }
}
