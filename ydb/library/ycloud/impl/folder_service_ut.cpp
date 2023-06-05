#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/folder_service_mock.h>
#include <library/cpp/grpc/server/grpc_server.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/retry/retry.h>
#include <util/string/builder.h>
#include "folder_service.h"

Y_UNIT_TEST_SUITE(TFolderServiceTest) {
    Y_UNIT_TEST(ListFolder) {
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
        TString badFolderId = "yrm4564";
        TString goodFolderId = "yrm4566";
        TString cloudId = "cloud9999";

        // Folder Service
        ui16 servicePort = tp.GetPort(4284);
        IActor* folderService = NCloud::CreateFolderService("localhost:" + ToString(servicePort));
        runtime->Register(folderService);
        auto request = MakeHolder<NCloud::TEvFolderService::TEvListFolderRequest>();
        request->Request.set_id("xxx");
        runtime->Send(new IEventHandle(folderService->SelfId(), sender, request.Release()));
        auto result = runtime->GrabEdgeEvent<NCloud::TEvFolderService::TEvListFolderResponse>(handle);
        UNIT_ASSERT(result); // error because no service is listening
        // grpc core code prints full information about error code,
        // but let's check only the fact that we received error.
        UNIT_ASSERT_STRING_CONTAINS(result->Status.Msg, "failed to connect to all addresses; last error:");

        // Folder Service Mock
        TFolderServiceMock folderServiceMock;
        grpc::ServerBuilder builder;
        folderServiceMock.Folders[goodFolderId].set_cloud_id(cloudId);
        builder.AddListeningPort("[::]:" + ToString(servicePort), grpc::InsecureServerCredentials()).RegisterService(&folderServiceMock);
        std::unique_ptr<grpc::Server> folderServer(builder.BuildAndStart());

        // check for not found
        // retry if there is TRANSIENT_FAILURE
        auto sendEvent = [&] {
            auto request = MakeHolder<NCloud::TEvFolderService::TEvListFolderRequest>();
            request->Request.set_id(badFolderId);
            runtime->Send(new IEventHandle(folderService->SelfId(), sender, request.Release()));
            result = runtime->GrabEdgeEvent<NCloud::TEvFolderService::TEvListFolderResponse>(handle);
            return result->Status.GRpcStatusCode != 14;
        };
        DoWithRetryOnRetCode(sendEvent, TRetryOptions{});
        Cerr << result->Status.Msg << " " << result->Status.GRpcStatusCode << "\n";

        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Msg == "Not Found");
        // || "channel is in state TRANSIENT_FAILURE"

        // check for found
        request = MakeHolder<NCloud::TEvFolderService::TEvListFolderRequest>();
        request->Request.set_id(goodFolderId);
        runtime->Send(new IEventHandle(folderService->SelfId(), sender, request.Release()));
        result = runtime->GrabEdgeEvent<NCloud::TEvFolderService::TEvListFolderResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_EQUAL(result->Response.result(0).cloud_id(), cloudId);
    }
}
