#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/retry/retry.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/testlib/service_mocks/folder_service_transitional_mock.h>
#include <ydb/library/testlib/service_mocks/folder_service_mock.h>
#include "ydb/library/folder_service/folder_service.h"
#include <ydb/library/folder_service/events.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <util/string/builder.h>
#include "folder_service_transitional.h"
#include "folder_service.h"


using namespace NKikimr;
using namespace Tests;

struct TFolderServiceTestSetup {
    TPortManager PortManager;
    ui16 KikimrPort;

    THolder<TServer> Server;
    THolder<TClient> Client;
    THolder<NClient::TKikimr> Kikimr;
    TActorId EdgeActor;
    IActor* AccessServiceActor = nullptr;

    TFolderServiceTestSetup()
        : KikimrPort(PortManager.GetPort(2134))
    {
        StartKikimr();
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
    }

    std::unique_ptr<grpc::Server> StartGrpcService(const ui16 port, grpc::Service* service) {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:" + ToString(port), grpc::InsecureServerCredentials()).RegisterService(service);
        std::unique_ptr<grpc::Server> grpcServer(builder.BuildAndStart());
        return grpcServer;
    }

    IActor* RegisterFolderServiceAdapter(NKikimrProto::NFolderService::TFolderServiceConfig& config) {
        IActor* folderServiceAdapter = NKikimr::NFolderService::CreateFolderServiceActor(config);
        GetRuntime()->Register(folderServiceAdapter);
        return folderServiceAdapter;
    }

    IActor* RegisterFolderTransitionalServiceActor(const ui16 port) {
        IActor* folderServiceTransitional = NCloud::CreateFolderServiceTransitional("localhost:" + ToString(port));
        GetRuntime()->Register(folderServiceTransitional);
        return folderServiceTransitional;
    }

    IActor* RegisterFolderServiceActor(const ui16 port) {
        IActor* folderService = NCloud::CreateFolderService("localhost:" + ToString(port));
        GetRuntime()->Register(folderService);
        return folderService;
    }

    void SendListFolderRequest(IActor* to, TString folderId) {
        auto request = MakeHolder<NCloud::TEvFolderServiceTransitional::TEvListFolderRequest>();
        request->Request.set_id(folderId);
        GetRuntime()->Send(new IEventHandle(to->SelfId(), EdgeActor, request.Release()));
    }

    void SendResolveFoldersRequest(IActor* to, TString folderId) {
        auto request = MakeHolder<NCloud::TEvFolderService::TEvResolveFoldersRequest>();
        request->Request.add_folder_ids(folderId);
        GetRuntime()->Send(new IEventHandle(to->SelfId(), EdgeActor, request.Release()));
    }

    void SendGetCloudByFolderRequest(IActor* to, TString folderId) {
        auto request = MakeHolder<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest>();
        request->FolderId = folderId;
        GetRuntime()->Send(new IEventHandle(to->SelfId(), EdgeActor, request.Release()));
    }
};

Y_UNIT_TEST_SUITE(FolderServiceTest) {
    Y_UNIT_TEST(TFolderServiceTransitional) {
        TFolderServiceTestSetup setup;
        TAutoPtr<IEventHandle> handle;

        TString existsFolderId = "i_am_exists";
        TString notExistsFolderId = "i_am_not_exists";
        TString cloudId = "response_cloud_id";

        ui16 servicePort = setup.PortManager.GetPort(4284);
        auto folderServiceTransitional = setup.RegisterFolderTransitionalServiceActor(servicePort);

        // check failure because there is no listening gRPC service
        setup.SendListFolderRequest(folderServiceTransitional, notExistsFolderId);
        auto result = setup.GetRuntime() -> GrabEdgeEvent<NCloud::TEvFolderServiceTransitional::TEvListFolderResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_STRING_CONTAINS(result->Status.Msg, "failed to connect to all addresses");

        TFolderServiceTransitionalMock folderServiceTransitionalMock;
        folderServiceTransitionalMock.Folders[existsFolderId].set_cloud_id(cloudId);
        auto grpcServer = setup.StartGrpcService(servicePort, &folderServiceTransitionalMock);

        // check not found
        auto sendEvent = [&] {
            setup.SendListFolderRequest(folderServiceTransitional, notExistsFolderId);
            result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvFolderServiceTransitional::TEvListFolderResponse>(handle);
            return result->Status.GRpcStatusCode != 14;
        };
        DoWithRetryOnRetCode(sendEvent, TRetryOptions{});
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Msg == "Not Found");

        // check found
        setup.SendListFolderRequest(folderServiceTransitional, existsFolderId);
        result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvFolderServiceTransitional::TEvListFolderResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_EQUAL(result->Response.result(0).cloud_id(), cloudId);
    }

    Y_UNIT_TEST(TFolderService) {
        TFolderServiceTestSetup setup;
        TAutoPtr<IEventHandle> handle;

        TString existsFolderId = "i_am_exists";
        TString notExistsFolderId = "i_am_not_exists";
        TString cloudId = "response_cloud_id";

        ui16 servicePort = setup.PortManager.GetPort(4284);
        auto folderService = setup.RegisterFolderServiceActor(servicePort);

        // check failure because there is no listening gRPC service
        setup.SendResolveFoldersRequest(folderService, notExistsFolderId);
        auto result = setup.GetRuntime() -> GrabEdgeEvent<NCloud::TEvFolderService::TEvResolveFoldersResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_STRING_CONTAINS(result->Status.Msg, "failed to connect to all addresses");

        TFolderServiceMock folderServiceMock;
        folderServiceMock.Folders[existsFolderId].set_cloud_id(cloudId);
        auto grpcServer = setup.StartGrpcService(servicePort, &folderServiceMock);

        // check not found
        auto sendEvent = [&] {
            setup.SendResolveFoldersRequest(folderService, notExistsFolderId);
            result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvFolderService::TEvResolveFoldersResponse>(handle);
            return result->Status.GRpcStatusCode != grpc::StatusCode::UNAVAILABLE;
        };
        DoWithRetryOnRetCode(sendEvent, TRetryOptions{});
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.GRpcStatusCode == grpc::StatusCode::NOT_FOUND);

        // check found
        setup.SendResolveFoldersRequest(folderService, existsFolderId);
        result = setup.GetRuntime()->GrabEdgeEvent<NCloud::TEvFolderService::TEvResolveFoldersResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.Ok());
        UNIT_ASSERT_EQUAL(result->Response.resolved_folders(0).cloud_id(), cloudId);
    }

    Y_UNIT_TEST(TFolderServiceAdapter) {
        TFolderServiceTestSetup setup;
        TAutoPtr<IEventHandle> handle;

        TString existsFolderId = "i_am_exists";
        TString notExistsFolderId = "i_am_not_exists";
        TString cloudFromTransitionalService = "cloud_from_old_service";
        TString cloudFromNewService = "cloud_from_new_service";

        ui16 transitionalServicePort = setup.PortManager.GetPort(4284);
        ui16 newServicePort = setup.PortManager.GetPort(4285);

        TFolderServiceTransitionalMock folderServiceTransitionalMock;
        folderServiceTransitionalMock.Folders[existsFolderId].set_cloud_id(cloudFromTransitionalService);
        auto grpcServerTransitional = setup.StartGrpcService(transitionalServicePort, &folderServiceTransitionalMock);

        TFolderServiceMock folderServiceMock;
        folderServiceMock.Folders[existsFolderId].set_cloud_id(cloudFromNewService);
        auto grpcServer = setup.StartGrpcService(newServicePort, &folderServiceMock);

        NKikimrProto::NFolderService::TFolderServiceConfig oldConfig;
        oldConfig.SetEnable(true);
        oldConfig.SetEndpoint("localhost:" + ToString(transitionalServicePort));

        NKikimrProto::NFolderService::TFolderServiceConfig newConfig;
        newConfig.SetEnable(true);
        newConfig.SetEndpoint("localhost:" + ToString(transitionalServicePort));
        newConfig.SetResourceManagerEndpoint("localhost:" + ToString(newServicePort));

        auto folderServiceAdapterWithOldConfig = setup.RegisterFolderServiceAdapter(oldConfig);
        auto folderServiceAdapterWithNewConfig = setup.RegisterFolderServiceAdapter(newConfig);

        NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse* result;
        // checking that with the old config, the request goes to the transitional service
        auto sendToOld = [&] {
            setup.SendGetCloudByFolderRequest(folderServiceAdapterWithOldConfig, existsFolderId);
            result = setup.GetRuntime()->GrabEdgeEvent<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>(handle);
            return result->Status.GRpcStatusCode != grpc::StatusCode::UNAVAILABLE;
        };
        DoWithRetryOnRetCode(sendToOld, TRetryOptions{});
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->CloudId, cloudFromTransitionalService);

        // checking that with the new config, the request goes to the new service
        auto sendToNew = [&] {
            setup.SendGetCloudByFolderRequest(folderServiceAdapterWithNewConfig, existsFolderId);
            result = setup.GetRuntime()->GrabEdgeEvent<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>(handle);
            return result->Status.GRpcStatusCode != grpc::StatusCode::UNAVAILABLE;
        };
        DoWithRetryOnRetCode(sendToNew, TRetryOptions{});
        UNIT_ASSERT(result);
        UNIT_ASSERT_EQUAL(result->CloudId, cloudFromNewService);

        // check not found from the new service 
        setup.SendGetCloudByFolderRequest(folderServiceAdapterWithNewConfig, notExistsFolderId);
        result = setup.GetRuntime()->GrabEdgeEvent<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.GRpcStatusCode == grpc::StatusCode::NOT_FOUND);

        // check not found from the transitional service       
        setup.SendGetCloudByFolderRequest(folderServiceAdapterWithOldConfig, notExistsFolderId);
        result = setup.GetRuntime()->GrabEdgeEvent<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT(result->Status.GRpcStatusCode == grpc::StatusCode::NOT_FOUND);
    }

}
