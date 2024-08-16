#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/events.h>
#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>

#include <ydb/library/ycloud/impl/folder_service.h>
#include <ydb/library/ycloud/impl/folder_service_transitional.h>

#include <ydb/library/ycloud/api/folder_service.h>
#include <ydb/library/ycloud/api/folder_service_transitional.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>

namespace {

enum EFolderServiceEndpointType {
    AccessService,
    ResourceManager
};

class TFolderServiceRequestHandler: public NActors::TActor<TFolderServiceRequestHandler> {
    using TThis = TFolderServiceRequestHandler;
    using TBase = NActors::TActor<TFolderServiceRequestHandler>;

    EFolderServiceEndpointType FolderServiceEndpointType = EFolderServiceEndpointType::AccessService;

    NActors::TActorId Sender;
    NActors::TActorId Delegatee;

    void Handle(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest::TPtr& ev) {
        std::unique_ptr<NActors::IEventBase> request;
        switch (FolderServiceEndpointType) {
            case EFolderServiceEndpointType::ResourceManager:
                request = CreateResolveFolderRequest(ev);
                break;
            case EFolderServiceEndpointType::AccessService:
                request = CreateListFolderRequest(ev);
                break;
        }
        Send(Delegatee, request.release());
    }

    void Handle(NCloud::TEvFolderServiceTransitional::TEvListFolderResponse::TPtr& ev) {
        auto responseEvent = std::make_unique<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>();
        const auto& status = ev->Get()->Status;

        responseEvent->Status = status;
        if (status.Ok() && ev->Get()->Response.result_size() > 0) {
            responseEvent->CloudId = ev->Get()->Response.result(0).cloud_id();
            responseEvent->FolderId = ev->Get()->Response.result(0).id();
        }

        Send(Sender, responseEvent.release());
        PassAway();
    }

    void Handle(NCloud::TEvFolderService::TEvResolveFoldersResponse::TPtr& ev) {
        auto responseEvent = std::make_unique<NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>();
        const auto& status = ev->Get()->Status;

        responseEvent->Status = status;
        if (status.Ok()) {
            responseEvent->CloudId = ev->Get()->Response.resolved_folders(0).cloud_id();
            responseEvent->FolderId = ev->Get()->Response.resolved_folders(0).id();
        }

        Send(Sender, responseEvent.release());
        PassAway();
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        Y_ABORT("Can't deliver local message");
    }

    std::unique_ptr<NActors::IEventBase> CreateListFolderRequest(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest::TPtr& ev) {
        auto request = std::make_unique<NCloud::TEvFolderServiceTransitional::TEvListFolderRequest>();
        request->Request.set_id(ev->Get()->FolderId);
        request->Token = ev->Get()->Token;
        request->RequestId = ev->Get()->RequestId;
        return request;
    }

    std::unique_ptr<NActors::IEventBase> CreateResolveFolderRequest(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest::TPtr& ev) {
        auto request = std::make_unique<NCloud::TEvFolderService::TEvResolveFoldersRequest>();
        request->Request.add_folder_ids(ev->Get()->FolderId);
        request->Token = ev->Get()->Token;
        request->RequestId = ev->Get()->RequestId;
        return request;
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest, Handle);
            hFunc(NCloud::TEvFolderService::TEvResolveFoldersResponse, Handle);
            hFunc(NCloud::TEvFolderServiceTransitional::TEvListFolderResponse, Handle);
            hFunc(NActors::TEvents::TEvUndelivered, Handle);
            cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
        }
    }

public:
    TFolderServiceRequestHandler(EFolderServiceEndpointType folderServiceEndpoint, NActors::TActorId sender, NActors::TActorId delegatee)
        : TBase(&TThis::StateWork)
        , FolderServiceEndpointType(folderServiceEndpoint)
        , Sender(sender)
        , Delegatee(delegatee)
    {
    }
};
};

namespace NKikimr::NFolderService {

class TFolderServiceAdapter: public NActors::TActorBootstrapped<TFolderServiceAdapter> {
    using TThis = TFolderServiceAdapter;
    using TBase = NActors::TActor<TFolderServiceAdapter>;

    NKikimrProto::NFolderService::TFolderServiceConfig Config;
    NActors::TActorId Delegatee;
    EFolderServiceEndpointType FolderServiceEndpointType = EFolderServiceEndpointType::AccessService;

    void Handle(NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest::TPtr& ev) {
        auto actor = Register(new TFolderServiceRequestHandler(FolderServiceEndpointType, ev->Sender, Delegatee));
        Send(actor, ev->Release().Release());
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(::NKikimr::NFolderService::TEvFolderService::TEvGetCloudByFolderRequest, Handle);
            cFunc(NActors::TEvents::TSystem::PoisonPill, PassAway);
        }
    }

    void RegisterFolderService() {
        NCloud::TFolderServiceSettings settings;
        if (!Config.GetPathToRootCA().empty())
            settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
        settings.Endpoint = Config.GetResourceManagerEndpoint();
        Delegatee = Register(NCloud::CreateFolderServiceWithCache(settings));
        FolderServiceEndpointType = EFolderServiceEndpointType::ResourceManager;
    }

    void RegisterTransitionalFolderService() {
        NCloud::TFolderServiceTransitionalSettings settings;
        if (!Config.GetPathToRootCA().empty())
            settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
        settings.Endpoint = Config.GetEndpoint();
        Delegatee = Register(NCloud::CreateFolderServiceTransitionalWithCache(settings));
        FolderServiceEndpointType = EFolderServiceEndpointType::AccessService;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FOLDER_SERVICE_ACTOR;
    }

    void Bootstrap() {
        if (Config.HasResourceManagerEndpoint() && !Config.GetResourceManagerEndpoint().empty()) {
            RegisterFolderService();
        }   
        else {
            RegisterTransitionalFolderService();
        }
        Become(&TThis::StateWork);
    }

    explicit TFolderServiceAdapter(const NKikimrProto::NFolderService::TFolderServiceConfig& config)
        : Config(config)
    {
    }
};

NActors::IActor* CreateFolderServiceActor(
        const NKikimrProto::NFolderService::TFolderServiceConfig& config) {
    if (config.GetEnable()) {
        return new NKikimr::NFolderService::TFolderServiceAdapter(config);
    } else {
        return NKikimr::NFolderService::CreateMockFolderServiceAdapterActor(config, Nothing());
    }
}

NActors::IActor* CreateFolderServiceActor(
        const NKikimrProto::NFolderService::TFolderServiceConfig& config,
        TString mockedCloudId) {
    if (config.GetEnable()) {
        return new NKikimr::NFolderService::TFolderServiceAdapter(config);
    } else {
        return NKikimr::NFolderService::CreateMockFolderServiceAdapterActor(config, mockedCloudId);
    }
}
} // namespace NKikimr::NFolderService
