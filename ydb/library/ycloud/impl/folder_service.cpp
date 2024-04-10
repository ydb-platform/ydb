#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/resourcemanager/folder_service.grpc.pb.h>
#include "folder_service.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>

namespace NCloud {

using namespace NKikimr;

class TFolderService : public NActors::TActor<TFolderService>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::resourcemanager::v1::FolderService> {
    using TThis = TFolderService;
    using TBase = NActors::TActor<TFolderService>;

    struct TResolveFoldersGrpcRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::resourcemanager::v1::FolderService::Stub::AsyncResolve;
        using TRequestEventType = TEvFolderService::TEvResolveFoldersRequest;
        using TResponseEventType = TEvFolderService::TEvResolveFoldersResponse;
    };

    void Handle(TEvFolderService::TEvResolveFoldersRequest::TPtr& ev) {
        MakeCall<TResolveFoldersGrpcRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FOLDER_SERVICE_ACTOR; }

    TFolderService(const TFolderServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvFolderService::TEvResolveFoldersRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateFolderService(const TFolderServiceSettings& settings) {
    return new TFolderService(settings);
}

IActor* CreateFolderServiceWithCache(const TFolderServiceSettings& settings) {
    IActor* folderService = CreateFolderService(settings);
    folderService = NGrpcActorClient::CreateGrpcServiceCache<TEvFolderService::TEvResolveFoldersRequest, TEvFolderService::TEvResolveFoldersResponse>(folderService);
    return folderService;
}

} // namespace NCloud
