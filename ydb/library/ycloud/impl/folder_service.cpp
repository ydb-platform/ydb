#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/resourcemanager/folder_service.grpc.pb.h>
#include "folder_service.h"
#include "grpc_service_client.h"
#include "grpc_service_cache.h"

namespace NCloud {

using namespace NKikimr;

class TFolderService : public NActors::TActor<TFolderService>, TGrpcServiceClient<yandex::cloud::priv::resourcemanager::v1::transitional::FolderService> {
    using TThis = TFolderService;
    using TBase = NActors::TActor<TFolderService>;

    struct TListFolderRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::resourcemanager::v1::transitional::FolderService::Stub::AsyncList;
        using TRequestEventType = TEvFolderService::TEvListFolderRequest;
        using TResponseEventType = TEvFolderService::TEvListFolderResponse;
    };

    void Handle(TEvFolderService::TEvListFolderRequest::TPtr& ev) {
        MakeCall<TListFolderRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FOLDER_SERVICE_ACTOR; }

    TFolderService(const TFolderServiceSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvFolderService::TEvListFolderRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateFolderService(const TFolderServiceSettings& settings) {
    return new TFolderService(settings);
}

IActor* CreateFolderServiceWithCache(const TFolderServiceSettings& settings) {
    IActor* folderService = CreateFolderService(settings);
    folderService = CreateGrpcServiceCache<TEvFolderService::TEvListFolderRequest, TEvFolderService::TEvListFolderResponse>(folderService);
    return folderService;
}

}
