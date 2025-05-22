#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/public/api/client/yc_private/resourcemanager/transitional/folder_service.grpc.pb.h>
#include "folder_service_transitional.h"
#include <ydb/library/grpc/actor_client/grpc_service_client.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>

namespace NCloud {

using namespace NKikimr;

class TFolderServiceTransitional : public NActors::TActor<TFolderServiceTransitional>, NGrpcActorClient::TGrpcServiceClient<yandex::cloud::priv::resourcemanager::v1::transitional::FolderService> {
    using TThis = TFolderServiceTransitional;
    using TBase = NActors::TActor<TFolderServiceTransitional>;

    struct TListFolderRequest : TGrpcRequest {
        static constexpr auto Request = &yandex::cloud::priv::resourcemanager::v1::transitional::FolderService::Stub::AsyncList;
        using TRequestEventType = TEvFolderServiceTransitional::TEvListFolderRequest;
        using TResponseEventType = TEvFolderServiceTransitional::TEvListFolderResponse;
    };

    void Handle(TEvFolderServiceTransitional::TEvListFolderRequest::TPtr& ev) {
        MakeCall<TListFolderRequest>(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FOLDER_SERVICE_ACTOR; }

    TFolderServiceTransitional(const TFolderServiceTransitionalSettings& settings)
        : TBase(&TThis::StateWork)
        , TGrpcServiceClient(settings)
    {}

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvFolderServiceTransitional::TEvListFolderRequest, Handle);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }
};


IActor* CreateFolderServiceTransitional(const TFolderServiceTransitionalSettings& settings) {
    return new TFolderServiceTransitional(settings);
}

IActor* CreateFolderServiceTransitionalWithCache(const TFolderServiceTransitionalSettings& settings) {
    IActor* folderServiceTransitional = CreateFolderServiceTransitional(settings);
    folderServiceTransitional = NGrpcActorClient::CreateGrpcServiceCache<TEvFolderServiceTransitional::TEvListFolderRequest, TEvFolderServiceTransitional::TEvListFolderResponse>(folderServiceTransitional);
    return folderServiceTransitional;
}

} // namespace NCloud
