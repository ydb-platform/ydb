#include "node_warden.h"
#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blob_depot/s3_router.h>
#include <ydb/core/blob_depot/s3_router_events.h>

namespace NKikimr::NStorage {

    void TNodeWarden::Handle(TAutoPtr<TEventHandle<TEvNodeWardenAcquireBlobDepotS3Router>> ev) {
        auto& msg = *ev->Get();
        const ui64 tabletId = msg.TabletId;
        YDB_LOG_DEBUG_COMP(BS_NODE, "TEvNodeWardenAcquireBlobDepotS3Router",
            {"marker", "NW70"},
            {"tabletId", tabletId},
            {"sender", ev->Sender});

        TActorSystem* const as = TActivationContext::ActorSystem();
        auto& rec = BlobDepotS3Routers[tabletId];

        if (!rec.Router) {
            rec.Settings = msg.Settings;
            IActor* routerActor = NBlobDepot::CreateBlobDepotS3Router(msg.Settings);
            rec.Router = Register(routerActor, TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
            as->RegisterLocalService(MakeBlobDepotS3RouterID(tabletId), rec.Router);
            YDB_LOG_INFO_COMP(BS_NODE, "BlobDepotS3Router created",
                {"marker", "NW71"},
                {"tabletId", tabletId},
                {"router", rec.Router});
        } else {
            YDB_LOG_DEBUG_COMP(BS_NODE, "BlobDepotS3Router reused",
                {"marker", "NW72"},
                {"tabletId", tabletId},
                {"router", rec.Router});
        }

        rec.Consumers.insert(ev->Sender);
    }

    void TNodeWarden::Handle(TAutoPtr<TEventHandle<TEvNodeWardenReleaseBlobDepotS3Router>> ev) {
        auto& msg = *ev->Get();
        const ui64 tabletId = msg.TabletId;
        YDB_LOG_DEBUG_COMP(BS_NODE, "TEvNodeWardenReleaseBlobDepotS3Router",
            {"marker", "NW73"},
            {"tabletId", tabletId},
            {"sender", ev->Sender});

        auto it = BlobDepotS3Routers.find(tabletId);
        if (it == BlobDepotS3Routers.end()) {
            return;
        }

        auto& rec = it->second;
        rec.Consumers.erase(ev->Sender);
        if (rec.Consumers.empty()) {
            YDB_LOG_INFO_COMP(BS_NODE, "BlobDepotS3Router terminating",
                {"marker", "NW74"},
                {"tabletId", tabletId},
                {"router", rec.Router});
            TActorSystem* const as = TActivationContext::ActorSystem();
            as->RegisterLocalService(MakeBlobDepotS3RouterID(tabletId), TActorId());
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, rec.Router,
                SelfId(), nullptr, 0));
            BlobDepotS3Routers.erase(it);
        }
    }

    void TNodeWarden::TerminateBlobDepotS3Routers() {
        TActorSystem* const as = TActivationContext::ActorSystem();
        for (auto& [tabletId, rec] : BlobDepotS3Routers) {
            as->RegisterLocalService(MakeBlobDepotS3RouterID(tabletId), TActorId());
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, rec.Router,
                SelfId(), nullptr, 0));
        }
        BlobDepotS3Routers.clear();
    }

} // NKikimr::NStorage
