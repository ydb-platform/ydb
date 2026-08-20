#include "node_warden.h"
#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blob_depot/s3_router.h>
#include <ydb/core/blob_depot/s3_router_events.h>

namespace NKikimr::NStorage {

    void TNodeWarden::Handle(TAutoPtr<TEventHandle<TEvNodeWardenAcquireBlobDepotS3Router>> ev) {
        auto& msg = *ev->Get();
        const ui64 tabletId = msg.TabletId;
        STLOG(PRI_DEBUG, BS_NODE, NW70, "TEvNodeWardenAcquireBlobDepotS3Router",
            (TabletId, tabletId), (Sender, ev->Sender));

        TActorSystem* const as = TActivationContext::ActorSystem();
        auto& rec = BlobDepotS3Routers[tabletId];

        if (!rec.Router) {
            rec.Settings = msg.Settings;
            IActor* routerActor = NBlobDepot::CreateBlobDepotS3Router(msg.Settings);
            rec.Router = Register(routerActor, TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
            as->RegisterLocalService(MakeBlobDepotS3RouterID(tabletId), rec.Router);
            STLOG(PRI_INFO, BS_NODE, NW71, "BlobDepotS3Router created",
                (TabletId, tabletId), (Router, rec.Router));
        } else {
            STLOG(PRI_DEBUG, BS_NODE, NW72, "BlobDepotS3Router reused",
                (TabletId, tabletId), (Router, rec.Router));
        }

        rec.Consumers.insert(ev->Sender);
    }

    void TNodeWarden::Handle(TAutoPtr<TEventHandle<TEvNodeWardenReleaseBlobDepotS3Router>> ev) {
        auto& msg = *ev->Get();
        const ui64 tabletId = msg.TabletId;
        STLOG(PRI_DEBUG, BS_NODE, NW73, "TEvNodeWardenReleaseBlobDepotS3Router",
            (TabletId, tabletId), (Sender, ev->Sender));

        auto it = BlobDepotS3Routers.find(tabletId);
        if (it == BlobDepotS3Routers.end()) {
            return;
        }

        auto& rec = it->second;
        rec.Consumers.erase(ev->Sender);
        if (rec.Consumers.empty()) {
            STLOG(PRI_INFO, BS_NODE, NW74, "BlobDepotS3Router terminating",
                (TabletId, tabletId), (Router, rec.Router));
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
