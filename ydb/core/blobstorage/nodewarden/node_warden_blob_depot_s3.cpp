#include "node_warden.h"
#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blob_depot/s3_router.h>
#include <ydb/core/blob_depot/s3_router_events.h>

#include <google/protobuf/util/message_differencer.h>

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

        if (!rec.Router || (msg.Generation > rec.Generation &&
                !google::protobuf::util::MessageDifferencer::Equals(rec.Settings, msg.Settings))) {
            const TActorId previous = rec.Router;
            rec.Settings = std::move(msg.Settings);
            IActor* routerActor = NBlobDepot::CreateBlobDepotS3Router(rec.Settings, tabletId);
            rec.Router = Register(routerActor, TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
            as->RegisterLocalService(MakeBlobDepotS3RouterID(tabletId), rec.Router);
            if (previous) {
                Send(previous, new TEvents::TEvPoison);
            }
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

        rec.Generation = Max(rec.Generation, msg.Generation);
        auto& generation = rec.Consumers[ev->Sender];
        generation = Max(generation, msg.Generation);
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
        const auto consumer = rec.Consumers.find(ev->Sender);
        if (consumer == rec.Consumers.end() || consumer->second != msg.Generation) {
            return;
        }
        rec.Consumers.erase(consumer);
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
