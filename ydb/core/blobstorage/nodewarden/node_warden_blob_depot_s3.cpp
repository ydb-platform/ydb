#include "node_warden.h"
#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blob_depot/s3_route.h>
#include <ydb/core/blob_depot/s3_router.h>
#include <ydb/core/blob_depot/s3_router_events.h>

#include <google/protobuf/util/message_differencer.h>

#include <algorithm>

namespace NKikimr::NStorage {

    static NOpenSsl::NSha256::TDigest S3RouteDigest(const NKikimrBlobDepot::TS3BackendSettings& settings) {
        return NOpenSsl::NSha256::Calc(NBlobDepot::GetS3Route(settings).SerializeAsString());
    }

    void TNodeWarden::ExpireS3EndpointHandoffs() {
        const auto now = TActivationContext::Monotonic();
        while (!S3EndpointHandoffs.empty() && S3EndpointHandoffs.front().Expires <= now) {
            S3EndpointHandoffs.pop_front();
        }
    }

    void TNodeWarden::Handle(TAutoPtr<TEventHandle<TEvNodeWardenAcquireBlobDepotS3Router>> ev) {
        auto& msg = *ev->Get();
        const ui64 tabletId = msg.TabletId;
        YDB_LOG_DEBUG_COMP(BS_NODE, "TEvNodeWardenAcquireBlobDepotS3Router",
            {"marker", "NW70"},
            {"tabletId", tabletId},
            {"sender", ev->Sender});

        TActorSystem* const as = TActivationContext::ActorSystem();
        ExpireS3EndpointHandoffs();
        const auto handoff = std::ranges::find(S3EndpointHandoffs, tabletId, &TS3EndpointHandoff::TabletId);
        if (handoff != S3EndpointHandoffs.end() && msg.Generation <= handoff->Generation) {
            Send(ev->Sender, new TEvNodeWardenBlobDepotS3RouterReady(tabletId, msg.Generation, {}), 0, ev->Cookie);
            return;
        }
        auto& rec = BlobDepotS3Routers[tabletId];

        using google::protobuf::util::MessageDifferencer;
        const bool changed = !rec.Router || !MessageDifferencer::Equals(rec.Settings, msg.Settings);
        if (msg.Generation < rec.Generation || (rec.Router && msg.Generation == rec.Generation && changed)) {
            Send(ev->Sender, new TEvNodeWardenBlobDepotS3RouterReady(tabletId, msg.Generation, {}), 0, ev->Cookie);
            return;
        }
        if (changed) {
            const TActorId previous = rec.Router;
            if (!rec.LastEndpoint || !MessageDifferencer::Equals(
                    NBlobDepot::GetS3Route(rec.Settings), NBlobDepot::GetS3Route(msg.Settings))) {
                rec.LastEndpoint = msg.Settings.GetBalancerHost().empty() ? nullptr : std::make_shared<NBlobDepot::TS3BalancerEndpoint>();
            }
            if (handoff != S3EndpointHandoffs.end()) {
                if (rec.LastEndpoint && handoff->Route == S3RouteDigest(msg.Settings)) {
                    rec.LastEndpoint->Set(std::move(handoff->Host), handoff->Port);
                }
                S3EndpointHandoffs.erase(handoff);
            }
            rec.Settings = std::move(msg.Settings);
            IActor* routerActor = NBlobDepot::CreateBlobDepotS3Router(rec.Settings, tabletId, rec.LastEndpoint);
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
        Send(ev->Sender, new TEvNodeWardenBlobDepotS3RouterReady(tabletId, msg.Generation, rec.Router), 0, ev->Cookie);
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
            if (msg.RestartingForConfigChange && rec.LastEndpoint && msg.Generation == rec.Generation) {
                // The last consumer is restarting on this node. Keep only its confirmed proxy and route digest.
                auto [host, port] = rec.LastEndpoint->Get();
                constexpr size_t MaxHandoffs = 128;
                constexpr size_t MaxHostBytes = 255;
                if (!host.empty() && host.size() <= MaxHostBytes) {
                    if (S3EndpointHandoffs.size() == MaxHandoffs) {
                        S3EndpointHandoffs.pop_front();
                    }
                    const auto expires = TActivationContext::Monotonic() + TDuration::Minutes(2);
                    S3EndpointHandoffs.push_back({tabletId, rec.Generation, expires, S3RouteDigest(rec.Settings),
                        std::move(host), port});
                }
            }
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
