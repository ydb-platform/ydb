#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blobstorage/syncer/syncer.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    void TNodeWarden::UpdateSyncRateQuoter() {
        const ui64 rate = !SyncRateConfigLimit ? SyncRateControllerLimit
            : !SyncRateControllerLimit ? SyncRateConfigLimit
            : Min(SyncRateConfigLimit, SyncRateControllerLimit);
        SyncRateQuoter->UpdateBytesPerSecond(rate);
    }

    void TNodeWarden::ApplyWorkingSyncers(const NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate& update) {
        std::set<TWorkingSyncer> toStop = WorkingSyncers;

        for (const auto& item : update.GetSyncers()) {
            using T = std::decay_t<decltype(item)>;
            const auto [it, inserted] = WorkingSyncers.emplace(TWorkingSyncer{
                .BridgeProxyGroupId = TGroupId::FromProto(&item, &T::GetBridgeProxyGroupId),
                .SourceGroupId = TGroupId::FromProto(&item, &T::GetSourceGroupId),
                .TargetGroupId = TGroupId::FromProto(&item, &T::GetTargetGroupId),
                .Reconcile = item.GetReconcile(),
                .Thorough = item.GetThorough(),
            });
            auto& syncer = const_cast<TWorkingSyncer&>(*it);
            syncer.PendingBridgeProxyGroupGeneration = item.GetBridgeProxyGroupGeneration();
            StartSyncerIfNeeded(syncer);
            toStop.erase(syncer);
        }

        for (const TWorkingSyncer& syncer : toStop) {
            if (syncer.ActorId) {
                YDB_LOG_DEBUG("ApplyWorkingSyncers: stopping",
                    {"marker", "NW65"},
                    {"bridgeProxyGroupId", syncer.BridgeProxyGroupId},
                    {"bridgeProxyGroupGeneration", syncer.BridgeProxyGroupGeneration},
                    {"sourceGroupId", syncer.SourceGroupId},
                    {"targetGroupId", syncer.TargetGroupId},
                    {"reconcile", syncer.Reconcile},
                    {"pendingBridgeProxyGroupGeneration", syncer.PendingBridgeProxyGroupGeneration},
                    {"actorId", syncer.ActorId});
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, syncer.ActorId, {}, nullptr, 0));
            }
            WorkingSyncers.erase(syncer);
        }
    }

    void TNodeWarden::StartSyncerIfNeeded(TWorkingSyncer& syncer) {
        auto& group = Groups[syncer.BridgeProxyGroupId.GetRawId()];

        const bool stopCurrent = syncer.BridgeProxyGroupGeneration < syncer.PendingBridgeProxyGroupGeneration
            && syncer.ActorId;
        const bool startNew = (stopCurrent || !syncer.ActorId)
            && group.Info
            && group.Info->GroupGeneration == syncer.PendingBridgeProxyGroupGeneration;

        const ui32 prevBridgeProxyGroupGeneration = syncer.BridgeProxyGroupGeneration;
        const TActorId prevActorId = syncer.ActorId;

        if (stopCurrent) {
            // we've got already running syncer, but group generation gets changed, we need to restart it
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, syncer.ActorId, {}, nullptr, 0));
            syncer.ActorId = {};
            ++syncer.NumStop;
        }
        if (startNew) {
            syncer.BridgeProxyGroupGeneration = syncer.PendingBridgeProxyGroupGeneration;
            syncer.SyncerDataStats = std::make_unique<NGroupSyncer::TSyncerDataStats>();
            syncer.ForceProgressReport = true;
            syncer.ActorId = Register(NGroupSyncer::CreateSyncerActor(group.Info, syncer.SourceGroupId, syncer.TargetGroupId,
                syncer.SyncerDataStats, SyncRateQuoter, syncer.Reconcile, syncer.Thorough));
            syncer.Finished = false;
            syncer.ErrorReason.reset();
            ++syncer.NumStart;
        }
        if (stopCurrent || startNew) {
            YDB_LOG_DEBUG("StartSyncerIfNeeded",
                {"marker", "NW64"},
                {"bridgeProxyGroupId", syncer.BridgeProxyGroupId},
                {"prevBridgeProxyGroupGeneration", prevBridgeProxyGroupGeneration},
                {"bridgeProxyGroupGeneration", syncer.BridgeProxyGroupGeneration},
                {"sourceGroupId", syncer.SourceGroupId},
                {"targetGroupId", syncer.TargetGroupId},
                {"reconcile", syncer.Reconcile},
                {"thorough", syncer.Thorough},
                {"pendingBridgeProxyGroupGeneration", syncer.PendingBridgeProxyGroupGeneration},
                {"prevActorId", prevActorId},
                {"actorId", syncer.ActorId},
                {"hasGroupInfo", static_cast<bool>(group.Info)},
                {"groupInfoGeneration", group.Info ? std::make_optional(group.Info->GroupGeneration) : std::nullopt},
                {"stopCurrent", stopCurrent},
                {"startNew", startNew});
        }
    }

    void TNodeWarden::Handle(TEvNodeWardenNotifySyncerFinished::TPtr ev) {
        auto& msg = *ev->Get();
        const auto it = WorkingSyncers.find(TWorkingSyncer{
            .BridgeProxyGroupId = msg.BridgeProxyGroupId,
            .SourceGroupId = msg.SourceGroupId,
            .TargetGroupId = msg.TargetGroupId,
            .Reconcile = msg.Reconcile,
            .Thorough = msg.Thorough,
        });
        if (it == WorkingSyncers.end()) {
            return;
        }
        auto& syncer = const_cast<TWorkingSyncer&>(*it);
        if (msg.BridgeProxyGroupGeneration != syncer.BridgeProxyGroupGeneration || ev->Sender != syncer.ActorId) {
            return; // completion from a stopped or replaced syncer
        }

        syncer.Finished = true;
        syncer.ErrorReason = std::move(msg.ErrorReason);
        syncer.LastErrorReason = syncer.ErrorReason;
        syncer.ActorId = {};
        ++(syncer.ErrorReason ? syncer.NumFinishError : syncer.NumFinishOK);

        auto notify = std::make_unique<TEvBlobStorage::TEvControllerUpdateSyncerState>();
        FillInWorkingSyncer(&notify->Record, syncer, true);
        SendToController(std::move(notify));
    }

    void TNodeWarden::Handle(TEvNodeWardenStandbyDirty::TPtr ev) {
        auto& msg = *ev->Get();
        StandbyDirty[msg.TargetGroupId] |= msg.Data;
    }

    void TNodeWarden::Handle(TEvNodeWardenRegisterBridgeProxy::TPtr ev) {
        auto& msg = *ev->Get();
        if (msg.Registered) {
            BridgeProxies[msg.GroupId].insert(ev->Sender);
        } else if (const auto it = BridgeProxies.find(msg.GroupId); it != BridgeProxies.end()) {
            it->second.erase(ev->Sender);
            if (it->second.empty()) {
                BridgeProxies.erase(it);
            }
        }
    }

    void TNodeWarden::ApplyStandbySeals(const NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate& update) {
        bool report = false;
        for (const auto& item : update.GetStandbySeals()) {
            using T = std::decay_t<decltype(item)>;
            const auto groupId = TGroupId::FromProto(&item, &T::GetBridgeProxyGroupId);
            const auto targetGroupId = TGroupId::FromProto(&item, &T::GetTargetGroupId);
            const auto it = BridgeProxies.find(groupId);
            if (it == BridgeProxies.end()) {
                // no proxy of the group runs here, so nothing is written to the part from this node
                StandbySealed.emplace_back(targetGroupId, item.GetSealId());
                report = true;
                continue;
            }
            const ui64 cookie = ++LastStandbySealCookie;
            StandbySealsInProgress.emplace(cookie, TStandbySealInProgress{
                .TargetGroupId = targetGroupId,
                .SealId = item.GetSealId(),
                .ProxiesPending = static_cast<ui32>(it->second.size()),
            });
            for (const TActorId& proxyId : it->second) {
                Send(proxyId, new TEvNodeWardenStandbySeal(targetGroupId, item.GetBridgeProxyGroupGeneration()),
                    IEventHandle::FlagTrackDelivery, cookie);
            }
        }
        if (report) {
            auto notify = std::make_unique<TEvBlobStorage::TEvControllerUpdateSyncerState>();
            FillInStandbyReports(&notify->Record);
            SendToController(std::move(notify));
        }
    }

    void TNodeWarden::Handle(TEvNodeWardenStandbySealed::TPtr ev) {
        OnStandbySealResolved(ev->Cookie);
    }

    void TNodeWarden::OnStandbySealResolved(ui64 cookie) {
        const auto it = StandbySealsInProgress.find(cookie);
        if (it == StandbySealsInProgress.end()) {
            Y_DEBUG_ABORT();
            return;
        }
        if (--it->second.ProxiesPending) {
            return;
        }
        StandbySealed.emplace_back(it->second.TargetGroupId, it->second.SealId);
        StandbySealsInProgress.erase(it);
        // the promotion waits for it, so it is reported at once
        auto notify = std::make_unique<TEvBlobStorage::TEvControllerUpdateSyncerState>();
        FillInStandbyReports(&notify->Record);
        SendToController(std::move(notify));
    }

    bool TNodeWarden::FillInStandbyReports(NKikimrBlobStorage::TEvControllerUpdateSyncerState *update) {
        // the marks lost along with the pipe don't matter: BSC takes every standby part for dirty when the node registers;
        // the marks go before the seals, as the proxies have sent them before answering
        const bool res = !StandbyDirty.empty() || !StandbySealed.empty();
        for (const auto& [targetGroupId, data] : std::exchange(StandbyDirty, {})) {
            auto *item = update->AddStandbyDirty();
            targetGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetTargetGroupId);
            if (data) {
                item->SetData(true);
            }
        }
        for (const auto& [targetGroupId, sealId] : std::exchange(StandbySealed, {})) {
            auto *item = update->AddStandbySealed();
            targetGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetTargetGroupId);
            item->SetSealId(sealId);
        }
        return res;
    }

    bool TNodeWarden::FillInWorkingSyncer(NKikimrBlobStorage::TEvControllerUpdateSyncerState *update,
            TWorkingSyncer& syncer, bool forceProgress) {
        auto *item = update->AddSyncers();
        syncer.BridgeProxyGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetBridgeProxyGroupId);
        // Completion and progress belong to the generation actually processed by the actor.
        // A newer assignment can arrive before its group configuration, including across reconnects.
        item->SetBridgeProxyGroupGeneration(syncer.SyncerDataStats
            ? syncer.BridgeProxyGroupGeneration
            : syncer.PendingBridgeProxyGroupGeneration);
        syncer.SourceGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetSourceGroupId);
        syncer.TargetGroupId.CopyToProto(item, &std::decay_t<decltype(*item)>::SetTargetGroupId);
        if (syncer.Finished) {
            item->SetFinished(true);
        }
        if (syncer.Reconcile) {
            item->SetReconcile(true);
        }
        if (syncer.ErrorReason) {
            item->SetErrorReason(*syncer.ErrorReason);
        }

        if (!syncer.SyncerDataStats) {
            return false; // hasn't started yet (maybe waiting for correct group infos)
        }

        // report syncer progress
        auto& stats = *syncer.SyncerDataStats;
        auto res = false;
        forceProgress |= std::exchange(syncer.ForceProgressReport, false);

#define ISSUE_METRIC(NAME) \
        const ui64 current##NAME = stats.NAME; \
        if (forceProgress || syncer.Reported##NAME != current##NAME) { \
            item->Set##NAME(current##NAME); \
            const_cast<TWorkingSyncer&>(syncer).Reported##NAME = current##NAME; \
            res = true; \
        }

        ISSUE_METRIC(BytesDone)
        ISSUE_METRIC(BytesTotal)
        ISSUE_METRIC(BytesError)
        ISSUE_METRIC(BlobsDone)
        ISSUE_METRIC(BlobsTotal)
        ISSUE_METRIC(BlobsError)
        ISSUE_METRIC(BytesToCopy)
        ISSUE_METRIC(BytesCopied)

#undef ISSUE_METRIC

        if (const bool scanFinished = stats.ScanFinished;
                forceProgress || syncer.ReportedScanFinished != scanFinished) {
            item->SetScanFinished(scanFinished);
            syncer.ReportedScanFinished = scanFinished;
            res = true;
        }

        return res;
    }

    void TNodeWarden::NotifySyncersProgress() {
        auto notify = std::make_unique<TEvBlobStorage::TEvControllerUpdateSyncerState>();
        bool doSend = FillInStandbyReports(&notify->Record);
        for (const TWorkingSyncer& syncer : WorkingSyncers) {
            if (FillInWorkingSyncer(&notify->Record, const_cast<TWorkingSyncer&>(syncer), false)) {
                doSend = true;
            } else {
                notify->Record.MutableSyncers()->RemoveLast();
            }
        }
        if (doSend) {
            SendToController(std::move(notify));
        }
    }

} // NKikimr::NStorage
