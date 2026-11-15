#include "node_warden_impl.h"
#include "node_warden_events.h"

#include <ydb/core/blobstorage/syncer/syncer.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_NODE

namespace NKikimr::NStorage {

    void TNodeWarden::Handle(NGroupSyncer::TEvRunMirror3dcRepair::TPtr& ev) {
        const NKikimrBlobStorage::TEvRunMirror3dcRepair& request = ev->Get()->Record;
        ui32 groupId = request.GetGroupId();
        ui32 generation = request.GetGeneration();
        auto reply = [&](TActorId recipient, ui64 cookie, ui32 replyGeneration, NKikimrProto::EReplyStatus status) {
            std::unique_ptr<NGroupSyncer::TEvRunMirror3dcRepairResult> result = std::make_unique<NGroupSyncer::TEvRunMirror3dcRepairResult>();
            result->Record.SetGroupId(groupId);
            result->Record.SetGeneration(replyGeneration);
            result->Record.SetStatus(status);
            Send(recipient, result.release(), 0, cookie);
        };
        auto running = RepairSyncers.find(groupId);
        if (request.GetCancel()) {
            if (running != RepairSyncers.end()) {
                const TRepairSyncer& syncer = running->second;
                bool sameRequest = syncer.Requester == ev->Sender && syncer.Cookie == ev->Cookie && syncer.Generation == generation;
                if (sameRequest) {
                    Send(syncer.ActorId, new TEvents::TEvPoisonPill);
                    RepairSyncerGroups.erase(syncer.ActorId);
                    RepairSyncers.erase(running);
                }
            }
            return;
        }
        TIntrusivePtr<TBlobStorageGroupInfo> info = NeedGroupInfo(groupId);
        if (!info || info->GroupGeneration != generation) {
            return reply(ev->Sender, ev->Cookie, generation, NKikimrProto::RACE);
        }
        if (running != RepairSyncers.end()) {
            const TRepairSyncer& syncer = running->second;
            bool sameRequest = syncer.Requester == ev->Sender && syncer.Cookie == ev->Cookie && syncer.Generation == generation;
            if (sameRequest) {
                return;
            }
            Send(syncer.ActorId, new TEvents::TEvPoisonPill);
            reply(syncer.Requester, syncer.Cookie, syncer.Generation, NKikimrProto::RACE);
            RepairSyncerGroups.erase(syncer.ActorId);
            RepairSyncers.erase(running);
        }
        TActorId actor = Register(NGroupSyncer::CreateMirror3dcRepairSyncer(info, SelfId()));
        RepairSyncers.emplace(groupId, TRepairSyncer{generation, actor, ev->Sender, ev->Cookie});
        RepairSyncerGroups.emplace(actor, groupId);
    }

    void TNodeWarden::Handle(NGroupSyncer::TEvMirror3dcRepairScanResult::TPtr& ev) {
        const auto actor = RepairSyncerGroups.find(ev->Sender);
        if (actor == RepairSyncerGroups.end()) {
            return;
        }
        auto running = RepairSyncers.find(actor->second);
        Y_ABORT_UNLESS(running != RepairSyncers.end() && running->second.ActorId == ev->Sender);
        const TRepairSyncer& syncer = running->second;
        std::unique_ptr<NGroupSyncer::TEvRunMirror3dcRepairResult> result = std::make_unique<NGroupSyncer::TEvRunMirror3dcRepairResult>();
        result->Record.SetGroupId(actor->second);
        result->Record.SetGeneration(syncer.Generation);
        result->Record.SetStatus(ev->Get()->Status);
        Send(syncer.Requester, result.release(), 0, syncer.Cookie);
        RepairSyncers.erase(running);
        RepairSyncerGroups.erase(actor);
    }

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
                syncer.SyncerDataStats, SyncRateQuoter));
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
        ISSUE_METRIC(ScannedItems)
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
        bool doSend = false;
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
