#include "blob_checker.h"
#include "blob_checker_actors.h"
#include "blob_checker_events.h"

#include <ydb/core/util/stlog.h>

#include <unordered_set>

namespace NKikimr {
namespace NBsController {

///////////////////////////////////////////////////////////////////////////////////
// TBlobCheckerWorker
///////////////////////////////////////////////////////////////////////////////////

class TBlobCheckerWorker : public TActorBootstrapped<TBlobCheckerWorker> {
public:
    TBlobCheckerWorker(TGroupId groupId, TActorId orchestratorActorId, TLogoBlobID maxCheckedBlob)
        : GroupId(groupId)
        , OrchestratorActorId(orchestratorActorId)
        , MaxCheckedBlob(maxCheckedBlob)
    {}

    void Bootstrap() {
        STLOG(PRI_NOTICE, BLOB_CHECKER_WORKER, BSW01, "Bootstrapping BlobCheckerWorker",
                (GroupId, GroupId));

        QuantumStart = TActivationContext::Monotonic();
        RequestNextPage();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_CHECKER_WORKER_ACTOR;
    }

private:
    STRICT_STFUNC(StateAssimilating, {
        hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
    });

    STRICT_STFUNC(StateCheckingIntegrity, {
        hFunc(TEvBlobStorage::TEvCheckIntegrityResult, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
    });

private:
    void HandlePoison() {
        // The orchestrator uses this final result to release the controller's
        // scrub exclusion before allowing a replacement worker to start.
        FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
    }

    void Handle(const TEvBlobStorage::TEvAssimilateResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BLOB_CHECKER_WORKER, BSW10, "Handle TEvAssimilateResult",
                (GroupId, GroupId),
                (Event, ev->Get()->ToString()));

        TEvBlobStorage::TEvAssimilateResult* res = ev->Get();
        if (res->Status != NKikimrProto::OK) {
            // Unable to collect blobs to check from group
            // We terminate worker and try again later
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
            return;
        }

        // Blocks and barriers are deliberately skipped by RequestNextPage().
        Y_DEBUG_ABORT_UNLESS(res->Blocks.empty() && res->Barriers.empty());

        BlobsToCheck.swap(res->Blobs);
        if (BlobsToCheck.empty()) {
            // Assimilation is paginated. An empty page is the end-of-stream marker.
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::FinishOk);
            return;
        }

        // assure that BlobsToCheck queue is sorted
        std::sort(BlobsToCheck.begin(), BlobsToCheck.end(),
                [](const TBlob& left, const TBlob& right) { return left.Id < right.Id; });
        Become(&TThis::StateCheckingIntegrity);

        CheckNext();
    }

    void Handle(const TEvBlobStorage::TEvCheckIntegrityResult::TPtr& ev) {
        const TEvBlobStorage::TEvCheckIntegrityResult* res = ev->Get();
        STLOG(PRI_DEBUG, BLOB_CHECKER_WORKER, BSW11, "Handle TEvCheckIntegrityResult",
                (GroupId, GroupId),
                (Event, res->ToString()));

        if (res->Status != NKikimrProto::OK) {
            // Most likely CheckIntegrity fails when group is in DISINTEGRATED state
            // or when it is deleted
            // In either case we should stop worker
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
            return;
        }

        TLogoBlobID blobId = res->Id;
        MaxCheckedBlob = std::max(MaxCheckedBlob, blobId);

        switch (res->PlacementStatus) {
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_UNKNOWN:
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_REPLICATION_IN_PROGRESS:
                // Treat transient placement states as a blob being written or deleted.
                // It will be observed again by a later full scan.
                ++UnknownPlacementStatusCount;
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_LOST:
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE:
                ++PlacementIssuesCount;
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_OK:
            default:
                break; // nothing to do
        }

        switch (res->DataStatus) {
            case TEvBlobStorage::TEvCheckIntegrityResult::DS_UNKNOWN:
                // Unreadable blobs are intentionally skipped for now; they are
                // presumed to be in the middle of a write or deletion.
                ++UnknownDataStatusCount;
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR:
                BlobsWithDataIssues.push_back(blobId);
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::DS_OK:
            default:
                break; // nothing to do
        }

        CheckNext();
    }

    void CheckNext() {
        TMonotonic now = TActivationContext::Monotonic();

        if (BlobsToCheck.empty()) {
            RequestNextPage();
            return;
        }

        if (now - QuantumStart > QuantumDuration) {
            // Report intermediate status and write MaxCheckedBlob on disk to save checker progress
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::IntermediateOk);
        }

        TLogoBlobID blobId = BlobsToCheck.front().Id;
        BlobsToCheck.pop_front();

        STLOG(PRI_DEBUG, BLOB_CHECKER_WORKER, BSW12, "Send TEvCheckIntegrity",
                (GroupId, GroupId),
                (BlobId, blobId.ToString()));

        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvCheckIntegrity(blobId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::LowRead, true));
    }

    void FinishQuantum(EBlobCheckerWorkerQuantumStatus quantumStatus) {
        STLOG(PRI_DEBUG, BLOB_CHECKER_WORKER, BSW20, "Finish Quantum",
                (GroupId, GroupId),
                (QuantumStatus, BlobCheckerWorkerQuantumStatusToString(quantumStatus)));

        Send(OrchestratorActorId, new TEvBlobCheckerFinishQuantum(GroupId, quantumStatus, MaxCheckedBlob,
                std::exchange(UnknownDataStatusCount, 0),
                std::exchange(UnknownPlacementStatusCount, 0),
                std::move(BlobsWithDataIssues),
                std::exchange(PlacementIssuesCount, 0)));

        if (quantumStatus != EBlobCheckerWorkerQuantumStatus::IntermediateOk) {
            PassAway();
            return;
        }

        QuantumStart = TActivationContext::Monotonic();
    }

    void RequestNextPage() {
        std::optional<TLogoBlobID> skipBlobsUpTo;
        if (MaxCheckedBlob) {
            skipBlobsUpTo.emplace(MaxCheckedBlob);
        }

        Become(&TThis::StateAssimilating);
        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvAssimilate(
                Max<ui64>(), std::make_tuple(Max<ui64>(), Max<ui8>()), skipBlobsUpTo,
                /*ignoreDecommitState=*/true, /*reverse=*/false));
    }

private:
    constexpr static TDuration QuantumDuration = TDuration::Minutes(30);

private:
    TGroupId GroupId;
    TActorId OrchestratorActorId;
    TLogoBlobID MaxCheckedBlob;
    TMonotonic QuantumStart;

    using TBlob = TEvBlobStorage::TEvAssimilateResult::TBlob;
    std::deque<TBlob> BlobsToCheck;

    // Unable to resolve status, some disks are unavailable or replicating
    ui32 UnknownDataStatusCount = 0;
    ui32 UnknownPlacementStatusCount = 0;

    // Certain problems
    std::vector<TLogoBlobID> BlobsWithDataIssues = {}; // The most severe issue, report full list
    ui32 PlacementIssuesCount = 0;

};

///////////////////////////////////////////////////////////////////////////////////
// TBlobCheckerOrchestrator
///////////////////////////////////////////////////////////////////////////////////

class TBlobCheckerOrchestrator : public TActorBootstrapped<TBlobCheckerOrchestrator> {
public:
    TBlobCheckerOrchestrator(TActorId bscActorId,
            std::unordered_map<TGroupId, TString>&& serializedGroups,
            TDuration periodicity, ::NMonitoring::TDynamicCounterPtr counters)
        : BSCActorId(bscActorId)
        , CheckPeriodicity(periodicity)
        , Counters(counters->GetSubgroup("subsystem", "blob_checker"))
        , DataIssues(Counters->GetCounter("DataIssues", false))
        , PlacementIssues(Counters->GetCounter("PlacementIssues", false))
        , WorkersCreated(Counters->GetCounter("WorkersCreated", false))
        , WorkersTerminated(Counters->GetCounter("WorkersTerminated", false))
        , ChecksCompleted(Counters->GetCounter("ChecksCompleted", false))
    {
        AddGroups(std::move(serializedGroups));
    }

    void Bootstrap() {
        STLOG(PRI_NOTICE, BLOB_CHECKER_ORCHESTRATOR, BSO01, "Bootstrapping BlobCheckerOrchestrator");
        Become(&TThis::StateFunc);
        HandleWakeup();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_CHECKER_ORCHESTRATOR_ACTOR;
    }

private:
    static constexpr TDuration BSCRequestDelay = TDuration::Minutes(1);
    static constexpr TDuration InitialRetryDelay = TDuration::Minutes(1);
    static constexpr TDuration MaxRetryDelay = TDuration::Hours(1);

    struct TGroupCheckInfo {
        TBlobCheckerGroupStatus Status;
        bool RequestPending = false;
        bool CancellationPending = false;
        bool DeleteAfterWorker = false;
        std::optional<TActorId> WorkerId = std::nullopt;
        std::optional<TBlobCheckerGroupStatus> ReplacementStatus;
        TDuration RetryDelay = InitialRetryDelay;
    };

    STRICT_STFUNC(StateFunc, {
        hFunc(TEvBlobCheckerFinishQuantum, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
        hFunc(TEvBlobCheckerUpdateGroupSet, Handle);
        hFunc(TEvBlobCheckerDecision, Handle);
        hFunc(TEvBlobCheckerUpdateSettings, Handle);
        cFunc(TEvents::TEvWakeup::EventType, HandleWakeup);
    })

private:
    void HandlePoison() {
        STLOG(PRI_NOTICE, BLOB_CHECKER_ORCHESTRATOR, BSO30, "Received Poison");

        for (const auto& [id, info] : Groups) {
            if (info.WorkerId) {
                Send(*info.WorkerId, new TEvents::TEvPoisonPill);
                ++*WorkersTerminated;
            }
        }

        PassAway();
    }

    void Handle(const TEvBlobCheckerUpdateGroupSet::TPtr& ev) {
        AddGroups(std::move(ev->Get()->NewGroups));
        std::unordered_set<TGroupId> deletedGroups(ev->Get()->DeletedGroups.begin(),
                ev->Get()->DeletedGroups.end());
        DeleteGroups(std::move(deletedGroups));
    }

    void Handle(const TEvBlobCheckerDecision::TPtr& ev) {
        TGroupId groupId = ev->Get()->GroupId;
        STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO21, "Got decision from BSC",
                (GroupId, groupId),
                (Status, NKikimrProto::EReplyStatus_Name(ev->Get()->Status)));

        const auto it = Groups.find(groupId);
        if (it == Groups.end()) {
            STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO24,
                    "Ignoring decision for a removed BlobChecker group",
                    (GroupId, groupId));
            return;
        }

        TGroupCheckInfo& info = it->second;
        const bool requestWasPending = std::exchange(info.RequestPending, false);

        TInstant now = TActivationContext::Now();
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            if (!requestWasPending) {
                STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO25,
                        "Ignoring stale successful BlobChecker decision",
                        (GroupId, groupId));
                if (!info.WorkerId) {
                    // A plan request from a deleted incarnation can be
                    // accepted after the same group id is re-created. It must
                    // still release the controller's planner locks.
                    Send(BSCActorId, new TEvBlobCheckerUpdateGroupStatus(groupId,
                            info.Status.SerializeProto(), /*finishScan=*/true));
                }
                break;
            }
            if (info.WorkerId) {
                break;
            }
            if (CheckPeriodicity == TDuration::Zero()) {
                // A decision may have crossed a disable request. Complete it
                // without spawning a worker so BSC can release its group lock.
                Send(BSCActorId, new TEvBlobCheckerUpdateGroupStatus(groupId,
                        info.Status.SerializeProto(), /*finishScan=*/true));
                break;
            }
            if (info.Status.ShortStatus & EBlobCheckerResultStatusFlags::ScanFinished) {
                info.Status.ShortStatus = 0;
                info.Status.MaxCheckedBlob = TLogoBlobID{};
            }
            info.WorkerId.emplace(Register(CreateBlobCheckerWorkerActor(
                    groupId, SelfId(), info.Status.MaxCheckedBlob)));
            ++*WorkersCreated;
            break;
        }
        case NKikimrProto::ERROR:
            if (info.WorkerId) {
                if (!info.CancellationPending) {
                    info.CancellationPending = true;
                    Send(*info.WorkerId, new TEvents::TEvPoisonPill);
                }
            } else if (requestWasPending) {
                ScheduleRetry(groupId, info, now);
            }
            break;
        default:
            Y_DEBUG_ABORT_S("Unexpected status# " << NKikimrProto::EReplyStatus_Name(ev->Get()->Status));
        }
    }

    void Handle(const TEvBlobCheckerFinishQuantum::TPtr& ev) {
        TEvBlobCheckerFinishQuantum* res = ev->Get();
        TGroupId groupId = res->GroupId;
        STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO20, "Worker finished quantum",
                (Event, res->ToString()));

        auto it = Groups.find(groupId);
        if (it == Groups.end()) {
            // Group was deleted, nothing to do
            return;
        }

        TGroupCheckInfo& info = it->second;
        if (!info.WorkerId || *info.WorkerId != ev->Sender) {
            STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO23,
                    "Ignoring result from a stale BlobChecker worker",
                    (GroupId, groupId),
                    (Sender, ev->Sender),
                    (CurrentWorkerId, info.WorkerId ? info.WorkerId->ToString() : TString("<none>")));
            return;
        }

        bool finishScan = false;
        TInstant now = TActivationContext::Now();

        if (info.DeleteAfterWorker &&
                res->QuantumStatus == EBlobCheckerWorkerQuantumStatus::IntermediateOk) {
            // A checkpoint may cross the deletion request. Do not publish old
            // incarnation state after its record has been removed/re-created.
            return;
        }

        switch (res->QuantumStatus) {
        case EBlobCheckerWorkerQuantumStatus::FinishOk: {
            info.Status.LastScanFinishedTimestamp = now;
            info.Status.ShortStatus |= EBlobCheckerResultStatusFlags::ScanFinished;
            info.Status.MaxCheckedBlob = res->MaxCheckedBlob;
            info.WorkerId.reset();
            info.CancellationPending = false;
            info.RetryDelay = InitialRetryDelay;
            finishScan = true;
            ++*ChecksCompleted;
            ++*WorkersTerminated;
            if (!info.DeleteAfterWorker && CheckPeriodicity != TDuration::Zero()) {
                CheckOrder.emplace(info.Status.LastScanFinishedTimestamp, groupId);
            }
            break;
        }
        case EBlobCheckerWorkerQuantumStatus::Error: {
            finishScan = true;
            info.WorkerId.reset();
            info.CancellationPending = false;
            ++*WorkersTerminated;
            if (!info.DeleteAfterWorker) {
                ScheduleRetry(groupId, info, now);
            }
            break;
        }
        case EBlobCheckerWorkerQuantumStatus::IntermediateOk:
            info.Status.MaxCheckedBlob = res->MaxCheckedBlob;
            break;
        }

        if (res->PlacementIssuesCount) {
            *PlacementIssues += res->PlacementIssuesCount;
            info.Status.ShortStatus |= EBlobCheckerResultStatusFlags::PlacementIssues;
            STLOG(PRI_INFO, BLOB_CHECKER_ORCHESTRATOR, BSO50, "BlobChecker found placement issues",
                    (PlacementIssuesCount, res->PlacementIssuesCount));
        }
        if (!res->BlobsWithDataIssues.empty()) {
            *DataIssues += res->BlobsWithDataIssues.size();
            TStringStream str;
            str << "[ ";
            for (const TLogoBlobID& blobId : res->BlobsWithDataIssues) {
                str << blobId.ToString() << " ";
            }
            str << "]";

            info.Status.ShortStatus |= EBlobCheckerResultStatusFlags::DataIssues;
            STLOG(PRI_CRIT, BLOB_CHECKER_ORCHESTRATOR, BSO51, "BlobChecker found data issues",
                    (BlobIds, str.Str()));
        }

        const bool deleteAfterWorker = finishScan && info.DeleteAfterWorker;
        std::optional<TBlobCheckerGroupStatus> replacementStatus;
        if (deleteAfterWorker) {
            replacementStatus = std::move(info.ReplacementStatus);
        }

        Send(BSCActorId, new TEvBlobCheckerUpdateGroupStatus(groupId,
                replacementStatus ? replacementStatus->SerializeProto() : info.Status.SerializeProto(),
                finishScan));

        if (deleteAfterWorker) {
            Groups.erase(it);
            if (replacementStatus) {
                auto [newIt, inserted] = Groups.try_emplace(groupId);
                Y_ABORT_UNLESS(inserted);
                newIt->second.Status = std::move(*replacementStatus);
                if (CheckPeriodicity != TDuration::Zero()) {
                    CheckOrder.emplace(newIt->second.Status.LastScanFinishedTimestamp, groupId);
                }
            }
        }
    }

    void Handle(const TEvBlobCheckerUpdateSettings::TPtr& ev) {
        STLOG(PRI_INFO, BLOB_CHECKER_ORCHESTRATOR, BSO11, "Handle TEvBlobCheckerUpdateSettings",
                (Event, ev->ToString()));
        const bool wasEnabled = CheckPeriodicity != TDuration::Zero();
        CheckPeriodicity = ev->Get()->Periodicity;

        if (CheckPeriodicity == TDuration::Zero()) {
            CheckOrder.clear();
            OutgoingRequests.clear();
            for (auto& [groupId, info] : Groups) {
                if (std::exchange(info.RequestPending, false)) {
                    // Requests queued behind another group's node locks may
                    // have no decision yet. Explicitly finish them so disable
                    // cannot strand planner locks across re-enable.
                    Send(BSCActorId, new TEvBlobCheckerUpdateGroupStatus(groupId,
                            info.Status.SerializeProto(), /*finishScan=*/true));
                }
                if (info.WorkerId && !info.CancellationPending) {
                    info.CancellationPending = true;
                    Send(*info.WorkerId, new TEvents::TEvPoisonPill);
                }
            }
            return;
        }

        if (!wasEnabled) {
            // Rebuild scheduling only for groups that are not still finishing a
            // worker or awaiting a decision from BSC.
            for (const auto& [groupId, info] : Groups) {
                if (!info.WorkerId && !info.RequestPending && !info.DeleteAfterWorker) {
                    CheckOrder.emplace(info.Status.LastScanFinishedTimestamp, groupId);
                }
            }
        }
        CheckGroups();
    }

    void HandleWakeup() {
        if (CheckPeriodicity == TDuration::Zero()) {
            Schedule(BSCRequestDelay, new TEvents::TEvWakeup);
            return;
        }

        const TInstant now = TActivationContext::Now();
        while (!OutgoingRequests.empty() && OutgoingRequests.begin()->first <= now) {
            const TGroupId groupId = OutgoingRequests.begin()->second;
            OutgoingRequests.erase(OutgoingRequests.begin());
            SendRequest(groupId);
        }
        CheckGroups();
        Schedule(BSCRequestDelay, new TEvents::TEvWakeup);
    }

private:
    void AddGroups(std::unordered_map<TGroupId, TString>&& newGroups) {
        STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO10, "Adding new groups",
                (NewGroupsCount, newGroups.size()));
        for (const auto& [groupId, serializedState] : newGroups) {
            TBlobCheckerGroupStatus status = TBlobCheckerGroupStatus::Deserialize(serializedState);
            const auto [it, inserted] = Groups.try_emplace(groupId);
            if (!inserted) {
                if (it->second.DeleteAfterWorker) {
                    // Do not overlap a re-created group with the worker of its
                    // previous incarnation. Install the replacement after the
                    // old worker acknowledges cancellation.
                    it->second.ReplacementStatus = std::move(status);
                }
                // Group-set updates may be retried. Keep them idempotent so a
                // duplicate neither overwrites live state nor adds a schedule.
                continue;
            }
            it->second.Status = std::move(status);
            if (CheckPeriodicity != TDuration::Zero()) {
                CheckOrder.emplace(it->second.Status.LastScanFinishedTimestamp, groupId);
            }
        }
    }

    void DeleteGroups(std::unordered_set<TGroupId>&& deletedGroups) {
        for (const TGroupId groupId : deletedGroups) {
            const auto it = Groups.find(groupId);
            if (it != Groups.end()) {
                TGroupCheckInfo& info = it->second;
                if (info.WorkerId) {
                    info.DeleteAfterWorker = true;
                    info.ReplacementStatus.reset();
                    if (!info.CancellationPending) {
                        info.CancellationPending = true;
                        Send(*info.WorkerId, new TEvents::TEvPoisonPill);
                    }
                } else {
                    Groups.erase(it);
                }
            }
        }

        auto eraseDeleted = [&deletedGroups](auto& scheduled) {
            for (auto it = scheduled.begin(); it != scheduled.end(); ) {
                if (deletedGroups.contains(it->second)) {
                    it = scheduled.erase(it);
                } else {
                    ++it;
                }
            }
        };
        eraseDeleted(CheckOrder);
        eraseDeleted(OutgoingRequests);
    }

    void SendRequest(TGroupId groupId) {
        const auto it = Groups.find(groupId);
        if (it == Groups.end() || it->second.RequestPending || it->second.WorkerId ||
                it->second.DeleteAfterWorker ||
                CheckPeriodicity == TDuration::Zero()) {
            return;
        }

        STLOG(PRI_NOTICE, BLOB_CHECKER_ORCHESTRATOR, BSO22, "Sending request to BSC",
                (GroupId, groupId));
        it->second.RequestPending = true;
        Send(BSCActorId, new TEvBlobCheckerPlanCheck(groupId));
    }

    void CheckGroups() {
        if (CheckPeriodicity == TDuration::Zero()) {
            return;
        }

        const TInstant now = TActivationContext::Now();
        for (auto it = CheckOrder.begin(); it != CheckOrder.end(); ) {
            const auto [ts, groupId] = *it;
            if (ts + CheckPeriodicity > now) {
                break;
            }

            it = CheckOrder.erase(it);
            SendRequest(groupId);
        }
    }

    void ScheduleRetry(TGroupId groupId, TGroupCheckInfo& info, TInstant now) {
        if (CheckPeriodicity == TDuration::Zero()) {
            return;
        }
        OutgoingRequests.emplace(now + info.RetryDelay, groupId);
        info.RetryDelay = TDuration::MicroSeconds(Min<ui64>(
                info.RetryDelay.MicroSeconds() * 2, MaxRetryDelay.MicroSeconds()));
    }

private:
    TActorId BSCActorId;

    std::unordered_map<TGroupId, TGroupCheckInfo> Groups;
    std::multimap<TInstant, TGroupId> CheckOrder;
    std::multimap<TInstant, TGroupId> OutgoingRequests;

    TDuration CheckPeriodicity = TDuration::Days(30);

    // counters
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr DataIssues;
    ::NMonitoring::TDynamicCounters::TCounterPtr PlacementIssues;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkersCreated;
    ::NMonitoring::TDynamicCounters::TCounterPtr WorkersTerminated;
    ::NMonitoring::TDynamicCounters::TCounterPtr ChecksCompleted;
};

///////////////////////////////////////////////////////////////////////////////////
// Actor Creators
///////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId,
        std::unordered_map<TGroupId, TString> serializedGroups,
        TDuration periodicity, ::NMonitoring::TDynamicCounterPtr counters) {
    return new TBlobCheckerOrchestrator(bscActorId, std::move(serializedGroups),
            periodicity, counters);
}

NActors::IActor* CreateBlobCheckerWorkerActor(TGroupId groupId, TActorId orchestratorId,
        TLogoBlobID maxCheckedBlob) {
    return new TBlobCheckerWorker(groupId, orchestratorId, maxCheckedBlob);
}


} // namespace NBsController
} // namespace NKikimr
