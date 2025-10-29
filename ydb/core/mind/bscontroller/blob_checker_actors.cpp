#include "blob_checker.h"
#include "blob_checker_actors.h"
#include "blob_checker_events.h"

#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NBsController {

///////////////////////////////////////////////////////////////////////////////////
// TBlobCheckerWorker
///////////////////////////////////////////////////////////////////////////////////

class TBlobCheckerWorker : public TActorBootstrapped<TBlobCheckerWorker> {
public:
    TBlobCheckerWorker(TGroupId groupId, TActorId orchestratorActorId)
        : GroupId(groupId)
        , OrchestratorActorId(orchestratorActorId)
    {}

    void Bootstrap() {
        STLOG(PRI_NOTICE, BLOB_CHECKER_WORKER, BSW01, "Bootstrapping BlobScannerWorker",
                (GroupId, GroupId));

        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvAssimilate(
                std::nullopt, std::nullopt, std::nullopt, /*ignoreDecommitState=*/true,
                /*reverse=*/false));
        Become(&TThis::StateAssimilating);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_CHECKER_WORKER_ACTOR;
    }

private:
    STRICT_STFUNC(StateAssimilating, {
        hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    });

    STRICT_STFUNC(StateCheckingIntegrity, {
        hFunc(TEvBlobStorage::TEvCheckIntegrityResult, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    });

private:
    void Handle(const TEvBlobStorage::TEvAssimilateResult::TPtr& ev) {
        STLOG(PRI_DEBUG, BLOB_CHECKER_WORKER, BSW10, "Handle TEvAssimilateResult",
                (GroupId, GroupId),
                (Event, ev->Get()->ToString()));

        if (ev->Get()->Status != NKikimrProto::OK) {
            // Unable to collect blobs to check from group
            // We terminate worker and try again later
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
        }
        BlobsToCheck.swap(ev->Get()->Blobs);
        // assure that BlobsToCheck queue is sorted
        std::sort(BlobsToCheck.begin(), BlobsToCheck.end(),
                [](const TBlob& left, const TBlob& right) { return left.Id < right.Id; });
        Become(&TThis::StateCheckingIntegrity);

        QuantumStart = TActivationContext::Monotonic();
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
            // All blobs successfully checked
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::FinishOk);
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

private:
    constexpr static TDuration QuantumDuration = TDuration::Minutes(30);

private:
    TGroupId GroupId;
    TActorId OrchestratorActorId;

    using TBlob = TEvBlobStorage::TEvAssimilateResult::TBlob;
    std::deque<TBlob> BlobsToCheck;

    // Unable to resolve status, some disks are unavailable or replicating
    ui32 UnknownDataStatusCount = 0;
    ui32 UnknownPlacementStatusCount = 0;

    // Certain problems
    std::vector<TLogoBlobID> BlobsWithDataIssues = {}; // The most severe issue, report full list
    ui32 PlacementIssuesCount = 0;

    TMonotonic QuantumStart;
    TLogoBlobID MaxCheckedBlob = TLogoBlobID{};
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
        AddGroups(std::forward<std::unordered_map<TGroupId, TString>>(serializedGroups));
    }

    ~TBlobCheckerOrchestrator() {
        Counters->RemoveSubgroup("subsystem", "blob_checker");
    }

    void Bootstrap() {
        STLOG(PRI_NOTICE, BLOB_CHECKER_ORCHESTRATOR, BSO01, "Bootstrapping BlobScannerOrchestrator");
        Become(&TThis::StateFunc);
        HandleWakeup();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_CHECKER_ORCHESTRATOR_ACTOR;
    }

private:
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
            }
        }

        PassAway();
    }

    void Handle(const TEvBlobCheckerUpdateGroupSet::TPtr& ev) {
        AddGroups(std::move(ev->Get()->NewGroups));
        for (const TGroupId groupId : ev->Get()->DeletedGroups) {
            const auto it = Groups.find(groupId);
            if (it != Groups.end()) {
                TGroupCheckInfo& info = it->second;
                if (info.WorkerId) {
                    Send(*info.WorkerId, new TEvents::TEvPoisonPill);
                }
                Groups.erase(it);
            }
        }
    }

    void Handle(const TEvBlobCheckerDecision::TPtr& ev) {
        TGroupId groupId = ev->Get()->GroupId;
        STLOG(PRI_DEBUG, BLOB_CHECKER_ORCHESTRATOR, BSO21, "Got decision from BSC",
                (GroupId, groupId),
                (Status, NKikimrProto::EReplyStatus_Name(ev->Get()->Status)));

        const auto it = Groups.find(groupId);
        if (it == Groups.end()) {
            Y_DEBUG_ABORT_S("Unknown GroupId# " << groupId);
            return;
        }

        TGroupCheckInfo& info = it->second;

        TMonotonic now = TActivationContext::Monotonic();
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            info.Status.ShortStatus = 0;
            info.WorkerId.emplace(Register(CreateBlobCheckerWorkerActor(groupId, SelfId())));
            ++*WorkersCreated;
            break;
        }
        case NKikimrProto::ERROR:
            if (info.WorkerId) {
                Send(*info.WorkerId, new TEvents::TEvPoisonPill);
                info.WorkerId.reset();
            }
            OutgoingRequests.emplace(now + RetryDelay, groupId);
            ++*WorkersTerminated;
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
        bool finishScan = false;

        switch (res->QuantumStatus) {
        case EBlobCheckerWorkerQuantumStatus::FinishOk:
            info.Status.LastScanFinishedTimestamp = TActivationContext::Monotonic();
            ++*ChecksCompleted;
            [[fallthrough]];
        case EBlobCheckerWorkerQuantumStatus::Error:
            finishScan = true;
            info.Status.ShortStatus |= EBlobCheckerResultStatusFlags::ScanFinished;
            CheckOrder.emplace(info.Status.LastScanFinishedTimestamp, groupId);
            ++*WorkersTerminated;
            [[fallthrough]];
        case EBlobCheckerWorkerQuantumStatus::IntermediateOk:
            info.Status.MaxCheckedBlob = res->MaxCheckedBlob;
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
        }

        Send(BSCActorId, new TEvBlobCheckerUpdateGroupStatus(groupId,
                info.Status.SerializeProto(), finishScan));
    }

    void Handle(const TEvBlobCheckerUpdateSettings::TPtr& ev) {
        STLOG(PRI_INFO, BLOB_CHECKER_ORCHESTRATOR, BSO11, "Handle TEvBlobCheckerUpdateSettings",
                (Event, ev->ToString()));
        CheckPeriodicity = ev->Get()->Periodicity;
        CheckGroups();
    }

    void HandleWakeup() {
        if (!OutgoingRequests.empty()) {
            TGroupId groupId = OutgoingRequests.begin()->second;
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
            Groups[groupId].Status = status;
            CheckOrder.emplace(status.LastScanFinishedTimestamp, groupId);
        }
    }

    void SendRequest(TGroupId groupId) {
        STLOG(PRI_NOTICE, BLOB_CHECKER_ORCHESTRATOR, BSO22, "Sending request to BSC",
                (GroupId, groupId));
        Send(BSCActorId, new TEvBlobCheckerPlanCheck(groupId));
    }

    void CheckGroups() {
        TMonotonic now = TActivationContext::Monotonic();
        for (auto it = CheckOrder.begin(); it != CheckOrder.end();) {
            const auto [ts, groupId] = *it;
            if (ts + CheckPeriodicity < now && !Groups[groupId].WorkerId) {
                SendRequest(groupId);
            }

            CheckOrder.erase(it++);
        }
    }

private:
    struct TGroupCheckInfo {
        TBlobCheckerGroupStatus Status;
        bool RequestPending = false;
        std::optional<TActorId> WorkerId = std::nullopt;
    };

private:
    TActorId BSCActorId;

    std::unordered_map<TGroupId, TGroupCheckInfo> Groups;
    std::multimap<TMonotonic, TGroupId> CheckOrder;
    std::multimap<TMonotonic, TGroupId> OutgoingRequests;

    TDuration CheckPeriodicity = TDuration::Days(30);
    // TODO: set it via ICB
    constexpr static TDuration BSCRequestDelay = TDuration::Minutes(1);
    // TODO: something smarter
    constexpr static TDuration RetryDelay = TDuration::Hours(1);

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

NActors::IActor* CreateBlobCheckerWorkerActor(TGroupId groupId, TActorId orchestratorId) {
    return new TBlobCheckerWorker(groupId, orchestratorId);
}
    

} // namespace NBsController
} // namespace NKikimr
