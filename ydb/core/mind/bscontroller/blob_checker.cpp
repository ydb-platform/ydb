#include "blob_scanner.h"

namespace NKikimr {

TBlobCheckerGroupState::TBlobCheckerGroupState(NKikimrBlobChecker::TBlobCheckerGroupState serializedState) {
    ShortStatus = serializedState.GetShortStatus();
    LastScanFinishedTimestamp = reinterpret_cast<TMonotonic>(serializedState.GetLastScanFinishedTimestamp());
    MaxCheckedBlob = LogoBlobIDFromLogoBlobID(serializedState.GetMaxCheckedBlob());
}


TEvFinishQuantum::TEvFinishQuantum(TGroupId groupId, EBlobCheckerWorkerQuantumStatus quantumStatus,
        TLogoBlobID maxCheckedBlob, ui32 unknownDataStatusCount, ui32 unknownPlacementStatusCount,
        std::vector<TLogoBlobID>&& blobsWithDataIssues, ui32 placementIssuesCount)
    : GroupId(groupId)
    , QuantumStatus(quantumStatus)
    , MaxCheckedBlob(maxCheckedBlob)
    , UnknownDataStatusCount(unknownDataStatusCount)
    , UnknownPlacementStatusCount(unknownPlacementStatusCount)
    , BlobsWithDataIssues(std::forward<std::vector<TLogoBlobID>>(blobsWithDataIssues))
    , PlacementIssuesCount(placementIssuesCount)
{}


class TBlobCheckerWorker : public TActorBootstrapped<TBlobCheckerWorker> {
public:
    TBlobCheckerWorker(TGroupId groupId, TActorId orchestratorActorId)
        : GroupId(groupId)
        , OrchestratorActorId(orchestratorActorId)
    {}

    void Bootstrap() {
        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvAssimilate(
                std::nullopt, std::nullopt, std::nullopt, /*ignoreDecommitState=*/true, /*reverse=*/false));
        Become(TThis::StateAssimilating);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_SCANNER_WORKER_ACTOR;
    }

private:
    STRICT_STFUNC(TThis::StateAssimilating, {
        hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    });

    STRICT_STFUNC(TThis::StateCheckingIntegrity, {
        hFunc(TEvBlobStorage::TEvCheckIntegrityResult, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
    });

private:
    void Handle(const TEvBlobStorage::TEvAssimilateResult::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            // Unable to collect blobs to check from group
            // We terminate worker and try again later
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
        }
        BlobsToCheck.swap(ev->Get()->Blobs);
        // assure that BlobsToCheck queue is sorted
        std::sort(BlobsToCheck.begin(), BlobsToCheck.end());
        Become(TThis::StateCheckingIntegrity);

        QuantumStart = TActivationContext::Monotonic();
        CheckNext();
    }

    void Handle(const TEvBlobStorage::TEvCheckIntegrityResult::TPtr& ev) {
        const TEvBlobStorage::TEvCheckIntegrityResult* res = ev->Get();
        if (res->Status != NKikimrProto::OK) {
            // Most likely CheckIntegrity fails when group is in DISINTEGRATED state
            // or when it is deleted
            // In either case we should stop worker
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
        }
        TLogoBlobId blobId = res->Id;
        MaxCheckedBlob = std::max(MaxCheckedBlob, blobId);

        switch (res->Get()->PlacementStatus) {
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

        switch (res->Get()->DataStatus) {
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
    }

    void CheckNext() {
        TMonotonic now = TActivationContext::Monotonic();

        if (BlobsToCheck.empty()) {
            // All blobs successfully checked
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::LastQuantumOk);
            return;
        }

        if (now - QuantumStart > QuantumDuration) {
            // Report intermeidate status and write MaxCheckedBlob on disk to save checker progress
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::IntermediateOk);
        }

        TLogoBlobID blobId = BlobsToCheck.front();
        BlobsToCheck.pop_front();

        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvCheckIntegrity(blobId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::LowRead, true));
    }

    void FinishQuantum(EBlobCheckerWorkerQuantumStatus quantumStatus) {
        Send(OrchestratorActorId, new TEvFinishQuantum(GroupId, quantumStatus, MaxCheckedBlob,
                std::exchange(UnknownDataStatusCount, 0),
                std::exchange(UnknownPlacementStatusCount, 0),
                std::move(BlobsWithDataIssues),
                std::exchange(PlacementIssuesCount, 0)));

        if (quantumStatus != EBlobCheckerWorkerQuantumStatus::IntermediateOk) {
            PassAway();
        }
    }

private:
    constexpr static TDuration QuantumDuration = TDuration::Minutes(30);

private:
    TGroupId GroupId;
    TActorId OrchestratorActorId;

    std::deque<TLogoBlobID> BlobsToCheck;

    // Unable to resolve status, some disks are unavailable or replicating
    ui32 UnknownDataStatusCount = 0;
    ui32 UnknownPlacementStatusCount = 0;

    // Certain problems
    std::vector<TLogoBlobID> BlobsWithDataIssues = {}; // The most severe issue, report full list
    ui32 PlacementIssuesCount = 0;

    TMonotonic QuantumStart;
    TLogoBlobID MaxCheckedBlob = TLogoBlobID{};
};


class TBlobCheckerOrchestrator : public TActorBootstrapped<TBlobCheckerOrchestrator> {
public:
    TBlobCheckerOrchestrator(TActorId bscActorId, TSerializedBlobCheckerGroupState serializedState)
        : BSCActorId(bscActorId)
    {
        AddGroups(srd)
    }

    void Bootstrap() {
        Become(TThis::StateFunc);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_SCANNER_ORCHESTRATOR_ACTOR;
    }

private:
    STRICT_STFUNC(StateFunc, {
        hFunc(TEvFinishQuantum, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
        hFunc(TEvControllerBlobCheckerUpdateGroupSet, Handle);
    })

private:
    void HandlePoison() {
        for (const TActorId& actorId : ActiveWorkers) {
            Send(actorId, new TEvents::TEvPoisonPill);
        }

        PassAway();
    }

    void Handle(const TEvControllerBlobCheckerUpdateGroupSet::TPtr& ev) {
        for (const TActorId& actorId : ActiveWorkers) {
            Send(actorId, new TEvents::TEvPoisonPill);
        }

        PassAway();
    }

    void Handle(const TEvFinishQuantum::TPtr& ev) {
        switch (ev->Get()->QuantumStatus) {
            case EBlobCheckerWorkerQuantumStatus::IntermediateOk:
                break;
            case EBlobCheckerWorkerQuantumStatus::LastQuantumOk:
                break;
            case EBlobCheckerWorkerQuantumStatus::Error:
                break;
        }
    }

    void AddGroups(std::unordered_map<TGroupId, TString>&& newGroups) {
        for (const auto& [groupId, serializedState] : newGroups) {
            GroupState[groupId] = TBlobCheckerGroupState(serializedState);
        }
    }

    void EnqueueGroupCheck(TGroupId groupId) {
        auto it = GroupCheckOrderLookup.find(groupId);
        if (it != GroupCheckOrderLookup.end()) {
            GroupCheckOrder.erase(it->second);
            GroupCheckOrderLookup.erase(it);
        }
    }

private:
    // TODO: set it via ICB or something
    constexpr static TDuration GroupCheckPeriodicity = TDuration::Days(30);

private:
    TActorId BSCActorId;
    std::map<TMonotonic, TGroupId> GroupCheckOrder;
    std::unordered_map<TGroupId, TGroupChecksOrder::iterator> GroupCheckOrderLookup;

    std::unordered_map<TGroupId, TBlobCheckerGroupState> GroupState;
    std::unordered_map<TGroupId, TActorId> ActiveWorkers;
};


NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId) {
    return new TBlobCheckerOrchestrator(TActorId bscActorId);
}

} // namespace NKikimr
