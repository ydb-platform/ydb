#include "blob_scanner.h"
#include "impl.h"

namespace NKikimr {

///////////////////////////////////////////////////////////////////////////////////
// TBlobCheckerGroupState
///////////////////////////////////////////////////////////////////////////////////

TBlobCheckerGroupState::TBlobCheckerGroupState(ui64 status, TMonotonic lastScan,
        TLogoBlobID maxChecked)
    : ShortStatus(status)
    , LastScanFinishedTimestamp(lastScan)
    , MaxCheckedBlob(blob)
{}

TBlobCheckerGroupState TBlobCheckerGroupState::Deserialize(
        NKikimrBlobChecker::TBlobCheckerGroupState serializedState) {
    ui64 status = serializedState.GetShortStatus();
    TMonotonic timestamp = reinterpret_cast<TMonotonic>(serializedState.GetLastScanFinishedTimestamp());
    TLogoBlobID blob = LogoBlobIDFromLogoBlobID(serializedState.GetMaxCheckedBlob());
    return TBlobCheckerGroupState(status, timestamp, blob);
}

///////////////////////////////////////////////////////////////////////////////////
// Events
///////////////////////////////////////////////////////////////////////////////////

TEvBlobCheckerFinishQuantum::TEvBlobCheckerFinishQuantum(TGroupId groupId, EBlobCheckerWorkerQuantumStatus quantumStatus,
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

TString TEvBlobCheckerFinishQuantum::ToString() const {
    TStringStream str;
    str << "TEvBlobCheckerFinishQuantum {";
    str << " GroupId# " << GroupId.GetRawId();
    str << " QuantumStatus# " << BlobCheckerWorkerQuantumStatusToString(QuantumStatus);
    str << " MaxCheckedBlob# " << MaxCheckedBlob.ToString();
    str << " UnknownDataStatusCount# " << UnknownDataStatusCount;
    str << " UnknownPlacementStatusCount# " << UnknownPlacementStatusCount;
    str << " BlobsWithDataIssues# " << BlobsWithDataIssues.size();
    str << " PlacementIssuesCount# " << PlacementIssuesCount;
    str << " }";

    return str.Str();
}

TEvControllerBlobCheckerUpdateGroupStatus::TEvControllerBlobCheckerUpdateGroupStatus(
        TGroupId groupId, TString serializedState, bool finishScan)
    : GroupId(groupId)
    , SerializedState(serializedState)
    , FinishScan(finishScan)
{}

TEvBlobCheckerUpdateGroupSet::TEvBlobCheckerUpdateGroupSet(
        std::vector<TGroupIdWithSerializedState>&& newGroups,
        std::vector<TGroupId>&& deletedGroups)
    : NewGroups(newGroups)
    , DeletedGroups(deletedGroups)
{}

TEvBlobCheckerPlanScan::TEvBlobCheckerPlanScan(TGroupId groupId)
    : GroupId(groupId)
{}

TEvBlobCheckerScanDecision::TEvBlobCheckerScanDecision(TGroupId groupId,
        NKikimrProto::EReplyStatus status)
    : GroupId(groupId)
    , Status(status)
{}

TEvBlobCheckerScanDecision::TEvUpdateBlobCheckerSettings(TDuration periodicity)
    : Periodicity(periodicity)
{}

///////////////////////////////////////////////////////////////////////////////////
// Misc
///////////////////////////////////////////////////////////////////////////////////

TString BlobCheckerWorkerQuantumStatusToString(EBlobCheckerWorkerQuantumStatus value) {
    switch (value) {
    case EBlobCheckerWorkerQuantumStatus::IntermediateOk:
        return "IntermediateOk";
    case EBlobCheckerWorkerQuantumStatus::Error:
        return "Error";
    case EBlobCheckerWorkerQuantumStatus::FinishOk:
        return "FinishOk";
    }
}

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
        STLOG(PRI_NOTICE, BLOB_SCANNER_WORKER_ACTOR, BSW01, "Bootstrapping BlobScannerWorker",
                (GroupId, GroupId));

        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvAssimilate(
                std::nullopt, std::nullopt, std::nullopt, /*ignoreDecommitState=*/true,
                /*reverse=*/false));
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
        STLOG(PRI_DEBUG, BLOB_SCANNER_WORKER_ACTOR, BSW10, "Handle TEvAssimilateResult",
                (GroupId, GroupId),
                (Event, ev->Get()->ToString()));

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
        STLOG(PRI_DEBUG, BLOB_SCANNER_WORKER_ACTOR, BSW11, "Handle TEvCheckIntegrityResult",
                (GroupId, GroupId),
                (Event, ev->Get()->ToString()));

        const TEvBlobStorage::TEvCheckIntegrityResult* res = ev->Get();
        if (res->Status != NKikimrProto::OK) {
            // Most likely CheckIntegrity fails when group is in DISINTEGRATED state
            // or when it is deleted
            // In either case we should stop worker
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::Error);
            return;
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
            // Report intermeidate status and write MaxCheckedBlob on disk to save checker progress
            FinishQuantum(EBlobCheckerWorkerQuantumStatus::IntermediateOk);
        }

        TLogoBlobID blobId = BlobsToCheck.front();
        BlobsToCheck.pop_front();

        STLOG(PRI_DEBUG, BLOB_SCANNER_WORKER_ACTOR, BSW12, "Send TEvCheckIntegrity",
                (GroupId, GroupId),
                (BlobId, blobId.ToString()));

        SendToBSProxy(SelfId(), GroupId, new TEvBlobStorage::TEvCheckIntegrity(blobId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::LowRead, true));
    }

    void FinishQuantum(EBlobCheckerWorkerQuantumStatus quantumStatus) {
        STLOG(PRI_DEBUG, BLOB_SCANNER_WORKER_ACTOR, BSW20, "Finish Quantum",
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

///////////////////////////////////////////////////////////////////////////////////
// TBlobCheckerOrchestrator
///////////////////////////////////////////////////////////////////////////////////

class TBlobCheckerOrchestrator : public TActorBootstrapped<TBlobCheckerOrchestrator> {
public:
    TBlobCheckerOrchestrator(TActorId bscActorId, std::vector<std::pair<TGroupId, TString>>&& serializedState)
        : BSCActorId(bscActorId)
    {
        AddGroups(srd)
    }

    void Bootstrap() {
        STLOG(PRI_NOTICE, BLOB_SCANNER_ORCHESTRATOR_ACTOR, BSO01, "Bootstrapping BlobScannerOrchestrator");
        Become(TThis::StateFunc);
        HandleWakeup();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_SCANNER_ORCHESTRATOR_ACTOR;
    }

private:
    STRICT_STFUNC(StateFunc, {
        hFunc(TEvBlobCheckerFinishQuantum, Handle);
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison);
        hFunc(TEvBlobCheckerUpdateGroupSet, Handle);
        hFunc(EvBlobCheckerScanDecision, Handle);
        cFunc(TEvent::TEvWakeup::EventType, HandleWakeup);
    })

private:
    void HandlePoison() {
        STLOG(PRI_NOTICE, BLOB_SCANNER_ORCHESTRATOR_ACTOR, BSO30, "Received Poison",
                (ActiveWorkers, ActiveWorkers.size()));

        for (const TActorId& actorId : ActiveWorkers) {
            Send(actorId, new TEvents::TEvPoisonPill);
        }

        PassAway();
    }

    void Handle(const TEvBlobCheckerUpdateGroupSet::TPtr& ev) {
        AddGroups(std::move(ev->Get()->NewGroups));
        for (const TGroupId groupId : ev->Get()->DeletedGroups) {
            const auto it = Groups.find(groupId);
            if (it != Group.end()) {
                TGroupCheckStatus& status = it->second;
                if (status.WorkerId) {
                    Send(*status.WorkerId, new TEvents::TEvPoisonPill);
                }
                Groups.erase(it);
            }
        }
    }

    void Handle(const EvBlobCheckerScanDecision::TPtr& ev) {
        TGroupId groupId = ev->Get()->GroupId;
        STLOG(PRI_DEBUG, BLOB_SCANNER_ORCHESTRATOR_ACTOR, BSO21, "Got decision from BSC",
                (GroupId, groupId),
                (Status, NKikimrProto::EReplyStatus_Name(ev->Get()->Status)));

        const auto it = Groups.find(groupId);
        if (it == Groups.end()) {
            Y_DEBUG_ABORT_S("Unknown GroupId# " << groupId);
            return;
        }

        TGroupCheckStatus& status = it->second;

        TMonotonic now = TActivationContext::Monotonic();
        switch (ev->Get()->Status) {
        case NKikimrProto::OK: {
            status.WorkerId.emplace(Register(CreateBlobCheckerWorkerActor(groupId, SelfId())));
        }
        case NKikimrProto::ERROR:
            if (status.WorkerId) {
                Send(*status.WorkerId, new TEvents::TEvPoisonPill);
                status.WorkerId.reset();
            }
            OutgoingRequests.insert(now + RetryDelay, groupId);
        }
    }

    void Handle(const TEvBlobCheckerFinishQuantum::TPtr& ev) {
        TGroupId groupId = ev->Get()->GroupId;
        STLOG(PRI_DEBUG, BLOB_SCANNER_ORCHESTRATOR_ACTOR, BSO20, "Worker finished quantum",
                (Event, ev->Get()->ToString()));

        Send(BSCActorId, new TEvControllerBlobCheckerUpdateGroupStatus(
            groupId,

        ));
    }

    void HandleWakeup() {
        if (!OutgoingRequests.empty()) {
            TMonotonic now = TActivationContext::Monotonic();
            TGroupId groupId = OutgoingRequests.begin()->second;
            OutgoingRequests.erase(OutgoingRequests.begin());
            SendRequest(groupId);
        }
        Schedule(BSCRequestDelay, new TEvents::TEvWakeup);
    }

private:
    void AddGroups(std::vector<std::pair<TGroupId, TString>>&& newGroups) {
        STLOG(PRI_DEBUG, BLOB_SCANNER_ORCHESTRATOR_ACTOR, BSO10, "Adding new groups",
                (NewGroupsCount, newGroups.size()));
        for (const auto& [groupId, serializedState] : newGroups) {
            Groups[groupId].State = TBlobCheckerGroupState::Deserialize(serializedState);
        }
    }

    void SendRequest(TGroupId groupId) {
        STLOG(PRI_NOTICE, BLOB_SCANNER_ORCHESTRATOR_ACTOR, BSO22, "Sending request to BSC",
                (GroupId, groupId));
        Send(BSCActorId, new TEvBlobCheckerPlanScan(groupId))
    }

private:
    // TODO: set it via ICB or something
    constexpr static TDuration GroupCheckPeriodicity = TDuration::Days(30);
    constexpr static TDuration BSCRequestDelay = TDuration::Minutes(1);
    // TODO: something smarter
    constexpr static TDuration RetryDelay = TDuration::Hours(1);

private:
    struct TGroupCheckStatus {
        TBlobCheckerGroupState State;
        std::optional<TActorId> WorkerId;
    };

private:
    TActorId BSCActorId;

    std::unordered_map<TGroupId, TGroupCheckStatus> Groups;
    std::multimap<TMonotonic, TGroupId> OutgoingRequests;
};

///////////////////////////////////////////////////////////////////////////////////
// Actor Creators
///////////////////////////////////////////////////////////////////////////////////

NActors::IActor* CreateBlobCheckerOrchestratorActor(TActorId bscActorId) {
    return new TBlobCheckerOrchestrator(TActorId bscActorId);
}

NActors::IActor* CreateBlobCheckerWorkerActor(TGroupId groupId, TActorId orchestratorId) {
    return new TBlobCheckerWorker(TGroupId groupId, TActorId orchestratorId);
}

} // namespace NKikimr
