#include "impl.h"
#include "blob_checker_actors.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/util/stlog.h>

namespace NKikimr {
namespace NBsController {

/////////////////////////////////////////////////////////////////////////////////////
// Update blob checker group state transaction
/////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageController::TTxBlobCheckerUpdateGroupStatus : public TTransactionBase<TBlobStorageController> {
    TGroupId GroupId;

public:
    TTxBlobCheckerUpdateGroupStatus(TGroupId groupId, TBlobStorageController *controller)
        : TBase(controller)
        , GroupId(groupId)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_BLOB_CHECKER_UPDATE_GROUP_STATUS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        const auto it = Self->BlobCheckerGroupRecords.find(GroupId);
        if (it == Self->BlobCheckerGroupRecords.end()) {
            db.Table<Schema::BlobCheckerGroupStatus>().Key(GroupId.GetRawId()).Delete();
            return true;
        }

        db.Table<Schema::BlobCheckerGroupStatus>().Key(GroupId.GetRawId())
                .Update<Schema::BlobCheckerGroupStatus::SerializedStatus>(it->second);
        return true;
    }

    void Complete(const TActorContext&) override {}
};

/////////////////////////////////////////////////////////////////////////////////////
// Event Handlers
/////////////////////////////////////////////////////////////////////////////////////

void TBlobStorageController::Handle(const TEvBlobCheckerUpdateGroupStatus::TPtr& ev) {
    if (ev->Sender != BlobCheckerOrchestratorId) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC54, "Ignoring status from stale BlobChecker orchestrator",
                (Sender, ev->Sender),
                (OrchestratorId, BlobCheckerOrchestratorId));
        return;
    }

    TGroupId groupId = ev->Get()->GroupId;
    const bool deletionPending = BlobCheckerGroupDeletionsPending.contains(groupId);
    if (deletionPending && ev->Get()->FinishScan) {
        BlobCheckerGroupDeletionsPending.erase(groupId);
    }
    if (ev->Get()->FinishScan) {
        // Release planner and scrub-exclusion locks even when the group record
        // was removed while its worker was shutting down.
        DequeueCheckForGroup(groupId, /*notifyOrchestrator=*/false);
    }

    const auto it = BlobCheckerGroupRecords.find(groupId);
    if (it != BlobCheckerGroupRecords.end() && !deletionPending) {
        it->second = ev->Get()->SerializedState;
        PersistBlobCheckerGroupStatus(groupId);
    }
}

void TBlobStorageController::Handle(const TEvBlobCheckerPlanCheck::TPtr& ev) {
    TGroupId groupId = ev->Get()->GroupId;
    const TActorId orchestratorId = ev->Sender;
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC53, "Handle TEvBlobCheckerPlanCheck",
            (GroupId, groupId));
    if (!IsBlobCheckerEnabled() || !BlobCheckerPlanner || orchestratorId != BlobCheckerOrchestratorId ||
            BlobCheckerCancellationsPending.contains(groupId) ||
            BlobCheckerGroupDeletionsPending.contains(groupId) || !IsBlobCheckerGroupEligible(groupId)) {
        Send(orchestratorId, new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
        return;
    }

    TGroupInfo::TGroupFinder finder = [this](TGroupId groupId) { return FindGroup(groupId); };
    TStaticGroupInfo::TStaticGroupFinder staticFinder = [this](TGroupId groupId) -> TStaticGroupInfo* {
        const auto it = StaticGroups.find(groupId);
        if (it == StaticGroups.end()) {
            return nullptr;
        }
        return &it->second;
    };

    if (const TGroupInfo* groupInfo = finder(groupId)) {
        TGroupInfo::TGroupStatus status = groupInfo->GetStatus(finder, BridgeInfo.get());
        if (status.OperatingStatus == NKikimrBlobStorage::TGroupStatus::FULL &&
                !ScrubState.IsGroupScrubbed(groupId)) {
            BlobCheckerPlanner->EnqueueCheck(groupInfo);
            UpdateBlobCheckerState();
        } else {
            Send(orchestratorId, new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
        }
    } else if (TStaticGroupInfo* staticGroupInfo = staticFinder(groupId)) {
        staticGroupInfo->UpdateStatus(TActivationContext::Monotonic(), this);
        TGroupInfo::TGroupStatus status = staticGroupInfo->GetStatus(staticFinder, BridgeInfo.get());
        if (status.OperatingStatus == NKikimrBlobStorage::TGroupStatus::FULL &&
                !ScrubState.IsGroupScrubbed(groupId)) {
            BlobCheckerPlanner->EnqueueCheck(staticGroupInfo->Info.get());
            UpdateBlobCheckerState();
        } else {
            Send(orchestratorId, new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
        }
    } else {
        // The group may disappear between an orchestrator request and BSC handling it.
        // Always complete the request so that the orchestrator can retry or forget it.
        Send(orchestratorId, new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
    }
}

/////////////////////////////////////////////////////////////////////////////////////
// Auxiliary method implementations
/////////////////////////////////////////////////////////////////////////////////////

void TBlobStorageController::UpdateBlobCheckerState() {
    if (!IsBlobCheckerEnabled() || !BlobCheckerPlanner || !BlobCheckerOrchestratorId) {
        return;
    }

    TMonotonic now = TActivationContext::Monotonic();
    if (now >= NextAllowedBlobCheckerTimestamp) {
        NextAllowedBlobCheckerTimestamp = BlobCheckerPlanner->GetNextAllowedCheckTimestamp(now);

        const std::optional<TGroupId> groupToScan = BlobCheckerPlanner->ObtainNextGroupToCheck();
        if (groupToScan) {
            if (ScrubState.IsGroupScrubbed(*groupToScan)) {
                DequeueCheckForGroup(*groupToScan, /*notifyOrchestrator=*/true);
                return;
            }
            ScrubState.SetBlobCheckerInProgress(*groupToScan, true);
            Send(BlobCheckerOrchestratorId,
                    new TEvBlobCheckerDecision(*groupToScan, NKikimrProto::OK));
        }
    }
}


void TBlobStorageController::UpdateBlobCheckerSettings(TDuration periodicity) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC52, "Updating BlobChecker settings",
            (OldPeriodicity, BlobCheckerPeriodicity),
            (NewPeriodicity, periodicity));
    if (periodicity == BlobCheckerPeriodicity) {
        return;
    }

    bool wasEnabled = IsBlobCheckerEnabled();
    BlobCheckerPeriodicity = periodicity;
    if (!BlobCheckerPlanner) {
        // TxLoadEverything will initialize the planner and orchestrator from this value.
        return;
    }
    if (!wasEnabled) {
        if (IsBlobCheckerEnabled()) {
            BlobCheckerPlanner->SetPeriodicity(BlobCheckerPeriodicity);
            NextAllowedBlobCheckerTimestamp = TMonotonic::Zero();
            if (BlobCheckerOrchestratorId) {
                Send(BlobCheckerOrchestratorId, new TEvBlobCheckerUpdateSettings(periodicity));
            } else {
                InitializeBlobCheckerOrchestratorActor();
            }
        } else {
            return;
        }
    } else {
        if (IsBlobCheckerEnabled()) {
            BlobCheckerPlanner->SetPeriodicity(BlobCheckerPeriodicity);
            NextAllowedBlobCheckerTimestamp = TMonotonic::Zero();
            Send(BlobCheckerOrchestratorId, new TEvBlobCheckerUpdateSettings(periodicity));
        } else {
            STLOG(PRI_NOTICE, BS_CONTROLLER, BSC51, "Suspending BlobCheckerOrchestrator actor");
            // Retain group/node locks until active workers acknowledge cancellation,
            // but do not count the disabled interval as pacing debt on re-enable.
            BlobCheckerPlanner->ResetPacing();
            NextAllowedBlobCheckerTimestamp = TMonotonic::Zero();
            if (BlobCheckerOrchestratorId) {
                Send(BlobCheckerOrchestratorId, new TEvBlobCheckerUpdateSettings(periodicity));
            }
        }
    }
}

void TBlobStorageController::DequeueCheckForGroup(TGroupId groupId, bool notifyOrchestrator) {
    if (!notifyOrchestrator) {
        if (BlobCheckerPlanner) {
            BlobCheckerPlanner->DequeueCheck(groupId);
        }
        BlobCheckerCancellationsPending.erase(groupId);
        ScrubState.SetBlobCheckerInProgress(groupId, false);
        return;
    }

    if (ScrubState.IsBlobCheckerInProgress(groupId)) {
        // An active worker owns the planner's per-node locks until its final
        // acknowledgement; otherwise another group could overlap its shutdown.
        if (BlobCheckerCancellationsPending.insert(groupId).second && BlobCheckerOrchestratorId) {
            Send(BlobCheckerOrchestratorId,
                    new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
        }
    } else if (BlobCheckerPlanner && BlobCheckerPlanner->DequeueCheck(groupId) && BlobCheckerOrchestratorId) {
        Send(BlobCheckerOrchestratorId,
                new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
    }
}

void TBlobStorageController::DeleteBlobCheckerGroup(TGroupId groupId) {
    if (ScrubState.IsBlobCheckerInProgress(groupId)) {
        // The orchestrator keeps a tombstone for the deleted group until its
        // worker acknowledges Poison. Remember that this acknowledgement is
        // for the old incarnation in case the same group id is re-created.
        BlobCheckerGroupDeletionsPending.insert(groupId);
    }
    DequeueCheckForGroup(groupId, /*notifyOrchestrator=*/true);
}

bool TBlobStorageController::IsBlobCheckerEnabled() const {
    return BlobCheckerPeriodicity != TDuration::Zero();
}

void TBlobStorageController::InitializeBlobCheckerOrchestratorActor() {
    STLOG(PRI_NOTICE, BS_CONTROLLER, BSC50, "Initializing BlobCheckerOrchestrator actor",
            (BlobCheckerGroupRecordsSize, BlobCheckerGroupRecords.size()),
            (BlobCheckerPeriodicity, BlobCheckerPeriodicity));
    BlobCheckerOrchestratorId = Register(CreateBlobCheckerOrchestratorActor(
            SelfId(), BlobCheckerGroupRecords, BlobCheckerPeriodicity,
            GetServiceCounters(AppData()->Counters, "storage_pool_stat")));
}

void TBlobStorageController::PersistBlobCheckerGroupStatus(TGroupId groupId) {
    Execute(new TTxBlobCheckerUpdateGroupStatus(groupId, this));
}

} // namespace NBsController
} // namespace NKikimr
