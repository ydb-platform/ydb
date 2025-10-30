#include "impl.h"
#include "blob_checker_actors.h"

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
            // nonexistent group, probably deleted due to race, nothing to do
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
    TGroupId groupId = ev->Get()->GroupId;
    const auto it = BlobCheckerGroupRecords.find(groupId);
    if (it != BlobCheckerGroupRecords.end()) {
        it->second = ev->Get()->SerializedState;
        if (ev->Get()->FinishScan) {
            DequeueCheckForGroup(groupId, /*notifyOrchestrator=*/false);
        }
        Execute(new TTxBlobCheckerUpdateGroupStatus(groupId, this));
    }
}

void TBlobStorageController::Handle(const TEvBlobCheckerPlanCheck::TPtr& ev) {
    TGroupId groupId = ev->Get()->GroupId;
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC53, "Handle TEvBlobCheckerPlanCheck",
            (GroupId, groupId));
    if (!BlobCheckerPlanner) {
        Send(BlobCheckerOrchestratorId, new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
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
        if (status.OperatingStatus == NKikimrBlobStorage::TGroupStatus::FULL) {
            BlobCheckerPlanner->EnqueueCheck(groupInfo);
            UpdateBlobCheckerState();
        }
    } else if (const TStaticGroupInfo* staticGroupInfo = staticFinder(groupId)) {
        TGroupInfo::TGroupStatus status = staticGroupInfo->GetStatus(staticFinder, BridgeInfo.get());
        if (status.OperatingStatus == NKikimrBlobStorage::TGroupStatus::FULL) {
            BlobCheckerPlanner->EnqueueCheck(staticGroupInfo->Info.get());
            UpdateBlobCheckerState();
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////
// Auxiliary method implementations
/////////////////////////////////////////////////////////////////////////////////////

void TBlobStorageController::UpdateBlobCheckerState() {
    if (!BlobCheckerPlanner) {
        return;
    }

    TMonotonic now = TActivationContext::Monotonic();
    if (now >= NextAllowedBlobCheckerTimestamp) {
        NextAllowedBlobCheckerTimestamp = BlobCheckerPlanner->GetNextAllowedCheckTimestamp(now);

        const std::optional<TGroupId> groupToScan = BlobCheckerPlanner->ObtainNextGroupToCheck();
        if (groupToScan) {
            if (TGroupInfo* groupInfo = FindGroup(*groupToScan)) {
                groupInfo->IsCheckInProgress = true;
                ScrubState.UpdateGroupState(groupInfo);
            }
            Send(BlobCheckerOrchestratorId,
                    new TEvBlobCheckerDecision(*groupToScan, NKikimrProto::OK));
        }
    }
}


void TBlobStorageController::UpdateBlobCheckerSettings(TDuration periodicity) {
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSC52, "Updating BlobChecker settings",
            (OldPeriodicity, BlobCheckerPeriodicity),
            (NewPeriodicity, periodicity));
    if (!BlobCheckerPlanner) {
        // if BlobCheckerPlanner is uninitialized, then TxLoadEverything hasn't finished yet
        return;
    }

    bool wasEnabled = IsBlobCheckerEnabled();
    BlobCheckerPeriodicity = periodicity;
    if (!wasEnabled) {
        if (IsBlobCheckerEnabled()) {
            BlobCheckerPlanner->SetPeriodicity(BlobCheckerPeriodicity);
            InitializeBlobCheckerOrchestratorActor();
        } else {
            return;
        }
    } else {
        if (IsBlobCheckerEnabled()) {
            BlobCheckerPlanner->SetPeriodicity(BlobCheckerPeriodicity);
            Send(BlobCheckerOrchestratorId, new TEvBlobCheckerUpdateSettings(periodicity));
        } else {
            STLOG(PRI_NOTICE, BS_CONTROLLER, BSC51, "Terminating BlobCheckerOrchestrator actor");
            BlobCheckerPlanner->ResetState();
            Send(BlobCheckerOrchestratorId, new TEvents::TEvPoisonPill);
            BlobCheckerOrchestratorId = TActorId{};
        }
    }
}

void TBlobStorageController::DequeueCheckForGroup(TGroupId groupId, bool notifyOrchestrator) {
    bool scanWasPlanned = BlobCheckerPlanner->DequeueCheck(groupId);
    if (scanWasPlanned) {
        if (TGroupInfo* groupInfo = FindGroup(groupId)) {
            groupInfo->IsCheckInProgress = false;
            ScrubState.UpdateGroupState(groupInfo);
        }
        if (notifyOrchestrator) {
            Send(BlobCheckerOrchestratorId,
                    new TEvBlobCheckerDecision(groupId, NKikimrProto::ERROR));
        }
    }
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

} // namespace NBsController
} // namespace NKikimr
