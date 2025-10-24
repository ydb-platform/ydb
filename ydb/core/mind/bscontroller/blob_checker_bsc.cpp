#include "impl.h"
#include "blob_checker.h"

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
        const auto it = Self->BlobCheckerSerializedGroupStatuses.find(GroupId);
        if (it == Self->BlobCheckerSerializedGroupStatuses.end()) {
            // race occured, group was deleted, nothing to do
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

void TBlobStorageController::Handle(const TEvBlobCheckerUpdateSettings::TPtr& ev) {
    TDuration prev = std::exchange(BlobCheckerPeriodicity, ev->Get()->Periodicity);
    if (prev == TDuration::Zero()) {
        if (BlobCheckerPeriodicity == TDuration::Zero()) {
            // BlobChecker is disabled already
            return;
        } else {
            BlobCheckerPlanner.SetPeriodicity(BlobCheckerPeriodicity);
            BlobCheckerOrchestratorId = Register(new CreateBlobCheckerOrchestratorActor(
                    SelfId(), BlobCheckerSerializedGroupStatuses, BlobCheckerPeriodicity));
        }
    } else {
        if (BlobCheckerPeriodicity == TDuration::Zero()) {
            BlobCheckerPlanner.ResetState();
            Send(BlobCheckerOrchestratorId, new TEvents::TEvPoisonPill);
            BlobCheckerOrchestratorId = TActorId{};
        } else {
            BlobCheckerPlanner.SetPeriodicity(BlobCheckerPeriodicity);
            Send(ev->Forward(BlobCheckerOrchestratorId));
        }
    }
}

void TBlobStorageController::Handle(const TEvBlobCheckerUpdateGroupStatus::TPtr& ev) {
    TGroupId groupId = ev->Get()->GroupId;
    const auto it = BlobCheckerSerializedGroupStatuses.find(groupId);
    if (it != BlobCheckerSerializedGroupStatuses.end()) {
        it->second = ev->Get()->SerializedState;
        if (ev->Get()->FinishScan) {
            // if BlobCheckerPlanner is disabled DequeueCheck() will do nothing
            BlobCheckerPlanner.DequeueCheck(groupId);
        }

        Execute(new TTxBlobCheckerUpdateGroupStatus(groupId, this));
    }
}

void TBlobStorageController::Handle(const TEvBlobCheckerPlanCheck::TPtr& ev) {
    TGroupId groupId = ev->Get()->GroupId;
    TGroupInfo::TGroupFinder finder = [this](TGroupId groupId) { return FindGroup(groupId); };
    TGroupInfo::TStaticTGroupFinder staticFinder = [this](TGroupId groupId) {
        const auto it = StaticGroups.find(groupId);
        if (it == StaticGroups.end()) {
            return nullptr;
        }
        return &it->second;
    };

    if (const TGroupInfo* groupInfo = finder(groupId)) {
        TGroupInfo::TGroupStatus status = groupInfo->GetStatus(finder, BridgeInfo.get());
        if (status.OperatingStatus == NKikimrBlobStorage::TGroupStatus::FULL) {
            BlobCheckerPlanner.EnqueueCheck(groupInfo);
        }
    } else if (const TStaticGroupInfo* staticGroupInfo = staticFinder(groupId)) {
        TGroupInfo::TGroupStatus status = staticGroupInfo->GetStatus(staticFinder, BridgeInfo.get());
        if (status.OperatingStatus == NKikimrBlobStorage::TGroupStatus::FULL) {
            BlobCheckerPlanner.EnqueueCheck(staticGroupInfo->Info.get());
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////
// Auxiliary method implementations
/////////////////////////////////////////////////////////////////////////////////////

void TBlobStorageController::UpdateBlobCheckerState() {
    TMonotonic now = TActivationContext::Monotonic();
    if (now >= NextAllowedBlobCheckerTimestamp) {
        NextAllowedBlobCheckerTimestamp = BlobCheckerPlanner.GetNextAllowedCheckTimestamp(now);

        const std::optional<TGroupId> groupToScan = BlobCheckerPlanner.ObtainNextGroupToCheck();
        if (groupToScan) {
            Send(BlobCheckerOrchestratorId,
                    new TEvBlobCheckerDecision(*groupToScan, NKikimrProto::OK));
        }
    }
}

void TBlobStorageController::DequeueCheckForGroup(TGroupId groupId) {
    bool scanWasPlanned = BlobCheckerPlanner.DequeueCheck(groupId);
    if (scanWasPlanned) {
        NextAllowedBlobCheckerTimestamp = BlobCheckerPlanner.GetNextAllowedCheckTimestamp(now);
        Send(BlobCheckerOrchestratorId,
                new TEvBlobCheckerDecision(*groupToScan, NKikimrProto::ERROR));
    }
}

} // namespace NBsController

} // namespace NKikimr
