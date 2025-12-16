#include "impl.h"
#include "config.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxProposeGroupKey : public TTransactionBase<TBlobStorageController> {
protected:
    std::optional<TConfigState> State;
    TGroupId GroupId;
    ui32 LifeCyclePhase;
    TString MainKeyId;
    TString EncryptedGroupKey;
    ui64 MainKeyVersion;
    ui64 GroupKeyNonce;

public:
    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_PROPOSE_GROUP_KEY; }

    TTxProposeGroupKey(TEvBlobStorage::TEvControllerProposeGroupKey::TPtr event, TBlobStorageController *controller)
        : TBase(controller)
    {
        const auto& proto = event->Get()->Record;
        GroupId = TGroupId::FromProto(&proto, &NKikimrBlobStorage::TEvControllerProposeGroupKey::GetGroupId);
        LifeCyclePhase = proto.GetLifeCyclePhase();
        MainKeyId =  proto.GetMainKeyId();
        EncryptedGroupKey = proto.GetEncryptedGroupKey();
        MainKeyVersion = proto.GetMainKeyVersion();
        GroupKeyNonce = proto.GetGroupKeyNonce();
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXPGK07, "TTxProposeGroupKey Execute");

        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());

        if (TGroupInfo *group = State->Groups.FindForUpdate(GroupId)) {
            if (TGroupID(GroupId).ConfigurationType() != EGroupConfigurationType::Dynamic) {
                STLOG(PRI_CRIT, BS_CONTROLLER, BSCTXPGK01, "Can't propose key for non-dynamic group", (GroupId, GroupId));
            } else if (!group->EncryptionMode || *group->EncryptionMode == TBlobStorageGroupInfo::EEM_NONE) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK03, "Group is not encrypted", (GroupId, GroupId));
            } else if (group->LifeCyclePhase && *group->LifeCyclePhase != TBlobStorageGroupInfo::ELCP_INITIAL) {
                // this may be just a race with another proxy trying to propose key
                STLOG(PRI_WARN, BS_CONTROLLER, BSCTXPGK04, "Group LifeCyclePhase does not match ELCP_INITIAL",
                    (GroupId, GroupId), (LifeCyclePhase, group->LifeCyclePhase));
            } else if (group->MainKeyVersion.GetOrElse(0) != MainKeyVersion - 1) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK05, "Group MainKeyVersion does not match required MainKeyVersion",
                    (GroupId, GroupId), (MainKeyVersion, group->MainKeyVersion),
                    (RequiredMainKeyVersion, MainKeyVersion - 1));
            } else if (EncryptedGroupKey.size() != 32 + sizeof(ui32)) {
                STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK06, "Group does not accept EncryptedGroupKey size",
                    (GroupId, GroupId), (EncryptedGroupKeySize, EncryptedGroupKey.size()),
                    (ExpectedEncryptedGroupKeySize, 32 + sizeof(ui32)));
            } else {
                group->LifeCyclePhase = TBlobStorageGroupInfo::ELCP_IN_USE;
                group->MainKeyId = MainKeyId;
                group->EncryptedGroupKey = EncryptedGroupKey;
                group->GroupKeyNonce = GroupKeyNonce;
                group->MainKeyVersion = MainKeyVersion;
                State->GroupContentChanged.insert(GroupId);
            }
        } else if (!group) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK02, "Can't find group for key proposition", (GroupId, GroupId));
        }

        if (TString error; State->Changed() && !Self->CommitConfigUpdates(*State, true, true, true, txc, &error)) {
            State->Rollback();
            State.reset();
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXPGK08, "TTxProposeGroupKey Complete");

        if (State) {
            State->ApplyConfigUpdates();
        }
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerProposeGroupKey::TPtr &ev) {
    const NKikimrBlobStorage::TEvControllerProposeGroupKey& proto = ev->Get()->Record;
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXPGK11, "Handle TEvControllerProposeGroupKey", (Request, proto));
    Y_ABORT_UNLESS(AppData());
    Execute(new TTxProposeGroupKey(ev, this));
}

} // NBlobStorageController
} // NKikimr
