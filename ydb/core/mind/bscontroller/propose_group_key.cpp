#include "impl.h"

namespace NKikimr {
namespace NBsController {

class TBlobStorageController::TTxProposeGroupKey : public TTransactionBase<TBlobStorageController> {
protected:
    TEvBlobStorage::TEvControllerProposeGroupKey::TPtr Event;
    NKikimrProto::EReplyStatus Status = NKikimrProto::OK;
    ui32 NodeId = 0;
    TGroupId GroupId = TGroupId::Zero();
    ui32 LifeCyclePhase = 0;
    TString MainKeyId = "";
    TString EncryptedGroupKey = "";
    ui64 MainKeyVersion = 0;
    ui64 GroupKeyNonce = 0;
    bool IsAnotherTxInProgress = false;

public:
    TTxProposeGroupKey(TEvBlobStorage::TEvControllerProposeGroupKey::TPtr event, TBlobStorageController *controller)
        : TBase(controller)
        , Event(event)
    {
        const auto& proto = Event->Get()->Record;
        NodeId = proto.GetNodeId();
        GroupId = TGroupId::FromProto(&proto, &NKikimrBlobStorage::TEvControllerProposeGroupKey::GetGroupId);
        LifeCyclePhase = proto.GetLifeCyclePhase();
        MainKeyId =  proto.GetMainKeyId();
        EncryptedGroupKey = proto.GetEncryptedGroupKey();
        MainKeyVersion = proto.GetMainKeyVersion();
        GroupKeyNonce = proto.GetGroupKeyNonce();
    }

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_PROPOSE_GROUP_KEY; }

    void ReadStep() {
        const auto prevStatus = std::exchange(Status, NKikimrProto::ERROR); // assume error
        TGroupInfo *group = Self->FindGroup(GroupId);
        if (TGroupID(GroupId).ConfigurationType() != EGroupConfigurationType::Dynamic) {
            STLOG(PRI_CRIT, BS_CONTROLLER, BSCTXPGK01, "Can't propose key for non-dynamic group", (GroupId.GetRawId(), GroupId.GetRawId()));
        } else if (!group) {
            STLOG(PRI_CRIT, BS_CONTROLLER, BSCTXPGK02, "Can't read group info", (GroupId.GetRawId(), GroupId.GetRawId()));
        } else if (group->EncryptionMode.GetOrElse(0) == 0) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK03, "Group is not encrypted", (GroupId.GetRawId(), GroupId.GetRawId()));
        } else if (group->LifeCyclePhase.GetOrElse(0) != TBlobStorageGroupInfo::ELCP_INITIAL) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK04, "Group LifeCyclePhase does not match ELCP_INITIAL",
                (GroupId.GetRawId(), GroupId.GetRawId()), (LifeCyclePhase, group->LifeCyclePhase.GetOrElse(0)));
            IsAnotherTxInProgress = (group->LifeCyclePhase.GetOrElse(0) == TBlobStorageGroupInfo::ELCP_IN_TRANSITION);
        } else if (group->MainKeyVersion.GetOrElse(0) != (MainKeyVersion - 1)) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK05, "Group MainKeyVersion does not match required MainKeyVersion",
                (GroupId.GetRawId(), GroupId.GetRawId()), (MainKeyVersion, group->MainKeyVersion.GetOrElse(0)),
                (RequiredMainKeyVersion, MainKeyVersion - 1));
        } else if (EncryptedGroupKey.size() != 32 + sizeof(ui32)) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK06, "Group does not accept EncryptedGroupKey size",
                (GroupId.GetRawId(), GroupId.GetRawId()), (EncryptedGroupKeySize, EncryptedGroupKey.size()),
                (ExpectedEncryptedGroupKeySize, 32 + sizeof(ui32)));
        } else {
            Status = prevStatus; // return old status
        }
    }

    void WriteStep(TTransactionContext &txc) {
        NIceDb::TNiceDb db(txc.DB);
        // Reflect group structure in the database (pass)

        TGroupInfo *group = Self->FindGroup(GroupId);
        Y_ABORT_UNLESS(group); // the existence of this group must have been checked during ReadStep
        group->LifeCyclePhase = TBlobStorageGroupInfo::ELCP_IN_TRANSITION;
        group->MainKeyId = MainKeyId;
        group->EncryptedGroupKey = EncryptedGroupKey;
        group->GroupKeyNonce = GroupKeyNonce;
        group->MainKeyVersion = MainKeyVersion;
        db.Table<Schema::Group>().Key(GroupId.GetRawId()).Update(
            NIceDb::TUpdate<Schema::Group::LifeCyclePhase>(TBlobStorageGroupInfo::ELCP_IN_USE),
            NIceDb::TUpdate<Schema::Group::MainKeyId>(MainKeyId),
            NIceDb::TUpdate<Schema::Group::EncryptedGroupKey>(EncryptedGroupKey),
            NIceDb::TUpdate<Schema::Group::GroupKeyNonce>(GroupKeyNonce),
            NIceDb::TUpdate<Schema::Group::MainKeyVersion>(MainKeyVersion));
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXPGK07, "TTxProposeGroupKey Execute");
        if (!Self->ValidateIncomingNodeWardenEvent(*Event)) {
            Status = NKikimrProto::ERROR;
            return true;
        }
        Status = NKikimrProto::OK;
        ReadStep();
        if (Status == NKikimrProto::OK) {
            WriteStep(txc);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXPGK08, "TTxProposeGroupKey Complete");
        if (Status == NKikimrProto::OK) {
            TGroupInfo *group = Self->FindGroup(GroupId);
            Y_ABORT_UNLESS(group);
            if (group->LifeCyclePhase == TBlobStorageGroupInfo::ELCP_IN_TRANSITION) {
                group->LifeCyclePhase = TBlobStorageGroupInfo::ELCP_IN_USE;
            } else {
                STLOG(PRI_CRIT, BS_CONTROLLER, BSCTXPGK09, "TTxProposeGroupKey unexpected LifyCyclePhase",
                    (GroupId, GroupId), (LifeCyclePhase, group->LifeCyclePhase));
            }
        } else {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSCTXPGK10, "TTxProposeGroupKey error", (GroupId, GroupId),
                (Status, Status), (Request, Event->Get()->Record));
        }
        if (!IsAnotherTxInProgress) {
            // Get groupinfo for the group and send it to all whom it may concern.
            Self->NotifyNodesAwaitingKeysForGroups(GroupId.GetRawId());
        }
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerProposeGroupKey::TPtr &ev) {
    const NKikimrBlobStorage::TEvControllerProposeGroupKey& proto = ev->Get()->Record;
    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXPGK11, "Handle TEvControllerProposeGroupKey", (Request, proto));
    Y_ABORT_UNLESS(AppData());
    NodesAwaitingKeysForGroup[proto.GetGroupId()].insert(proto.GetNodeId());
    Execute(new TTxProposeGroupKey(ev, this));
}

} // NBlobStorageController
} // NKikimr
