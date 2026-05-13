#include "impl.h"
#include "config.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

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
        YDB_LOG_DEBUG("TTxProposeGroupKey Execute",
            {"Marker", "BSCTXPGK07"});

        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());

        if (TGroupInfo *group = State->Groups.FindForUpdate(GroupId)) {
            if (TGroupID(GroupId).ConfigurationType() != EGroupConfigurationType::Dynamic) {
                YDB_LOG_CRIT("Can't propose key for non-dynamic group",
                    {"Marker", "BSCTXPGK01"},
                    {"GroupId", GroupId});
            } else if (!group->EncryptionMode || *group->EncryptionMode == TBlobStorageGroupInfo::EEM_NONE) {
                YDB_LOG_ERROR("Group is not encrypted",
                    {"Marker", "BSCTXPGK03"},
                    {"GroupId", GroupId});
            } else if (group->LifeCyclePhase && *group->LifeCyclePhase != TBlobStorageGroupInfo::ELCP_INITIAL) {
                // this may be just a race with another proxy trying to propose key
                YDB_LOG_WARN("Group LifeCyclePhase does not match ELCP_INITIAL",
                    {"Marker", "BSCTXPGK04"},
                    {"GroupId", GroupId},
                    {"LifeCyclePhase", group->LifeCyclePhase});
            } else if (group->MainKeyVersion.GetOrElse(0) != MainKeyVersion - 1) {
                YDB_LOG_ERROR("Group MainKeyVersion does not match required MainKeyVersion",
                    {"Marker", "BSCTXPGK05"},
                    {"GroupId", GroupId},
                    {"MainKeyVersion", group->MainKeyVersion},
                    {"RequiredMainKeyVersion", MainKeyVersion - 1});
            } else if (EncryptedGroupKey.size() != 32 + sizeof(ui32)) {
                YDB_LOG_ERROR("Group does not accept EncryptedGroupKey size",
                    {"Marker", "BSCTXPGK06"},
                    {"GroupId", GroupId},
                    {"EncryptedGroupKeySize", EncryptedGroupKey.size()},
                    {"ExpectedEncryptedGroupKeySize", 32 + sizeof(ui32)});
            } else {
                group->LifeCyclePhase = TBlobStorageGroupInfo::ELCP_IN_USE;
                group->MainKeyId = MainKeyId;
                group->EncryptedGroupKey = EncryptedGroupKey;
                group->GroupKeyNonce = GroupKeyNonce;
                group->MainKeyVersion = MainKeyVersion;
                State->GroupContentChanged.insert(GroupId);
            }
        } else if (!group) {
            YDB_LOG_ERROR("Can't find group for key proposition",
                {"Marker", "BSCTXPGK02"},
                {"GroupId", GroupId});
        }

        Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc);

        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxProposeGroupKey Complete",
            {"Marker", "BSCTXPGK08"});

        if (State) {
            State->ApplyConfigUpdates();
        }
    }
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerProposeGroupKey::TPtr &ev) {
    const NKikimrBlobStorage::TEvControllerProposeGroupKey& proto = ev->Get()->Record;
    YDB_LOG_DEBUG("Handle TEvControllerProposeGroupKey",
        {"Marker", "BSCTXPGK11"},
        {"Request", proto});
    Y_ABORT_UNLESS(AppData());
    Execute(new TTxProposeGroupKey(ev, this));
}

} // NBlobStorageController
} // NKikimr
