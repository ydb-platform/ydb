#include "impl.h"
#include "config.h"
#include "group_state.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

void TBlobStorageController::UpdateGroupStateActor(TGroupId groupId) {
    if (!GroupStateActorId) {
        return;
    }
    std::unique_ptr<TEvGroupState::TEvUpdate> message = std::make_unique<TEvGroupState::TEvUpdate>(groupId.GetRawId());
    if (const TGroupInfo* group = FindGroup(groupId); group && group->HasExplicitFailRealmStates
            && group->FailRealmStates && group->ErasureSpecies == TErasureType::ErasureMirror3dc
            && group->Topology->GetTotalFailRealmsNum() == 3
            && group->VDisksInGroup.size() == group->Topology->GetTotalVDisksNum()
            && !StoragePools.at(group->StoragePoolId).BridgeMode) {
        message->Info.emplace();
        message->Info->Generation = group->Generation;
        message->Info->States = *group->FailRealmStates;
        for (const TIndirectReferable<TVSlotInfo>::TPtr& slot : group->VDisksInGroup) {
            message->Info->VDisks.push_back({slot->GetVDiskId(),
                MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId)});
        }
    }
    Send(GroupStateActorId, message.release());
}

class TBlobStorageController::TTxUpdateGroupState : public TTransactionBase<TBlobStorageController> {
    const TEvGroupState::TEvReplicationDone Message;
    std::optional<TConfigState> State;

public:
    TTxUpdateGroupState(TBlobStorageController* controller, const TEvGroupState::TEvReplicationDone& message)
        : TBase(controller), Message(message.GroupId, message.Generation, message.States) {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_CONFIG_CMD; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const TGroupId groupId = TGroupId::FromValue(Message.GroupId);
        const TGroupInfo* group = Self->FindGroup(groupId);
        if (!group || group->Generation != Message.Generation || !group->HasExplicitFailRealmStates
                || !group->FailRealmStates) {
            return true;
        }
        TFailRealmStates states = *group->FailRealmStates;
        for (ui32 realm = 0; realm < 3; ++realm) {
            if (states[realm] != Message.States[realm]) {
                const std::optional<EMirror3dcFailRealmState> target = GetReplicatedRealmState(states, realm);
                if (!target || *target != Message.States[realm]) {
                    return true;
                }
            }
        }
        if (states == Message.States) {
            return true;
        }
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());
        TGroupInfo* updated = State->Groups.FindForUpdate(groupId);
        updated->FailRealmStates = Message.States;
        State->GroupContentChanged.insert(groupId);
        const TConfigTxFlags flags{
            .SuppressFailModelChecking = true,
            .SuppressDegradedGroupsChecking = true,
            .SuppressDisintegratedGroupsChecking = true,
        };
        if (const std::optional<TString> error = Self->ValidateAndCommitConfigUpdate(State, flags, txc)) {
            YDB_LOG_ERROR("Could not promote replicated fail realm", {"groupId", Message.GroupId}, {"error", *error});
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
            State.reset();
        } else {
            // Successful commits notify the satellite through ApplyConfigUpdates.
            Self->UpdateGroupStateActor(TGroupId::FromValue(Message.GroupId));
        }
    }
};

void TBlobStorageController::Handle(TEvGroupState::TEvReplicationDone::TPtr& ev) {
    if (ev->Sender == GroupStateActorId) {
        Execute(new TTxUpdateGroupState(this, *ev->Get()));
    }
}

} // namespace NKikimr::NBsController
