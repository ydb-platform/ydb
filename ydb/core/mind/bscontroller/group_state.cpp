#include "impl.h"
#include "group_state.h"

namespace NKikimr::NBsController {

void TBlobStorageController::UpdateGroupStateActor(TGroupId groupId) {
    if (!GroupStateActorId) {
        return;
    }
    std::unique_ptr<TEvGroupState::TEvUpdate> message = std::make_unique<TEvGroupState::TEvUpdate>(groupId.GetRawId());
    if (const TGroupInfo* group = FindGroup(groupId); group
            && group->ErasureSpecies == TErasureType::ErasureMirror3dc
            && group->Topology->GetTotalFailRealmsNum() == 3
            && group->VDisksInGroup.size() == group->Topology->GetTotalVDisksNum()
            && !StoragePools.at(group->StoragePoolId).BridgeMode) {
        message->Info.emplace();
        message->Info->Generation = group->Generation;
        message->Info->States = group->FailRealmStates.value_or(TFailRealmStates{});
        for (const TIndirectReferable<TVSlotInfo>::TPtr& slot : group->VDisksInGroup) {
            message->Info->VDisks.push_back({slot->GetVDiskId(),
                MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId), slot->ConfigObservationEpoch});
        }
    }
    std::unique_ptr<TEvGroupState::TEvUpdate> configMessage = std::make_unique<TEvGroupState::TEvUpdate>(groupId.GetRawId());
    configMessage->Info = message->Info;
    Send(GroupConfigActorId, configMessage.release());
    Send(GroupStateActorId, message.release());
}

void TBlobStorageController::ResetGroupConfigObservations(TNodeId nodeId) {
    THashSet<TGroupId> groups;
    const TVSlotId startingId(nodeId, 0, 0);
    for (auto it = VSlots.lower_bound(startingId); it != VSlots.end() && it->first.NodeId == nodeId; ++it) {
        it->second->ConfirmedGroupGeneration = 0;
        ++it->second->ConfigObservationEpoch;
        if (it->second->Group) {
            groups.insert(it->second->GroupId);
        }
    }
    for (TGroupId groupId : groups) {
        UpdateGroupStateActor(groupId);
    }
}

void TBlobStorageController::Handle(TEvGroupState::TEvConfigObserved::TPtr& ev) {
    if (ev->Sender != GroupConfigActorId) {
        return;
    }
    const TEvGroupState::TEvConfigObserved& message = *ev->Get();
    TGroupInfo* group = FindGroup(TGroupId::FromValue(message.GroupId));
    if (!group || group->Generation != message.RequestedGeneration
            || message.VDiskId.GroupGeneration > group->Generation) {
        return;
    }
    for (const TIndirectReferable<TVSlotInfo>::TPtr& slot : group->VDisksInGroup) {
        const TActorId serviceId = MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId);
        if (serviceId == message.ServiceId && message.VDiskId.SameExceptGeneration(slot->GetVDiskId())
                && slot->ConfigObservationEpoch == message.ObservationEpoch) {
            FindVSlot(slot->VSlotId)->ConfirmedGroupGeneration = message.VDiskId.GroupGeneration;
            return;
        }
    }
}

} // namespace NKikimr::NBsController
