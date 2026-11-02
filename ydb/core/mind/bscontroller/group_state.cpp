#include "impl.h"
#include "group_state.h"

namespace NKikimr::NBsController {

void TBlobStorageController::UpdateGroupStateActor(TGroupId groupId) {
    if (!GroupStateActorId) {
        return;
    }
    std::unique_ptr<TEvGroupState::TEvUpdate> message = std::make_unique<TEvGroupState::TEvUpdate>(groupId.GetRawId());
    if (const TGroupInfo* group = FindGroup(groupId); group
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

} // namespace NKikimr::NBsController
