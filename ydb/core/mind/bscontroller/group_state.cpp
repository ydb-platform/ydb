#include "impl.h"
#include "group_state.h"

namespace NKikimr::NBsController {

std::optional<TExcludedRealms> TBlobStorageController::GetTargetExcludedRealms(const TGroupInfo& group) const {
    TExcludedRealms excluded{};
    if (!DataCenterStates.empty()) {
        for (const TVSlotInfo* slot : group.VDisksInGroup) {
            if (!HostRecords->GetHostId(slot->VSlotId.NodeId)) {
                return std::nullopt;
            }
            NActorsInterconnect::TNodeLocation location;
            HostRecords->GetLocation(slot->VSlotId.NodeId).Serialize(&location, false);
            auto it = DataCenterStates.find(location.GetDataCenter());
            excluded[slot->RingIdx] |= it != DataCenterStates.end() && IsExcluded(it->second);
        }
    }
    return excluded;
}

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
        message->Info->Topology = group->Topology;
        message->Info->States = group->FailRealmStates.value_or(TFailRealmStates{});
        message->Info->GroupState = group->Mirror3dcState.value_or(NKikimrBlobStorage::TGroupState::NORMAL);
        if (HasDataCenterPolicy) {
            message->Info->TargetExcluded = GetTargetExcludedRealms(*group);
            if (!message->Info->TargetExcluded) {
                message->Info.reset();
                Send(GroupStateActorId, message.release());
                return;
            }
        }
        for (const TIndirectReferable<TVSlotInfo>::TPtr& slot : group->VDisksInGroup) {
            message->Info->VDisks.push_back({slot->GetVDiskId(),
                MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId)});
        }
    }
    Send(GroupStateActorId, message.release());
}

} // namespace NKikimr::NBsController
