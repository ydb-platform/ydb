#include "impl.h"

namespace NKikimr::NBsController {

TBlobStorageController::TGroupPolicyStepResult TBlobStorageController::GetGroupPolicyStep(
        const TGroupInfo& group, const TMirror3dcGroupConfiguration& current) const {
    if (HasDataCenterPolicy) {
        std::array<bool, 3> excluded{};
        if (!DataCenterStates.empty()) {
            for (const TVSlotInfo* slot : group.VDisksInGroup) {
                if (!HostRecords->GetHostId(slot->VSlotId.NodeId)) {
                    return std::unexpected<TString>("Missing host location for group state transition");
                }
                NActorsInterconnect::TNodeLocation location;
                HostRecords->GetLocation(slot->VSlotId.NodeId).Serialize(&location, false);
                TDataCenterStates::const_iterator it = DataCenterStates.find(location.GetDataCenter());
                excluded[slot->RingIdx] |= it != DataCenterStates.end() && IsExcluded(it->second);
            }
        }
        // Return available realms first when the target swaps two data centers.
        bool pendingAvailabilityChange = false;
        for (bool takeDown : {false, true}) {
            for (ui32 realm = 0; realm < excluded.size(); ++realm) {
                if (excluded[realm] == takeDown && IsDown(current.Realms[realm]) != takeDown) {
                    pendingAvailabilityChange = true;
                    if (std::optional<TMirror3dcGroupConfiguration> next = ChangeMirror3dcRealmAvailability(current, realm, takeDown)) {
                        return next;
                    }
                }
            }
        }
        if (pendingAvailabilityChange) {
            return {}; // Wait for recovery to make the next DC transition possible.
        }
    }
    using NKikimrBlobStorage::TGroupState;
    if (group.TargetInvariant == TGroupState::INVARIANT_2) {
        if (current.State == TGroupState::DC_FAILURE_INV1) {
            return MakeMirror3dcGroupConfiguration(TGroupState::DC_FAILURE_INV2_PENDING, current.Realms);
        }
        if (current.State == TGroupState::DC_SOLO_INV1) {
            return MakeMirror3dcGroupConfiguration(TGroupState::DC_SOLO_INV2_PENDING, current.Realms);
        }
    }
    return {};
}

void TBlobStorageController::UpdateGroupStateActor(TGroupId groupId) {
    if (!GroupStateActorId) {
        return;
    }
    using TEvUpdate = NGroupSyncer::TEvGroupState::TEvUpdate;
    std::unique_ptr<TEvUpdate> message = std::make_unique<TEvUpdate>(groupId.GetRawId());
    if (const TGroupInfo* group = FindGroup(groupId); group
            && group->ErasureSpecies == TErasureType::ErasureMirror3dc
            && group->Topology->GetTotalFailRealmsNum() == 3
            && group->VDisksInGroup.size() == group->Topology->GetTotalVDisksNum()
            && !StoragePools.at(group->StoragePoolId).BridgeMode) {
        TMirror3dcGroupConfiguration current = group->GetMirror3dcConfiguration();
        TGroupPolicyStepResult step = GetGroupPolicyStep(*group, current);
        if (step) {
            NGroupSyncer::TGroupReplicationInfo& info = message->Info.emplace();
            info.Generation = group->Generation;
            info.Topology = group->Topology;
            info.States = current.Realms;
            info.GroupState = current.State;
            info.PolicyStep = step.value();
            for (const TIndirectReferable<TVSlotInfo>::TPtr& slot : group->VDisksInGroup) {
                info.VDisks.push_back({slot->GetVDiskId(),
                    MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId)});
            }
        }
    }
    Send(GroupStateActorId, message.release());
}

} // namespace NKikimr::NBsController
