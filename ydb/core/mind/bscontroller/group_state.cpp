#include "impl.h"

namespace NKikimr::NBsController {

std::expected<std::optional<TMirror3dcGroupConfiguration>, TString> TBlobStorageController::GetGroupPolicyStep(
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
                auto it = DataCenterStates.find(location.GetDataCenter());
                excluded[slot->RingIdx] |= it != DataCenterStates.end() && IsExcluded(it->second);
            }
        }
        // Return available realms first when the target swaps two data centers.
        bool pendingAvailabilityChange = false;
        for (bool takeDown : {false, true}) {
            for (ui32 realm = 0; realm < excluded.size(); ++realm) {
                if (excluded[realm] == takeDown && IsDown(current.Realms[realm]) != takeDown) {
                    pendingAvailabilityChange = true;
                    if (auto next = ChangeMirror3dcRealmAvailability(current, realm, takeDown)) {
                        return next;
                    }
                }
            }
        }
        if (pendingAvailabilityChange) {
            return {}; // Wait for recovery to make the next DC transition possible.
        }
    }
    using S = NKikimrBlobStorage::TGroupState;
    if (group.TargetInvariant == S::INVARIANT_2) {
        if (current.State == S::DC_FAILURE_INV1) {
            return MakeMirror3dcGroupConfiguration(S::DC_FAILURE_INV2_PENDING, current.Realms);
        }
        if (current.State == S::DC_SOLO_INV1) {
            return MakeMirror3dcGroupConfiguration(S::DC_SOLO_INV2_PENDING, current.Realms);
        }
    }
    return {};
}

void TBlobStorageController::UpdateGroupStateActor(TGroupId groupId) {
    if (!GroupStateActorId) {
        return;
    }
    auto message = std::make_unique<NGroupSyncer::TEvGroupState::TEvUpdate>(groupId.GetRawId());
    if (const TGroupInfo* group = FindGroup(groupId); group
            && group->ErasureSpecies == TErasureType::ErasureMirror3dc
            && group->Topology->GetTotalFailRealmsNum() == 3
            && group->VDisksInGroup.size() == group->Topology->GetTotalVDisksNum()
            && !StoragePools.at(group->StoragePoolId).BridgeMode) {
        message->Info.emplace();
        message->Info->Generation = group->Generation;
        message->Info->Topology = group->Topology;
        message->Info->States = group->FailRealmStates.value_or(NGroupSyncer::TFailRealmStates{});
        message->Info->GroupState = group->Mirror3dcState.value_or(NKikimrBlobStorage::TGroupState::NORMAL);
        auto step = GetGroupPolicyStep(*group, {*message->Info->GroupState, message->Info->States});
        if (!step) {
            message->Info.reset();
            Send(GroupStateActorId, message.release());
            return;
        }
        message->Info->PolicyStep = *step;
        for (const TIndirectReferable<TVSlotInfo>::TPtr& slot : group->VDisksInGroup) {
            message->Info->VDisks.push_back({slot->GetVDiskId(),
                MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId)});
        }
    }
    Send(GroupStateActorId, message.release());
}

} // namespace NKikimr::NBsController
