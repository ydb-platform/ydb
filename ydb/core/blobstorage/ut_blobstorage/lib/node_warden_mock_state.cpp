#include "node_warden_mock.h"
#include "node_warden_mock_state.h"

namespace NKikimr {
namespace NPDisk {
extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

TNodeWardenMockActor::TNodeWardenMockActor(TSetup::TPtr setup)
    : Setup(std::move(setup))
{}

void TNodeWardenMockActor::Bootstrap() {
    Become(&TThis::StateFunc);
    STLOG(PRI_INFO, BS_NODE, NWM01, "starting");
    Connect();
}

void TNodeWardenMockActor::OnConnected() {
    SendRegisterNode();
}

TNodeWardenMockActor::TPDiskState *TNodeWardenMockActor::GetPDisk(TPDiskId pdiskId) {
    const auto it = PDisks.find(pdiskId);
    return it != PDisks.end() ? it->second.get() : nullptr;
}

TNodeWardenMockActor::TVDiskState *TNodeWardenMockActor::GetVDisk(TVSlotId vslotId) {
    const auto it = VDisks.find(vslotId);
    return it != VDisks.end() ? it->second.get() : nullptr;
}

TNodeWardenMockActor::TGroupState *TNodeWardenMockActor::GetGroup(ui32 groupId) {
    const auto it = Groups.find(groupId);
    return it != Groups.end() ? it->second.get() : nullptr;
}

void TNodeWardenMockActor::TGroupState::UpdateGroup(TIntrusivePtr<TBlobStorageGroupInfo> info) {
    for (TVDiskState *vdisk : VDisksOfGroup) {
        if (Info->GetActorId(vdisk->VDiskId) == info->GetActorId(vdisk->VDiskId)) {
            vdisk->Generation = info->GroupGeneration; // just update generation
        } else {
            STLOG(PRI_INFO, BS_NODE, NWM11, "UpdateGroup", (VDiskId, vdisk->VDiskId),
                (PrevActorId, Info->GetActorId(vdisk->VDiskId)),
                (CurActorId, info->GetActorId(vdisk->VDiskId)));
            vdisk->RequireDonorMode = true;
        }
    }
    Info = std::move(info);
}

void TNodeWardenMockActor::TGroupState::AddVDisk(TVDiskState *vdisk) {
    auto&& [it, inserted] = VDisksOfGroup.insert(vdisk);
    UNIT_ASSERT(inserted);
}

void TNodeWardenMockActor::TGroupState::EraseVDisk(TVDiskState *vdisk) {
    const size_t num = VDisksOfGroup.erase(vdisk);
    UNIT_ASSERT(num);
}

void TNodeWardenMockActor::TVDiskState::FillInVDiskStatus(NKikimrBlobStorage::TVDiskStatus *pb) {
    VDiskIDFromVDiskID(GetVDiskId(), pb->MutableVDiskId());
    pb->SetNodeId(VSlotId.NodeId);
    pb->SetPDiskId(VSlotId.PDiskId);
    pb->SetVSlotId(VSlotId.VSlotId);
    pb->SetPDiskGuid(PDisk->PDiskGuid);
    pb->SetStatus(Status);
}

void TNodeWardenMockActor::TPDiskState::AddVSlot(TVDiskState *vdisk) {
    auto&& [it, inserted] = VSlotsOnPDisk.emplace(vdisk->VSlotId.VSlotId, vdisk);
    UNIT_ASSERT(inserted);
}

void TNodeWardenMockActor::TPDiskState::EraseVSlot(ui32 vslotId) {
    const size_t num = VSlotsOnPDisk.erase(vslotId);
    UNIT_ASSERT(num);
}

ui64 TNodeWardenMockActor::TPDiskState::GetAllocatedSize() const {
    ui64 res = 0;
    for (const auto& [vslotId, vdisk] : VSlotsOnPDisk) {
        res += vdisk->AllocatedSize;
    }
    return res;
}

template<>
void Out<TNodeWardenMockActor::TPDiskId>(IOutputStream& s, const TNodeWardenMockActor::TPDiskId& x) {
    s << x.ToString();
}

template<>
void Out<TNodeWardenMockActor::TVSlotId>(IOutputStream& s, const TNodeWardenMockActor::TVSlotId& x) {
    s << x.ToString();
}
