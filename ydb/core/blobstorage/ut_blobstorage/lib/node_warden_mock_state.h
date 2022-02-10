#pragma once

#include "node_warden_mock.h"

struct TNodeWardenMockActor::TGroupState {
    TIntrusivePtr<TBlobStorageGroupInfo> Info; // group info
    std::set<TVDiskState*> VDisksOfGroup; // vdisks of this group on this node

    TGroupState(TIntrusivePtr<TBlobStorageGroupInfo> info)
        : Info(std::move(info))
    {}

    void UpdateGroup(TIntrusivePtr<TBlobStorageGroupInfo> info);
    void AddVDisk(TVDiskState *vdisk);
    void EraseVDisk(TVDiskState *vdisk);
};

struct TNodeWardenMockActor::TVDiskState {
    const TVDiskID VDiskId; // VDiskId does not contain current generation!
    ui32 Generation = 0; // actual generation for this VDiskId
    const TVSlotId VSlotId; // location of the disk
    TGroupState* const Group; // group containing this VDisk
    TPDiskState* const PDisk; // PDisk containing this VDisk
    const ui64 TargetDataSize = 0;
    NKikimrBlobStorage::EVDiskStatus Status = NKikimrBlobStorage::EVDiskStatus::INIT_PENDING;
    ui64 AllocatedSize = 0;
    bool DonorMode = false;
    bool RequireDonorMode = false;
    IActor *Actor = nullptr;

    TVDiskState(TVDiskID vdiskId, TVSlotId vslotId, TGroupState *group, TPDiskState *pdisk, ui64 targetDataSize)
        : VDiskId(vdiskId)
        , Generation(vdiskId.GroupGeneration)
        , VSlotId(vslotId)
        , Group(group)
        , PDisk(pdisk)
        , TargetDataSize(targetDataSize)
    {}

    TVDiskID GetVDiskId() const {
        return TVDiskID(VDiskId.GroupID, Generation, VDiskId);
    }

    void FillInVDiskStatus(NKikimrBlobStorage::TVDiskStatus *pb);
};

struct TNodeWardenMockActor::TPDiskState {
    const ui64 PDiskGuid;
    const TString Path;
    const ui64 Size;
    std::unordered_map<ui32, TVDiskState*> VSlotsOnPDisk;

    TPDiskState(ui64 pdiskGuid, TString path, ui64 size)
        : PDiskGuid(pdiskGuid)
        , Path(std::move(path))
        , Size(size)
    {}

    void AddVSlot(TVDiskState *vdisk);
    void EraseVSlot(ui32 vslotId);
    ui64 GetAllocatedSize() const;
};
