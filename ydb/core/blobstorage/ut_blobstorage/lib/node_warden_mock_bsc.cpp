#include "node_warden_mock.h"
#include "node_warden_mock_state.h"
#include "node_warden_mock_vdisk.h"

void TNodeWardenMockActor::SendRegisterNode() {
    Y_ABORT_UNLESS(PipeId);

    TVector<ui32> startedDynamicGroups, groupGenerations;
    for (const auto& [groupId, group] : Groups) {
        startedDynamicGroups.push_back(group->Info->GroupID.GetRawId());
        groupGenerations.push_back(group->Info->GroupGeneration);
    }

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerRegisterNode>(SelfId().NodeId(), startedDynamicGroups,
        groupGenerations, TVector<NPDisk::TDriveData>());

    auto& record = ev->Record;
    for (const auto& [vslotId, vdisk] : VDisks) {
        vdisk->FillInVDiskStatus(record.AddVDiskStatus());
    }

    NTabletPipe::SendData(SelfId(), PipeId, ev.release());
}

void TNodeWardenMockActor::SendUpdateDiskStatus() {
    Y_ABORT_UNLESS(PipeId);

    auto ev = std::make_unique<TEvBlobStorage::TEvControllerUpdateDiskStatus>();

    auto& record = ev->Record;
    for (const auto& [vslotId, vdisk] : VDisks) {
        vdisk->FillInVDiskStatus(record.AddVDiskStatus());
    }

    for (const auto& [pdiskId, pdisk] : PDisks) {
        auto *m = record.AddPDisksMetrics();
        m->SetPDiskId(pdiskId.PDiskId);
        m->SetTotalSize(pdisk->Size);
        m->SetAvailableSize(pdisk->Size - pdisk->GetAllocatedSize());
    }

    for (const auto& [vslotId, vdisk] : VDisks) {
        auto *m = record.AddVDisksMetrics();
        VDiskIDFromVDiskID(vdisk->GetVDiskId(), m->MutableVDiskId());
        m->SetAllocatedSize(vdisk->AllocatedSize);
        m->SetAvailableSize(vdisk->PDisk->Size - vdisk->PDisk->GetAllocatedSize());
    }

    NTabletPipe::SendData(SelfId(), PipeId, ev.release());
}

void TNodeWardenMockActor::SendUpdateVDiskStatus(TVDiskState *vdisk) {
    if (IsPipeConnected) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerUpdateDiskStatus>();

        auto& record = ev->Record;
        vdisk->FillInVDiskStatus(record.AddVDiskStatus());

        {
            auto *m = record.AddPDisksMetrics();
            m->SetPDiskId(vdisk->VSlotId.PDiskId);
            m->SetTotalSize(vdisk->PDisk->Size);
            m->SetAvailableSize(vdisk->PDisk->Size - vdisk->PDisk->GetAllocatedSize());
        }

        {
            auto *m = record.AddVDisksMetrics();
            VDiskIDFromVDiskID(vdisk->GetVDiskId(), m->MutableVDiskId());
            m->SetAllocatedSize(vdisk->AllocatedSize);
            m->SetAvailableSize(vdisk->PDisk->Size - vdisk->PDisk->GetAllocatedSize());
        }

        NTabletPipe::SendData(SelfId(), PipeId, ev.release());
    }
}

void TNodeWardenMockActor::Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev) {
    auto& record = ev->Get()->Record;

    const auto& services = record.GetServiceSet();

    STLOG(PRI_INFO, BS_NODE, NWM10, "TEvControllerNodeServiceSetUpdate", (Record, record));

    for (const auto& group : services.GetGroups()) {
        auto info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr); // TODO(alexvru): group encryption?
        auto&& [it, inserted] = Groups.try_emplace(group.GetGroupID(), std::make_unique<TGroupState>(info));
        if (!inserted && it->second->Info->GroupGeneration < info->GroupGeneration) {
            it->second->UpdateGroup(info);
        }
    }

    std::unordered_set<TPDiskId, TPDiskId::THash> pdiskIds, pdiskIdsToRemove;
    for (const auto& [pdiskId, pdiskState] : PDisks) {
        pdiskIds.insert(pdiskId);
    }
    for (const auto& pdisk : services.GetPDisks()) {
        const TPDiskId pdiskId(pdisk.GetNodeID(), pdisk.GetPDiskID());
        pdiskIds.erase(pdiskId);

        STLOG(PRI_DEBUG, BS_NODE, NWM04, "PDisk", (Comprehensive, record.GetComprehensive()), (PDiskId, pdiskId),
            (EntityStatus, pdisk.GetEntityStatus()), (Path, pdisk.GetPath()), (PDiskGuid, pdisk.GetPDiskGuid()));

        switch (pdisk.GetEntityStatus()) {
            case NKikimrBlobStorage::EEntityStatus::INITIAL:
            case NKikimrBlobStorage::EEntityStatus::CREATE: {
                const auto setupIt = Setup->PDisks.find(std::make_tuple(pdiskId.NodeId, pdisk.GetPath()));
                Y_ABORT_UNLESS(setupIt != Setup->PDisks.end());
                const TSetup::TPDiskInfo& info = setupIt->second;

                auto&& [it, inserted] = PDisks.try_emplace(pdiskId, std::make_unique<TPDiskState>(pdisk.GetPDiskGuid(),
                    pdisk.GetPath(), info.Size));
                if (!inserted) {
                    UNIT_ASSERT_EQUAL(it->second->PDiskGuid, pdisk.GetPDiskGuid());
                    UNIT_ASSERT_EQUAL(it->second->Path, pdisk.GetPath());
                }
                break;
            }

            case NKikimrBlobStorage::EEntityStatus::DESTROY:
                pdiskIdsToRemove.insert(pdiskId);
                break;
            case NKikimrBlobStorage::EEntityStatus::RESTART:
                break;
        }
    }

    std::unordered_set<TVSlotId, TVSlotId::THash> vslotIds, vslotIdsToRemove;
    for (const auto& [vslotId, vdiskState] : VDisks) {
        vslotIds.insert(vslotId);
    }
    for (const auto& vdisk : services.GetVDisks()) {
        const auto& loc = vdisk.GetVDiskLocation();
        TVSlotId vslotId(loc.GetNodeID(), loc.GetPDiskID(), loc.GetVDiskSlotID());
        vslotIds.erase(vslotId);

        const TVDiskID& vdiskId = VDiskIDFromVDiskID(vdisk.GetVDiskID());

        STLOG(PRI_DEBUG, BS_NODE, NWM05, "VDisk", (Comprehensive, record.GetComprehensive()), (VSlotId, vslotId),
            (EntityStatus, vdisk.GetEntityStatus()), (VDiskId, vdiskId), (DoDestroy, vdisk.GetDoDestroy()),
            (DoWipe, vdisk.GetDoWipe()), (DonorMode, vdisk.HasDonorMode()));

        TVDiskState *vdiskp = GetVDisk(vslotId);

        if (vdisk.HasDonorMode()) { // switch already operating disk to donor mode
            UNIT_ASSERT(vdiskp);
            vdiskp->DonorMode = true;
        } else if (!vdisk.GetDoDestroy() && vdiskp) {
            UNIT_ASSERT(!vdiskp->DonorMode); // do not allow donor mode reverts
        }

        TGroupState *group = GetGroup(vdiskId.GroupID.GetRawId());
        UNIT_ASSERT(group);

        TPDiskState *pdisk = GetPDisk(vslotId);
        UNIT_ASSERT(pdisk);
        UNIT_ASSERT_VALUES_EQUAL(loc.GetPDiskGuid(), pdisk->PDiskGuid);

        if (vdisk.GetDoDestroy() || vdisk.GetDoWipe()) {
            if (vdiskp) {
                UNIT_ASSERT(vdiskp->Actor);
                TAutoPtr<IEventHandle> ev = new IEventHandle(TEvents::TSystem::Poison, 0, {}, {}, nullptr, 0);
                InvokeOtherActor(*vdiskp->Actor, &IActor::Receive, ev);
                UNIT_ASSERT(!vdiskp->Actor);
            }
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeReport>(SelfId().NodeId());
            auto& record = ev->Record;
            auto *report = record.AddVDiskReports();
            report->MutableVSlotId()->SetNodeId(vslotId.NodeId);
            report->MutableVSlotId()->SetPDiskId(vslotId.PDiskId);
            report->MutableVSlotId()->SetVSlotId(vslotId.VSlotId);
            report->MutableVDiskId()->CopyFrom(vdisk.GetVDiskID());
            if (vdisk.GetDoDestroy()) {
                report->SetPhase(NKikimrBlobStorage::TEvControllerNodeReport::DESTROYED);
            } else if (vdisk.GetDoWipe()) {
                report->SetPhase(NKikimrBlobStorage::TEvControllerNodeReport::WIPED);
            }
            UNIT_ASSERT(PipeId);
            NTabletPipe::SendData(SelfId(), PipeId, ev.release());
        }

        if (vdisk.GetDoDestroy()) {
            vslotIdsToRemove.insert(vslotId);
        } else if (vdisk.GetDoWipe() && vdiskp) {
            vdiskp->AllocatedSize = 0;
            vdiskp->Status = NKikimrBlobStorage::EVDiskStatus::INIT_PENDING;
        } else if (!vdiskp) {
            const auto groupIt = Setup->Groups.find(vdiskId.GroupID.GetRawId());
            UNIT_ASSERT(groupIt != Setup->Groups.end());
            const TSetup::TGroupInfo& groupInfo = groupIt->second;

            auto&& [it, inserted] = VDisks.emplace(vslotId, std::make_unique<TVDiskState>(vdiskId, vslotId, group, pdisk,
                groupInfo.SlotSize));
            group->AddVDisk(it->second.get());
            pdisk->AddVSlot(it->second.get());
            auto *actorSystem = TActivationContext::ActorSystem();
            const TActorId serviceId = MakeBlobStorageVDiskID(vslotId.NodeId, vslotId.PDiskId, vslotId.VSlotId);
            actorSystem->RegisterLocalService(serviceId, RegisterWithSameMailbox(new TVDiskMockActor(this, it->second)));
        }
    }

    if (record.GetComprehensive()) {
        pdiskIdsToRemove.merge(pdiskIds);
        vslotIdsToRemove.merge(vslotIds);
    }

    for (TVSlotId vslotId : vslotIdsToRemove) {
        if (TVDiskState *vdisk = GetVDisk(vslotId)) {
            vdisk->Group->EraseVDisk(vdisk);
            vdisk->PDisk->EraseVSlot(vdisk->VSlotId.VSlotId);
            VDisks.erase(vdisk->VSlotId);
        }
    }

    for (TPDiskId pdiskId : pdiskIdsToRemove) {
        if (const auto it = PDisks.find(pdiskId); it != PDisks.end()) {
            UNIT_ASSERT(it->second->VSlotsOnPDisk.empty());
            PDisks.erase(it);
        }
    }

    for (const auto& [vslotId, vdiskp] : VDisks) {
        UNIT_ASSERT(!vdiskp->RequireDonorMode || vdiskp->DonorMode);
    }

    SendUpdateDiskStatus();
}

void TNodeWardenMockActor::Handle(TEvNodeWardenQueryStorageConfig::TPtr ev) {
    Send(ev->Sender, new TEvNodeWardenStorageConfig(NKikimrBlobStorage::TStorageConfig(), nullptr));
}

void TNodeWardenMockActor::HandleUnsubscribe(STATEFN_SIG) {
    Y_UNUSED(ev);
}
