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
                MakeBlobStorageVDiskID(slot->VSlotId.NodeId, slot->VSlotId.PDiskId, slot->VSlotId.VSlotId)});
        }
    }
    Send(GroupStateActorId, message.release());
}


class TBlobStorageController::TTxPersistGroupConfigAcknowledgements : public TTransactionBase<TBlobStorageController> {
    using TBase = TTransactionBase<TBlobStorageController>;
    google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus> Reports;
    TMap<TVSlotId, TGroupConfigAcknowledgement> Committed;

public:
    TTxPersistGroupConfigAcknowledgements(TBlobStorageController* controller,
            const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus>& reports)
        : TBase(controller), Reports(reports) {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_DISK_METRICS; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        Committed.clear();
        for (const NKikimrBlobStorage::TVDiskStatus& report : Reports) {
            if (!report.GetPersistedGroupGeneration() || !report.HasVDiskId()) {
                continue;
            }
            const TVSlotId slotId(report.GetNodeId(), report.GetPDiskId(), report.GetVSlotId());
            const TVSlotInfo* slot = Self->FindVSlot(slotId);
            const TVDiskID disk = VDiskIDFromVDiskID(report.GetVDiskId());
            if (!slot || slot->IsBeingDeleted() || slot->PDisk->Guid != report.GetPDiskGuid()
                    || !disk.SameExceptGeneration(slot->GetVDiskId())
                    || report.GetPersistedGroupGeneration() > slot->GetVDiskId().GroupGeneration) {
                continue;
            }
            // Read the durable slot row too: an earlier config transaction may already have deleted it.
            using TSlot = Schema::VSlot;
            auto slotRow = db.Table<TSlot>().Key(slotId.GetKey()).Select();
            if (!slotRow.IsReady()) {
                return false;
            }
            if (!slotRow.IsValid() || slotRow.GetValue<TSlot::GroupID>() != disk.GroupID
                    || slotRow.GetValue<TSlot::RingIdx>() != disk.FailRealm
                    || slotRow.GetValue<TSlot::FailDomainIdx>() != disk.FailDomain
                    || slotRow.GetValue<TSlot::VDiskIdx>() != disk.VDisk
                    || slotRow.GetValue<TSlot::Mood>() == TMood::Delete) {
                continue;
            }
            using T = Schema::GroupConfigAcknowledgement;
            auto row = db.Table<T>().Key(slotId.GetKey()).Select();
            if (!row.IsReady()) {
                return false;
            }
            ui32 generation = report.GetPersistedGroupGeneration();
            if (row.IsValid() && row.GetValue<T::GroupId>() == disk.GroupID.GetRawId()
                    && row.GetValue<T::PDiskGuid>() == report.GetPDiskGuid()
                    && row.GetValue<T::FailRealm>() == disk.FailRealm
                    && row.GetValue<T::FailDomain>() == disk.FailDomain && row.GetValue<T::VDisk>() == disk.VDisk) {
                generation = Max(generation, row.GetValue<T::Generation>());
            }
            db.Table<T>().Key(slotId.GetKey()).Update<T::GroupId, T::Generation, T::PDiskGuid,
                T::FailRealm, T::FailDomain, T::VDisk>(disk.GroupID.GetRawId(), generation, report.GetPDiskGuid(),
                    disk.FailRealm, disk.FailDomain, disk.VDisk);
            Committed[slotId] = {disk, report.GetPDiskGuid(), generation};
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        for (const auto& [slotId, acknowledgement] : Committed) {
            const TVSlotInfo* slot = Self->FindVSlot(slotId);
            if (slot && !slot->IsBeingDeleted() && slot->PDisk->Guid == acknowledgement.PDiskGuid
                    && slot->GetVDiskId().SameExceptGeneration(acknowledgement.VDiskId)) {
                TGroupConfigAcknowledgement& current = Self->GroupConfigAcknowledgements[slotId];
                if (current.PDiskGuid != acknowledgement.PDiskGuid
                        || !current.VDiskId.SameExceptGeneration(acknowledgement.VDiskId)
                        || current.Generation < acknowledgement.Generation) {
                    current = acknowledgement;
                }
            }
        }
    }
};

void TBlobStorageController::PersistGroupConfigAcknowledgements(
        const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus>& reports) {
    google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus> changed;
    for (const NKikimrBlobStorage::TVDiskStatus& report : reports) {
        if (!report.GetPersistedGroupGeneration()) {
            continue;
        }
        const TVSlotId slotId(report.GetNodeId(), report.GetPDiskId(), report.GetVSlotId());
        const TVDiskID disk = VDiskIDFromVDiskID(report.GetVDiskId());
        const auto it = GroupConfigAcknowledgements.find(slotId);
        if (it == GroupConfigAcknowledgements.end() || it->second.Generation < report.GetPersistedGroupGeneration()
                || it->second.PDiskGuid != report.GetPDiskGuid() || !it->second.VDiskId.SameExceptGeneration(disk)) {
            changed.Add()->CopyFrom(report);
        }
    }
    if (!changed.empty()) {
        Execute(new TTxPersistGroupConfigAcknowledgements(this, changed));
    }
}

} // namespace NKikimr::NBsController
