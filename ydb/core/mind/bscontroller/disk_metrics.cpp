#include "impl.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxUpdateDiskMetrics : public TTransactionBase<TBlobStorageController> {
    std::vector<TPDiskId> PDiskIds;
    std::vector<TVSlotId> VSlotIds;

public:
    TTxUpdateDiskMetrics(TBlobStorageController *controller, std::vector<TPDiskId> pdiskIds,
            std::vector<TVSlotId> vslotIds)
        : TBase(controller)
        , PDiskIds(std::move(pdiskIds))
        , VSlotIds(std::move(vslotIds))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_DISK_METRICS; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_UPDATE_DISK_METRICS_USEC);

        NIceDb::TNiceDb db(txc.DB);

        for (const TPDiskId& pdiskId : PDiskIds) {
            if (TPDiskInfo *pdisk = Self->FindPDisk(pdiskId)) {
                using T = Schema::PDiskMetrics;
                db.Table<T>().Key(pdiskId.GetKey()).Update<T::Metrics>(pdisk->PersistedMetrics);
            }
        }

        for (const TVSlotId& vslotId : VSlotIds) {
            auto updateVSlot = [&](TVDiskID vdiskId, const NKikimrBlobStorage::TVDiskMetrics& metrics) {
                using T = Schema::VDiskMetrics;
                db.Table<T>().Key(vdiskId.GroupID.GetRawId(), vdiskId.GroupGeneration, vdiskId.FailRealm,
                    vdiskId.FailDomain, vdiskId.VDisk).Update<T::Metrics>(metrics);
            };

            if (TVSlotInfo *vslot = Self->FindVSlot(vslotId)) {
                updateVSlot(vslot->GetVDiskId(), vslot->PersistedMetrics);
            } else if (const auto it = Self->StaticVSlots.find(vslotId); it != Self->StaticVSlots.end()) {
                TStaticVSlotInfo& vslot = it->second;
                if (const auto& m = vslot.PersistedVDiskMetrics) {
                    updateVSlot(vslot.VDiskId, *m);
                } else {
                    Y_DEBUG_ABORT();
                }
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {}
};

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateDiskStatus::TPtr &ev) {
    TabletCounters->Cumulative()[NBlobStorageController::COUNTER_UPDATE_DISK_METRICS_COUNT].Increment(1);
    TRequestCounter counter(TabletCounters, NBlobStorageController::COUNTER_UPDATE_DISK_METRICS_USEC);

    const TInstant now = TActivationContext::Now();
    auto& record = ev->Get()->Record;
    THashSet<TGroupId> groupsToCheck;
    std::vector<TPDiskId> pdiskIds;
    std::vector<TVSlotId> vslotIds;

    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXUDM01, "Updating disk status", (Record, record));

    // apply VDisk metrics update
    std::set<const TGroupInfo*> dirtyGroups;
    for (const auto& m : record.GetVDisksMetrics()) {
        const TVDiskID vdiskId = VDiskIDFromVDiskID(m.GetVDiskId());
        if (const auto *slot = FindVSlot(vdiskId)) {
            // process persistent metrics
            NKikimrBlobStorage::TVDiskMetrics newMetrics(slot->PersistedMetrics);
            newMetrics.MergeFrom(m);
            newMetrics.DiscardUnknownFields();
            if (CompareMetrics(slot->PersistedMetrics, newMetrics, slot->MetricsCommitted)) {
                newMetrics.Swap(&slot->PersistedMetrics);
                slot->MetricsCommitted = true;
                vslotIds.push_back(slot->VSlotId);
            }

            // update in-memory metrics
            i64 allocatedSizeIncrement;
            if (slot->UpdateVDiskMetrics(m, &allocatedSizeIncrement) && slot->Group) {
                dirtyGroups.insert(slot->Group);
                groupsToCheck.insert(slot->Group->ID);
            }
            if (allocatedSizeIncrement && !slot->IsBeingDeleted()) {
                const TGroupInfo *group = FindGroup(slot->GroupId);
                Y_ABORT_UNLESS(group);
                StoragePoolStat->UpdateAllocatedSize(TStoragePoolStat::ConvertId(group->StoragePoolId), allocatedSizeIncrement);
            }

            // mark vslots and groups as changed in sysviews
            SysViewChangedVSlots.insert(slot->VSlotId);
            SysViewChangedGroups.insert(slot->GroupId);
        } else if (const auto it = StaticVDiskMap.find(vdiskId); it != StaticVDiskMap.end()) {
            TStaticVSlotInfo& info = StaticVSlots.at(it->second);

            NKikimrBlobStorage::TVDiskMetrics metrics;
            if (info.PersistedVDiskMetrics) {
                metrics.CopyFrom(*info.PersistedVDiskMetrics);
            }
            metrics.MergeFrom(m);
            metrics.DiscardUnknownFields();
            if (!info.PersistedVDiskMetrics || CompareMetrics(*info.PersistedVDiskMetrics, metrics, info.MetricsCommitted)) {
                info.PersistedVDiskMetrics = std::move(metrics);
                info.MetricsCommitted = true;
                vslotIds.push_back(it->second);
            }

            if (!info.VDiskMetrics) {
                info.VDiskMetrics.emplace(std::move(m));
            } else {
                info.VDiskMetrics->MergeFrom(m);
            }
            info.VDiskMetrics->DiscardUnknownFields();

            SysViewChangedVSlots.insert(it->second);
            SysViewChangedGroups.insert(vdiskId.GroupID);
        } else {
            STLOG(PRI_NOTICE, BS_CONTROLLER, BSCTXUDM02, "VDisk not found", (VDiskId, vdiskId));
        }
    }
    for (const TGroupInfo *group : dirtyGroups) {
        const TStorageStatusFlags flags = group->GetStorageStatusFlags();
        StoragePoolStat->Update(TStoragePoolStat::ConvertId(group->StoragePoolId), group->StatusFlags, flags);
        group->StatusFlags = flags;
    }

    // apply PDisk metrics update
    for (const auto& m : record.GetPDisksMetrics()) {
        const TPDiskId pdiskId(ev->Sender.NodeId(), m.GetPDiskId());
        if (auto *pdisk = FindPDisk(pdiskId)) {
            if (CompareMetrics(pdisk->PersistedMetrics, m, pdisk->MetricsCommitted)) {
                pdisk->PersistedMetrics.CopyFrom(m);
                pdisk->MetricsCommitted = true;
                pdiskIds.push_back(pdiskId);
            }

            if (pdisk->UpdatePDiskMetrics(m, now)) {
                for (auto& [id, slot] : pdisk->VSlotsOnPDisk) {
                    if (slot->Group) {
                        groupsToCheck.insert(slot->Group->ID);
                    }
                }
            }
            pdisk->UpdateOperational(true);

            SysViewChangedPDisks.insert(pdiskId);
        } else if (const auto it = StaticPDisks.find(pdiskId); it != StaticPDisks.end()) {
            it->second.PDiskMetrics = m;
            it->second.PDiskMetricsUpdateTimestamp = now;
        } else {
            STLOG(PRI_NOTICE, BS_CONTROLLER, BSCTXUDM03, "PDisk not found", (PDiskId, pdiskId));
        }
    }

    UpdateWaitingGroups(groupsToCheck);

    // process VDisk status
    ProcessVDiskStatus(record.GetVDiskStatus());

    Execute(new TTxUpdateDiskMetrics(this, std::move(pdiskIds), std::move(vslotIds)));
}

bool TBlobStorageController::CompareMetrics(const NKikimrBlobStorage::TPDiskMetrics& prev,
        const NKikimrBlobStorage::TPDiskMetrics& cur, bool committedAtLeastOnce) {
    if (prev.HasPDiskUsage() != cur.HasPDiskUsage()) {
        return true; // PDiskUsage field has been set or unset
    } else if (prev.HasPDiskUsage() && abs(prev.GetPDiskUsage() - cur.GetPDiskUsage()) >= 1.0) {
        return true; // significant change to persist
    }

    if (prev.HasAvailableSize() != cur.HasAvailableSize() || prev.HasTotalSize() != cur.HasTotalSize()) {
        return true;
    } else if (prev.HasTotalSize() && prev.HasAvailableSize() && prev.GetTotalSize() && cur.GetTotalSize()) {
        const double r1 = (double)prev.GetAvailableSize() / prev.GetTotalSize();
        const double r2 = (double)cur.GetAvailableSize() / cur.GetTotalSize();
        if (abs(r1 - r2) >= 0.01) {
            return true;
        }
    }

    google::protobuf::util::MessageDifferencer diff;
    if (committedAtLeastOnce) {
        diff.IgnoreField(prev.GetDescriptor()->FindFieldByNumber(NKikimrBlobStorage::TPDiskMetrics::kAvailableSizeFieldNumber));
        diff.IgnoreField(prev.GetDescriptor()->FindFieldByNumber(NKikimrBlobStorage::TPDiskMetrics::kPDiskUsageFieldNumber));
    }
    return !diff.Compare(prev, cur);
}

bool TBlobStorageController::CompareMetrics(const NKikimrBlobStorage::TVDiskMetrics& prev,
        const NKikimrBlobStorage::TVDiskMetrics& cur, bool committedAtLeastOnce) {
#define COMPARE_FIELD(NAME, MARGIN) \
    if (prev.Has##NAME() != cur.Has##NAME() || (prev.Has##NAME() && abs(prev.Get##NAME() - cur.Get##NAME()) >= MARGIN)) { \
        return true; \
    }
    COMPARE_FIELD(NormalizedOccupancy, 0.01)
    COMPARE_FIELD(VDiskSlotUsage, 1.0)
    COMPARE_FIELD(VDiskRawUsage, 1.0)
#undef COMPARE_FIELD

    if (prev.HasAvailableSize() != cur.HasAvailableSize() || prev.HasAllocatedSize() != cur.HasAllocatedSize()) {
        return true;
    }
    const ui64 prevTotalSize = prev.GetAvailableSize() + prev.GetAllocatedSize();
    const ui64 curTotalSize = cur.GetAvailableSize() + cur.GetAllocatedSize();
    if (prevTotalSize != curTotalSize) {
        return true;
    }
    if (prevTotalSize && curTotalSize) {
        const double r1 = (double)prev.GetAllocatedSize() / prevTotalSize;
        const double r2 = (double)cur.GetAllocatedSize() / curTotalSize;
        if (abs(r1 - r2) >= 0.01) {
            return true;
        }
    }

    if (prev.HasSatisfactionRank() != cur.HasSatisfactionRank()) {
        return true;
    }

    google::protobuf::util::MessageDifferencer diff;
    if (committedAtLeastOnce) {
        auto *desc = prev.GetDescriptor();
        diff.IgnoreField(desc->FindFieldByNumber(NKikimrBlobStorage::TVDiskMetrics::kSatisfactionRankFieldNumber));
        diff.IgnoreField(desc->FindFieldByNumber(NKikimrBlobStorage::TVDiskMetrics::kAvailableSizeFieldNumber));
        diff.IgnoreField(desc->FindFieldByNumber(NKikimrBlobStorage::TVDiskMetrics::kAllocatedSizeFieldNumber));
        diff.IgnoreField(desc->FindFieldByNumber(NKikimrBlobStorage::TVDiskMetrics::kNormalizedOccupancyFieldNumber));
        diff.IgnoreField(desc->FindFieldByNumber(NKikimrBlobStorage::TVDiskMetrics::kVDiskSlotUsageFieldNumber));
        diff.IgnoreField(desc->FindFieldByNumber(NKikimrBlobStorage::TVDiskMetrics::kVDiskRawUsageFieldNumber));
    }
    return !diff.Compare(prev, cur);
}

} // NKikimr::NBsController
