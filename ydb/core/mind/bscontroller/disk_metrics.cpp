#include "impl.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxUpdateDiskMetrics : public TTransactionBase<TBlobStorageController> {
public:
    TTxUpdateDiskMetrics(TBlobStorageController *controller)
        : TBase(controller)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_DISK_METRICS; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        TRequestCounter counter(Self->TabletCounters, NBlobStorageController::COUNTER_UPDATE_DISK_METRICS_USEC);

        NIceDb::TNiceDb db(txc.DB);

        for (const auto& [pdiskId, pdisk] : Self->PDisks) {
            if (std::exchange(pdisk->MetricsDirty, false)) {
                auto&& key = pdiskId.GetKey();
                auto value = pdisk->Metrics;
                value.ClearPDiskId();
                db.Table<Schema::PDiskMetrics>().Key(key).Update<Schema::PDiskMetrics::Metrics>(value);
                Self->SysViewChangedPDisks.insert(pdiskId);
            }
        }

        for (const auto& [vslotId, v] : Self->VSlots) {
            if (std::exchange(v->MetricsDirty, false)) {
                auto groupId = v->GroupId.GetRawId();
                auto&& key = std::tie(groupId, v->GroupGeneration, v->RingIdx, v->FailDomainIdx, v->VDiskIdx);
                auto value = v->Metrics;
                value.ClearVDiskId();
                db.Table<Schema::VDiskMetrics>().Key(key).Update<Schema::VDiskMetrics::Metrics>(value);
                Self->SysViewChangedVSlots.insert(vslotId);
                Self->SysViewChangedGroups.insert(v->GroupId);
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

    STLOG(PRI_DEBUG, BS_CONTROLLER, BSCTXUDM01, "Updating disk status", (Record, record));

    // apply VDisk metrics update
    std::set<const TGroupInfo*> dirtyGroups;
    for (const auto& m : record.GetVDisksMetrics()) {
        const TVDiskID vdiskId = VDiskIDFromVDiskID(m.GetVDiskId());
        if (const auto *slot = FindVSlot(vdiskId)) {
            i64 allocatedSizeIncrement;
            if (slot->UpdateVDiskMetrics(m, &allocatedSizeIncrement) && slot->Group) {
                dirtyGroups.insert(slot->Group);
            }
            if (allocatedSizeIncrement && !slot->IsBeingDeleted()) {
                const TGroupInfo *group = FindGroup(slot->GroupId);
                Y_ABORT_UNLESS(group);
                StoragePoolStat->UpdateAllocatedSize(TStoragePoolStat::ConvertId(group->StoragePoolId), allocatedSizeIncrement);
            }
        } else if (const auto it = StaticVDiskMap.find(vdiskId); it != StaticVDiskMap.end()) {
            TStaticVSlotInfo& info = StaticVSlots.at(it->second);
            info.VDiskMetrics = m;
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
    TSet<TList<TSelectGroupsQueueItem>::iterator, TPDiskToQueueComp> queues;
    for (const auto& m : record.GetPDisksMetrics()) {
        const TPDiskId pdiskId(ev->Sender.NodeId(), m.GetPDiskId());
        if (auto *pdisk = FindPDisk(pdiskId)) {
            if (pdisk->UpdatePDiskMetrics(m, now)) {
                const auto first = std::make_pair(pdiskId, TList<TSelectGroupsQueueItem>::iterator());
                for (auto it = PDiskToQueue.lower_bound(first); it != PDiskToQueue.end() && it->first == pdiskId; ++it) {
                    queues.insert(it->second);
                }
            }
            pdisk->UpdateOperational(true);
        } else if (const auto it = StaticPDisks.find(pdiskId); it != StaticPDisks.end()) {
            it->second.PDiskMetrics = m;
        } else {
            STLOG(PRI_NOTICE, BS_CONTROLLER, BSCTXUDM03, "PDisk not found", (PDiskId, pdiskId));
        }
    }
    for (const TList<TSelectGroupsQueueItem>::iterator it : queues) {
        ProcessSelectGroupsQueueItem(it);
    }

    // process VDisk status
    ProcessVDiskStatus(record.GetVDiskStatus());

    // commit into database if enough time has passed
    if (now - LastMetricsCommit >= TDuration::Seconds(15)) {
        Execute(new TTxUpdateDiskMetrics(this));
        LastMetricsCommit = now;
    }
}

} // NKikimr::NBsController
