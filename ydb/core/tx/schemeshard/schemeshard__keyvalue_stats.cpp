#include "schemeshard_impl.h"

#include <ydb/core/keyvalue/keyvalue_events.h>

namespace NKikimr::NSchemeShard {

struct TSchemeShard::TTxStoreKeyValueUsage : NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvKeyValue::TEvStorageUsageReport::TPtr Event;

    TTxStoreKeyValueUsage(TSchemeShard* self, TEvKeyValue::TEvStorageUsageReport::TPtr& ev)
        : TTransactionBase(self)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_STORE_PARTITION_STATS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& report = Event->Get()->Record;
        if (!report.has_total_bytes()) {
            return true;
        }

        const auto* idx = Self->TabletIdToShardIdx.FindPtr(TTabletId(report.tablet_id()));
        const auto* shard = idx ? Self->ShardInfos.FindPtr(*idx) : nullptr;
        if (!shard || shard->TabletType != ETabletType::KeyValue) {
            return true;
        }

        const auto* volume = Self->SolomonVolumes.FindPtr(shard->PathId);
        if (!volume || Self->PathsById.at(shard->PathId)->Dropped()) {
            return true;
        }

        const auto* partition = (*volume)->Partitions.FindPtr(*idx);
        if (!partition) {
            return true;
        }

        auto& previous = (*partition)->StorageUsage;
        if (TMessageSeqNo(report.generation(), report.round()) <= TMessageSeqNo(previous.generation(), previous.round())) {
            return true;
        }

        auto domain = Self->ResolveDomainInfo(shard->PathId);
        NKikimrSubDomains::TKeyValuePartitionUsage next;
        next.set_generation(report.generation());
        next.set_round(report.round());
        next.set_total_bytes(report.total_bytes());
        *next.mutable_bytes_by_pool_kind() = report.bytes_by_pool_kind();
        domain->UpdateKeyValueUsage(next, previous);
        previous.Swap(&next);
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::SolomonPartitions>().Key(shard->PathId.LocalPathId, idx->GetLocalId()).Update(
            NIceDb::TUpdate<Schema::SolomonPartitions::StorageUsage>(previous.SerializeAsString()));
        Self->CheckDiskQuotasAndPublish(db, Self->ResolvePathIdForDomain(shard->PathId), *domain, ctx);
        return true;
    }

    void Complete(const TActorContext&) override {}
};

void TSchemeShard::Handle(TEvKeyValue::TEvStorageUsageReportPtr& ev, const TActorContext& ctx) {
    Execute(new TTxStoreKeyValueUsage(this, ev), ctx);
}

} // namespace NKikimr::NSchemeShard
