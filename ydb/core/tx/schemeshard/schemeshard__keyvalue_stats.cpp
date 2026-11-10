#include "schemeshard_impl.h"

#include <ydb/core/keyvalue/keyvalue_events.h>

namespace NKikimr::NSchemeShard {

NKikimrSubDomains::TKeyValuePartitionUsage PrepareKeyValueStorageUsage(
    const NKikimrKeyValue::StorageUsageReport& report,
    const TChannelsBindings& bindings, const TStoragePools& pools)
{
    NKikimrSubDomains::TKeyValuePartitionUsage usage;
    usage.set_generation(report.generation());
    usage.set_round(report.round());
    for (const auto& [channel, bytes] : report.bytes_by_channel()) {
        const auto* binding = channel < bindings.size() ? &bindings[channel] : nullptr;
        // External data belongs to the user's object storage, not the database disk quota.
        if (binding && binding->HasExternalDataSourceId()) {
            continue;
        }

        usage.set_total_bytes(usage.total_bytes() + bytes);
        if (binding) {
            if (const auto* kind = GetPoolKind(*binding, pools)) {
                (*usage.mutable_bytes_by_pool_kind())[*kind] += bytes;
            }
        }
    }

    return usage;
}

struct TSchemeShard::TTxStoreKeyValueUsage : NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
    TEvKeyValue::TEvStorageUsageReport::TPtr Event;

    TTxStoreKeyValueUsage(TSchemeShard* self, TEvKeyValue::TEvStorageUsageReport::TPtr& ev)
        : TTransactionBase(self)
        , Event(ev)
    {}

    TTxType GetTxType() const override { return TXTYPE_STORE_PARTITION_STATS; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& report = Event->Get()->Record;
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
        auto next = PrepareKeyValueStorageUsage(report, shard->BindedChannels, domain->EffectiveStoragePools());
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
