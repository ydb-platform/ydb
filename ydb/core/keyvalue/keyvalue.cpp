#include "keyvalue_flat_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {

namespace NKeyValue {

namespace {

struct TQuotaSchema : NIceDb::Schema {
    // Keep quota state separate from the index scanned by older KV implementations.
    struct DomainQuota : Table<1> {
        struct OwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PathId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Exceeded : Column<3, NScheme::NTypeIds::Bool> {};
        using TKey = TableKey<OwnerId, PathId>;
        using TColumns = TableColumns<OwnerId, PathId, Exceeded>;
    };

    using TTables = SchemaTables<DomainQuota>;
};

using TDomainQuota = TQuotaSchema::DomainQuota;

}

bool TKeyValueFlat::WatchesDiskQuota() const {
    return Info()->TabletType == TTabletTypes::KeyValue && Info()->TenantPathId;
}

bool TKeyValueFlat::LoadDiskQuota(NTabletFlatExecutor::TTransactionContext& txc) {
    if (!WatchesDiskQuota() || !txc.DB.GetScheme().GetTableInfo(TDomainQuota::TableId)) {
        return true;
    }

    const auto& domain = Info()->TenantPathId;
    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<TDomainQuota>().Key(domain.OwnerId, domain.LocalPathId).Select();
    if (!row.IsReady()) {
        return false;
    }

    State.DiskQuotaExceeded = !row.EndOfSet() && row.GetValue<TDomainQuota::Exceeded>();
    return true;
}

struct TKeyValueFlat::TTxPersistDiskQuota : NTabletFlatExecutor::ITransaction {
    const TPathId Domain;
    const bool Exceeded;

    TTxPersistDiskQuota(TPathId domain, bool exceeded)
        : Domain(domain)
        , Exceeded(exceeded)
    {}

    TTxType GetTxType() const override { return TXTYPE_PERSIST_DISK_QUOTA; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        db.Materialize<TQuotaSchema>();
        db.Table<TDomainQuota>().Key(Domain.OwnerId, Domain.LocalPathId).Update(
            NIceDb::TUpdate<TDomainQuota::Exceeded>(Exceeded));
        return true;
    }

    void Complete(const TActorContext&) override {}
};

void TKeyValueFlat::Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev) {
    const auto* msg = ev->Get();
    if (!WatchesDiskQuota()) {
        return;
    }

    const auto& description = msg->Result->GetPathDescription();
    if (!description.HasDomainDescription()) {
        return;
    }

    const auto& domain = description.GetDomainDescription();
    const auto& tenant = Info()->TenantPathId;
    if (msg->PathId == tenant) {
        // The parent describes an external database, but its own SchemeShard publishes the quota.
        const ui64 schemeShard = domain.GetProcessingParams().GetSchemeShard();
        UpdateQuotaPath(schemeShard && schemeShard != tenant.OwnerId
            ? TPathId(schemeShard, NSchemeShard::RootPathId) : tenant);
    }

    if (msg->PathId != QuotaPathId) {
        return;
    }

    const bool exceeded = domain.GetDomainState().GetDiskQuotaExceeded();
    if (State.DiskQuotaExceeded != exceeded) {
        State.DiskQuotaExceeded = exceeded;
        Execute(new TTxPersistDiskQuota(tenant, exceeded), ActorContext());
    }
}

void TKeyValueFlat::UpdateQuotaPath(TPathId pathId) {
    if (QuotaPathId == pathId) {
        return;
    }

    const auto& tenant = Info()->TenantPathId;
    if (QuotaPathId && QuotaPathId != tenant) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(QuotaPathId.OwnerId));
    }

    QuotaPathId = pathId;
    if (QuotaPathId != tenant) {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(QuotaPathId, QuotaPathId.OwnerId));
    }

    ReportStorageUsage();
}

void TKeyValueFlat::ReportStorageUsage() {
    if (QuotaPathId && AppData()->FeatureFlags.GetEnableKeyValueStorageUsageReports()) {
        auto report = MakeHolder<TEvKeyValue::TEvStorageUsageReport>();
        State.FillStorageUsageReport(report->Record);
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(report.Release(), QuotaPathId.OwnerId, false));
    }
}

bool TKeyValueFlat::TTxRequest::CheckConsistency(NTabletFlatExecutor::TTransactionContext &txc) {
#ifdef KIKIMR_KEYVALUE_CONSISTENCY_CHECKS
    Y_ABORT_UNLESS(Self->State.GetStateBytes() == Self->State.RecountStateBytes(), "StateBytes# %s Recount# %s",
        Self->State.GetStateBytes().ToString().c_str(), Self->State.RecountStateBytes().ToString().c_str());
    // Load feeds the tablet counters and resource metrics, so the scratch state needs its own
    TKeyValueState state;
    state.SetupTabletCounters(MakeTabletCounters());
    NMetrics::TResourceMetrics scratchMetrics(Self->TabletID(), 0, TActorId());
    state.SetupResourceMetrics(&scratchMetrics);
    if (!TTxInit::LoadStateFromDB(state, txc.DB)) {
        return false;
    }
    Y_ABORT_UNLESS(!state.GetIsDamaged());
    state.VerifyEqualIndex(Self->State);
    txc.DB.NoMoreReadsForTx();
    return true;
#else
    Y_UNUSED(txc);
    return true;
#endif
}

} // NKeyValue

IActor* CreateKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NKeyValue::TKeyValueFlat(tablet, info);
}

} // NKikimr
