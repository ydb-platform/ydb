#include "keyvalue_flat_impl.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {

namespace NKeyValue {

bool TKeyValueFlat::WatchesDiskQuota() const {
    return Info()->TabletType == TTabletTypes::KeyValue && Info()->TenantPathId;
}

struct TKeyValueFlat::TTxPersistDiskQuota : NTabletFlatExecutor::ITransaction {
    TKeyValueFlat* const Self;

    explicit TTxPersistDiskQuota(TKeyValueFlat* self)
        : Self(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_PERSIST_DISK_QUOTA; }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        TVector<TLogoBlobID> trashBeingCommitted;
        TSimpleDbFlat db(txc.DB, trashBeingCommitted);
        Self->State.PersistState(db);
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
        Execute(new TTxPersistDiskQuota(this), ActorContext());
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
