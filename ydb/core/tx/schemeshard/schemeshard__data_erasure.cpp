#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartDataErasure(const TPathId& pathId) {
    UpdateDataErasureQueueMetrics();

    auto ctx = ActorContext();

    auto it = SubDomains.find(pathId);
    if (it == SubDomains.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Start] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& tenantSchemeShardId = it->second->GetTenantSchemeShardID();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Start] Data erasure "
        "for pathId# " << pathId
        << ", tenant schemeshard# " << tenantSchemeShardId
        << ", next wakeup# " << DataErasureQueue->GetWakeupDelta()
        << ", rate# " << DataErasureQueue->GetRate()
        << ", in queue# " << DataErasureQueue->Size() << " tenants"
        << ", running# " << DataErasureQueue->RunningSize() << " tenants"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvSchemeShard::TEvDataClenupRequest> request(
        new TEvSchemeShard::TEvDataClenupRequest(DataErasureGeneration));

    RunningDataErasureForTenants[pathId] = PipeClientCache->Send(
        ctx,
        ui64(tenantSchemeShardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnDataErasureTimeout(const TPathId& pathId) {
    UpdateDataErasureQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_DATA_ERASURE_TIMEOUT].Increment(1);

    RunningDataErasureForTenants.erase(pathId);

    auto ctx = ActorContext();

    if (!SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Timeout] Failed to resolve subdomain info "
            "for path# " << pathId
            << " at schemeshard# " << TabletID());
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Timeout] Data erasure timeouted "
        "for pathId# " << pathId
        << ", next wakeup in# " << DataErasureQueue->GetWakeupDelta()
        << ", rate# " << DataErasureQueue->GetRate()
        << ", in queue# " << DataErasureQueue->Size() << " tenants"
        << ", running# " << DataErasureQueue->RunningSize() << " tenants"
        << " at schemeshard " << TabletID());

    // retry
    EnqueueDataErasure(pathId);
}

void TSchemeShard::EnqueueDataErasure(const TPathId& pathId) {
    if (!DataErasureQueue)
        return;

    auto ctx = ActorContext();

    if (DataErasureQueue->Enqueue(pathId)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[DataErasure] [Enqueue] Enqueued pathId# " << pathId << " at schemeshard " << TabletID());
        UpdateDataErasureQueueMetrics();
    } else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[DataErasure] [Enqueue] Skipped or already exists pathId# " << pathId << " at schemeshard " << TabletID());
    }
}

void TSchemeShard::DataErasureHandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    const auto shardIdx = GetShardIdx(tabletId);
    if (!ShardInfos.contains(shardIdx)) {
        return;
    }

    const auto& pathId = ShardInfos.at(shardIdx).PathId;
    if (!TTLEnabledTables.contains(pathId)) {
        return;
    }

    const auto it = RunningDataErasureForTenants.find(pathId);
    if (it == RunningDataErasureForTenants.end()) {
        return;
    }

    if (it->second != clientId) {
        return;
    }

    RunningDataErasureForTenants.erase(pathId);

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Disconnect] Data erasure disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << TabletID());

    StartDataErasure(pathId);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvDataCleanupResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetCurrentGeneration() == DataErasureGeneration) {
        Execute(CreateTxCompleteDataErasure(ev), ctx);
    }

    auto pathId = TPathId(
        record.GetPathId().GetOwnerId(),
        record.GetPathId().GetLocalId());

    auto duration = DataErasureQueue->OnDone(pathId);

    if (!SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Finished] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << DataErasureQueue->GetWakeupDelta()
            << ", rate# " << DataErasureQueue->GetRate()
            << ", in queue# " << DataErasureQueue->Size() << " tenants"
            << ", running# " << DataErasureQueue->RunningSize() << " tenants"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Finished] Data erasure completed "
            "for pathId# " << pathId
            << " in# " << duration.MilliSeconds()
            << ", next wakeup# " << DataErasureQueue->GetWakeupDelta()
            << ", rate# " << DataErasureQueue->GetRate()
            << ", in queue# " << DataErasureQueue->Size() << " tenants"
            << ", running# " << DataErasureQueue->RunningSize() << " tenants"
            << " at schemeshard " << TabletID());
    }

    RunningDataErasureForTenants.erase(pathId);

    TabletCounters->Cumulative()[COUNTER_DATA_ERASURE_OK].Increment(1);
    UpdateDataErasureQueueMetrics();

    // if (RequestedDataErasureForTenants.IsCompleted()) {
        // send clean up request to BSC
    //}
}

void TSchemeShard::UpdateDataErasureQueueMetrics() {
    if (!DataErasureQueue) {
        return;
    }

    TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_SIZE].Set(DataErasureQueue->Size());
    TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_RUNNING].Set(DataErasureQueue->RunningSize());
}

struct TSchemeShard::TTxRunDataErasure : public TSchemeShard::TRwTxBase {
    ui64 RequestedGeneration;

    TTxRunDataErasure(TSelf *self, ui64 generation)
        : TRwTxBase(self)
        , RequestedGeneration(generation)
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxRunDataErasure Execute at schemeshard: " << Self->TabletID());
        NIceDb::TNiceDb db(txc.DB);
        if (Self->DataErasureGeneration < RequestedGeneration) {
            Self->DataErasureGeneration = RequestedGeneration;
            Self->DataErasureQueue->Clear();
            for (auto& [pathId, subdomain] : Self->SubDomains) {
                auto path = TPath::Init(pathId, Self);
                if (path->IsRoot()) {
                    continue;
                }
                if (subdomain->GetTenantSchemeShardID() == InvalidTabletId) { // no tenant schemeshard
                    continue;
                }
                Self->DataErasureQueue->Enqueue(pathId);
                Self->RequestedDataErasureForTenants[pathId] = false;
                db.Table<Schema::DataErasure>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::DataErasure::IsCompleted, Schema::DataErasure::Generation>(false, Self->DataErasureGeneration);
            }
        } else {
            Self->DataErasureQueue->Clear();
            for (const auto& [pathId, isCompleted] : Self->RequestedDataErasureForTenants) {
                if (!isCompleted) {
                    Self->DataErasureQueue->Enqueue(pathId);
                }
            }
        }
    }
    void DoComplete(const TActorContext& /*ctx*/) override {}
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunDataErasure(ui64 generation) {
    return new TTxRunDataErasure(this, generation);
}

struct TSchemeShard::TTxCompleteDataErasure : public TSchemeShard::TRwTxBase {
    const TEvSchemeShard::TEvDataCleanupResult::TPtr Ev;

    TTxCompleteDataErasure(TSelf* self, const TEvSchemeShard::TEvDataCleanupResult::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxCompleteDataErasure Execute at schemeshard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        const auto& record = Ev->Get()->Record;
        auto pathId = TPathId(
            record.GetPathId().GetOwnerId(),
            record.GetPathId().GetLocalId());
        Self->RequestedDataErasureForTenants[pathId] = true;
        db.Table<Schema::DataErasure>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::DataErasure::IsCompleted>(true);
    }

    void DoComplete(const TActorContext& /*ctx*/) override {}
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasure(TEvSchemeShard::TEvDataCleanupResult::TPtr& ev) {
    return new TTxCompleteDataErasure(this, ev);
}

} // NKikimr::NSchemeShard
