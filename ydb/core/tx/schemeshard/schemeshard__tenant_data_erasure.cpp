#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TSchemeShard::Handle(TEvSchemeShard::TEvDataClenupRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxRunTenantDataErasure(ev), ctx);
}

NOperationQueue::EStartStatus TSchemeShard::StartTenantDataErasure(const TShardIdx& shardIdx) {
    UpdateTenantDataErasureQueueMetrics();

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Start] Failed to resolve shard info "
            "for data erasure# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Start] Data erasure "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << TenantDataErasureQueue->GetWakeupDelta()
        << ", rate# " << TenantDataErasureQueue->GetRate()
        << ", in queue# " << TenantDataErasureQueue->Size() << " shards"
        << ", running# " << TenantDataErasureQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvDataShard::TEvForceDataCleanup> request(
        new TEvDataShard::TEvForceDataCleanup(DataErasureGeneration));

    ActiveDataErasureShards[shardIdx] = PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnTenantDataErasureTimeout(const TShardIdx& shardIdx) {
    UpdateTenantDataErasureQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_TENANT_DATA_ERASURE_TIMEOUT].Increment(1);

    ActiveDataErasureShards.erase(shardIdx);

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Timeout] Failed to resolve shard info "
            "for timeout data erasure# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Timeout] Data erasure timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << TenantDataErasureQueue->GetWakeupDelta()
        << ", in queue# " << TenantDataErasureQueue->Size() << " shards"
        << ", running# " << TenantDataErasureQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    // retry
    EnqueueTenantDataErasure(shardIdx);
}

void TSchemeShard::Handle(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    const ui64 completedGeneration = record.GetDataCleanupGeneration();
    if (completedGeneration != DataErasureGeneration) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Handle TEvForceDataCleanupResult: Unknown generation#" << completedGeneration << ", Expected gen# " << DataErasureGeneration << " at schemestard: " << TabletID());
        return;
    }

    const TTabletId tabletId(record.GetTabletId());
    const TShardIdx shardIdx = GetShardIdx(tabletId);
    const auto it = ShardInfos.find(shardIdx);

    auto duration = TenantDataErasureQueue->OnDone(shardIdx);

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Finished] Failed to resolve shard info "
            "for pathId# " << (it != ShardInfos.end() ? it->second.PathId.ToString() : "") << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup in# " << TenantDataErasureQueue->GetWakeupDelta()
            << ", rate# " << TenantDataErasureQueue->GetRate()
            << ", in queue# " << TenantDataErasureQueue->Size() << " shards"
            << ", running# " << TenantDataErasureQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Finished] Data erasure is completed "
            "for pathId# " << (it != ShardInfos.end() ? it->second.PathId.ToString() : "") << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup in# " << TenantDataErasureQueue->GetWakeupDelta()
            << ", rate# " << TenantDataErasureQueue->GetRate()
            << ", in queue# " << TenantDataErasureQueue->Size() << " shards"
            << ", running# " << TenantDataErasureQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    }

    ActiveDataErasureShards.erase(shardIdx);

    TabletCounters->Cumulative()[COUNTER_TENANT_DATA_ERASURE_OK].Increment(1);
    UpdateTenantDataErasureQueueMetrics();

    if (ActiveDataErasureShards.empty()) {
        std::unique_ptr<TEvSchemeShard::TEvDataCleanupResult> response(
            new TEvSchemeShard::TEvDataCleanupResult(ParentDomainId, DataErasureGeneration, true));

        const ui64 rootSchemeshard = ParentDomainId.OwnerId;

        /*RunningDataErasureForTenants[pathId] = */PipeClientCache->Send(
            ctx,
            ui64(rootSchemeshard),
            response.release());
    }
}

void TSchemeShard::TenantDataErasureHandleDisconnect(TTabletId tabletId, const TActorId& clientId) {
    auto tabletIt = TabletIdToShardIdx.find(tabletId);
    if (tabletIt == TabletIdToShardIdx.end())
        return; // just sanity check
    const auto& shardIdx = tabletIt->second;

    auto it = ActiveDataErasureShards.find(shardIdx);
    if (it == ActiveDataErasureShards.end())
        return;

    if (it->second != clientId)
        return;

    ActiveDataErasureShards.erase(it);

    StartTenantDataErasure(shardIdx);
}

void TSchemeShard::EnqueueTenantDataErasure(const TShardIdx& shardIdx) {
    if (!TenantDataErasureQueue)
        return;

    auto ctx = ActorContext();

    if (TenantDataErasureQueue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasure] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << TabletID());
        UpdateTenantDataErasureQueueMetrics();
    }  else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasure] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << TabletID());
    }
}
void TSchemeShard::RemoveTenantDataErasure(const TShardIdx& shardIdx) {
    if (!TenantDataErasureQueue)
        return;

    ActiveDataErasureShards.erase(shardIdx);
    TenantDataErasureQueue->Remove(shardIdx);
    UpdateTenantDataErasureQueueMetrics();
}

void TSchemeShard::UpdateTenantDataErasureQueueMetrics() {
    if (!TenantDataErasureQueue) {
        return;
    }

    TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_SIZE].Set(TenantDataErasureQueue->Size());
    TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_RUNNING].Set(TenantDataErasureQueue->RunningSize());
}

struct TSchemeShard::TTxRunTenantDataErasure : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvDataClenupRequest::TPtr Ev;

    TTxRunTenantDataErasure(TSelf *self, TEvSchemeShard::TEvDataClenupRequest::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_TENANT_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantDataErasure Execute at schemestard: " << Self->TabletID());
        if (Self->IsDomainSchemeShard) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Request] Cannot run data erasure on root schemeshard");
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        const auto& record = Ev->Get()->Record;
        if (Self->DataErasureGeneration < record.GetGeneration()) {
            Self->DataErasureGeneration = record.GetGeneration();
            for (const auto& [shardIdx, shardInfo] : Self->ShardInfos) {
                if (shardInfo.TabletType == ETabletType::DataShard) {
                    Self->EnqueueTenantDataErasure(shardIdx); // forward generation
                }
            }
            // write new DataErasureGeneration to local DB
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantDataErasure Execute at schemestard: " << Self->TabletID());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunTenantDataErasure(TEvSchemeShard::TEvDataClenupRequest::TPtr& ev) {
    return new TTxRunTenantDataErasure(this, ev);
}

} // NKikimr::NSchemeShard
