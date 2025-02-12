#include "schemeshard_impl.h"

#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>

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

    std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureRequest> request(
        new TEvSchemeShard::TEvTenantDataErasureRequest(DataErasureGeneration));

    RunningDataErasureTenants[pathId] = PipeClientCache->Send(
        ctx,
        ui64(tenantSchemeShardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnDataErasureTimeout(const TPathId& pathId) {
    UpdateDataErasureQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_DATA_ERASURE_TIMEOUT].Increment(1);

    RunningDataErasureTenants.erase(pathId);

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

    const auto it = RunningDataErasureTenants.find(pathId);
    if (it == RunningDataErasureTenants.end()) {
        return;
    }

    if (it->second != clientId) {
        return;
    }

    RunningDataErasureTenants.erase(pathId);

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Disconnect] Data erasure disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << TabletID());

    StartDataErasure(pathId);
}

void TSchemeShard::Handle(TEvSchemeShard::TEvTenantDataErasureResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetGeneration() == DataErasureGeneration) {
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
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup# " << DataErasureQueue->GetWakeupDelta()
            << ", rate# " << DataErasureQueue->GetRate()
            << ", in queue# " << DataErasureQueue->Size() << " tenants"
            << ", running# " << DataErasureQueue->RunningSize() << " tenants"
            << " at schemeshard " << TabletID());
    }

    ActiveDataErasureTenants.erase(pathId);
    RunningDataErasureTenants.erase(pathId);

    TabletCounters->Cumulative()[COUNTER_DATA_ERASURE_OK].Increment(1);
    UpdateDataErasureQueueMetrics();

    bool isDataErasureCompleted = true;
    for (const auto& [pathId, status] : ActiveDataErasureTenants) {
        if (status == EDataErasureStatus::IN_PROGRESS) {
            isDataErasureCompleted = false;
            break;
        }
    }

    if (isDataErasureCompleted) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Data erasure in tenants is completed. Send request to BS controller");
        std::unique_ptr<TEvBlobStorage::TEvControllerShredRequest> request(
            new TEvBlobStorage::TEvControllerShredRequest(DataErasureGeneration));

        PipeClientCache->Send(ctx, MakeBSControllerID(), request.release());
    }
}

void TSchemeShard::Handle(TEvBlobStorage::TEvControllerShredResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record;

    if (record.GetCurrentGeneration() != DataErasureGeneration) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvControllerShredResponse: Get unexpected generation " << record.GetCurrentGeneration());
        return;
    }

    if (record.GetCompleted()) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvControllerShredResponse: Data shred in BSC is completed");
    } else {
        // Schedule new request to BS controller to get data shred progress
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Handle TEvControllerShredResponse: Progress data shred in BSC " << record.GetProgress10k());
    }
    // BS Controller always return completed as false
    ctx.Send(SelfId(), new TEvSchemeShard::TEvCompleteDataErasure(DataErasureGeneration));
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
    TInstant StartTime;

    TTxRunDataErasure(TSelf *self, ui64 generation, const TInstant& startTime)
        : TRwTxBase(self)
        , RequestedGeneration(generation)
        , StartTime(startTime)
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
                Self->ActiveDataErasureTenants[pathId] = EDataErasureStatus::IN_PROGRESS;
                db.Table<Schema::DataErasureScheduler>().Key(Self->DataErasureGeneration).Update<Schema::DataErasureScheduler::Status,
                                                                                                 Schema::DataErasureScheduler::StartTime>(static_cast<ui32>(Self->DataErasureScheduler->GetStatus()),
                                                                                                                                          StartTime.MicroSeconds());
                db.Table<Schema::ActiveDataErasureTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::ActiveDataErasureTenants::Status>(static_cast<ui32>(Self->ActiveDataErasureTenants[pathId]));
            }
        } else if (Self->DataErasureGeneration == RequestedGeneration) {
            Self->DataErasureQueue->Clear();
            for (const auto& [pathId, status] : Self->ActiveDataErasureTenants) {
                if (status == EDataErasureStatus::IN_PROGRESS) {
                    Self->DataErasureQueue->Enqueue(pathId);
                }
            }
        }
    }
    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunDataErasure Complete at schemeshard: " << Self->TabletID());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunDataErasure(ui64 generation, const TInstant& startTime) {
    return new TTxRunDataErasure(this, generation, startTime);
}

struct TSchemeShard::TTxCompleteDataErasure : public TSchemeShard::TRwTxBase {
    const TEvSchemeShard::TEvTenantDataErasureResponse::TPtr Ev;

    TTxCompleteDataErasure(TSelf* self, const TEvSchemeShard::TEvTenantDataErasureResponse::TPtr& ev)
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
        Self->ActiveDataErasureTenants[pathId] = EDataErasureStatus::COMPLETED;
        db.Table<Schema::ActiveDataErasureTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::ActiveDataErasureTenants::Status>(static_cast<ui32>(Self->ActiveDataErasureTenants[pathId]));
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasure Complete at schemeshard: " << Self->TabletID());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasure(TEvSchemeShard::TEvTenantDataErasureResponse::TPtr& ev) {
    return new TTxCompleteDataErasure(this, ev);
}

struct TSchemeShard::TTxDataErasureSchedulerInit : public TSchemeShard::TRwTxBase {
    TTxDataErasureSchedulerInit(TSelf* self)
        : TRwTxBase(self)
    {}

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxDataErasureSchedulerInit Execute at schemeshard: " << Self->TabletID());
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::DataErasureScheduler>().Key(0).Update<Schema::DataErasureScheduler::Status,
                                                               Schema::DataErasureScheduler::StartTime>(static_cast<ui32>(TDataErasureScheduler::EStatus::COMPLETED), AppData(ctx)->TimeProvider->Now().MicroSeconds());
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxDataErasureSchedulerInit Complete at schemeshard: " << Self->TabletID());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxDataErasureSchedulerInit() {
    return new TTxDataErasureSchedulerInit(this);
}

} // NKikimr::NSchemeShard
