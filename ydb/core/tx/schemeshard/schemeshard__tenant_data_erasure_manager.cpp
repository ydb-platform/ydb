#include "schemeshard__data_erasure_manager.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard {

TTenantDataErasureManager::TStarter::TStarter(TTenantDataErasureManager* const manager)
    : Manager(manager)
{}

NOperationQueue::EStartStatus TTenantDataErasureManager::TStarter::StartOperation(const TShardIdx& shardIdx) {
    return Manager->StartDataErasure(shardIdx);
}

void TTenantDataErasureManager::TStarter::OnTimeout(const TShardIdx& shardIdx) {
    Manager->OnTimeout(shardIdx);
}

TTenantDataErasureManager::TTenantDataErasureManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config)
    : TDataErasureManager(schemeShard)
    , Starter(this)
    , Queue(new TQueue(ConvertConfig(config), Starter))
{
    const auto ctx = SchemeShard->ActorContext();
    ctx.RegisterWithSameMailbox(Queue);

    const auto& tenantDataErasureConfig = config.GetTenantDataErasureConfig();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Created: Timeout# " << tenantDataErasureConfig.GetTimeoutSeconds()
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << tenantDataErasureConfig.GetInflightLimit());
}

void TTenantDataErasureManager::UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TTenantDataErasureManager::TQueue::TConfig queueConfig = ConvertConfig(config);
    Queue->UpdateConfig(queueConfig);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Config updated: Timeout# " << queueConfig.Timeout
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << queueConfig.InflightLimit);
}

void TTenantDataErasureManager::Start() {
    TDataErasureManager::Start();
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Start: Status# " << static_cast<ui32>(Status));

    Queue->Start();
    if (Status == EStatus::COMPLETED) {
        SendResponseToRootSchemeShard();
    } else if (Status == EStatus::IN_PROGRESS) {
        ClearOperationQueue();
        Continue();
    }
}

void TTenantDataErasureManager::Stop() {
    TDataErasureManager::Stop();
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Stop");

    Queue->Stop();
}

void TTenantDataErasureManager::ClearOperationQueue() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Clear operation queue and active pipes");

    Queue->Clear();
    ActivePipes.clear();
}

void TTenantDataErasureManager::WakeupToRunDataErasure(TEvSchemeShard::TEvWakeupToRunDataErasure::TPtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [WakeupToRunDataErasure] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantDataErasureManager::ClearWaitingDataErasureRequests(NIceDb::TNiceDb& db) {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Clear WaitingDataErasureShards: Size# " << WaitingDataErasureShards.size());

    for (const auto& [shardIdx, status] : WaitingDataErasureShards) {
        db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }
    ClearWaitingDataErasureRequests();
}

void TTenantDataErasureManager::ClearWaitingDataErasureRequests() {
    WaitingDataErasureShards.clear();
}

void TTenantDataErasureManager::Run(NIceDb::TNiceDb& db) {
    Status = EStatus::IN_PROGRESS;
    for (const auto& [shardIdx, shardInfo] : SchemeShard->ShardInfos) {
        if (shardInfo.TabletType == ETabletType::DataShard) {
            Enqueue(shardIdx); // forward generation
            WaitingDataErasureShards[shardIdx] = EStatus::IN_PROGRESS;
            db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingDataErasureShards::Status>(static_cast<ui32>(WaitingDataErasureShards[shardIdx]));
        }
    }
    if (WaitingDataErasureShards.empty()) {
        Status = EStatus::COMPLETED;
    }
    db.Table<Schema::TenantDataErasureGenerations>().Key(Generation).Update<Schema::TenantDataErasureGenerations::Status>(static_cast<ui32>(Status));

    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Run: Queue.Size# " << Queue->Size()
        << ", WaitingDataErasureShards.size# " << WaitingDataErasureShards.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TTenantDataErasureManager::Continue() {
    for (const auto& [shardIdx, status] : WaitingDataErasureShards) {
        if (status == EStatus::IN_PROGRESS) {
            Enqueue(shardIdx); // forward generation
        }
    }

    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Continue: Queue.Size# " << Queue->Size()
        << ", Status# " << static_cast<ui32>(Status));
}

NOperationQueue::EStartStatus TTenantDataErasureManager::StartDataErasure(const TShardIdx& shardIdx) {
    UpdateMetrics();

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->ShardInfos.find(shardIdx);
    if (it == SchemeShard->ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Start] Failed to resolve shard info "
            "for data erasure# " << shardIdx
            << " at schemeshard# " << SchemeShard->TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Start] Data erasure "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " shards"
        << ", running# " << Queue->RunningSize() << " shards"
        << " at schemeshard " << SchemeShard->TabletID());

    std::unique_ptr<TEvDataShard::TEvForceDataCleanup> request(
        new TEvDataShard::TEvForceDataCleanup(Generation));

    ActivePipes[shardIdx] = SchemeShard->PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TTenantDataErasureManager::OnTimeout(const TShardIdx& shardIdx) {
    UpdateMetrics();
    SchemeShard->TabletCounters->Cumulative()[COUNTER_TENANT_DATA_ERASURE_TIMEOUT].Increment(1);

    ActivePipes.erase(shardIdx);

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->ShardInfos.find(shardIdx);
    if (it == SchemeShard->ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Timeout] Failed to resolve shard info "
            "for timeout data erasure# " << shardIdx
            << " at schemeshard# " << SchemeShard->TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Timeout] Data erasure timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", in queue# " << Queue->Size() << " shards"
        << ", running# " << Queue->RunningSize() << " shards"
        << " at schemeshard " << SchemeShard->TabletID());

    // retry
    Enqueue(shardIdx);
}

void TTenantDataErasureManager::Enqueue(const TShardIdx& shardIdx) {
    auto ctx = SchemeShard->ActorContext();

    if (Queue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasureManager] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
        WaitingDataErasureShards[shardIdx] = EStatus::IN_PROGRESS;
        UpdateMetrics();
    }  else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasureManager] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
    }
}

void TTenantDataErasureManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Disconnect] Data erasure disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());

    if (tabletId == TTabletId(SchemeShard->ParentDomainId.OwnerId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasureManager] HandleDisconnect resend response to root schemeshard at schemeshard " << SchemeShard->TabletID());

        SendResponseToRootSchemeShard();
        return;
    }
    auto tabletIt = SchemeShard->TabletIdToShardIdx.find(tabletId);
    if (tabletIt == SchemeShard->TabletIdToShardIdx.end()) {
        return; // just sanity check
    }

    const auto& shardIdx = tabletIt->second;
    auto it = ActivePipes.find(shardIdx);
    if (it == ActivePipes.end()) {
        return;
    }

    if (it->second != clientId) {
        return;
    }

    ActivePipes.erase(it);
    StartDataErasure(shardIdx);
}

void TTenantDataErasureManager::OnDone(const TPathId&, NIceDb::TNiceDb&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] [OnDone] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantDataErasureManager::OnDone(const TTabletId& tabletId, NIceDb::TNiceDb& db) {
    const TShardIdx shardIdx = SchemeShard->GetShardIdx(tabletId);
    const auto it = SchemeShard->ShardInfos.find(shardIdx);

    auto duration = Queue->OnDone(shardIdx);

    auto ctx = SchemeShard->ActorContext();
    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Finished] Failed to resolve shard info "
            "for pathId# " << (it != SchemeShard->ShardInfos.end() ? it->second.PathId.ToString() : "") << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " shards"
            << ", running# " << Queue->RunningSize() << " shards"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Finished] Data erasure is completed "
            "for pathId# " << (it != SchemeShard->ShardInfos.end() ? it->second.PathId.ToString() : "") << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " shards"
            << ", running# " << Queue->RunningSize() << " shards"
            << " at schemeshard " << SchemeShard->TabletID());
    }

    ActivePipes.erase(shardIdx);
    {
        auto it = WaitingDataErasureShards.find(shardIdx);
        if (it != WaitingDataErasureShards.end()) {
            it->second = EStatus::COMPLETED;
        }
        db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingDataErasureShards::Status>(static_cast<ui32>(it->second));
    }

    SchemeShard->TabletCounters->Cumulative()[COUNTER_TENANT_DATA_ERASURE_OK].Increment(1);
    UpdateMetrics();

    bool isTenantDataErasureCompleted = true;
    for (const auto& [shardIdx, status] : WaitingDataErasureShards) {
        if (status == EStatus::IN_PROGRESS) {
            isTenantDataErasureCompleted = false;
        }
    }
    if (isTenantDataErasureCompleted) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasureManager] Data erasure in shards is completed. Send response to root schemeshard");
        Complete();
        db.Table<Schema::TenantDataErasureGenerations>().Key(Generation).Update<Schema::TenantDataErasureGenerations::Status>(static_cast<ui32>(Status));
    }
}

void TTenantDataErasureManager::ScheduleRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] [ScheduleRequestToBSC] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantDataErasureManager::SendRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] [SendRequestToBSC] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantDataErasureManager::Complete() {
    Status = EStatus::COMPLETED;

    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Complete: Generation# " << Generation);
}

bool TTenantDataErasureManager::Restore(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<Schema::TenantDataErasureGenerations>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            ui64 generation = rowset.GetValue<Schema::TenantDataErasureGenerations::Generation>();
            if (generation >= Generation) {
                Generation = generation;
                ui32 statusValue = rowset.GetValue<Schema::TenantDataErasureGenerations::Status>();
                Status = EStatus::UNSPECIFIED;
                if (statusValue >= static_cast<ui32>(EStatus::UNSPECIFIED) &&
                    statusValue <= static_cast<ui32>(EStatus::IN_PROGRESS_BSC)) {
                        Status = static_cast<EStatus>(statusValue);
                }
            }

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    ui64 numberDataErasureShardsInRunning = 0;
    {
        auto rowset = db.Table<Schema::WaitingDataErasureShards>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TOwnerId ownerId = rowset.GetValue<Schema::WaitingDataErasureShards::OwnerShardIdx>();
            TLocalShardIdx localShardId = rowset.GetValue<Schema::WaitingDataErasureShards::LocalShardIdx>();
            TShardIdx shardId(ownerId, localShardId);

            ui32 statusValue = rowset.GetValue<Schema::WaitingDataErasureShards::Status>();
            EStatus status = EStatus::COMPLETED;
            if (statusValue >= static_cast<ui32>(EStatus::UNSPECIFIED) &&
                statusValue <= static_cast<ui32>(EStatus::IN_PROGRESS_BSC)) {
                    status = static_cast<EStatus>(statusValue);
            }
            WaitingDataErasureShards[shardId] = status;
            if (status == EStatus::IN_PROGRESS) {
                numberDataErasureShardsInRunning++;
            }

            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EStatus::IN_PROGRESS && (WaitingDataErasureShards.empty() || numberDataErasureShardsInRunning == 0)) {
            Status = EStatus::COMPLETED;
        }
    }

    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Restore: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", NumberDataErasureShardsInRunning# " << numberDataErasureShardsInRunning);

    return true;
}

bool TTenantDataErasureManager::Remove(const TPathId&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] [Remove] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
    return false;
}

bool TTenantDataErasureManager::Remove(const TShardIdx& shardIdx) {
    auto it = WaitingDataErasureShards.find(shardIdx);
    if (it != WaitingDataErasureShards.end()) {
        Queue->Remove(shardIdx);
        ActivePipes.erase(shardIdx);
        it->second = EStatus::COMPLETED;
        bool isTenantDataErasureCompleted = true;
        for (const auto& [shardIdx, status] : WaitingDataErasureShards) {
            if (status == EStatus::IN_PROGRESS) {
                isTenantDataErasureCompleted = false;
            }
        }
        if (isTenantDataErasureCompleted) {
            auto ctx = SchemeShard->ActorContext();
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[TenantDataErasureManager] [Remove] Data erasure in shards is completed. Send response to root schemeshard");
            Complete();
        }
        return true;
    }
    return false;
}

void TTenantDataErasureManager::HandleNewPartitioning(const std::vector<TShardIdx>& dataErasureShards, NIceDb::TNiceDb& db) {
    Status = EStatus::IN_PROGRESS;
    for (const auto& shardIdx : dataErasureShards) {
        Enqueue(shardIdx); // forward generation
        db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingDataErasureShards::Status>(static_cast<ui32>(WaitingDataErasureShards[shardIdx]));
    }
    if (WaitingDataErasureShards.empty()) {
        Status = EStatus::COMPLETED;
    }
    db.Table<Schema::TenantDataErasureGenerations>().Key(Generation).Update<Schema::TenantDataErasureGenerations::Status>(static_cast<ui32>(Status));

    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] HandleNewPartitioning: Queue.Size# " << Queue->Size()
        << ", WaitingDataErasureShards.size# " << WaitingDataErasureShards.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TTenantDataErasureManager::UpdateMetrics() {
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_SIZE].Set(Queue->Size());
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_RUNNING].Set(Queue->RunningSize());
}

void TTenantDataErasureManager::SendResponseToRootSchemeShard() {
    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] SendResponseToRootSchemeShard: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", RootSchemeshard# " << SchemeShard->ParentDomainId.OwnerId);

    std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureResponse> response(
        new TEvSchemeShard::TEvTenantDataErasureResponse(SchemeShard->ParentDomainId, Generation, TEvSchemeShard::TEvTenantDataErasureResponse::EStatus::COMPLETED));

    const ui64 rootSchemeshard = SchemeShard->ParentDomainId.OwnerId;
    SchemeShard->PipeClientCache->Send(
        ctx,
        ui64(rootSchemeshard),
        response.release());
}


TTenantDataErasureManager::TQueue::TConfig TTenantDataErasureManager::ConvertConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TQueue::TConfig queueConfig;
    const auto& tenantDataErasureConfig = config.GetTenantDataErasureConfig();
    queueConfig.IsCircular = false;
    queueConfig.MaxRate = tenantDataErasureConfig.GetMaxRate();
    queueConfig.InflightLimit = tenantDataErasureConfig.GetInflightLimit();
    queueConfig.Timeout = TDuration::Seconds(tenantDataErasureConfig.GetTimeoutSeconds());

    return queueConfig;
}

struct TSchemeShard::TTxRunTenantDataErasure : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvTenantDataErasureRequest::TPtr Ev;
    bool NeedResponseComplete = false;

    TTxRunTenantDataErasure(TSelf *self, TEvSchemeShard::TEvTenantDataErasureRequest::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_DATA_ERASURE_TENANT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantDataErasure Execute at schemestard: " << Self->TabletID());

        if (Self->IsDomainSchemeShard) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Request] Cannot run data erasure on root schemeshard");
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        const auto& record = Ev->Get()->Record;
        auto& dataErasureManager = Self->DataErasureManager;
        if (dataErasureManager->GetGeneration() < record.GetGeneration()) {
            dataErasureManager->SetGeneration(record.GetGeneration());
            dataErasureManager->ClearOperationQueue();
            dataErasureManager->ClearWaitingDataErasureRequests(db);
            dataErasureManager->Run(db);
        }
        if (Self->DataErasureManager->GetGeneration() == record.GetGeneration() && Self->DataErasureManager->GetStatus() == TDataErasureManager::EStatus::COMPLETED) {
            NeedResponseComplete = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantDataErasure Complete at schemestard: " << Self->TabletID()
            << ", NeedResponseComplete# " << (NeedResponseComplete ? "true" : "false"));

        if (NeedResponseComplete) {
            std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureResponse> response(
                new TEvSchemeShard::TEvTenantDataErasureResponse(Self->ParentDomainId, Self->DataErasureManager->GetGeneration(), TEvSchemeShard::TEvTenantDataErasureResponse::EStatus::COMPLETED));

            const ui64 rootSchemeshard = Self->ParentDomainId.OwnerId;
            Self->PipeClientCache->Send(
                ctx,
                ui64(rootSchemeshard),
                response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunTenantDataErasure(TEvSchemeShard::TEvTenantDataErasureRequest::TPtr& ev) {
    return new TTxRunTenantDataErasure(this, ev);
}

struct TSchemeShard::TTxCompleteDataErasureShard : public TSchemeShard::TRwTxBase {
    TEvDataShard::TEvForceDataCleanupResult::TPtr Ev;
    bool NeedResponseComplete = false;

    TTxCompleteDataErasureShard(TSelf *self, TEvDataShard::TEvForceDataCleanupResult::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_DATA_ERASURE_TENANT ; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureShard Execute at schemestard: " << Self->TabletID());
        const auto& record = Ev->Get()->Record;

        auto& manager = Self->DataErasureManager;
        const ui64 cleanupGeneration = record.GetDataCleanupGeneration();
        if (cleanupGeneration != manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteDataErasureShard: Unknown generation#" << cleanupGeneration
                                             << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        manager->OnDone(TTabletId(record.GetTabletId()), db);
        if (Self->DataErasureManager->GetStatus() == TDataErasureManager::EStatus::COMPLETED) {
            NeedResponseComplete = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureShard Complete at schemestard: " << Self->TabletID()
            << ", NeedResponseComplete# " << (NeedResponseComplete ? "true" : "false"));

        if (NeedResponseComplete) {
            std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureResponse> response(
                new TEvSchemeShard::TEvTenantDataErasureResponse(Self->ParentDomainId, Self->DataErasureManager->GetGeneration(), TEvSchemeShard::TEvTenantDataErasureResponse::EStatus::COMPLETED));

            const ui64 rootSchemeshard = Self->ParentDomainId.OwnerId;
            Self->PipeClientCache->Send(
                ctx,
                ui64(rootSchemeshard),
                response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasureShard(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev) {
    return new TTxCompleteDataErasureShard(this, ev);
}

struct TSchemeShard::TTxAddEntryToDataErasure : public TSchemeShard::TRwTxBase {
    const std::vector<TShardIdx> DataErasureShards;
    bool NeedResponseComplete = false;

    TTxAddEntryToDataErasure(TSelf *self, const std::vector<TShardIdx>& dataErasureShards)
        : TRwTxBase(self)
        , DataErasureShards(std::move(dataErasureShards))
    {}

    TTxType GetTxType() const override { return TXTYPE_ADD_SHARDS_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToDataErasure Execute at schemestard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        Self->DataErasureManager->HandleNewPartitioning(DataErasureShards, db);
        if (Self->DataErasureManager->GetStatus() == TDataErasureManager::EStatus::COMPLETED) {
            if (Self->DataErasureManager->IsRunning()) {
                NeedResponseComplete = true;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToDataErasure Complete at schemestard: " << Self->TabletID());
        if (NeedResponseComplete) {
            std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureResponse> response(
                new TEvSchemeShard::TEvTenantDataErasureResponse(Self->ParentDomainId, Self->DataErasureManager->GetGeneration(), TEvSchemeShard::TEvTenantDataErasureResponse::EStatus::COMPLETED));

            const ui64 rootSchemeshard = Self->ParentDomainId.OwnerId;
            Self->PipeClientCache->Send(
                ctx,
                ui64(rootSchemeshard),
                response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxAddEntryToDataErasure(const std::vector<TShardIdx>& dataErasureShards) {
    return new TTxAddEntryToDataErasure(this, dataErasureShards);
}

struct TSchemeShard::TTxCancelDataErasureShards : public TSchemeShard::TRwTxBase {
    const std::vector<TShardIdx> DataErasureShards;
    bool NeedResponseComplete = false;

    TTxCancelDataErasureShards(TSelf *self, const std::vector<TShardIdx>& dataErasureShards)
        : TRwTxBase(self)
        , DataErasureShards(std::move(dataErasureShards))
    {}

    TTxType GetTxType() const override { return TXTYPE_CANCEL_SHARDS_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelDataErasureShards Execute at schemestard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        for (const auto& shard : DataErasureShards) {
            if (Self->DataErasureManager->Remove(shard)) {
                db.Table<Schema::WaitingDataErasureShards>().Key(shard.GetOwnerId(), shard.GetLocalId()).Update<Schema::WaitingDataErasureShards::Status>(static_cast<ui32>(TDataErasureManager::EStatus::COMPLETED));
            }
        }
        if (Self->DataErasureManager->GetStatus() == TDataErasureManager::EStatus::COMPLETED) {
            db.Table<Schema::TenantDataErasureGenerations>().Key(Self->DataErasureManager->GetGeneration()).Update<Schema::TenantDataErasureGenerations::Status>(static_cast<ui32>(Self->DataErasureManager->GetStatus()));
            if (Self->DataErasureManager->IsRunning()) {
                NeedResponseComplete = true;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelDataErasureShards Complete at schemestard: " << Self->TabletID());
        if (NeedResponseComplete) {
            std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureResponse> response(
                new TEvSchemeShard::TEvTenantDataErasureResponse(Self->ParentDomainId, Self->DataErasureManager->GetGeneration(), TEvSchemeShard::TEvTenantDataErasureResponse::EStatus::COMPLETED));

            const ui64 rootSchemeshard = Self->ParentDomainId.OwnerId;
            Self->PipeClientCache->Send(
                ctx,
                ui64(rootSchemeshard),
                response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCancelDataErasureShards(const std::vector<TShardIdx>& oldShards) {
    return new TTxCancelDataErasureShards(this, oldShards);
}

} // NKikimr::NSchemeShard
