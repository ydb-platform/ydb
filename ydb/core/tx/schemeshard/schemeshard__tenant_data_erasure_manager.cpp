#include "schemeshard__data_erasure_manager.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/keyvalue/keyvalue_events.h>

namespace NKikimr::NSchemeShard {

namespace {

void SendResponseToRootSchemeShard(TSchemeShard* const schemeShard, const TActorContext& ctx) {
    std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureResponse> response(
        new TEvSchemeShard::TEvTenantDataErasureResponse(schemeShard->ParentDomainId, schemeShard->DataErasureManager->GetGeneration(), NKikimrScheme::TEvTenantDataErasureResponse::COMPLETED));

    const ui64 rootSchemeshard = schemeShard->ParentDomainId.OwnerId;
    schemeShard->PipeClientCache->Send(
        ctx,
        ui64(rootSchemeshard),
        response.release());
}

} // namespace

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
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Start: Status# " << static_cast<ui32>(Status));

    Queue->Start();
    if (Status == EDataErasureStatus::COMPLETED) {
        SendResponseToRootSchemeShard();
    } else if (Status == EDataErasureStatus::IN_PROGRESS) {
        ClearOperationQueue();
        Continue();
    }
}

void TTenantDataErasureManager::Stop() {
    TDataErasureManager::Stop();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
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
    CounterDataErasureOk = 0;
    CounterDataErasureTimeout = 0;
    Status = EDataErasureStatus::IN_PROGRESS;
    for (const auto& [shardIdx, shardInfo] : SchemeShard->ShardInfos) {
        switch (shardInfo.TabletType) {
        case NKikimr::NSchemeShard::ETabletType::DataShard:
        case NKikimr::NSchemeShard::ETabletType::PersQueue: {
            Enqueue(shardIdx);
            WaitingDataErasureShards[shardIdx] = EDataErasureStatus::IN_PROGRESS;
            db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingDataErasureShards::Status>(WaitingDataErasureShards[shardIdx]);
            break;
        }
        default:
            break;
        }
    }
    if (WaitingDataErasureShards.empty()) {
        Status = EDataErasureStatus::COMPLETED;
    }
    db.Table<Schema::TenantDataErasureGenerations>().Key(Generation).Update<Schema::TenantDataErasureGenerations::Status>(Status);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] Run: Queue.Size# " << Queue->Size()
        << ", WaitingDataErasureShards.size# " << WaitingDataErasureShards.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TTenantDataErasureManager::Continue() {
    for (const auto& [shardIdx, status] : WaitingDataErasureShards) {
        if (status == EDataErasureStatus::IN_PROGRESS) {
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

    const auto& tabletId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Start] Data erasure "
        "for pathId# " << pathId << ", tabletId# " << tabletId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " shards"
        << ", running# " << Queue->RunningSize() << " shards"
        << " at schemeshard " << SchemeShard->TabletID());

    std::unique_ptr<IEventBase> request = nullptr;
    switch (it->second.TabletType) {
    case NKikimr::NSchemeShard::ETabletType::DataShard: {
        request.reset(new TEvDataShard::TEvForceDataCleanup(Generation));
        break;
    }
    case NKikimr::NSchemeShard::ETabletType::PersQueue: {
        request.reset(new TEvKeyValue::TEvCleanUpDataRequest(Generation));
        break;
    }
    default:
        return NOperationQueue::EStartStatus::EOperationRemove;
    }



    ActivePipes[shardIdx] = SchemeShard->PipeClientCache->Send(
        ctx,
        ui64(tabletId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TTenantDataErasureManager::OnTimeout(const TShardIdx& shardIdx) {
    CounterDataErasureTimeout++;
    UpdateMetrics();

    ActivePipes.erase(shardIdx);

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->ShardInfos.find(shardIdx);
    if (it == SchemeShard->ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Timeout] Failed to resolve shard info "
            "for timeout data erasure# " << shardIdx
            << " at schemeshard# " << SchemeShard->TabletID());
        return;
    }

    const auto& tabletId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Timeout] Data erasure timeout "
        "for pathId# " << pathId << ", tabletId# " << tabletId
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
        WaitingDataErasureShards[shardIdx] = EDataErasureStatus::IN_PROGRESS;
        UpdateMetrics();
    }  else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasureManager] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
    }
}

void TTenantDataErasureManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
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

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Disconnect] Data erasure disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());

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
            "for pathId# " << (it != SchemeShard->ShardInfos.end() ? it->second.PathId.ToString() : "") << ", tabletId# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " shards"
            << ", running# " << Queue->RunningSize() << " shards"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasureManager] [Finished] Data erasure is completed "
            "for pathId# " << (it != SchemeShard->ShardInfos.end() ? it->second.PathId.ToString() : "") << ", tabletId# " << tabletId
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
            db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
            WaitingDataErasureShards.erase(it);
        }
    }

    CounterDataErasureOk++;
    UpdateMetrics();

    if (WaitingDataErasureShards.empty()) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasureManager] Data erasure in shards is completed. Send response to root schemeshard");
        Complete();
        db.Table<Schema::TenantDataErasureGenerations>().Key(Generation).Update<Schema::TenantDataErasureGenerations::Status>(Status);
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
    Status = EDataErasureStatus::COMPLETED;

    auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
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
                Status = rowset.GetValue<Schema::TenantDataErasureGenerations::Status>();
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

            EDataErasureStatus status = rowset.GetValue<Schema::WaitingDataErasureShards::Status>();
            WaitingDataErasureShards[shardId] = status;
            if (status == EDataErasureStatus::IN_PROGRESS) {
                numberDataErasureShardsInRunning++;
            }

            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EDataErasureStatus::IN_PROGRESS && (WaitingDataErasureShards.empty() || numberDataErasureShardsInRunning == 0)) {
            Status = EDataErasureStatus::COMPLETED;
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
        WaitingDataErasureShards.erase(it);
        if (WaitingDataErasureShards.empty()) {
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
    Status = EDataErasureStatus::IN_PROGRESS;
    for (const auto& shardIdx : dataErasureShards) {
        Enqueue(shardIdx); // forward generation
        db.Table<Schema::WaitingDataErasureShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingDataErasureShards::Status>(WaitingDataErasureShards[shardIdx]);
    }
    if (WaitingDataErasureShards.empty()) {
        Status = EDataErasureStatus::COMPLETED;
    }
    db.Table<Schema::TenantDataErasureGenerations>().Key(Generation).Update<Schema::TenantDataErasureGenerations::Status>(Status);

    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] HandleNewPartitioning: Queue.Size# " << Queue->Size()
        << ", WaitingDataErasureShards.size# " << WaitingDataErasureShards.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TTenantDataErasureManager::SyncBscGeneration(NIceDb::TNiceDb&, ui64) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] [SyncBscGeneration] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantDataErasureManager::UpdateMetrics() {
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_SIZE].Set(Queue->Size());
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_RUNNING].Set(Queue->RunningSize());
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_OK].Set(CounterDataErasureOk);
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_TIMEOUT].Set(CounterDataErasureTimeout);
}

void TTenantDataErasureManager::SendResponseToRootSchemeShard() {
    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantDataErasureManager] SendResponseToRootSchemeShard: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", RootSchemeshard# " << SchemeShard->ParentDomainId.OwnerId);

    NKikimr::NSchemeShard::SendResponseToRootSchemeShard(SchemeShard, ctx);
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
        if (Self->DataErasureManager->GetGeneration() == record.GetGeneration() && Self->DataErasureManager->GetStatus() == EDataErasureStatus::COMPLETED) {
            NeedResponseComplete = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantDataErasure Complete at schemestard: " << Self->TabletID()
            << ", NeedResponseComplete# " << (NeedResponseComplete ? "true" : "false"));

        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunTenantDataErasure(TEvSchemeShard::TEvTenantDataErasureRequest::TPtr& ev) {
    return new TTxRunTenantDataErasure(this, ev);
}

template <typename TEvType>
struct TSchemeShard::TTxCompleteDataErasureShard : public TSchemeShard::TRwTxBase {
    TEvType Ev;
    bool NeedResponseComplete = false;

    TTxCompleteDataErasureShard(TSelf *self, TEvType& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_DATA_ERASURE_SHARD; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureShard Execute at schemestard: " << Self->TabletID());

        if (!IsSuccess(Ev)) {
            HandleBadStatus(Ev, ctx);
            return; // will be retried after timeout in the queue in TTenantDataErasureManager::OnTimeout()
        }

        const ui64 cleanupGeneration = GetCleanupGeneration(Ev);
        auto& manager = Self->DataErasureManager;
        if (cleanupGeneration != manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteDataErasureShard: Unknown generation#" << cleanupGeneration
                                             << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        manager->OnDone(TTabletId(GetTabletId(Ev)), db);
        if (Self->DataErasureManager->GetStatus() == EDataErasureStatus::COMPLETED) {
            NeedResponseComplete = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureShard Complete at schemestard: " << Self->TabletID()
            << ", NeedResponseComplete# " << (NeedResponseComplete ? "true" : "false"));

        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }

private:
    bool IsSuccess(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.GetStatus() == NKikimrTxDataShard::TEvForceDataCleanupResult::OK;
    }

    void HandleBadStatus(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev, const TActorContext& ctx) const {
        const auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureShard: data erasure failed at DataShard#" << record.GetTabletId()
            << " with status: " << NKikimrTxDataShard::TEvForceDataCleanupResult::EStatus_Name(record.GetStatus())
            << ", schemestard: " << Self->TabletID());
    }

    ui64 GetCleanupGeneration(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.GetDataCleanupGeneration();
    }

    ui64 GetTabletId(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.GetTabletId();
    }

    bool IsSuccess(TEvKeyValue::TEvCleanUpDataResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        return record.status() == NKikimrKeyValue::CleanUpDataResponse::STATUS_SUCCESS;
    }

    void HandleBadStatus(TEvKeyValue::TEvCleanUpDataResponse::TPtr& ev, const TActorContext& ctx) const {
        const auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureShard: data erasure failed at KeyValue#" << record.tablet_id()
            << " with status: " << NKikimrKeyValue::CleanUpDataResponse::Status_Name(record.status())
            << ", schemestard: " << Self->TabletID());
    }

    ui64 GetCleanupGeneration(TEvKeyValue::TEvCleanUpDataResponse::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.actual_generation();
    }

    ui64 GetTabletId(TEvKeyValue::TEvCleanUpDataResponse::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.tablet_id();
    }
};

template <typename TEvType>
NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasureShard(TEvType& ev) {
    return new TTxCompleteDataErasureShard(this, ev);
}

template NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasureShard<TEvDataShard::TEvForceDataCleanupResult::TPtr>(TEvDataShard::TEvForceDataCleanupResult::TPtr& ev);
template NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasureShard<TEvKeyValue::TEvCleanUpDataResponse::TPtr>(TEvKeyValue::TEvCleanUpDataResponse::TPtr& ev);

struct TSchemeShard::TTxAddNewShardToDataErasure : public TSchemeShard::TRwTxBase {
    TEvPrivate::TEvAddNewShardToDataErasure::TPtr Ev;
    bool NeedResponseComplete = false;

    TTxAddNewShardToDataErasure(TSelf *self, TEvPrivate::TEvAddNewShardToDataErasure::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ADD_SHARDS_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToDataErasure Execute at schemestard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        Self->DataErasureManager->HandleNewPartitioning(Ev->Get()->Shards, db);
        if (Self->DataErasureManager->GetStatus() == EDataErasureStatus::COMPLETED) {
            if (Self->DataErasureManager->IsRunning()) {
                NeedResponseComplete = true;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToDataErasure Complete at schemestard: " << Self->TabletID());
        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxAddNewShardToDataErasure(TEvPrivate::TEvAddNewShardToDataErasure::TPtr& ev) {
    return new TTxAddNewShardToDataErasure(this, ev);
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
                db.Table<Schema::WaitingDataErasureShards>().Key(shard.GetOwnerId(), shard.GetLocalId()).Delete();
            }
        }
        if (Self->DataErasureManager->GetStatus() == EDataErasureStatus::COMPLETED) {
            db.Table<Schema::TenantDataErasureGenerations>().Key(Self->DataErasureManager->GetGeneration()).Update<Schema::TenantDataErasureGenerations::Status>(Self->DataErasureManager->GetStatus());
            if (Self->DataErasureManager->IsRunning()) {
                NeedResponseComplete = true;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelDataErasureShards Complete at schemestard: " << Self->TabletID());
        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCancelDataErasureShards(const std::vector<TShardIdx>& oldShards) {
    return new TTxCancelDataErasureShards(this, oldShards);
}

} // NKikimr::NSchemeShard
