#include "schemeshard__shred_manager.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard {

namespace {

void SendResponseToRootSchemeShard(TSchemeShard* const schemeShard, const TActorContext& ctx) {
    std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> response(
        new TEvSchemeShard::TEvTenantShredResponse(schemeShard->ParentDomainId, schemeShard->ShredManager->GetGeneration(), NKikimrScheme::TEvTenantShredResponse::COMPLETED));

    const ui64 rootSchemeshard = schemeShard->ParentDomainId.OwnerId;
    schemeShard->PipeClientCache->Send(
        ctx,
        ui64(rootSchemeshard),
        response.release());
}

} // namespace

TTenantShredManager::TStarter::TStarter(TTenantShredManager* const manager)
    : Manager(manager)
{}

NOperationQueue::EStartStatus TTenantShredManager::TStarter::StartOperation(const TShardIdx& shardIdx) {
    return Manager->StartShred(shardIdx);
}

void TTenantShredManager::TStarter::OnTimeout(const TShardIdx& shardIdx) {
    Manager->OnTimeout(shardIdx);
}

TTenantShredManager::TTenantShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config)
    : TShredManager(schemeShard)
    , Starter(this)
    , Queue(new TQueue(ConvertConfig(config), Starter))
{
    const auto ctx = SchemeShard->ActorContext();
    ctx.RegisterWithSameMailbox(Queue);

    const auto& tenantShredConfig = config.GetTenantDataErasureConfig();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Created: Timeout# " << tenantShredConfig.GetTimeoutSeconds()
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << tenantShredConfig.GetInflightLimit());
}

void TTenantShredManager::UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TTenantShredManager::TQueue::TConfig queueConfig = ConvertConfig(config);
    Queue->UpdateConfig(queueConfig);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Config updated: Timeout# " << queueConfig.Timeout
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << queueConfig.InflightLimit);
}

void TTenantShredManager::Start() {
    TShredManager::Start();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Start: Status# " << static_cast<ui32>(Status));

    Queue->Start();
    if (Status == EShredStatus::COMPLETED) {
        SendResponseToRootSchemeShard();
    } else if (Status == EShredStatus::IN_PROGRESS) {
        ClearOperationQueue();
        Continue();
    }
}

void TTenantShredManager::Stop() {
    TShredManager::Stop();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Stop");

    Queue->Stop();
}

void TTenantShredManager::ClearOperationQueue() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Clear operation queue and active pipes");

    Queue->Clear();
    ActivePipes.clear();
}

void TTenantShredManager::WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [WakeupToRunShred] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantShredManager::ClearWaitingShredRequests(NIceDb::TNiceDb& db) {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Clear WaitingShredShards: Size# " << WaitingShredShards.size());

    for (const auto& [shardIdx, status] : WaitingShredShards) {
        db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }
    ClearWaitingShredRequests();
}

void TTenantShredManager::ClearWaitingShredRequests() {
    WaitingShredShards.clear();
}

void TTenantShredManager::Run(NIceDb::TNiceDb& db) {
    CounterShredOk = 0;
    CounterShredTimeout = 0;
    Status = EShredStatus::IN_PROGRESS;
    for (const auto& [shardIdx, shardInfo] : SchemeShard->ShardInfos) {
        switch (shardInfo.TabletType) {
        case NKikimr::NSchemeShard::ETabletType::DataShard:
        case NKikimr::NSchemeShard::ETabletType::PersQueue: {
            Enqueue(shardIdx);
            WaitingShredShards[shardIdx] = EShredStatus::IN_PROGRESS;
            db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingShredShards::Status>(WaitingShredShards[shardIdx]);
            break;
        }
        default:
            break;
        }
    }
    if (WaitingShredShards.empty()) {
        Status = EShredStatus::COMPLETED;
    }
    db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Run: Queue.Size# " << Queue->Size()
        << ", WaitingShredShards.size# " << WaitingShredShards.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TTenantShredManager::Continue() {
    for (const auto& [shardIdx, status] : WaitingShredShards) {
        if (status == EShredStatus::IN_PROGRESS) {
            Enqueue(shardIdx); // forward generation
        }
    }

    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Continue: Queue.Size# " << Queue->Size()
        << ", Status# " << static_cast<ui32>(Status));
}

NOperationQueue::EStartStatus TTenantShredManager::StartShred(const TShardIdx& shardIdx) {
    UpdateMetrics();

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->ShardInfos.find(shardIdx);
    if (it == SchemeShard->ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Start] Failed to resolve shard info "
            "for shred# " << shardIdx
            << " at schemeshard# " << SchemeShard->TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& tabletId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Start] Shred "
        "for pathId# " << pathId << ", tabletId# " << tabletId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " shards"
        << ", running# " << Queue->RunningSize() << " shards"
        << " at schemeshard " << SchemeShard->TabletID());

    std::unique_ptr<IEventBase> request = nullptr;
    switch (it->second.TabletType) {
    case NKikimr::NSchemeShard::ETabletType::DataShard: {
        request.reset(new TEvDataShard::TEvVacuum(Generation));
        break;
    }
    case NKikimr::NSchemeShard::ETabletType::PersQueue: {
        request.reset(new TEvKeyValue::TEvVacuumRequest(Generation));
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

void TTenantShredManager::OnTimeout(const TShardIdx& shardIdx) {
    CounterShredTimeout++;
    UpdateMetrics();

    ActivePipes.erase(shardIdx);

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->ShardInfos.find(shardIdx);
    if (it == SchemeShard->ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Timeout] Failed to resolve shard info "
            "for timeout shred# " << shardIdx
            << " at schemeshard# " << SchemeShard->TabletID());
        return;
    }

    const auto& tabletId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Timeout] Shred timeout "
        "for pathId# " << pathId << ", tabletId# " << tabletId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", in queue# " << Queue->Size() << " shards"
        << ", running# " << Queue->RunningSize() << " shards"
        << " at schemeshard " << SchemeShard->TabletID());

    // retry
    Enqueue(shardIdx);
}

void TTenantShredManager::Enqueue(const TShardIdx& shardIdx) {
    auto ctx = SchemeShard->ActorContext();

    if (Queue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
        WaitingShredShards[shardIdx] = EShredStatus::IN_PROGRESS;
        UpdateMetrics();
    }  else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
    }
}

void TTenantShredManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    if (tabletId == TTabletId(SchemeShard->ParentDomainId.OwnerId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] HandleDisconnect resend response to root schemeshard at schemeshard " << SchemeShard->TabletID());

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

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Disconnect] Shred disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());

    ActivePipes.erase(it);
    StartShred(shardIdx);
}

void TTenantShredManager::OnDone(const TPathId&, NIceDb::TNiceDb&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] [OnDone] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantShredManager::OnDone(const TTabletId& tabletId, NIceDb::TNiceDb& db) {
    const TShardIdx shardIdx = SchemeShard->GetShardIdx(tabletId);
    const auto it = SchemeShard->ShardInfos.find(shardIdx);

    auto duration = Queue->OnDone(shardIdx);

    auto ctx = SchemeShard->ActorContext();
    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Finished] Failed to resolve shard info "
            "for pathId# " << (it != SchemeShard->ShardInfos.end() ? it->second.PathId.ToString() : "") << ", tabletId# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " shards"
            << ", running# " << Queue->RunningSize() << " shards"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Finished] Shred is completed "
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
        auto it = WaitingShredShards.find(shardIdx);
        if (it != WaitingShredShards.end()) {
            db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
            WaitingShredShards.erase(it);
        }
    }

    CounterShredOk++;
    UpdateMetrics();

    if (WaitingShredShards.empty()) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] Shred in shards is completed. Send response to root schemeshard");
        Complete();
        db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);
    }
}

void TTenantShredManager::ScheduleRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] [ScheduleRequestToBSC] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantShredManager::SendRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] [SendRequestToBSC] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantShredManager::Complete() {
    Status = EShredStatus::COMPLETED;

    auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Complete: Generation# " << Generation);
}

bool TTenantShredManager::Restore(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<Schema::TenantShredGenerations>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            ui64 generation = rowset.GetValue<Schema::TenantShredGenerations::Generation>();
            if (generation >= Generation) {
                Generation = generation;
                Status = rowset.GetValue<Schema::TenantShredGenerations::Status>();
            }

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    ui64 numberShredShardsInRunning = 0;
    {
        auto rowset = db.Table<Schema::WaitingShredShards>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TOwnerId ownerId = rowset.GetValue<Schema::WaitingShredShards::OwnerShardIdx>();
            TLocalShardIdx localShardId = rowset.GetValue<Schema::WaitingShredShards::LocalShardIdx>();
            TShardIdx shardId(ownerId, localShardId);

            EShredStatus status = rowset.GetValue<Schema::WaitingShredShards::Status>();
            WaitingShredShards[shardId] = status;
            if (status == EShredStatus::IN_PROGRESS) {
                numberShredShardsInRunning++;
            }

            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EShredStatus::IN_PROGRESS && (WaitingShredShards.empty() || numberShredShardsInRunning == 0)) {
            Status = EShredStatus::COMPLETED;
        }
    }

    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Restore: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", NumberShredShardsInRunning# " << numberShredShardsInRunning);

    return true;
}

bool TTenantShredManager::Remove(const TPathId&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] [Remove] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
    return false;
}

bool TTenantShredManager::Remove(const TShardIdx& shardIdx) {
    auto it = WaitingShredShards.find(shardIdx);
    if (it != WaitingShredShards.end()) {
        Queue->Remove(shardIdx);
        ActivePipes.erase(shardIdx);
        WaitingShredShards.erase(it);
        if (WaitingShredShards.empty()) {
            auto ctx = SchemeShard->ActorContext();
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[TenantShredManager] [Remove] Shred in shards is completed. Send response to root schemeshard");
            Complete();
        }
        return true;
    }
    return false;
}

void TTenantShredManager::HandleNewPartitioning(const std::vector<TShardIdx>& shredShards, NIceDb::TNiceDb& db) {
    Status = EShredStatus::IN_PROGRESS;
    for (const auto& shardIdx : shredShards) {
        Enqueue(shardIdx); // forward generation
        db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingShredShards::Status>(WaitingShredShards[shardIdx]);
    }
    if (WaitingShredShards.empty()) {
        Status = EShredStatus::COMPLETED;
    }
    db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);

    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] HandleNewPartitioning: Queue.Size# " << Queue->Size()
        << ", WaitingShredShards.size# " << WaitingShredShards.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TTenantShredManager::SyncBscGeneration(NIceDb::TNiceDb&, ui64) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] [SyncBscGeneration] Cannot execute in tenant schemeshard: " << SchemeShard->TabletID());
}

void TTenantShredManager::UpdateMetrics() {
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_SHRED_QUEUE_SIZE].Set(Queue->Size());
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_SHRED_QUEUE_RUNNING].Set(Queue->RunningSize());
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_SHRED_OK].Set(CounterShredOk);
    SchemeShard->TabletCounters->Simple()[COUNTER_TENANT_SHRED_TIMEOUT].Set(CounterShredTimeout);
}

void TTenantShredManager::SendResponseToRootSchemeShard() {
    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] SendResponseToRootSchemeShard: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", RootSchemeshard# " << SchemeShard->ParentDomainId.OwnerId);

    NKikimr::NSchemeShard::SendResponseToRootSchemeShard(SchemeShard, ctx);
}


TTenantShredManager::TQueue::TConfig TTenantShredManager::ConvertConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TQueue::TConfig queueConfig;
    const auto& tenantShredConfig = config.GetTenantDataErasureConfig();
    queueConfig.IsCircular = false;
    queueConfig.MaxRate = tenantShredConfig.GetMaxRate();
    queueConfig.InflightLimit = tenantShredConfig.GetInflightLimit();
    queueConfig.Timeout = TDuration::Seconds(tenantShredConfig.GetTimeoutSeconds());

    return queueConfig;
}

struct TSchemeShard::TTxRunTenantShred : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvTenantShredRequest::TPtr Ev;
    bool NeedResponseComplete = false;

    TTxRunTenantShred(TSelf *self, TEvSchemeShard::TEvTenantShredRequest::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_SHRED_TENANT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantShred Execute at schemestard: " << Self->TabletID());

        if (Self->IsDomainSchemeShard) {
            LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShred] [Request] Cannot run shred on root schemeshard");
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        const auto& record = Ev->Get()->Record;
        auto& shredManager = Self->ShredManager;
        if (shredManager->GetGeneration() < record.GetGeneration()) {
            shredManager->SetGeneration(record.GetGeneration());
            shredManager->ClearOperationQueue();
            shredManager->ClearWaitingShredRequests(db);
            shredManager->Run(db);
        }
        if (Self->ShredManager->GetGeneration() == record.GetGeneration() && Self->ShredManager->GetStatus() == EShredStatus::COMPLETED) {
            NeedResponseComplete = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantShred Complete at schemestard: " << Self->TabletID()
            << ", NeedResponseComplete# " << (NeedResponseComplete ? "true" : "false"));

        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunTenantShred(TEvSchemeShard::TEvTenantShredRequest::TPtr& ev) {
    return new TTxRunTenantShred(this, ev);
}

template <typename TEvType>
struct TSchemeShard::TTxCompleteShredShard : public TSchemeShard::TRwTxBase {
    TEvType Ev;
    bool NeedResponseComplete = false;

    TTxCompleteShredShard(TSelf *self, TEvType& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_SHRED_SHARD; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredShard Execute at schemestard: " << Self->TabletID());

        if (!IsSuccess(Ev)) {
            HandleBadStatus(Ev, ctx);
            return; // will be retried after timeout in the queue in TTenantShredManager::OnTimeout()
        }

        const ui64 cleanupGeneration = GetCleanupGeneration(Ev);
        auto& manager = Self->ShredManager;
        if (cleanupGeneration != manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredShard: Unknown generation#" << cleanupGeneration
                                             << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        manager->OnDone(TTabletId(GetTabletId(Ev)), db);
        if (Self->ShredManager->GetStatus() == EShredStatus::COMPLETED) {
            NeedResponseComplete = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredShard Complete at schemestard: " << Self->TabletID()
            << ", NeedResponseComplete# " << (NeedResponseComplete ? "true" : "false"));

        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }

private:
    bool IsSuccess(TEvDataShard::TEvVacuumResult::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.GetStatus() == NKikimrTxDataShard::TEvVacuumResult::OK;
    }

    void HandleBadStatus(TEvDataShard::TEvVacuumResult::TPtr& ev, const TActorContext& ctx) const {
        const auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredShard: shred failed at DataShard#" << record.GetTabletId()
            << " with status: " << NKikimrTxDataShard::TEvVacuumResult::EStatus_Name(record.GetStatus())
            << ", schemestard: " << Self->TabletID());
    }

    ui64 GetCleanupGeneration(TEvDataShard::TEvVacuumResult::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.GetVacuumGeneration();
    }

    ui64 GetTabletId(TEvDataShard::TEvVacuumResult::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.GetTabletId();
    }

    bool IsSuccess(TEvKeyValue::TEvVacuumResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        return record.status() == NKikimrKeyValue::VacuumResponse::STATUS_SUCCESS;
    }

    void HandleBadStatus(TEvKeyValue::TEvVacuumResponse::TPtr& ev, const TActorContext& ctx) const {
        const auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredShard: shred failed at KeyValue#" << record.tablet_id()
            << " with status: " << NKikimrKeyValue::VacuumResponse::Status_Name(record.status())
            << ", schemestard: " << Self->TabletID());
    }

    ui64 GetCleanupGeneration(TEvKeyValue::TEvVacuumResponse::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.actual_generation();
    }

    ui64 GetTabletId(TEvKeyValue::TEvVacuumResponse::TPtr& ev) const {
        const auto& record = ev->Get()->Record;
        return record.tablet_id();
    }
};

template <typename TEvType>
NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteShredShard(TEvType& ev) {
    return new TTxCompleteShredShard(this, ev);
}

template NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteShredShard<TEvDataShard::TEvVacuumResult::TPtr>(TEvDataShard::TEvVacuumResult::TPtr& ev);
template NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteShredShard<TEvKeyValue::TEvVacuumResponse::TPtr>(TEvKeyValue::TEvVacuumResponse::TPtr& ev);

struct TSchemeShard::TTxAddNewShardToShred : public TSchemeShard::TRwTxBase {
    TEvPrivate::TEvAddNewShardToShred::TPtr Ev;
    bool NeedResponseComplete = false;

    TTxAddNewShardToShred(TSelf *self, TEvPrivate::TEvAddNewShardToShred::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ADD_SHARDS_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToShred Execute at schemestard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        Self->ShredManager->HandleNewPartitioning(Ev->Get()->Shards, db);
        if (Self->ShredManager->GetStatus() == EShredStatus::COMPLETED) {
            if (Self->ShredManager->IsRunning()) {
                NeedResponseComplete = true;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToShred Complete at schemestard: " << Self->TabletID());
        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxAddNewShardToShred(TEvPrivate::TEvAddNewShardToShred::TPtr& ev) {
    return new TTxAddNewShardToShred(this, ev);
}

struct TSchemeShard::TTxCancelShredShards : public TSchemeShard::TRwTxBase {
    const std::vector<TShardIdx> ShredShards;
    bool NeedResponseComplete = false;

    TTxCancelShredShards(TSelf *self, const std::vector<TShardIdx>& shredShards)
        : TRwTxBase(self)
        , ShredShards(std::move(shredShards))
    {}

    TTxType GetTxType() const override { return TXTYPE_CANCEL_SHARDS_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelShredShards Execute at schemestard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        for (const auto& shard : ShredShards) {
            if (Self->ShredManager->Remove(shard)) {
                db.Table<Schema::WaitingShredShards>().Key(shard.GetOwnerId(), shard.GetLocalId()).Delete();
            }
        }
        if (Self->ShredManager->GetStatus() == EShredStatus::COMPLETED) {
            db.Table<Schema::TenantShredGenerations>().Key(Self->ShredManager->GetGeneration()).Update<Schema::TenantShredGenerations::Status>(Self->ShredManager->GetStatus());
            if (Self->ShredManager->IsRunning()) {
                NeedResponseComplete = true;
            }
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelShredShards Complete at schemestard: " << Self->TabletID());
        if (NeedResponseComplete) {
            NKikimr::NSchemeShard::SendResponseToRootSchemeShard(Self, ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCancelShredShards(const std::vector<TShardIdx>& oldShards) {
    return new TTxCancelShredShards(this, oldShards);
}

} // NKikimr::NSchemeShard
