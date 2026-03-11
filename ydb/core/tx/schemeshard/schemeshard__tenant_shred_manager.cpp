#include "schemeshard__tenant_shred_manager.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard {

namespace {

TString PrintStatus(const EShredStatus& status) {
    switch (status) {
    case EShredStatus::UNSPECIFIED:
        return "UNSPECIFIED";
    case EShredStatus::COMPLETED:
        return "COMPLITED";
    case EShredStatus::IN_PROGRESS:
        return "IN_PROGRESS";
    case EShredStatus::IN_PROGRESS_BSC:
        return "IN_PROGRESS_BSC";
    }
}

} // namespace

TTenantShredManager::TStarter::TStarter(TTenantShredManager* const manager)
    : Manager(manager)
{}

NOperationQueue::EStartStatus TTenantShredManager::TStarter::StartOperation(const TShardIdx& shardIdx) {
    return Manager->StartShredOperation(shardIdx);
}

NOperationQueue::EStartStatus TTenantShredManager::StartShredOperation(const TShardIdx& shardIdx) {
    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->ShardInfos.find(shardIdx);
    if (it == SchemeShard->ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Start] Failed to resolve shard info"
            << " for shred# " << shardIdx
            << " at schemeshard# " << SchemeShard->TabletID());
        return NOperationQueue::EStartStatus::EOperationRemove;
    }
    const auto& tabletId = it->second.TabletID;
    const auto& pathId = it->second.PathId;
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Start] Shred"
        << " for pathId# " << pathId
        << ", tabletId# " << tabletId
        << ", generation# " << Generation
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
    RunnigShardsCounter->Inc();
    WaitingShardsCounter->Dec();
    ActivePipes[shardIdx] = SchemeShard->PipeClientCache->Send(ctx, ui64(tabletId), request.release());
    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TTenantShredManager::TStarter::OnTimeout(const TShardIdx&) {
    // Do not use
}

TTenantShredManager::TTenantShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config)
    : SchemeShard(schemeShard)
    , Starter(this)
    , Queue(new TQueue(ConvertConfig(config), Starter))
{
    const auto ctx = SchemeShard->ActorContext();
    ctx.RegisterWithSameMailbox(Queue);

    TIntrusivePtr<NMonitoring::TDynamicCounters> rootCounters = AppData()->Counters;
    TIntrusivePtr<NMonitoring::TDynamicCounters> schemeShardCounters = GetServiceCounters(rootCounters, "SchemeShard");
    NMonitoring::TDynamicCounterPtr shredCounters = schemeShardCounters->GetSubgroup("subsystem", "TenantShredTotal");
    WaitingShardsCounter = shredCounters->GetSubgroup("TenantShred", "Waiting")->GetCounter("WaitingShredShards", true);
    RunnigShardsCounter = shredCounters->GetSubgroup("TenantShred", "Running")->GetCounter("RunningShredShards", true);
    CompletedShardsCounter = shredCounters->GetSubgroup("TenantShred", "Completed")->GetCounter("CompletedShredShards", true);

    const auto& tenantShredConfig = config.GetTenantDataErasureConfig();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Created: Rate# " << Queue->GetRate()
        << ", InflightLimit# " << tenantShredConfig.GetInflightLimit());
}

TTenantShredManager::TQueue::TConfig TTenantShredManager::ConvertConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TQueue::TConfig queueConfig;
    const auto& tenantShredConfig = config.GetTenantDataErasureConfig();
    queueConfig.IsCircular = false;
    queueConfig.MaxRate = tenantShredConfig.GetMaxRate();
    queueConfig.InflightLimit = tenantShredConfig.GetInflightLimit();
    queueConfig.Timeout = TDuration::Zero(); // unlimited
    return queueConfig;
}

void TTenantShredManager::UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TTenantShredManager::TQueue::TConfig queueConfig = ConvertConfig(config);
    Queue->UpdateConfig(queueConfig);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Config updated: Rate# " << Queue->GetRate()
        << ", InflightLimit# " << queueConfig.InflightLimit);
}

void TTenantShredManager::Start() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Start: "
        << "Status# " << PrintStatus(Status)
        << ", Generation# " << Generation);

    Queue->Start();
    if (Status == EShredStatus::COMPLETED) {
        std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(
            SchemeShard->ParentDomainId,
            SchemeShard->TenantShredManager->GetCompletedGeneration(),
            NKikimrScheme::TEvTenantShredResponse::COMPLETED
        );
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[Start] Send to domain schemeshard: "
            << "gen# " << SchemeShard->TenantShredManager->GetGeneration()
        );
        SchemeShard->PipeClientCache->Send(ctx, ui64(SchemeShard->ParentDomainId.OwnerId), response.release());
    } else if (Status == EShredStatus::IN_PROGRESS) {
        Queue->Clear();
        ActivePipes.clear();
        for (const auto& [shardIdx, status] : WaitingShredShards) {
            if (status == EShredStatus::IN_PROGRESS) {
                Queue->Enqueue(shardIdx);
            }
        }
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] Continue: "
            << "WaitingShredShards.size# " << WaitingShredShards.size()
            << ", Status# " << PrintStatus(Status));
    }
}

void TTenantShredManager::Stop() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Stop");
    Queue->Stop();
}

void TTenantShredManager::StartShred(NIceDb::TNiceDb& db, ui64 newGen) {
    const auto& ctx = SchemeShard->ActorContext();
    SetGeneration(newGen);
    Queue->Clear();
    ActivePipes.clear();
    for (const auto& [shardIdx, status] : WaitingShredShards) {
        db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }
    WaitingShredShards.clear();
    Status = EShredStatus::IN_PROGRESS;
    CompletedShardsCounter->Set(SchemeShard->ShardInfos.size());
    WaitingShardsCounter->Set(0);
    RunnigShardsCounter->Set(0);
    for (const auto& [shardIdx, shardInfo] : SchemeShard->ShardInfos) {
        switch (shardInfo.TabletType) {
        case NKikimr::NSchemeShard::ETabletType::DataShard:
        case NKikimr::NSchemeShard::ETabletType::PersQueue: {
            if (Queue->Enqueue(shardIdx)) {
                WaitingShredShards[shardIdx] = EShredStatus::IN_PROGRESS;
                WaitingShardsCounter->Inc();
                CompletedShardsCounter->Dec();
                db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingShredShards::Status>(WaitingShredShards[shardIdx]);
                LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "[TenantShredManager] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
            } else {
                LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "[TenantShredManager] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
            }
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

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] StartShred: "
        << "Generation# " << Generation
        << ", WaitingShredShards.size# " << WaitingShredShards.size()
        << ", Status# " << PrintStatus(Status));
}

void TTenantShredManager::StartShredForNewShards(NIceDb::TNiceDb& db, const std::vector<TShardIdx>& shredShards) {
    const auto& ctx = SchemeShard->ActorContext();
    for (const auto& shardIdx : shredShards) {
        if (Queue->Enqueue(shardIdx)) {
            WaitingShredShards[shardIdx] = EShredStatus::IN_PROGRESS;
            WaitingShardsCounter->Inc();
            db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingShredShards::Status>(WaitingShredShards[shardIdx]);
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[TenantShredManager] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[TenantShredManager] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << SchemeShard->TabletID());
        }
    }
    if (WaitingShredShards.empty()) {
        Status = EShredStatus::COMPLETED;
    }
    db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] StartShredForNewShards: "
        << "Generation# " << Generation
        << ", WaitingShredShards.size# " << WaitingShredShards.size()
        << ", Status# " << PrintStatus(Status));
}

void TTenantShredManager::FinishShred(NIceDb::TNiceDb& db, const TTabletId& tabletId) {
    const TShardIdx shardIdx = SchemeShard->GetShardIdx(tabletId);
    auto duration = Queue->OnDone(shardIdx);
    auto ctx = SchemeShard->ActorContext();
    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Finished] Failed to resolve shard info "
            << "for tabletId# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Finished] Shred is completed "
            << "for tabletId# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds() << " ms"
            << " at schemeshard " << SchemeShard->TabletID());
    }

    ActivePipes.erase(shardIdx);
    auto waitingShardIt = WaitingShredShards.find(shardIdx);
    if (waitingShardIt != WaitingShredShards.end()) {
        WaitingShredShards.erase(waitingShardIt);
        RunnigShardsCounter->Dec();
        CompletedShardsCounter->Inc();
        db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }
    if (WaitingShredShards.empty()) {
        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] Shred in shards is completed: "
            << "Generation# " << Generation
            << ". Send response to domain schemeshard");
        Queue->Clear();
        ActivePipes.clear();
        Status = EShredStatus::COMPLETED;
        CompletedGeneration = Generation;
        db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);
    }
}

void TTenantShredManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    if (tabletId == TTabletId(SchemeShard->ParentDomainId.OwnerId)) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantShredManager] [HandleDisconnect] Retry send response to domain schemeshard "
            << "form# " << SchemeShard->TabletID()
            << ", generation# " << SchemeShard->TenantShredManager->GetCompletedGeneration()
        );
        std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(
            SchemeShard->ParentDomainId,
            SchemeShard->TenantShredManager->GetCompletedGeneration(),
            NKikimrScheme::TEvTenantShredResponse::COMPLETED
        );
        SchemeShard->PipeClientCache->Send(ctx, ui64(SchemeShard->ParentDomainId.OwnerId), response.release());
        return;
    }
    auto tabletIt = SchemeShard->TabletIdToShardIdx.find(tabletId);
    if (tabletIt == SchemeShard->TabletIdToShardIdx.end()) {
        return; // just sanity check
    }
    const auto& shardIdx = tabletIt->second;
    auto it = ActivePipes.find(shardIdx);
    if (it == ActivePipes.end() ||
        it->second != clientId) {
        return;
    }
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantShredManager] [Disconnect] Shred disconnect "
        << "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());
    WaitingShardsCounter->Inc();
    RunnigShardsCounter->Dec();
    ActivePipes.erase(it);
    StartShredOperation(shardIdx);
}

void TTenantShredManager::RetryShred(const TTabletId& tabletId) {
    auto tabletIt = SchemeShard->TabletIdToShardIdx.find(tabletId);
    if (tabletIt == SchemeShard->TabletIdToShardIdx.end()) {
        return; // just sanity check
    }
    const auto& shardIdx = tabletIt->second;
    Queue->OnDone(shardIdx);
    Queue->Enqueue(shardIdx);
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
                if (Status == EShredStatus::COMPLETED) {
                    CompletedGeneration = Generation;
                }
            }
            if (!rowset.Next()) {
                return false;
            }
        }
    }
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
            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EShredStatus::IN_PROGRESS && WaitingShredShards.empty()) {
            Status = EShredStatus::COMPLETED;
        }
    }
    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[TenantShredManager] Restore: Generation# " << Generation
        << ", Status# " << PrintStatus(Status)
        << ", WaitingShredShardsg# " << WaitingShredShards.size()
        << ", CompletedGeneration# " << CompletedGeneration);
    return true;
}

bool TTenantShredManager::StopWaitingShred(const TShardIdx& shardIdx) {
    auto it = WaitingShredShards.find(shardIdx);
    if (it != WaitingShredShards.end()) {
        Queue->Remove(shardIdx);
        ActivePipes.erase(shardIdx);
        WaitingShredShards.erase(it);
        if (WaitingShredShards.empty()) {
            Status = EShredStatus::COMPLETED;
            CompletedGeneration = Generation;
            auto ctx = SchemeShard->ActorContext();
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[TenantShredManager] [StopWaitingShred] Shred in shards is completed for generation# " << CompletedGeneration
                << ". Send response to domain schemeshard");
        }
        return true;
    }
    return false;
}

struct TSchemeShard::TTxRunTenantShred : public TSchemeShard::TRwTxBase {
    TEvSchemeShard::TEvTenantShredRequest::TPtr Ev;
    std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> Response = nullptr;

    TTxRunTenantShred(TSelf *self, TEvSchemeShard::TEvTenantShredRequest::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_SHRED_TENANT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& record = Ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunTenantShred: Execute at schemestard: " << Self->TabletID()
        );
        auto& shredManager = Self->TenantShredManager;
        if (record.GetGeneration() > shredManager->GetGeneration()) {
            NIceDb::TNiceDb db(txc.DB);
            shredManager->StartShred(db, record.GetGeneration());
        }
        if (record.GetGeneration() < shredManager->GetGeneration() ||
            record.GetGeneration() == shredManager->GetGeneration() && shredManager->GetStatus() == EShredStatus::COMPLETED) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxRunTenantShred: Already complete"
                << " for requested generation# " << record.GetGeneration()
                << ", schemeshard# " << Self->TabletID()
                << ", requested from# " << Ev->Sender
            );
            Response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(Self->ParentDomainId, shredManager->GetGeneration(), NKikimrScheme::TEvTenantShredResponse::COMPLETED);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        if (Response) {
            ctx.Send(Ev->Sender, std::move(Response));
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunTenantShred(TEvSchemeShard::TEvTenantShredRequest::TPtr& ev) {
    return new TTxRunTenantShred(this, ev);
}

template <typename TEvType>
struct TSchemeShard::TTxCompleteShredShard : public TSchemeShard::TRwTxBase {
    TEvType Ev;
    std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> Response = nullptr;

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
            return;
        }
        const ui64 complitedGeneration = GetComplitedGeneration(Ev);
        auto& shredManager = Self->TenantShredManager;
        if (complitedGeneration != shredManager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredShard: Unknown generation#" << complitedGeneration
                << ", Expected gen# " << shredManager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        shredManager->FinishShred(db, TTabletId(GetTabletId(Ev)));
        if (shredManager->GetStatus() == EShredStatus::COMPLETED) {
            Response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(Self->ParentDomainId, shredManager->GetGeneration(), NKikimrScheme::TEvTenantShredResponse::COMPLETED);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        if (Response) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCompleteShredShard Complete. Send to domain schemeshard: "
                "gen# " << Self->TenantShredManager->GetGeneration()
            );
            Self->PipeClientCache->Send(ctx, ui64(Self->ParentDomainId.OwnerId), Response.release());
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
        Self->TenantShredManager->RetryShred(TTabletId(record.GetTabletId()));
    }

    ui64 GetComplitedGeneration(TEvDataShard::TEvVacuumResult::TPtr& ev) const {
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
        Self->TenantShredManager->RetryShred(TTabletId(record.tablet_id()));
    }

    ui64 GetComplitedGeneration(TEvKeyValue::TEvVacuumResponse::TPtr& ev) const {
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
    std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> Response = nullptr;

    TTxAddNewShardToShred(TSelf *self, TEvPrivate::TEvAddNewShardToShred::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_ADD_SHARDS_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxAddEntryToShred Execute at schemestard: " << Self->TabletID());

        auto& shredManager = Self->TenantShredManager;
        if (shredManager->GetStatus() != EShredStatus::IN_PROGRESS) {
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        if (!Ev->Get()->Shards.empty()) {
            shredManager->StartShredForNewShards(db, Ev->Get()->Shards);
        }
        if (shredManager->GetStatus() == EShredStatus::COMPLETED) {
            Response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(Self->ParentDomainId, shredManager->GetGeneration(), NKikimrScheme::TEvTenantShredResponse::COMPLETED);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        if (Response) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxAddEntryToShred Complete "
                << "at schemestard: " << Self->TabletID()
                << "Send to domain schemeshard: gen# " << Self->TenantShredManager->GetGeneration()
            );
            Self->PipeClientCache->Send(ctx, ui64(Self->ParentDomainId.OwnerId), Response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxAddNewShardToShred(TEvPrivate::TEvAddNewShardToShred::TPtr& ev) {
    return new TTxAddNewShardToShred(this, ev);
}

struct TSchemeShard::TTxCancelShredShards : public TSchemeShard::TRwTxBase {
    const std::vector<TShardIdx> ShredShards;
    std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> Response = nullptr;

    TTxCancelShredShards(TSelf *self, const std::vector<TShardIdx>& shredShards)
        : TRwTxBase(self)
        , ShredShards(std::move(shredShards))
    {}

    TTxType GetTxType() const override { return TXTYPE_CANCEL_SHARDS_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelShredShards Execute at schemestard: " << Self->TabletID());

        auto& shredManager = Self->TenantShredManager;
        if (shredManager->GetStatus() != EShredStatus::IN_PROGRESS) {
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& shard : ShredShards) {
            if (shredManager->StopWaitingShred(shard)) {
                db.Table<Schema::WaitingShredShards>().Key(shard.GetOwnerId(), shard.GetLocalId()).Delete();
            }
        }
        if (shredManager->GetStatus() == EShredStatus::COMPLETED) {
            db.Table<Schema::TenantShredGenerations>().Key(shredManager->GetGeneration()).Update<Schema::TenantShredGenerations::Status>(shredManager->GetStatus());
            Response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(Self->ParentDomainId, shredManager->GetGeneration(), NKikimrScheme::TEvTenantShredResponse::COMPLETED);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCancelShredShards Complete at schemestard: " << Self->TabletID());
        if (Response) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCancelShredShards Complete "
                << "at schemestard: " << Self->TabletID()
                << "Send to domain schemeshard: gen# " << Self->TenantShredManager->GetGeneration()
            );
            Self->PipeClientCache->Send(ctx, ui64(Self->ParentDomainId.OwnerId), Response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCancelShredShards(const std::vector<TShardIdx>& oldShards) {
    return new TTxCancelShredShards(this, oldShards);
}


} // NKikimr::NSchemeShard
