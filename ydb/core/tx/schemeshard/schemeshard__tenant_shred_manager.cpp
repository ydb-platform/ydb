#include "schemeshard__tenant_shred_manager.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

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
        YDB_LOG_CTX_WARN(ctx, "[TenantShredManager] [Start] Failed to resolve shard info for",
            {"shred", shardIdx},
            {"at_schemeshard", SchemeShard->TabletID()});
        return NOperationQueue::EStartStatus::EOperationRemove;
    }
    const auto& tabletId = it->second.TabletID;
    const auto& pathId = it->second.PathId;
    YDB_LOG_CTX_INFO(ctx, "[TenantShredManager] [Start] Shred for at schemeshard",
        {"pathId", pathId},
        {"tabletId", tabletId},
        {"generation", Generation},
        {"TabletID", SchemeShard->TabletID()});

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
    RunningShardsCounter->Inc();
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
    RunningShardsCounter = shredCounters->GetSubgroup("TenantShred", "Running")->GetCounter("RunningShredShards", true);
    CompletedShardsCounter = shredCounters->GetSubgroup("TenantShred", "Completed")->GetCounter("CompletedShredShards", true);

    const auto& tenantShredConfig = config.GetTenantDataErasureConfig();
    YDB_LOG_CTX_NOTICE(ctx, "[TenantShredManager] Created:",
        {"Rate", Queue->GetRate()},
        {"InflightLimit", tenantShredConfig.GetInflightLimit()});
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
    YDB_LOG_CTX_NOTICE(ctx, "[TenantShredManager] Config updated:",
        {"Rate", Queue->GetRate()},
        {"InflightLimit", queueConfig.InflightLimit});
}

void TTenantShredManager::Start() {
    const auto ctx = SchemeShard->ActorContext();
    YDB_LOG_CTX_NOTICE(ctx, "[TenantShredManager] Start:",
        {"Status", Status},
        {"Generation", Generation});

    Queue->Start();
    if (Status == EShredStatus::COMPLETED) {
        std::unique_ptr<TEvSchemeShard::TEvTenantShredResponse> response = std::make_unique<TEvSchemeShard::TEvTenantShredResponse>(
            SchemeShard->ParentDomainId,
            SchemeShard->TenantShredManager->GetCompletedGeneration(),
            NKikimrScheme::TEvTenantShredResponse::COMPLETED
        );
        YDB_LOG_CTX_DEBUG(ctx, "[Start] Send to root schemeshard:",
            {"gen", SchemeShard->TenantShredManager->GetGeneration()});
        SchemeShard->PipeClientCache->Send(ctx, ui64(SchemeShard->ParentDomainId.OwnerId), response.release());
    } else if (Status == EShredStatus::IN_PROGRESS) {
        Queue->Clear();
        ActivePipes.clear();
        for (const auto& [shardIdx, status] : WaitingShredShards) {
            if (status == EShredStatus::IN_PROGRESS) {
                Queue->Enqueue(shardIdx);
            }
        }
        YDB_LOG_CTX_TRACE(ctx, "[TenantShredManager] Continue:",
            {"WaitingShredShards.size", WaitingShredShards.size()},
            {"Status", Status});
    }
}

void TTenantShredManager::Stop() {
    const auto ctx = SchemeShard->ActorContext();
    YDB_LOG_CTX_NOTICE(ctx, "[TenantShredManager] Stop");
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
    RunningShardsCounter->Set(0);
    for (const auto& [shardIdx, shardInfo] : SchemeShard->ShardInfos) {
        switch (shardInfo.TabletType) {
        case NKikimr::NSchemeShard::ETabletType::DataShard:
        case NKikimr::NSchemeShard::ETabletType::PersQueue: {
            if (Queue->Enqueue(shardIdx)) {
                WaitingShredShards[shardIdx] = EShredStatus::IN_PROGRESS;
                WaitingShardsCounter->Inc();
                CompletedShardsCounter->Dec();
                db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingShredShards::Status>(WaitingShredShards[shardIdx]);
                YDB_LOG_CTX_TRACE(ctx, "[TenantShredManager] [Enqueue] Enqueued at schemeshard",
                    {"shard", shardIdx},
                    {"TabletID", SchemeShard->TabletID()});
            } else {
                YDB_LOG_CTX_TRACE(ctx, "[TenantShredManager] [Enqueue] Skipped or already exists at schemeshard",
                    {"shard", shardIdx},
                    {"TabletID", SchemeShard->TabletID()});
            }
            break;
        }
        default:
            break;
        }
    }
    if (WaitingShredShards.empty()) {
        Status = EShredStatus::COMPLETED;
        CompletedGeneration = Generation;
    }
    db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);

    YDB_LOG_CTX_NOTICE(ctx, "[TenantShredManager] StartShred:",
        {"Generation", Generation},
        {"WaitingShredShards.size", WaitingShredShards.size()},
        {"Status", Status});
}

void TTenantShredManager::StartShredForNewShards(NIceDb::TNiceDb& db, const std::vector<TShardIdx>& shredShards) {
    const auto& ctx = SchemeShard->ActorContext();
    for (const auto& shardIdx : shredShards) {
        if (Queue->Enqueue(shardIdx)) {
            WaitingShredShards[shardIdx] = EShredStatus::IN_PROGRESS;
            WaitingShardsCounter->Inc();
            db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Update<Schema::WaitingShredShards::Status>(WaitingShredShards[shardIdx]);
            YDB_LOG_CTX_TRACE(ctx, "[TenantShredManager] [Enqueue] Enqueued at schemeshard",
                {"shard", shardIdx},
                {"TabletID", SchemeShard->TabletID()});
        } else {
            YDB_LOG_CTX_TRACE(ctx, "[TenantShredManager] [Enqueue] Skipped or already exists at schemeshard",
                {"shard", shardIdx},
                {"TabletID", SchemeShard->TabletID()});
        }
    }
    if (WaitingShredShards.empty()) {
        Status = EShredStatus::COMPLETED;
        CompletedGeneration = Generation;
    }
    db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);

    YDB_LOG_CTX_TRACE(ctx, "[TenantShredManager] StartShredForNewShards:",
        {"Generation", Generation},
        {"WaitingShredShards.size", WaitingShredShards.size()},
        {"Status", Status});
}

void TTenantShredManager::FinishShred(NIceDb::TNiceDb& db, const TTabletId& tabletId) {
    const TShardIdx shardIdx = SchemeShard->GetShardIdx(tabletId);
    auto duration = Queue->OnDone(shardIdx);
    auto ctx = SchemeShard->ActorContext();
    if (shardIdx == InvalidShardIdx) {
        YDB_LOG_CTX_WARN(ctx, "[TenantShredManager] [Finished] Failed to resolve shard info for ms at schemeshard",
            {"tabletId", tabletId},
            {"in", duration.MilliSeconds()},
            {"TabletID", SchemeShard->TabletID()});
    } else {
        YDB_LOG_CTX_INFO(ctx, "[TenantShredManager] [Finished] Shred is completed for ms at schemeshard",
            {"tabletId", tabletId},
            {"shardIdx", shardIdx},
            {"in", duration.MilliSeconds()},
            {"TabletID", SchemeShard->TabletID()});
    }

    bool wasRunning = ActivePipes.erase(shardIdx) > 0;
    auto waitingShardIt = WaitingShredShards.find(shardIdx);
    if (waitingShardIt != WaitingShredShards.end()) {
        WaitingShredShards.erase(waitingShardIt);
        if (wasRunning) {
            RunningShardsCounter->Dec();
        } else {
            WaitingShardsCounter->Dec();
        }
        CompletedShardsCounter->Inc();
        db.Table<Schema::WaitingShredShards>().Key(shardIdx.GetOwnerId(), shardIdx.GetLocalId()).Delete();
    }
    if (WaitingShredShards.empty()) {
        YDB_LOG_CTX_NOTICE(ctx, "[TenantShredManager] Shred in shards is completed:. Send response to root schemeshard",
            {"Generation", Generation});
        Queue->Clear();
        ActivePipes.clear();
        Status = EShredStatus::COMPLETED;
        CompletedGeneration = Generation;
        db.Table<Schema::TenantShredGenerations>().Key(Generation).Update<Schema::TenantShredGenerations::Status>(Status);
    }
}

void TTenantShredManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    if (tabletId == TTabletId(SchemeShard->ParentDomainId.OwnerId)) {
        YDB_LOG_CTX_DEBUG(ctx, "[TenantShredManager] [HandleDisconnect] Retry send response to root schemeshard",
            {"from", SchemeShard->TabletID()},
            {"generation", SchemeShard->TenantShredManager->GetCompletedGeneration()});
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
    YDB_LOG_CTX_INFO(ctx, "[TenantShredManager] [Disconnect] Shred disconnect",
        {"to_tablet", tabletId},
        {"at_schemeshard", SchemeShard->TabletID()});
    WaitingShardsCounter->Inc();
    RunningShardsCounter->Dec();
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
    if (ActivePipes.erase(shardIdx)) {
        WaitingShardsCounter->Inc();
        RunningShardsCounter->Dec();
    }
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
            CompletedGeneration = Generation;
        }
    }
    RunningShardsCounter->Set(0);
    WaitingShardsCounter->Set(WaitingShredShards.size());
    if (SchemeShard->ShardInfos.size() > WaitingShredShards.size()) {
        CompletedShardsCounter->Set(SchemeShard->ShardInfos.size() - WaitingShredShards.size());
    } else {
        CompletedShardsCounter->Set(0);
    }
    auto ctx = SchemeShard->ActorContext();
    YDB_LOG_CTX_INFO(ctx, "[TenantShredManager] Restore:",
        {"Generation", Generation},
        {"Status", Status},
        {"WaitingShredShards", WaitingShredShards.size()},
        {"CompletedGeneration", CompletedGeneration});
    return true;
}

bool TTenantShredManager::StopWaitingShred(const TShardIdx& shardIdx) {
    auto it = WaitingShredShards.find(shardIdx);
    if (it != WaitingShredShards.end()) {
        Queue->Remove(shardIdx);
        if (ActivePipes.erase(shardIdx)) {
            RunningShardsCounter->Dec();
        } else {
            WaitingShardsCounter->Dec();
        }
        WaitingShredShards.erase(it);
        if (WaitingShredShards.empty()) {
            Status = EShredStatus::COMPLETED;
            CompletedGeneration = Generation;
            auto ctx = SchemeShard->ActorContext();
            YDB_LOG_CTX_INFO(ctx, "[TenantShredManager] [StopWaitingShred] Shred in shards is completed for. Send response to root schemeshard",
                {"generation", CompletedGeneration});
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxRunTenantShred: Execute",
            {"at_schemeshard", Self->TabletID()});
        auto& shredManager = Self->TenantShredManager;
        if (record.GetGeneration() > shredManager->GetGeneration()) {
            NIceDb::TNiceDb db(txc.DB);
            shredManager->StartShred(db, record.GetGeneration());
        }
        if (record.GetGeneration() <= shredManager->GetCompletedGeneration()) {
            YDB_LOG_CTX_DEBUG(ctx, "TTxRunTenantShred: Already complete for requested, requested",
                {"generation", record.GetGeneration()},
                {"schemeshard", Self->TabletID()},
                {"from", Ev->Sender});
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxCompleteShredShard Execute",
            {"at_schemeshard", Self->TabletID()});
        if (!IsSuccess(Ev)) {
            HandleBadStatus(Ev, ctx);
            return;
        }
        const ui64 completedGeneration = GetCompletedGeneration(Ev);
        auto& shredManager = Self->TenantShredManager;
        if (completedGeneration != shredManager->GetGeneration()) {
            YDB_LOG_CTX_DEBUG(ctx, "TTxCompleteShredShard: Unknown, Expected",
                {"generation", completedGeneration},
                {"gen", shredManager->GetGeneration()},
                {"at_schemeshard", Self->TabletID()});
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
            YDB_LOG_CTX_DEBUG(ctx, "TTxCompleteShredShard Complete. Send to root schemeshard",
                {"gen", Self->TenantShredManager->GetGeneration()});
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxCompleteShredShard: shred failed with",
            {"at_DataShard", record.GetTabletId()},
            {"status", NKikimrTxDataShard::TEvVacuumResult::EStatus_Name(record.GetStatus())},
            {"schemeshard", Self->TabletID()});
        Self->TenantShredManager->RetryShred(TTabletId(record.GetTabletId()));
    }

    ui64 GetCompletedGeneration(TEvDataShard::TEvVacuumResult::TPtr& ev) const {
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxCompleteShredShard: shred failed with",
            {"at_KeyValue", record.tablet_id()},
            {"status", NKikimrKeyValue::VacuumResponse::Status_Name(record.status())},
            {"schemeshard", Self->TabletID()});
        Self->TenantShredManager->RetryShred(TTabletId(record.tablet_id()));
    }

    ui64 GetCompletedGeneration(TEvKeyValue::TEvVacuumResponse::TPtr& ev) const {
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxAddEntryToShred Execute",
            {"at_schemeshard", Self->TabletID()});

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
            YDB_LOG_CTX_DEBUG(ctx, "TTxAddEntryToShred Complete Send to root schemeshard:",
                {"at_schemeshard", Self->TabletID()},
                {"gen", Self->TenantShredManager->GetGeneration()});
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
        , ShredShards(shredShards)
    {}

    TTxType GetTxType() const override { return TXTYPE_CANCEL_SHARDS_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxCancelShredShards Execute",
            {"at_schemeshard", Self->TabletID()});

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
        YDB_LOG_CTX_DEBUG(ctx, "TTxCancelShredShards Complete",
            {"at_schemeshard", Self->TabletID()});
        if (Response) {
            YDB_LOG_CTX_DEBUG(ctx, "TTxCancelShredShards Complete Send to root schemeshard:",
                {"at_schemeshard", Self->TabletID()},
                {"gen", Self->TenantShredManager->GetGeneration()});
            Self->PipeClientCache->Send(ctx, ui64(Self->ParentDomainId.OwnerId), Response.release());
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCancelShredShards(const std::vector<TShardIdx>& oldShards) {
    return new TTxCancelShredShards(this, oldShards);
}


} // NKikimr::NSchemeShard
