#include "schemeshard__domain_shred_manager.h"

#include <ydb/core/base/counters.h>
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

TDomainShredManager::TStarter::TStarter(TDomainShredManager* const manager)
    : Manager(manager)
{}

NOperationQueue::EStartStatus TDomainShredManager::TStarter::StartOperation(const TPathId& pathId) {
    return Manager->StartShredOperation(pathId);
}

void TDomainShredManager::TStarter::OnTimeout(const TPathId&) {
    // Do not use
}

TDomainShredManager::TDomainShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config)
    : SchemeShard(schemeShard)
    , Starter(this)
    , Queue(new TQueue(ConvertConfig(config), Starter))
    , ShredInterval(TDuration::Seconds(config.GetDataErasureIntervalSeconds()))
    , ShredBSCInterval(TDuration::Seconds(config.GetBlobStorageControllerRequestIntervalSeconds()))
    , CurrentWakeupInterval(ShredInterval)
    , BSC(MakeBSControllerID())
    , IsManualStartup((ShredInterval.Seconds() == 0 ? true : false))
{
    const auto ctx = SchemeShard->ActorContext();
    ctx.RegisterWithSameMailbox(Queue);

    TIntrusivePtr<NMonitoring::TDynamicCounters> rootCounters = AppData()->Counters;
    TIntrusivePtr<NMonitoring::TDynamicCounters> schemeShardCounters = GetServiceCounters(rootCounters, "SchemeShard");
    NMonitoring::TDynamicCounterPtr shredCounters = schemeShardCounters->GetSubgroup("subsystem", "DomainShredTotal");
    WaitingTenantsCounter = shredCounters->GetSubgroup("DomainShred", "Waiting")->GetCounter("WaitingShredTenants", true);
    RunungTenantsCounter = shredCounters->GetSubgroup("DomainShred", "Runing")->GetCounter("RuningShredTenants", true);
    CompletedTenantsCounter = shredCounters->GetSubgroup("DomainShred", "Completed")->GetCounter("CompletedShredTenants", true);

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Created: InflightLimit# " << config.GetInflightLimit()
        << ", ShredInterval# " << ShredInterval
        << ", ShredBSCInterval# " << ShredBSCInterval
        << ", CurrentWakeupInterval# " << CurrentWakeupInterval
        << ", IsManualStartup# " << (IsManualStartup ? "true" : "false")
    );
}

void TDomainShredManager::UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TDomainShredManager::TQueue::TConfig queueConfig = ConvertConfig(config);
    Queue->UpdateConfig(queueConfig);
    ShredInterval = TDuration::Seconds(config.GetDataErasureIntervalSeconds());
    ShredBSCInterval = TDuration::Seconds(config.GetBlobStorageControllerRequestIntervalSeconds());
    CurrentWakeupInterval = ShredInterval;
    BSC = TTabletId(MakeBSControllerID());
    IsManualStartup = (ShredInterval.Seconds() == 0 ? true : false);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Config updated: InflightLimit# " << queueConfig.InflightLimit
        << ", ShredInterval# " << ShredInterval
        << ", ShredBSCInterval# " << ShredBSCInterval
        << ", CurrentWakeupInterval# " << CurrentWakeupInterval
        << ", IsManualStartup# " << (IsManualStartup ? "true" : "false")
    );
}

void TDomainShredManager::Start() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Start: "
        << "Generation# " << Generation
        << ", Status# " << PrintStatus(Status)
    );
    Queue->Start();
    if (Status == EShredStatus::UNSPECIFIED) {
        SchemeShard->InitDomainShred();
        ScheduleShredWakeup();
    } else if (Status == EShredStatus::COMPLETED) {
        ScheduleShredWakeup();
    } else {
        Continue();
    }
}

void TDomainShredManager::Stop() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Stop"
    );
    Queue->Stop();
}

void TDomainShredManager::StartShred(NIceDb::TNiceDb& db) {
    const auto ctx = SchemeShard->ActorContext();
    ++Generation;
    ++BscGeneration;
    Queue->Clear();
    ActivePipes.clear();
    for (const auto& [pathId, status] : WaitingShredTenants) {
        db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }
    WaitingShredTenants.clear();
    Status = EShredStatus::IN_PROGRESS;
    CompletedTenantsCounter->Set(SchemeShard->SubDomains.size());
    WaitingTenantsCounter->Set(0);
    RunungTenantsCounter->Set(0);
    StartTime = AppData(SchemeShard->ActorContext())->TimeProvider->Now();
    for (auto& [pathId, subdomain] : SchemeShard->SubDomains) {
        if (subdomain->GetTenantSchemeShardID() == InvalidTabletId) { // no tenant schemeshard
            continue;
        }
        if (Queue->Enqueue(pathId)) {
            WaitingShredTenants[pathId] = EShredStatus::IN_PROGRESS;
            WaitingTenantsCounter->Inc();
            CompletedTenantsCounter->Dec();
            db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::WaitingShredTenants::Status>(WaitingShredTenants[pathId]);
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[DomainShredManager] [Enqueue] Enqueued pathId# " << pathId << " at schemeshard " << SchemeShard->TabletID());
        } else {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[DomainShredManager] [Enqueue] Skipped or already exists pathId# " << pathId << " at schemeshard " << SchemeShard->TabletID());
        }
    }
    if (WaitingShredTenants.empty()) {
        Status = EShredStatus::IN_PROGRESS_BSC;
    }
    db.Table<Schema::ShredGenerations>().Key(Generation).Update<Schema::ShredGenerations::Status,
                                                                      Schema::ShredGenerations::StartTime>(Status, StartTime.MicroSeconds());

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Run: "
        << "Generation# " << Generation
        << ", WaitingShredTenants.size# " << WaitingShredTenants.size()
        << ", Status# " << PrintStatus(Status));
}

void TDomainShredManager::Continue() {
    Queue->Clear();
    ActivePipes.clear();
    if (Status == EShredStatus::IN_PROGRESS) {
        for (const auto& [pathId, status] : WaitingShredTenants) {
            if (status == EShredStatus::IN_PROGRESS) {
                Queue->Enqueue(pathId);
            }
        }
    } else if (Status == EShredStatus::IN_PROGRESS_BSC) {
        SendRequestToBSC();
    }
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Continue: "
        << "Generation# " << Generation
        << ", Status# " << PrintStatus(Status));
}

void TDomainShredManager::ScheduleShredWakeup() {
    if (IsManualStartup || IsShredWakeupScheduled) {
        return;
    }
    const auto ctx = SchemeShard->ActorContext();
    ctx.Schedule(CurrentWakeupInterval, new TEvSchemeShard::TEvWakeupToRunShred);
    IsShredWakeupScheduled = true;
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] ScheduleShredWakeup: Next shred iteration wiil run at " << AppData(ctx)->TimeProvider->Now() + CurrentWakeupInterval);
}

void TDomainShredManager::WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    IsShredWakeupScheduled = false;
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] WakeupToRunShred: Timestamp# " << AppData(ctx)->TimeProvider->Now());
    SchemeShard->RunDomainShred();
}

NOperationQueue::EStartStatus TDomainShredManager::StartShredOperation(const TPathId& pathId) {
    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->SubDomains.find(pathId);
    if (it == SchemeShard->SubDomains.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DomainShredManager] [Start] Failed to resolve subdomain info "
            << "for pathId# " << pathId
            << " at schemeshard# " << SchemeShard->TabletID());
        return NOperationQueue::EStartStatus::EOperationRemove;
    }
    const auto& tenantSchemeShardId = it->second->GetTenantSchemeShardID();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DomainShredManager] [Start] Shred "
        << "for pathId# " << pathId
        << ", tenant schemeshard# " << tenantSchemeShardId
        << " at schemeshard " << SchemeShard->TabletID());
    std::unique_ptr<TEvSchemeShard::TEvTenantShredRequest> request(
        new TEvSchemeShard::TEvTenantShredRequest(Generation));
    ActivePipes[pathId] = SchemeShard->PipeClientCache->Send(ctx, ui64(tenantSchemeShardId), request.release());
    WaitingTenantsCounter->Dec();
    RunungTenantsCounter->Inc();
    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TDomainShredManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    if (tabletId == BSC) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[DomainShredManager] HandleDisconnect resend request to BSC at schemeshard " << SchemeShard->TabletID());
        SendRequestToBSC();
        return;
    }
    const auto shardIdx = SchemeShard->GetShardIdx(tabletId);
    if (!SchemeShard->ShardInfos.contains(shardIdx)) {
        return;
    }
    const auto& pathId = SchemeShard->ShardInfos.at(shardIdx).PathId;
    if (!SchemeShard->TTLEnabledTables.contains(pathId)) {
        return;
    }
    const auto it = ActivePipes.find(pathId);
    if (it == ActivePipes.end() || it->second != clientId) {
        return;
    }
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DomainShredManager] [Disconnect] Shred disconnect "
        << "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());
    ActivePipes.erase(pathId);
    WaitingTenantsCounter->Inc();
    RunungTenantsCounter->Dec();
    StartShredOperation(pathId);
}

void TDomainShredManager::FinishShred(NIceDb::TNiceDb& db, const TPathId& pathId) {
    auto duration = Queue->OnDone(pathId);
    auto ctx = SchemeShard->ActorContext();
    if (!SchemeShard->SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DomainShredManager] [Finished] Failed to resolve subdomain info "
            << "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DomainShredManager] [Finished] Shred completed "
            << "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << " at schemeshard " << SchemeShard->TabletID());
    }
    ActivePipes.erase(pathId);
    auto it = WaitingShredTenants.find(pathId);
    if (it != WaitingShredTenants.end()) {
        WaitingShredTenants.erase(it);
        RunungTenantsCounter->Dec();
        CompletedTenantsCounter->Inc();
        db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }
    if (WaitingShredTenants.empty()) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[DomainShredManager] Shred in tenants is completed. Send request to BS controller");
        Queue->Clear();
        ActivePipes.clear();
        Status = EShredStatus::IN_PROGRESS_BSC;
        db.Table<Schema::ShredGenerations>().Key(Generation).Update<Schema::ShredGenerations::Status>(Status);
    }
}

void TDomainShredManager::ScheduleRequestToBSC() {
    if (IsRequestToBSCScheduled) {
        return;
    }
    auto ctx = SchemeShard->ActorContext();
    ctx.Schedule(ShredBSCInterval, new TEvSchemeShard::TEvWakeupToRunShredBSC);
    IsRequestToBSCScheduled = true;
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] ScheduleRequestToBSC: Interval# " << ShredBSCInterval);
}

void TDomainShredManager::WakeupSendRequestToBSC() {
    IsRequestToBSCScheduled = false;
    SendRequestToBSC();
}

void TDomainShredManager::SendRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] SendRequestToBSC: "
        << "Generation# " << Generation
        << ", BscGeneration# " <<  BscGeneration);
    std::unique_ptr<TEvBlobStorage::TEvControllerShredRequest> request(
        new TEvBlobStorage::TEvControllerShredRequest(BscGeneration));
    SchemeShard->PipeClientCache->Send(ctx, MakeBSControllerID(), request.release());
}

void TDomainShredManager::Complete() {
    Status = EShredStatus::COMPLETED;
    auto ctx = SchemeShard->ActorContext();
    FinishTime = AppData(ctx)->TimeProvider->Now();
    TDuration shredDuration = FinishTime - StartTime;
    if (shredDuration > ShredInterval) {
        if (!IsManualStartup) {
            SchemeShard->RunDomainShred();
        }
    } else {
        CurrentWakeupInterval = ShredInterval - shredDuration;
        ScheduleShredWakeup();
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Complete: Generation# " << Generation
        << ", duration# " << shredDuration.Seconds() << " s");
}

bool TDomainShredManager::Restore(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<Schema::ShredGenerations>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        if (rowset.EndOfSet()) {
            Status = EShredStatus::UNSPECIFIED;
        } else {
            Generation = 0;
            Status = EShredStatus::UNSPECIFIED;
            while (!rowset.EndOfSet()) {
                ui64 generation = rowset.GetValue<Schema::ShredGenerations::Generation>();
                if (generation >= Generation) {
                    Generation = generation;
                    BscGeneration = Generation;
                    StartTime = TInstant::FromValue(rowset.GetValue<Schema::ShredGenerations::StartTime>());
                    Status = rowset.GetValue<Schema::ShredGenerations::Status>();
                }
                if (!rowset.Next()) {
                    return false;
                }
            }
            if (Status == EShredStatus::UNSPECIFIED || Status == EShredStatus::COMPLETED) {
                auto ctx = SchemeShard->ActorContext();
                TDuration interval = AppData(ctx)->TimeProvider->Now() - StartTime;
                if (interval > ShredInterval) {
                    CurrentWakeupInterval = TDuration::Zero();
                } else {
                    CurrentWakeupInterval = ShredInterval - interval;
                }
            }
        }
    }
    {
        auto rowset = db.Table<Schema::WaitingShredTenants>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TOwnerId ownerPathId = rowset.GetValue<Schema::WaitingShredTenants::OwnerPathId>();
            TLocalPathId localPathId = rowset.GetValue<Schema::WaitingShredTenants::LocalPathId>();
            TPathId pathId(ownerPathId, localPathId);
            Y_VERIFY_S(SchemeShard->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
            TPathElement::TPtr path = SchemeShard->PathsById.at(pathId);
            Y_VERIFY_S(path->IsDomainRoot(), "Path is not a subdomain, pathId: " << pathId);

            Y_ABORT_UNLESS(SchemeShard->SubDomains.contains(pathId));

            EShredStatus status = rowset.GetValue<Schema::WaitingShredTenants::Status>();
            WaitingShredTenants[pathId] = status;
            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EShredStatus::IN_PROGRESS && WaitingShredTenants.empty()) {
            Status = EShredStatus::IN_PROGRESS_BSC;
        }
    }
    RunungTenantsCounter->Set(0);
    WaitingTenantsCounter->Set(WaitingShredTenants.size());
    CompletedTenantsCounter->Set(SchemeShard->SubDomains.size() - WaitingShredTenants.size());
    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[DomainShredManager] Restore: Generation# " << Generation
        << ", Status# " << PrintStatus(Status)
        << ", WakeupInterval# " << CurrentWakeupInterval.Seconds() << " s"
        << ", WaitingShredTenants# " << WaitingShredTenants.size());
    return true;
}

bool TDomainShredManager::Remove(const TPathId& pathId) {
    auto it = WaitingShredTenants.find(pathId);
    if (it != WaitingShredTenants.end()) {
        Queue->Remove(pathId);
        ActivePipes.erase(pathId);
        WaitingShredTenants.erase(it);

        if (WaitingShredTenants.empty()) {
            Status = EShredStatus::IN_PROGRESS_BSC;
            SendRequestToBSC();
        }
        return true;
    }
    return false;
}

void TDomainShredManager::SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) {
    db.Table<Schema::ShredGenerations>().Key(GetGeneration()).Delete();
    SetGeneration(currentBscGeneration + 1);
    db.Table<Schema::ShredGenerations>().Key(GetGeneration()).Update<Schema::ShredGenerations::Status,
                                                                           Schema::ShredGenerations::StartTime>(GetStatus(), StartTime.MicroSeconds());
}

TDomainShredManager::TQueue::TConfig TDomainShredManager::ConvertConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TQueue::TConfig queueConfig;
    queueConfig.IsCircular = false;
    queueConfig.InflightLimit = config.GetInflightLimit();
    queueConfig.Timeout = TDuration::Zero(); // unlimited
    return queueConfig;
}

struct TSchemeShard::TTxShredManagerInit : public TSchemeShard::TRwTxBase {
    TTxShredManagerInit(TSelf* self)
        : TRwTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SHRED_INIT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxShredManagerInit Execute at schemeshard: " << Self->TabletID());
        NIceDb::TNiceDb db(txc.DB);
        auto& shredManager = Self->DomainShredManager;
        shredManager->SetStatus(EShredStatus::COMPLETED);
        db.Table<Schema::ShredGenerations>().Key(shredManager->GetGeneration()).Update<Schema::ShredGenerations::Status,
                                                               Schema::ShredGenerations::StartTime>(shredManager->GetStatus(), AppData(ctx)->TimeProvider->Now().MicroSeconds());
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxShredManagerInit Complete at schemeshard: " << Self->TabletID());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxShredManagerInit() {
    return new TTxShredManagerInit(this);
}

struct TSchemeShard::TTxRunShred : public TSchemeShard::TRwTxBase {
    bool NeedSendRequestToBSC = false;

    TTxRunShred(TSelf *self)
        : TRwTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxRunShred Execute at schemeshard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        auto& shredManager = Self->DomainShredManager;
        shredManager->StartShred(db);
        if (shredManager->GetStatus() == EShredStatus::IN_PROGRESS_BSC) {
            NeedSendRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunShred Complete at schemeshard: " << Self->TabletID()
            << ", NeedSendRequestToBSC# " << (NeedSendRequestToBSC ? "true" : "false"));

        if (NeedSendRequestToBSC) {
            Self->DomainShredManager->SendRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunShred() {
    return new TTxRunShred(this);
}

struct TSchemeShard::TTxCompleteShredTenant : public TSchemeShard::TRwTxBase {
    const TEvSchemeShard::TEvTenantShredResponse::TPtr Ev;
    bool NeedSendRequestToBSC = false;

    TTxCompleteShredTenant(TSelf* self, const TEvSchemeShard::TEvTenantShredResponse::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_SHRED_TENANT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxCompleteShredTenant Execute at schemeshard: " << Self->TabletID());

        const auto& record = Ev->Get()->Record;
        auto& shredManager = Self->DomainShredManager;
        const ui64 completedGeneration = record.GetGeneration();
        if (completedGeneration != shredManager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredTenant Unknown generation#" << completedGeneration << ", Expected gen# " << shredManager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }
        if (shredManager->GetStatus() == EShredStatus::IN_PROGRESS_BSC ||
            shredManager->GetStatus() == EShredStatus::COMPLETED) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredTenant Generation#" << completedGeneration << " marked as " << PrintStatus(shredManager->GetStatus()));
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        auto pathId = TPathId(
            record.GetPathId().GetOwnerId(),
            record.GetPathId().GetLocalId());
        shredManager->FinishShred(db, pathId);
        if (shredManager->GetStatus() == EShredStatus::IN_PROGRESS_BSC) {
            NeedSendRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredTenant Complete at schemeshard: " << Self->TabletID()
            << ", NeedSendRequestToBSC# " << (NeedSendRequestToBSC ? "true" : "false"));
        if (NeedSendRequestToBSC) {
            Self->DomainShredManager->SendRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteShredTenant(TEvSchemeShard::TEvTenantShredResponse::TPtr& ev) {
    return new TTxCompleteShredTenant(this, ev);
}

struct TSchemeShard::TTxCompleteShredBSC : public TSchemeShard::TRwTxBase {
    const TEvBlobStorage::TEvControllerShredResponse::TPtr Ev;
    bool NeedScheduleRequestToBSC = false;

    TTxCompleteShredBSC(TSelf* self, const TEvBlobStorage::TEvControllerShredResponse::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_SHRED_BSC; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredBSC Execute at schemeshard: " << Self->TabletID());

        const auto& record = Ev->Get()->Record;
        auto& shredManager = Self->DomainShredManager;
        if (record.GetCurrentGeneration() < shredManager->GetBscGeneration()) {
            return;
        }
        if (ui64 currentBscGeneration = record.GetCurrentGeneration(); currentBscGeneration > shredManager->GetBscGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredBSC Unknown generation#" << currentBscGeneration << ", Expected BscGen# " << shredManager->GetBscGeneration() << " at schemestard: " << Self->TabletID());
            shredManager->SetBscGeneration(currentBscGeneration + 1);
            if (shredManager->GetStatus() == EShredStatus::IN_PROGRESS_BSC) {
                shredManager->SendRequestToBSC();
            }
            return;
        }
        NIceDb::TNiceDb db(txc.DB);
        if (record.GetCompleted()) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCompleteShredBSC: Data shred in BSC is completed");
            shredManager->Complete();
            db.Table<Schema::ShredGenerations>().Key(shredManager->GetGeneration()).Update<Schema::ShredGenerations::Status>(shredManager->GetStatus());
        } else {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCompleteShredBSC: Progress data shred in BSC " << static_cast<double>(record.GetProgress10k()) / 100 << "%");
            NeedScheduleRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredBSC Complete at schemeshard: " << Self->TabletID()
            << ", NeedScheduleRequestToBSC# " << (NeedScheduleRequestToBSC ? "true" : "false"));

        if (NeedScheduleRequestToBSC) {
            Self->DomainShredManager->ScheduleRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteShredBSC(TEvBlobStorage::TEvControllerShredResponse::TPtr& ev) {
    return new TTxCompleteShredBSC(this, ev);
}

} // NKikimr::NSchemeShard
