#include "schemeshard__shred_manager.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard {

TRootShredManager::TStarter::TStarter(TRootShredManager* const manager)
    : Manager(manager)
{}

NOperationQueue::EStartStatus TRootShredManager::TStarter::StartOperation(const TPathId& pathId) {
    return Manager->StartShred(pathId);
}

void TRootShredManager::TStarter::OnTimeout(const TPathId& pathId) {
    Manager->OnTimeout(pathId);
}

TRootShredManager::TRootShredManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config)
    : TShredManager(schemeShard)
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

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Created: Timeout# " << config.GetTimeoutSeconds()
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << config.GetInflightLimit()
        << ", ShredInterval# " << ShredInterval
        << ", ShredBSCInterval# " << ShredBSCInterval
        << ", CurrentWakeupInterval# " << CurrentWakeupInterval
        << ", IsManualStartup# " << (IsManualStartup ? "true" : "false"));
}

void TRootShredManager::UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TRootShredManager::TQueue::TConfig queueConfig = ConvertConfig(config);
    Queue->UpdateConfig(queueConfig);
    ShredInterval = TDuration::Seconds(config.GetDataErasureIntervalSeconds());
    ShredBSCInterval = TDuration::Seconds(config.GetBlobStorageControllerRequestIntervalSeconds());
    CurrentWakeupInterval = ShredInterval;
    BSC = TTabletId(MakeBSControllerID());
    IsManualStartup = (ShredInterval.Seconds() == 0 ? true : false);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Config updated: Timeout# " << queueConfig.Timeout
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << queueConfig.InflightLimit
        << ", ShredInterval# " << ShredInterval
        << ", ShredBSCInterval# " << ShredBSCInterval
        << ", CurrentWakeupInterval# " << CurrentWakeupInterval
        << ", IsManualStartup# " << (IsManualStartup ? "true" : "false"));
}

void TRootShredManager::Start() {
    TShredManager::Start();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Start: Status# " << static_cast<ui32>(Status));

    Queue->Start();
    if (Status == EShredStatus::UNSPECIFIED) {
        SchemeShard->MarkFirstRunRootShredManager();
        ScheduleShredWakeup();
    } else if (Status == EShredStatus::COMPLETED) {
        ScheduleShredWakeup();
    } else {
        ClearOperationQueue();
        Continue();
    }
}

void TRootShredManager::Stop() {
    TShredManager::Stop();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Stop");

    Queue->Stop();
}

void TRootShredManager::ClearOperationQueue() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Clear operation queue and active pipes");

    Queue->Clear();
    ActivePipes.clear();
}

void TRootShredManager::ClearWaitingShredRequests(NIceDb::TNiceDb& db) {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Clear WaitingShredTenants: Size# " << WaitingShredTenants.size());

    for (const auto& [pathId, status] : WaitingShredTenants) {
        db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }
    ClearWaitingShredRequests();
}

void TRootShredManager::ClearWaitingShredRequests() {
    WaitingShredTenants.clear();
}

void TRootShredManager::Run(NIceDb::TNiceDb& db) {
    CounterShredOk = 0;
    CounterShredTimeout = 0;
    Status = EShredStatus::IN_PROGRESS;
    StartTime = AppData(SchemeShard->ActorContext())->TimeProvider->Now();
    for (auto& [pathId, subdomain] : SchemeShard->SubDomains) {
        auto path = TPath::Init(pathId, SchemeShard);
        if (path->IsRoot()) {
            continue;
        }
        if (subdomain->GetTenantSchemeShardID() == InvalidTabletId) { // no tenant schemeshard
            continue;
        }
        Enqueue(pathId);
        WaitingShredTenants[pathId] = EShredStatus::IN_PROGRESS;
        db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::WaitingShredTenants::Status>(WaitingShredTenants[pathId]);
    }
    if (WaitingShredTenants.empty()) {
        Status = EShredStatus::IN_PROGRESS_BSC;
    }
    db.Table<Schema::ShredGenerations>().Key(Generation).Update<Schema::ShredGenerations::Status,
                                                                      Schema::ShredGenerations::StartTime>(Status, StartTime.MicroSeconds());

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Run: Queue.Size# " << Queue->Size()
        << ", WaitingShredTenants.size# " << WaitingShredTenants.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TRootShredManager::Continue() {
    if (Status == EShredStatus::IN_PROGRESS) {
        for (const auto& [pathId, status] : WaitingShredTenants) {
            if (status == EShredStatus::IN_PROGRESS) {
                Enqueue(pathId);
            }
        }
    } else if (Status == EShredStatus::IN_PROGRESS_BSC) {
        SendRequestToBSC();
    }

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Continue: Queue.Size# " << Queue->Size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TRootShredManager::ScheduleShredWakeup() {
    if (IsManualStartup || IsShredWakeupScheduled) {
        return;
    }

    const auto ctx = SchemeShard->ActorContext();
    ctx.Schedule(CurrentWakeupInterval, new TEvSchemeShard::TEvWakeupToRunShred);
    IsShredWakeupScheduled = true;

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] ScheduleShredWakeup: Interval# " << CurrentWakeupInterval << ", Timestamp# " << AppData(ctx)->TimeProvider->Now());
}

void TRootShredManager::WakeupToRunShred(TEvSchemeShard::TEvWakeupToRunShred::TPtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    IsShredWakeupScheduled = false;
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] WakeupToRunShred: Timestamp# " << AppData(ctx)->TimeProvider->Now());
    SchemeShard->RunShred(true);
}

NOperationQueue::EStartStatus TRootShredManager::StartShred(const TPathId& pathId) {
    UpdateMetrics();

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->SubDomains.find(pathId);
    if (it == SchemeShard->SubDomains.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Start] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " at schemeshard# " << SchemeShard->TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& tenantSchemeShardId = it->second->GetTenantSchemeShardID();

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Start] Shred "
        "for pathId# " << pathId
        << ", tenant schemeshard# " << tenantSchemeShardId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " tenants"
        << ", running# " << Queue->RunningSize() << " tenants"
        << " at schemeshard " << SchemeShard->TabletID());

    std::unique_ptr<TEvSchemeShard::TEvTenantShredRequest> request(
        new TEvSchemeShard::TEvTenantShredRequest(Generation));

    ActivePipes[pathId] = SchemeShard->PipeClientCache->Send(
        ctx,
        ui64(tenantSchemeShardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TRootShredManager::OnTimeout(const TPathId& pathId) {
    CounterShredTimeout++;
    UpdateMetrics();

    ActivePipes.erase(pathId);

    auto ctx = SchemeShard->ActorContext();
    if (!SchemeShard->SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Timeout] Failed to resolve subdomain info "
            "for path# " << pathId
            << " at schemeshard# " << SchemeShard->TabletID());
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Timeout] Shred timeouted "
        "for pathId# " << pathId
        << ", next wakeup in# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " tenants"
        << ", running# " << Queue->RunningSize() << " tenants"
        << " at schemeshard " << SchemeShard->TabletID());

    // retry
    Enqueue(pathId);
}

void TRootShredManager::Enqueue(const TPathId& pathId) {
    auto ctx = SchemeShard->ActorContext();

    if (Queue->Enqueue(pathId)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootShredManager] [Enqueue] Enqueued pathId# " << pathId << " at schemeshard " << SchemeShard->TabletID());
        UpdateMetrics();
    } else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootShredManager] [Enqueue] Skipped or already exists pathId# " << pathId << " at schemeshard " << SchemeShard->TabletID());
    }
}

void TRootShredManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    if (tabletId == BSC) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootShredManager] HandleDisconnect resend request to BSC at schemeshard " << SchemeShard->TabletID());

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
    if (it == ActivePipes.end()) {
        return;
    }

    if (it->second != clientId) {
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Disconnect] Shred disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());

    ActivePipes.erase(pathId);
    StartShred(pathId);
}

void TRootShredManager::OnDone(const TPathId& pathId, NIceDb::TNiceDb& db) {
    auto duration = Queue->OnDone(pathId);

    auto ctx = SchemeShard->ActorContext();
    if (!SchemeShard->SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Finished] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " tenants"
            << ", running# " << Queue->RunningSize() << " tenants"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Finished] Shred completed "
            "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " tenants"
            << ", running# " << Queue->RunningSize() << " tenants"
            << " at schemeshard " << SchemeShard->TabletID());
    }

    ActivePipes.erase(pathId);
    auto it = WaitingShredTenants.find(pathId);
    if (it != WaitingShredTenants.end()) {
        db.Table<Schema::WaitingShredTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
        WaitingShredTenants.erase(it);
    }

    CounterShredOk++;
    UpdateMetrics();

    if (WaitingShredTenants.empty()) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootShredManager] Shred in tenants is completed. Send request to BS controller");
        Status = EShredStatus::IN_PROGRESS_BSC;
        db.Table<Schema::ShredGenerations>().Key(Generation).Update<Schema::ShredGenerations::Status>(Status);
    }
}

void TRootShredManager::OnDone(const TTabletId&, NIceDb::TNiceDb&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [OnDone] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
}

void TRootShredManager::ScheduleRequestToBSC() {
    if (IsRequestToBSCScheduled) {
        return;
    }

    auto ctx = SchemeShard->ActorContext();
    ctx.Schedule(ShredBSCInterval, new TEvSchemeShard::TEvWakeupToRunShredBSC);
    IsRequestToBSCScheduled = true;

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] ScheduleRequestToBSC: Interval# " << ShredBSCInterval);
}

void TRootShredManager::SendRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] SendRequestToBSC: Generation# " << Generation);

    IsRequestToBSCScheduled = false;
    std::unique_ptr<TEvBlobStorage::TEvControllerShredRequest> request(
        new TEvBlobStorage::TEvControllerShredRequest(Generation));
    SchemeShard->PipeClientCache->Send(ctx, MakeBSControllerID(), request.release());
}

void TRootShredManager::Complete() {
    Status = EShredStatus::COMPLETED;
    auto ctx = SchemeShard->ActorContext();
    FinishTime = AppData(ctx)->TimeProvider->Now();
    TDuration shredDuration = FinishTime - StartTime;
    if (shredDuration > ShredInterval) {
        if (!IsManualStartup) {
            SchemeShard->RunShred(true);
        }
    } else {
        CurrentWakeupInterval = ShredInterval - shredDuration;
        ScheduleShredWakeup();
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Complete: Generation# " << Generation
        << ", duration# " << shredDuration.Seconds() << " s");
}

bool TRootShredManager::Restore(NIceDb::TNiceDb& db) {
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

    ui32 numberShredTenantsInRunning = 0;
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
            if (status == EShredStatus::IN_PROGRESS) {
                numberShredTenantsInRunning++;
            }

            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EShredStatus::IN_PROGRESS && (WaitingShredTenants.empty() || numberShredTenantsInRunning == 0)) {
            Status = EShredStatus::IN_PROGRESS_BSC;
        }
    }

    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootShredManager] Restore: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", WakeupInterval# " << CurrentWakeupInterval.Seconds() << " s"
        << ", NumberShredTenantsInRunning# " << numberShredTenantsInRunning);

    return true;
}

bool TRootShredManager::Remove(const TPathId& pathId) {
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

bool TRootShredManager::Remove(const TShardIdx&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [Remove] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
    return false;
}

void TRootShredManager::HandleNewPartitioning(const std::vector<TShardIdx>&, NIceDb::TNiceDb&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootShredManager] [HandleNewPartitioning] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
}

void TRootShredManager::SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) {
    db.Table<Schema::ShredGenerations>().Key(GetGeneration()).Delete();
    SetGeneration(currentBscGeneration + 1);
    db.Table<Schema::ShredGenerations>().Key(GetGeneration()).Update<Schema::ShredGenerations::Status,
                                                                           Schema::ShredGenerations::StartTime>(GetStatus(), StartTime.MicroSeconds());
}

void TRootShredManager::UpdateMetrics() {
    SchemeShard->TabletCounters->Simple()[COUNTER_SHRED_QUEUE_SIZE].Set(Queue->Size());
    SchemeShard->TabletCounters->Simple()[COUNTER_SHRED_QUEUE_RUNNING].Set(Queue->RunningSize());
    SchemeShard->TabletCounters->Simple()[COUNTER_SHRED_OK].Set(CounterShredOk);
    SchemeShard->TabletCounters->Simple()[COUNTER_SHRED_TIMEOUT].Set(CounterShredTimeout);
}

TRootShredManager::TQueue::TConfig TRootShredManager::ConvertConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TQueue::TConfig queueConfig;
    queueConfig.IsCircular = false;
    queueConfig.MaxRate = config.GetMaxRate();
    queueConfig.InflightLimit = config.GetInflightLimit();
    queueConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());

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
        Self->ShredManager->SetStatus(EShredStatus::COMPLETED);
        db.Table<Schema::ShredGenerations>().Key(0).Update<Schema::ShredGenerations::Status,
                                                               Schema::ShredGenerations::StartTime>(Self->ShredManager->GetStatus(), AppData(ctx)->TimeProvider->Now().MicroSeconds());
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
    bool IsNewShred;
    bool NeedSendRequestToBSC = false;

    TTxRunShred(TSelf *self, bool isNewShred)
        : TRwTxBase(self)
        , IsNewShred(isNewShred)
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_SHRED; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxRunShred Execute at schemeshard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        auto& shredManager = Self->ShredManager;
        if (IsNewShred) {
            shredManager->ClearOperationQueue();

            shredManager->ClearWaitingShredRequests(db);
            shredManager->IncGeneration();
            shredManager->Run(db);
        }
        if (Self->ShredManager->GetStatus() == EShredStatus::IN_PROGRESS_BSC) {
            NeedSendRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunShred Complete at schemeshard: " << Self->TabletID()
            << ", NeedSendRequestToBSC# " << (NeedSendRequestToBSC ? "true" : "false"));

        if (NeedSendRequestToBSC) {
            Self->ShredManager->SendRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunShred(bool isNewShred) {
    return new TTxRunShred(this, isNewShred);
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
        auto& manager = Self->ShredManager;
        const ui64 completedGeneration = record.GetGeneration();
        if (completedGeneration != manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredTenant Unknown generation#" << completedGeneration << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        auto pathId = TPathId(
            record.GetPathId().GetOwnerId(),
            record.GetPathId().GetLocalId());
        manager->OnDone(pathId, db);
        if (manager->GetStatus() == EShredStatus::IN_PROGRESS_BSC) {
            NeedSendRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteShredTenant Complete at schemeshard: " << Self->TabletID()
            << ", NeedSendRequestToBSC# " << (NeedSendRequestToBSC ? "true" : "false"));
        if (NeedSendRequestToBSC) {
            Self->ShredManager->SendRequestToBSC();
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
        auto& manager = Self->ShredManager;
        NIceDb::TNiceDb db(txc.DB);
        if (ui64 currentBscGeneration = record.GetCurrentGeneration(); currentBscGeneration > manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteShredBSC Unknown generation#" << currentBscGeneration << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            manager->SyncBscGeneration(db, currentBscGeneration);
            manager->SendRequestToBSC();
            return;
        }

        if (record.GetCompleted()) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCompleteShredBSC: Data shred in BSC is completed");
            manager->Complete();
            db.Table<Schema::ShredGenerations>().Key(Self->ShredManager->GetGeneration()).Update<Schema::ShredGenerations::Status>(Self->ShredManager->GetStatus());
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
            Self->ShredManager->ScheduleRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteShredBSC(TEvBlobStorage::TEvControllerShredResponse::TPtr& ev) {
    return new TTxCompleteShredBSC(this, ev);
}

} // NKikimr::NSchemeShard
