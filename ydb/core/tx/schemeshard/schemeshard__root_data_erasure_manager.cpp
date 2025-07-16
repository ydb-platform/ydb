#include "schemeshard__data_erasure_manager.h"

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard {

TRootDataErasureManager::TStarter::TStarter(TRootDataErasureManager* const manager)
    : Manager(manager)
{}

NOperationQueue::EStartStatus TRootDataErasureManager::TStarter::StartOperation(const TPathId& pathId) {
    return Manager->StartDataErasure(pathId);
}

void TRootDataErasureManager::TStarter::OnTimeout(const TPathId& pathId) {
    Manager->OnTimeout(pathId);
}

TRootDataErasureManager::TRootDataErasureManager(TSchemeShard* const schemeShard, const NKikimrConfig::TDataErasureConfig& config)
    : TDataErasureManager(schemeShard)
    , Starter(this)
    , Queue(new TQueue(ConvertConfig(config), Starter))
    , DataErasureInterval(TDuration::Seconds(config.GetDataErasureIntervalSeconds()))
    , DataErasureBSCInterval(TDuration::Seconds(config.GetBlobStorageControllerRequestIntervalSeconds()))
    , CurrentWakeupInterval(DataErasureInterval)
    , BSC(MakeBSControllerID())
    , IsManualStartup((DataErasureInterval.Seconds() == 0 ? true : false))
{
    const auto ctx = SchemeShard->ActorContext();
    ctx.RegisterWithSameMailbox(Queue);

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Created: Timeout# " << config.GetTimeoutSeconds()
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << config.GetInflightLimit()
        << ", DataErasureInterval# " << DataErasureInterval
        << ", DataErasureBSCInterval# " << DataErasureBSCInterval
        << ", CurrentWakeupInterval# " << CurrentWakeupInterval
        << ", IsManualStartup# " << (IsManualStartup ? "true" : "false"));
}

void TRootDataErasureManager::UpdateConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TRootDataErasureManager::TQueue::TConfig queueConfig = ConvertConfig(config);
    Queue->UpdateConfig(queueConfig);
    DataErasureInterval = TDuration::Seconds(config.GetDataErasureIntervalSeconds());
    DataErasureBSCInterval = TDuration::Seconds(config.GetBlobStorageControllerRequestIntervalSeconds());
    CurrentWakeupInterval = DataErasureInterval;
    BSC = TTabletId(MakeBSControllerID());
    IsManualStartup = (DataErasureInterval.Seconds() == 0 ? true : false);

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Config updated: Timeout# " << queueConfig.Timeout
        << ", Rate# " << Queue->GetRate()
        << ", InflightLimit# " << queueConfig.InflightLimit
        << ", DataErasureInterval# " << DataErasureInterval
        << ", DataErasureBSCInterval# " << DataErasureBSCInterval
        << ", CurrentWakeupInterval# " << CurrentWakeupInterval
        << ", IsManualStartup# " << (IsManualStartup ? "true" : "false"));
}

void TRootDataErasureManager::Start() {
    TDataErasureManager::Start();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Start: Status# " << static_cast<ui32>(Status));

    Queue->Start();
    if (Status == EDataErasureStatus::UNSPECIFIED) {
        SchemeShard->MarkFirstRunRootDataErasureManager();
        ScheduleDataErasureWakeup();
    } else if (Status == EDataErasureStatus::COMPLETED) {
        ScheduleDataErasureWakeup();
    } else {
        ClearOperationQueue();
        Continue();
    }
}

void TRootDataErasureManager::Stop() {
    TDataErasureManager::Stop();
    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Stop");

    Queue->Stop();
}

void TRootDataErasureManager::ClearOperationQueue() {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Clear operation queue and active pipes");

    Queue->Clear();
    ActivePipes.clear();
}

void TRootDataErasureManager::ClearWaitingDataErasureRequests(NIceDb::TNiceDb& db) {
    const auto ctx = SchemeShard->ActorContext();
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Clear WaitingDataErasureTenants: Size# " << WaitingDataErasureTenants.size());

    for (const auto& [pathId, status] : WaitingDataErasureTenants) {
        db.Table<Schema::WaitingDataErasureTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
    }
    ClearWaitingDataErasureRequests();
}

void TRootDataErasureManager::ClearWaitingDataErasureRequests() {
    WaitingDataErasureTenants.clear();
}

void TRootDataErasureManager::Run(NIceDb::TNiceDb& db) {
    CounterDataErasureOk = 0;
    CounterDataErasureTimeout = 0;
    Status = EDataErasureStatus::IN_PROGRESS;
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
        WaitingDataErasureTenants[pathId] = EDataErasureStatus::IN_PROGRESS;
        db.Table<Schema::WaitingDataErasureTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Update<Schema::WaitingDataErasureTenants::Status>(WaitingDataErasureTenants[pathId]);
    }
    if (WaitingDataErasureTenants.empty()) {
        Status = EDataErasureStatus::IN_PROGRESS_BSC;
    }
    db.Table<Schema::DataErasureGenerations>().Key(Generation).Update<Schema::DataErasureGenerations::Status,
                                                                      Schema::DataErasureGenerations::StartTime>(Status, StartTime.MicroSeconds());

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Run: Queue.Size# " << Queue->Size()
        << ", WaitingDataErasureTenants.size# " << WaitingDataErasureTenants.size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TRootDataErasureManager::Continue() {
    if (Status == EDataErasureStatus::IN_PROGRESS) {
        for (const auto& [pathId, status] : WaitingDataErasureTenants) {
            if (status == EDataErasureStatus::IN_PROGRESS) {
                Enqueue(pathId);
            }
        }
    } else if (Status == EDataErasureStatus::IN_PROGRESS_BSC) {
        SendRequestToBSC();
    }

    const auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Continue: Queue.Size# " << Queue->Size()
        << ", Status# " << static_cast<ui32>(Status));
}

void TRootDataErasureManager::ScheduleDataErasureWakeup() {
    if (IsManualStartup || IsDataErasureWakeupScheduled) {
        return;
    }

    const auto ctx = SchemeShard->ActorContext();
    ctx.Schedule(CurrentWakeupInterval, new TEvSchemeShard::TEvWakeupToRunDataErasure);
    IsDataErasureWakeupScheduled = true;

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] ScheduleDataErasureWakeup: Interval# " << CurrentWakeupInterval << ", Timestamp# " << AppData(ctx)->TimeProvider->Now());
}

void TRootDataErasureManager::WakeupToRunDataErasure(TEvSchemeShard::TEvWakeupToRunDataErasure::TPtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    IsDataErasureWakeupScheduled = false;
    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] WakeupToRunDataErasure: Timestamp# " << AppData(ctx)->TimeProvider->Now());
    SchemeShard->RunDataErasure(true);
}

NOperationQueue::EStartStatus TRootDataErasureManager::StartDataErasure(const TPathId& pathId) {
    UpdateMetrics();

    auto ctx = SchemeShard->ActorContext();
    auto it = SchemeShard->SubDomains.find(pathId);
    if (it == SchemeShard->SubDomains.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Start] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " at schemeshard# " << SchemeShard->TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& tenantSchemeShardId = it->second->GetTenantSchemeShardID();

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Start] Data erasure "
        "for pathId# " << pathId
        << ", tenant schemeshard# " << tenantSchemeShardId
        << ", next wakeup# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " tenants"
        << ", running# " << Queue->RunningSize() << " tenants"
        << " at schemeshard " << SchemeShard->TabletID());

    std::unique_ptr<TEvSchemeShard::TEvTenantDataErasureRequest> request(
        new TEvSchemeShard::TEvTenantDataErasureRequest(Generation));

    ActivePipes[pathId] = SchemeShard->PipeClientCache->Send(
        ctx,
        ui64(tenantSchemeShardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TRootDataErasureManager::OnTimeout(const TPathId& pathId) {
    CounterDataErasureTimeout++;
    UpdateMetrics();

    ActivePipes.erase(pathId);

    auto ctx = SchemeShard->ActorContext();
    if (!SchemeShard->SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Timeout] Failed to resolve subdomain info "
            "for path# " << pathId
            << " at schemeshard# " << SchemeShard->TabletID());
        return;
    }

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Timeout] Data erasure timeouted "
        "for pathId# " << pathId
        << ", next wakeup in# " << Queue->GetWakeupDelta()
        << ", rate# " << Queue->GetRate()
        << ", in queue# " << Queue->Size() << " tenants"
        << ", running# " << Queue->RunningSize() << " tenants"
        << " at schemeshard " << SchemeShard->TabletID());

    // retry
    Enqueue(pathId);
}

void TRootDataErasureManager::Enqueue(const TPathId& pathId) {
    auto ctx = SchemeShard->ActorContext();

    if (Queue->Enqueue(pathId)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootDataErasureManager] [Enqueue] Enqueued pathId# " << pathId << " at schemeshard " << SchemeShard->TabletID());
        UpdateMetrics();
    } else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootDataErasureManager] [Enqueue] Skipped or already exists pathId# " << pathId << " at schemeshard " << SchemeShard->TabletID());
    }
}

void TRootDataErasureManager::HandleDisconnect(TTabletId tabletId, const TActorId& clientId, const TActorContext& ctx) {
    if (tabletId == BSC) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootDataErasureManager] HandleDisconnect resend request to BSC at schemeshard " << SchemeShard->TabletID());

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

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Disconnect] Data erasure disconnect "
        "to tablet: " << tabletId
        << ", at schemeshard: " << SchemeShard->TabletID());

    ActivePipes.erase(pathId);
    StartDataErasure(pathId);
}

void TRootDataErasureManager::OnDone(const TPathId& pathId, NIceDb::TNiceDb& db) {
    auto duration = Queue->OnDone(pathId);

    auto ctx = SchemeShard->ActorContext();
    if (!SchemeShard->SubDomains.contains(pathId)) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Finished] Failed to resolve subdomain info "
            "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup in# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " tenants"
            << ", running# " << Queue->RunningSize() << " tenants"
            << " at schemeshard " << SchemeShard->TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Finished] Data erasure completed "
            "for pathId# " << pathId
            << " in# " << duration.MilliSeconds() << " ms"
            << ", next wakeup# " << Queue->GetWakeupDelta()
            << ", rate# " << Queue->GetRate()
            << ", in queue# " << Queue->Size() << " tenants"
            << ", running# " << Queue->RunningSize() << " tenants"
            << " at schemeshard " << SchemeShard->TabletID());
    }

    ActivePipes.erase(pathId);
    auto it = WaitingDataErasureTenants.find(pathId);
    if (it != WaitingDataErasureTenants.end()) {
        db.Table<Schema::WaitingDataErasureTenants>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
        WaitingDataErasureTenants.erase(it);
    }

    CounterDataErasureOk++;
    UpdateMetrics();

    if (WaitingDataErasureTenants.empty()) {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[RootDataErasureManager] Data erasure in tenants is completed. Send request to BS controller");
        Status = EDataErasureStatus::IN_PROGRESS_BSC;
        db.Table<Schema::DataErasureGenerations>().Key(Generation).Update<Schema::DataErasureGenerations::Status>(Status);
    }
}

bool TRootDataErasureManager::OnDone(const TTabletId&, NIceDb::TNiceDb&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [OnDone] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
    return false;
}

void TRootDataErasureManager::ScheduleRequestToBSC() {
    if (IsRequestToBSCScheduled) {
        return;
    }

    auto ctx = SchemeShard->ActorContext();
    ctx.Schedule(DataErasureBSCInterval, new TEvSchemeShard::TEvWakeupToRunDataErasureBSC);
    IsRequestToBSCScheduled = true;

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] ScheduleRequestToBSC: Interval# " << DataErasureBSCInterval);
}

void TRootDataErasureManager::SendRequestToBSC() {
    auto ctx = SchemeShard->ActorContext();
    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] SendRequestToBSC: Generation# " << Generation);

    IsRequestToBSCScheduled = false;
    std::unique_ptr<TEvBlobStorage::TEvControllerShredRequest> request(
        new TEvBlobStorage::TEvControllerShredRequest(Generation));
    SchemeShard->PipeClientCache->Send(ctx, MakeBSControllerID(), request.release());
}

void TRootDataErasureManager::Complete() {
    Status = EDataErasureStatus::COMPLETED;
    auto ctx = SchemeShard->ActorContext();
    FinishTime = AppData(ctx)->TimeProvider->Now();
    TDuration dataErasureDuration = FinishTime - StartTime;
    if (dataErasureDuration > DataErasureInterval) {
        if (!IsManualStartup) {
            SchemeShard->RunDataErasure(true);
        }
    } else {
        CurrentWakeupInterval = DataErasureInterval - dataErasureDuration;
        ScheduleDataErasureWakeup();
    }

    LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Complete: Generation# " << Generation
        << ", duration# " << dataErasureDuration.Seconds() << " s");
}

bool TRootDataErasureManager::Restore(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<Schema::DataErasureGenerations>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        if (rowset.EndOfSet()) {
            Status = EDataErasureStatus::UNSPECIFIED;
        } else {
            Generation = 0;
            Status = EDataErasureStatus::UNSPECIFIED;
            while (!rowset.EndOfSet()) {
                ui64 generation = rowset.GetValue<Schema::DataErasureGenerations::Generation>();
                if (generation >= Generation) {
                    Generation = generation;
                    StartTime = TInstant::FromValue(rowset.GetValue<Schema::DataErasureGenerations::StartTime>());
                    Status = rowset.GetValue<Schema::DataErasureGenerations::Status>();
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
            if (Status == EDataErasureStatus::UNSPECIFIED || Status == EDataErasureStatus::COMPLETED) {
                auto ctx = SchemeShard->ActorContext();
                TDuration interval = AppData(ctx)->TimeProvider->Now() - StartTime;
                if (interval > DataErasureInterval) {
                    CurrentWakeupInterval = TDuration::Zero();
                } else {
                    CurrentWakeupInterval = DataErasureInterval - interval;
                }
            }
        }
    }

    ui32 numberDataErasureTenantsInRunning = 0;
    {
        auto rowset = db.Table<Schema::WaitingDataErasureTenants>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        while (!rowset.EndOfSet()) {
            TOwnerId ownerPathId = rowset.GetValue<Schema::WaitingDataErasureTenants::OwnerPathId>();
            TLocalPathId localPathId = rowset.GetValue<Schema::WaitingDataErasureTenants::LocalPathId>();
            TPathId pathId(ownerPathId, localPathId);
            Y_VERIFY_S(SchemeShard->PathsById.contains(pathId), "Path doesn't exist, pathId: " << pathId);
            TPathElement::TPtr path = SchemeShard->PathsById.at(pathId);
            Y_VERIFY_S(path->IsDomainRoot(), "Path is not a subdomain, pathId: " << pathId);

            Y_ABORT_UNLESS(SchemeShard->SubDomains.contains(pathId));

            EDataErasureStatus status = rowset.GetValue<Schema::WaitingDataErasureTenants::Status>();
            WaitingDataErasureTenants[pathId] = status;
            if (status == EDataErasureStatus::IN_PROGRESS) {
                numberDataErasureTenantsInRunning++;
            }

            if (!rowset.Next()) {
                return false;
            }
        }
        if (Status == EDataErasureStatus::IN_PROGRESS && (WaitingDataErasureTenants.empty() || numberDataErasureTenantsInRunning == 0)) {
            Status = EDataErasureStatus::IN_PROGRESS_BSC;
        }
    }

    auto ctx = SchemeShard->ActorContext();
    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "[RootDataErasureManager] Restore: Generation# " << Generation
        << ", Status# " << static_cast<ui32>(Status)
        << ", WakeupInterval# " << CurrentWakeupInterval.Seconds() << " s"
        << ", NumberDataErasureTenantsInRunning# " << numberDataErasureTenantsInRunning);

    return true;
}

bool TRootDataErasureManager::Remove(const TPathId& pathId) {
    auto it = WaitingDataErasureTenants.find(pathId);
    if (it != WaitingDataErasureTenants.end()) {
        Queue->Remove(pathId);
        ActivePipes.erase(pathId);
        WaitingDataErasureTenants.erase(it);

        if (WaitingDataErasureTenants.empty()) {
            Status = EDataErasureStatus::IN_PROGRESS_BSC;
            SendRequestToBSC();
        }
        return true;
    }
    return false;
}

bool TRootDataErasureManager::Remove(const TShardIdx&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [Remove] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
    return false;
}

void TRootDataErasureManager::HandleNewPartitioning(const std::vector<TShardIdx>&, NIceDb::TNiceDb&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [HandleNewPartitioning] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
}

void TRootDataErasureManager::SyncBscGeneration(NIceDb::TNiceDb& db, ui64 currentBscGeneration) {
    db.Table<Schema::DataErasureGenerations>().Key(GetGeneration()).Delete();
    SetGeneration(currentBscGeneration + 1);
    db.Table<Schema::DataErasureGenerations>().Key(GetGeneration()).Update<Schema::DataErasureGenerations::Status,
                                                                           Schema::DataErasureGenerations::StartTime>(GetStatus(), StartTime.MicroSeconds());
}

bool TRootDataErasureManager::CanDeleteShard(const TShardIdx&) {
    return true;
}

void TRootDataErasureManager::MarkShardForDelete(const TShardIdx&) {
    auto ctx = SchemeShard->ActorContext();
    LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[RootDataErasureManager] [MarkShardForDelete] Cannot execute in root schemeshard: " << SchemeShard->TabletID());
}

void TRootDataErasureManager::UpdateMetrics() {
    SchemeShard->TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_SIZE].Set(Queue->Size());
    SchemeShard->TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_RUNNING].Set(Queue->RunningSize());
    SchemeShard->TabletCounters->Simple()[COUNTER_DATA_ERASURE_OK].Set(CounterDataErasureOk);
    SchemeShard->TabletCounters->Simple()[COUNTER_DATA_ERASURE_TIMEOUT].Set(CounterDataErasureTimeout);
}

TRootDataErasureManager::TQueue::TConfig TRootDataErasureManager::ConvertConfig(const NKikimrConfig::TDataErasureConfig& config) {
    TQueue::TConfig queueConfig;
    queueConfig.IsCircular = false;
    queueConfig.MaxRate = config.GetMaxRate();
    queueConfig.InflightLimit = config.GetInflightLimit();
    queueConfig.Timeout = TDuration::Seconds(config.GetTimeoutSeconds());

    return queueConfig;
}

struct TSchemeShard::TTxDataErasureManagerInit : public TSchemeShard::TRwTxBase {
    TTxDataErasureManagerInit(TSelf* self)
        : TRwTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_DATA_ERASURE_INIT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxDataErasureManagerInit Execute at schemeshard: " << Self->TabletID());
        NIceDb::TNiceDb db(txc.DB);
        Self->DataErasureManager->SetStatus(EDataErasureStatus::COMPLETED);
        db.Table<Schema::DataErasureGenerations>().Key(0).Update<Schema::DataErasureGenerations::Status,
                                                               Schema::DataErasureGenerations::StartTime>(Self->DataErasureManager->GetStatus(), AppData(ctx)->TimeProvider->Now().MicroSeconds());
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxDataErasureManagerInit Complete at schemeshard: " << Self->TabletID());
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxDataErasureManagerInit() {
    return new TTxDataErasureManagerInit(this);
}

struct TSchemeShard::TTxRunDataErasure : public TSchemeShard::TRwTxBase {
    bool IsNewDataErasure;
    bool NeedSendRequestToBSC = false;

    TTxRunDataErasure(TSelf *self, bool isNewDataErasure)
        : TRwTxBase(self)
        , IsNewDataErasure(isNewDataErasure)
    {}

    TTxType GetTxType() const override { return TXTYPE_RUN_DATA_ERASURE; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxRunDataErasure Execute at schemeshard: " << Self->TabletID());

        NIceDb::TNiceDb db(txc.DB);
        auto& dataErasureManager = Self->DataErasureManager;
        if (IsNewDataErasure) {
            dataErasureManager->ClearOperationQueue();

            dataErasureManager->ClearWaitingDataErasureRequests(db);
            dataErasureManager->IncGeneration();
            dataErasureManager->Run(db);
        }
        if (Self->DataErasureManager->GetStatus() == EDataErasureStatus::IN_PROGRESS_BSC) {
            NeedSendRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxRunDataErasure Complete at schemeshard: " << Self->TabletID()
            << ", NeedSendRequestToBSC# " << (NeedSendRequestToBSC ? "true" : "false"));

        if (NeedSendRequestToBSC) {
            Self->DataErasureManager->SendRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxRunDataErasure(bool isNewDataErasure) {
    return new TTxRunDataErasure(this, isNewDataErasure);
}

struct TSchemeShard::TTxCompleteDataErasureTenant : public TSchemeShard::TRwTxBase {
    const TEvSchemeShard::TEvTenantDataErasureResponse::TPtr Ev;
    bool NeedSendRequestToBSC = false;

    TTxCompleteDataErasureTenant(TSelf* self, const TEvSchemeShard::TEvTenantDataErasureResponse::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_DATA_ERASURE_TENANT; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TTxCompleteDataErasureTenant Execute at schemeshard: " << Self->TabletID());

        const auto& record = Ev->Get()->Record;
        auto& manager = Self->DataErasureManager;
        const ui64 completedGeneration = record.GetGeneration();
        if (completedGeneration != manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteDataErasureTenant Unknown generation#" << completedGeneration << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            return;
        }

        NIceDb::TNiceDb db(txc.DB);
        auto pathId = TPathId(
            record.GetPathId().GetOwnerId(),
            record.GetPathId().GetLocalId());
        manager->OnDone(pathId, db);
        if (manager->GetStatus() == EDataErasureStatus::IN_PROGRESS_BSC) {
            NeedSendRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureTenant Complete at schemeshard: " << Self->TabletID()
            << ", NeedSendRequestToBSC# " << (NeedSendRequestToBSC ? "true" : "false"));
        if (NeedSendRequestToBSC) {
            Self->DataErasureManager->SendRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasureTenant(TEvSchemeShard::TEvTenantDataErasureResponse::TPtr& ev) {
    return new TTxCompleteDataErasureTenant(this, ev);
}

struct TSchemeShard::TTxCompleteDataErasureBSC : public TSchemeShard::TRwTxBase {
    const TEvBlobStorage::TEvControllerShredResponse::TPtr Ev;
    bool NeedScheduleRequestToBSC = false;

    TTxCompleteDataErasureBSC(TSelf* self, const TEvBlobStorage::TEvControllerShredResponse::TPtr& ev)
        : TRwTxBase(self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_COMPLETE_DATA_ERASURE_BSC; }

    void DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureBSC Execute at schemeshard: " << Self->TabletID());

        const auto& record = Ev->Get()->Record;
        auto& manager = Self->DataErasureManager;
        NIceDb::TNiceDb db(txc.DB);
        if (ui64 currentBscGeneration = record.GetCurrentGeneration(); currentBscGeneration > manager->GetGeneration()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TTxCompleteDataErasureBSC Unknown generation#" << currentBscGeneration << ", Expected gen# " << manager->GetGeneration() << " at schemestard: " << Self->TabletID());
            manager->SyncBscGeneration(db, currentBscGeneration);
            manager->SendRequestToBSC();
            return;
        }

        if (record.GetCompleted()) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCompleteDataErasureBSC: Data shred in BSC is completed");
            manager->Complete();
            db.Table<Schema::DataErasureGenerations>().Key(Self->DataErasureManager->GetGeneration()).Update<Schema::DataErasureGenerations::Status>(Self->DataErasureManager->GetStatus());
        } else {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxCompleteDataErasureBSC: Progress data shred in BSC " << static_cast<double>(record.GetProgress10k()) / 100 << "%");
            NeedScheduleRequestToBSC = true;
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "TTxCompleteDataErasureBSC Complete at schemeshard: " << Self->TabletID()
            << ", NeedScheduleRequestToBSC# " << (NeedScheduleRequestToBSC ? "true" : "false"));

        if (NeedScheduleRequestToBSC) {
            Self->DataErasureManager->ScheduleRequestToBSC();
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxCompleteDataErasureBSC(TEvBlobStorage::TEvControllerShredResponse::TPtr& ev) {
    return new TTxCompleteDataErasureBSC(this, ev);
}

} // NKikimr::NSchemeShard
