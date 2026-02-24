#include "columnshard_impl.h"

#include "bg_tasks/manager/manager.h"
#include "blobs_reader/actor.h"
#include "counters/aggregation/table_stats.h"
#include "engines/column_engine_logs.h"
#include "engines/writer/buffer/actor2.h"
#include "hooks/abstract/abstract.h"
#include "resource_subscriber/actor.h"
#include "transactions/locks/read_finished.h"

#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/adapter/adapter.h>
#include <ydb/core/tx/columnshard/diagnostics/scan_diagnostics_actor.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/probes.h>
#include <ydb/core/tx/columnshard/tablet/write_queue.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/priorities/usage/service.h>
#include <ydb/core/tx/tiering/manager.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

namespace NKikimr {

IActor* CreateColumnShard(const TActorId& tablet, TTabletStorageInfo* info) {
    return new NColumnShard::TColumnShard(info, tablet);
}

}   // namespace NKikimr

namespace NKikimr::NColumnShard {

void TColumnShard::CleanupActors(const TActorContext& ctx) {
    if (BackgroundSessionsManager) {
        BackgroundSessionsManager->Stop();
    }
    InFlightReadsTracker.Stop(this);
    ctx.Send(ResourceSubscribeActor, new TEvents::TEvPoisonPill);
    ctx.Send(BufferizationPortionsWriteActorId, new TEvents::TEvPoisonPill);
    NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>::KillSource(SelfId());
    if (!!OperationsManager) {
        OperationsManager->StopWriting(TStringBuilder{} << "ColumnShard tablet id: " << TabletID() << " was stoped");
    }
    if (PrioritizationClientId) {
        NPrioritiesQueue::TCompServiceOperator::UnregisterClient(PrioritizationClientId);
    }
    for (auto&& i : ActorsToStop) {
        ctx.Send(i, new TEvents::TEvPoisonPill);
    }

    StoragesManager->Stop();
    DataLocksManager->Stop();
    if (Tiers) {
        Tiers->Stop(true);
    }
    NYDBTest::TControllers::GetColumnShardController()->OnCleanupActors(TabletID());
}

void TColumnShard::BecomeBroken(const TActorContext& ctx) {
    Become(&TThis::StateBroken);
    ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
    CleanupActors(ctx);
}

void TColumnShard::TrySwitchToWork(const TActorContext& ctx) {
    if (Tiers->GetAwaitedConfigsCount()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "skip_switch_to_work")("reason", "tiering_metadata_not_ready");
        return;
    }
    if (!IsTxInitFinished) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "skip_switch_to_work")("reason", "db_reading_not_finished");
        return;
    }
    ProgressTxController->OnTabletInit();
    {
        const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())(
            "self_id", SelfId())("process", "SwitchToWork");
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "SwitchToWork");
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "SignalTabletActive");
        TryRegisterMediatorTimeCast();
        EnqueueProgressTx(ctx, std::nullopt);
        OnTieringModified();
    }
    Counters.GetCSCounters().OnIndexMetadataLimit(NOlap::IColumnEngine::GetMetadataLimit());
    EnqueueBackgroundActivities();
    BackgroundSessionsManager->Start();
    ctx.Send(SelfId(), new NActors::TEvents::TEvWakeup());
    ctx.Send(SelfId(), new TEvPrivate::TEvPeriodicWakeup());
    ctx.Send(SelfId(), new TEvPrivate::TEvPingSnapshotsUsage());
    ExecutorStatsEvInflight++;
    ctx.Send(SelfId(), new TEvPrivate::TEvReportExecutorStatistics);
    ScheduleBaseStatistics();

    NYDBTest::TControllers::GetColumnShardController()->OnSwitchToWork(TabletID());
    AFL_VERIFY(!!StartInstant);
    Counters.GetCSCounters().Initialization.OnSwitchToWork(TMonotonic::Now() - *StartInstant, TMonotonic::Now() - CreateInstant);
    NYDBTest::TControllers::GetColumnShardController()->OnTabletInitCompleted(*this);
}

void TColumnShard::OnActivateExecutor(const TActorContext& ctx) {
    using namespace NOlap::NReader;
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YDB_CS_READER));
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YDB_CS));
    StartInstant = TMonotonic::Now();
    Counters.GetCSCounters().Initialization.OnActivateExecutor(TMonotonic::Now() - CreateInstant);
    const TLogContextGuard gLogging =
        NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("self_id", SelfId());
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "OnActivateExecutor");
    Executor()->RegisterExternalTabletCounters(TabletCountersHolder.release());

    const auto selfActorId = SelfId();
    StoragesManager->Initialize(Executor()->Generation());
    Tiers = std::make_shared<TTiersManager>(TabletID(), SelfId(), [selfActorId](const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "tiering_new_event");
        ctx.Send(selfActorId, new TEvPrivate::TEvTieringModified);
    });
    Tiers->Start(Tiers);
    if (const auto& tiersSnapshot = NYDBTest::TControllers::GetColumnShardController()->GetOverrideTierConfigs(); !tiersSnapshot.empty()) {
        for (const auto& [id, tier] : tiersSnapshot) {
            Tiers->ActivateTiers({ NTiers::TExternalStorageId(id) }, false);
            Tiers->UpdateTierConfig(tier, NTiers::TExternalStorageId(id), false);
        }
    }
    BackgroundSessionsManager = std::make_shared<NOlap::NBackground::TSessionsManager>(
        std::make_shared<NBackground::TAdapter>(selfActorId, (NOlap::TTabletId)TabletID(), *this));

    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "initialize_tiring_finished");
    auto& icb = *AppData(ctx)->Icb;
    SpaceWatcherId = RegisterWithSameMailbox(SpaceWatcher);
    ScanDiagnosticsActorId = Register(new NDiagnostics::TScanDiagnosticsActor());
    ActorsToStop.push_back(ScanDiagnosticsActorId);
    Limits.RegisterControls(icb);
    Settings.RegisterControls(icb);
    ResourceSubscribeActor = ctx.Register(new NOlap::NResourceBroker::NSubscribe::TActor(TabletID(), SelfId()));
    BufferizationPortionsWriteActorId = ctx.Register(new NOlap::NWritingPortions::TActor(TabletID(), SelfId()));
    DataAccessorsManager = std::make_shared<NOlap::NDataAccessorControl::TActorAccessorsManager>(SelfId());
    ColumnDataManager = std::make_shared<NOlap::NColumnFetching::TColumnDataManager>(SelfId());
    NormalizerController.SetDataAccessorsManager(DataAccessorsManager);
    PrioritizationClientId = NPrioritiesQueue::TCompServiceOperator::RegisterClient();
    Execute(CreateTxInitSchema(), ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvTieringModified::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
    if (const auto& tiersSnapshot = NYDBTest::TControllers::GetColumnShardController()->GetOverrideTierConfigs(); !tiersSnapshot.empty()) {
        for (const auto& [id, tier] : tiersSnapshot) {
            Tiers->UpdateTierConfig(tier, NTiers::TExternalStorageId(id), false);
        }
    }

    OnTieringModified();
    NYDBTest::TControllers::GetColumnShardController()->OnTieringModified(Tiers);
}

void TColumnShard::HandleInit(TEvPrivate::TEvTieringModified::TPtr& /*ev*/, const TActorContext& ctx) {
    TrySwitchToWork(ctx);
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    if (clientId == StatsReportPipe) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            LOG_S_DEBUG("Connected to " << tabletId << " at tablet " << TabletID());
        } else {
            LOG_S_INFO("Failed to connect to " << tabletId << " at tablet " << TabletID());
            LastStats = {};
            StatsReportPipe = {};
        }
        ExecutorStatsEvInflight++;
        ActorContext().Send(SelfId(), new TEvPrivate::TEvReportExecutorStatistics());
        return;
    }

    if (PipeClientCache->OnConnect(ev)) {
        LOG_S_DEBUG("Connected to " << tabletId << " at tablet " << TabletID());
        return;
    }

    LOG_S_INFO("Failed to connect to " << tabletId << " at tablet " << TabletID());
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    LOG_S_DEBUG("Client pipe reset to " << tabletId << " at tablet " << TabletID());

    if (clientId == StatsReportPipe) {
        StatsReportPipe = {};
        LastStats = {};
        return;
    }

    PipeClientCache->OnDisconnect(ev);
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&) {
    PipeServersInterconnectSessions.emplace(ev->Get()->ServerId, ev->Get()->InterconnectSession);
    LOG_S_DEBUG("Server pipe connected at tablet " << TabletID());
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx) {
    PipeServersInterconnectSessions.erase(ev->Get()->ServerId);
    ctx.Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        std::make_unique<NOverload::TEvOverloadPipeServerDisconnected>(
            NOverload::TColumnShardInfo{.ColumnShardId = SelfId(), .TabletId = TabletID()}, NOverload::TPipeServerInfo{.PipeServerId = ev->Get()->ServerId, .InterconnectSessionId = {}}));
    LOG_S_DEBUG("Server pipe reset at tablet " << TabletID());
}

void TColumnShard::Handle(TEvPrivate::TEvScanStats::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);

    Counters.GetTabletCounters()->IncCounter(COUNTER_SCANNED_ROWS, ev->Get()->Rows);
    Counters.GetTabletCounters()->IncCounter(COUNTER_SCANNED_BYTES, ev->Get()->Bytes);
}

void TColumnShard::Handle(TEvPrivate::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    ui64 readCookie = ev->Get()->RequestCookie;
    LOG_S_DEBUG("Finished read cookie: " << readCookie << " at tablet " << TabletID());
    const NOlap::TVersionedIndex* index = nullptr;
    if (HasIndex()) {
        index = &GetIndexAs<NOlap::TColumnEngineForLogs>().GetVersionedIndex();
    }

    auto readMetaBase = InFlightReadsTracker.ExtractInFlightRequest(ev->Get()->RequestCookie, index, TInstant::Now());
    readMetaBase->OnReadFinished(*this);

    ui64 txId = ev->Get()->TxId;
    if (ScanTxInFlight.contains(txId)) {
        TDuration duration = TAppData::TimeProvider->Now() - ScanTxInFlight[txId];
        Counters.GetTabletCounters()->IncCounter(COUNTER_SCAN_LATENCY, duration);
        ScanTxInFlight.erase(txId);
        Counters.GetTabletCounters()->SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
        Counters.GetTabletCounters()->IncCounter(COUNTER_IMMEDIATE_TX_COMPLETED);
    }
}

void TColumnShard::Handle(TEvPrivate::TEvPingSnapshotsUsage::TPtr& /*ev*/, const TActorContext& ctx) {
    const TDuration stalenessLivetime = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStaleness();
    const TDuration stalenessInMem = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStalenessInMem();
    const TDuration usedLivetime = NYDBTest::TControllers::GetColumnShardController()->GetUsedSnapshotLivetime();
    AFL_VERIFY(usedLivetime < stalenessInMem || (stalenessInMem == usedLivetime && usedLivetime == TDuration::Zero()))("used", usedLivetime)(
                                                "staleness", stalenessInMem);
    const TDuration ping = 0.3 * std::min(stalenessInMem - usedLivetime, stalenessLivetime - stalenessInMem);
    if (auto writeTx = InFlightReadsTracker.Ping(this, stalenessInMem, usedLivetime, TInstant::Now())) {
        Execute(writeTx.release(), ctx);
    }
    ctx.Schedule(NYDBTest::TControllers::GetColumnShardController()->GetStalenessLivetimePing(ping), new TEvPrivate::TEvPingSnapshotsUsage());
}

void TColumnShard::Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Manual) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvPrivate::TEvPeriodicWakeup::MANUAL")("tablet_id", TabletID());
        EnqueueBackgroundActivities();
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvPrivate::TEvPeriodicWakeup")("tablet_id", TabletID());
        SendWaitPlanStep(GetOutdatedStep());
        EnqueueBackgroundActivities();
        ctx.Schedule(PeriodicWakeupActivationPeriod, new TEvPrivate::TEvPeriodicWakeup());
    }
}

void TColumnShard::Handle(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Tag == 0) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvPrivate::TEvPeriodicWakeup::MANUAL")("tablet_id", TabletID());
        const TMonotonic now = TMonotonic::Now();
        GetProgressTxController().PingTimeouts(now);
        ctx.Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup(0));
    } else if (ev->Get()->Tag == 1) {
        WriteTasksQueue->Drain(true, ctx);
    }
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext&) {
    const auto* msg = ev->Get();
    Y_ABORT_UNLESS(msg->TabletId == TabletID());
    MediatorTimeCastEntry = msg->Entry;
    Y_ABORT_UNLESS(MediatorTimeCastEntry);
    LOG_S_DEBUG("Registered with mediator time cast at tablet " << TabletID());

    RescheduleWaitingReads();
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext&) {
    const auto* msg = ev->Get();
    Y_ABORT_UNLESS(msg->TabletId == TabletID());

    Y_ABORT_UNLESS(MediatorTimeCastEntry);
    ui64 step = MediatorTimeCastEntry->Get(TabletID());
    LOG_S_DEBUG("Notified by mediator time cast with PlanStep# " << step << " at tablet " << TabletID());

    for (auto it = MediatorTimeCastWaitingSteps.begin(); it != MediatorTimeCastWaitingSteps.end();) {
        if (step < *it) {
            break;
        }
        it = MediatorTimeCastWaitingSteps.erase(it);
    }

    RescheduleWaitingReads();
    EnqueueBackgroundActivities(true);
}

void TColumnShard::UpdateIndexCounters() {
    if (!TablesManager.HasPrimaryIndex()) {
        return;
    }

    const std::shared_ptr<const TTabletCountersHandle>& counters = Counters.GetTabletCounters();
    counters->SetCounter(COUNTER_INDEX_TABLES, Counters.GetPortionIndexCounters()->GetTablesCount());

    auto insertedStats =
        Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TPortionsByType<NOlap::NPortion::EProduced::INSERTED>());
    counters->SetCounter(COUNTER_INSERTED_PORTIONS, insertedStats.GetCount());
    //    counters->SetCounter(COUNTER_INSERTED_BLOBS, insertedStats.GetBlobs());
    counters->SetCounter(COUNTER_INSERTED_ROWS, insertedStats.GetRecordsCount());
    counters->SetCounter(COUNTER_INSERTED_BYTES, insertedStats.GetBlobBytes());
    counters->SetCounter(COUNTER_INSERTED_RAW_BYTES, insertedStats.GetRawBytes());

    auto compactedStats =
        Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TPortionsByType<NOlap::NPortion::EProduced::COMPACTED>());
    counters->SetCounter(COUNTER_COMPACTED_PORTIONS, compactedStats.GetCount());
    //    counters->SetCounter(COUNTER_COMPACTED_BLOBS, compactedStats.GetBlobs());
    counters->SetCounter(COUNTER_COMPACTED_ROWS, compactedStats.GetRecordsCount());
    counters->SetCounter(COUNTER_COMPACTED_BYTES, compactedStats.GetBlobBytes());
    counters->SetCounter(COUNTER_COMPACTED_RAW_BYTES, compactedStats.GetRawBytes());

    auto splitCompactedStats =
        Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TPortionsByType<NOlap::NPortion::EProduced::SPLIT_COMPACTED>());
    counters->SetCounter(COUNTER_SPLIT_COMPACTED_PORTIONS, splitCompactedStats.GetCount());
    //    counters->SetCounter(COUNTER_SPLIT_COMPACTED_BLOBS, splitCompactedStats.GetBlobs());
    counters->SetCounter(COUNTER_SPLIT_COMPACTED_ROWS, splitCompactedStats.GetRecordsCount());
    counters->SetCounter(COUNTER_SPLIT_COMPACTED_BYTES, splitCompactedStats.GetBlobBytes());
    counters->SetCounter(COUNTER_SPLIT_COMPACTED_RAW_BYTES, splitCompactedStats.GetRawBytes());

    auto inactiveStats =
        Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TPortionsByType<NOlap::NPortion::EProduced::INACTIVE>());
    counters->SetCounter(COUNTER_INACTIVE_PORTIONS, inactiveStats.GetCount());
    //    counters->SetCounter(COUNTER_INACTIVE_BLOBS, inactiveStats.GetBlobs());
    counters->SetCounter(COUNTER_INACTIVE_ROWS, inactiveStats.GetRecordsCount());
    counters->SetCounter(COUNTER_INACTIVE_BYTES, inactiveStats.GetBlobBytes());
    counters->SetCounter(COUNTER_INACTIVE_RAW_BYTES, inactiveStats.GetRawBytes());

    auto evictedStats =
        Counters.GetPortionIndexCounters()->GetTotalStats(TPortionIndexStats::TPortionsByType<NOlap::NPortion::EProduced::EVICTED>());
    counters->SetCounter(COUNTER_EVICTED_PORTIONS, evictedStats.GetCount());
    //    counters->SetCounter(COUNTER_EVICTED_BLOBS, evictedStats.GetBlobs());
    counters->SetCounter(COUNTER_EVICTED_ROWS, evictedStats.GetRecordsCount());
    counters->SetCounter(COUNTER_EVICTED_BYTES, evictedStats.GetBlobBytes());
    counters->SetCounter(COUNTER_EVICTED_RAW_BYTES, evictedStats.GetRawBytes());

    LOG_S_DEBUG("Index: tables " << Counters.GetPortionIndexCounters()->GetTablesCount() << " inserted " << insertedStats.DebugString()
                                 << " compacted " << compactedStats.DebugString() << " s-compacted " << splitCompactedStats.DebugString()
                                 << " inactive " << inactiveStats.DebugString() << " evicted " << evictedStats.DebugString() << " at tablet "
                                 << TabletID());
}

ui64 TColumnShard::MemoryUsage() const {
    ui64 memory = ProgressTxController->GetMemoryUsage() + ScanTxInFlight.size() * (sizeof(ui64) + sizeof(TInstant)) +
                  (WaitingScans.size()) * (sizeof(NOlap::TSnapshot) + sizeof(void*));
    memory += TablesManager.GetMemoryUsage();
    return memory;
}

void TColumnShard::UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage) {
    auto* metrics = Executor()->GetResourceMetrics();
    if (!metrics) {
        return;
    }

    ui64 storageBytes =
        Counters.GetTabletCounters()->GetValue(COUNTER_PREPARED_BYTES) + Counters.GetTabletCounters()->GetValue(COUNTER_COMMITTED_BYTES) +
        Counters.GetTabletCounters()->GetValue(COUNTER_INSERTED_BYTES) + Counters.GetTabletCounters()->GetValue(COUNTER_COMPACTED_BYTES) +
        Counters.GetTabletCounters()->GetValue(COUNTER_SPLIT_COMPACTED_BYTES) + Counters.GetTabletCounters()->GetValue(COUNTER_INACTIVE_BYTES);

    ui64 memory = MemoryUsage();

    TInstant now = TAppData::TimeProvider->Now();
    metrics->CPU.Increment(usage.CPUExecTime, now);
    metrics->Network.Increment(usage.Network, now);
    //metrics->StorageSystem
    metrics->StorageUser.Set(storageBytes);
    metrics->Memory.Set(memory);
    //metrics->ReadThroughput
    //metrics->WriteThroughput

    metrics->TryUpdate(ctx);
}

void TColumnShard::FillOlapStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev, IExecutor* executor) {
    ev->Record.SetShardState(2);
    ev->Record.SetRound(StatsReportRound);

    if (executor) {
        ev->Record.SetGeneration(executor->Generation());
    }

    ev->Record.SetNodeId(ctx.SelfID.NodeId());
    ev->Record.SetStartTime(StartTime().MilliSeconds());

    if (executor) {
        if (auto* resourceMetrics = executor->GetResourceMetrics()) {
            resourceMetrics->Fill(*ev->Record.MutableTabletMetrics());
        }
    }

    TTableStatsBuilder statsBuilder(Counters, executor);
    statsBuilder.FillTotalTableStats(*ev->Record.MutableTableStats());
}

void TColumnShard::FillColumnTableStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev, IExecutor* executor) {
    auto tables = TablesManager.GetTables();
    TTableStatsBuilder tableStatsBuilder(Counters);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("There are stats for tables", tables.size());
    ev->Record.ClearTables();
    for (const auto& [internalPathId, table] : tables) {
        for (const auto& unifiedPathId : table.GetPathIds()) {
            const auto& schemeShardLocalPathId = unifiedPathId.SchemeShardLocalPathId;
            auto* periodicTableStats = ev->Record.AddTables();
            periodicTableStats->SetDatashardId(TabletID());
            periodicTableStats->SetTableLocalId(schemeShardLocalPathId.GetRawValue());

            periodicTableStats->SetShardState(2);
            if (ev->Record.HasGeneration()) {
                periodicTableStats->SetGeneration(ev->Record.GetGeneration());
            }
            periodicTableStats->SetRound(StatsReportRound);
            periodicTableStats->SetNodeId(ctx.SelfID.NodeId());
            periodicTableStats->SetStartTime(StartTime().MilliSeconds());

            if (executor) {
                if (auto* resourceMetrics = executor->GetResourceMetrics()) {
                    resourceMetrics->Fill(*ev->Record.MutableTabletMetrics());
                }
            }

            tableStatsBuilder.FillTableStats(internalPathId, *(periodicTableStats->MutableTableStats()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("Add stats for table, tableLocalID", schemeShardLocalPathId);
        }
    }
}

void TColumnShard::SendPeriodicStats(bool withExecutor) {
    if (!CurrentSchemeShardId) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("No CurrentSchemeShardId", TabletID());
        return;
    }

    if (!StatsReportPipe) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("StatsReportPipe created", TabletID());
        StatsReportPipe = ActorContext().Register(NTabletPipe::CreateClient(ActorContext().SelfID, CurrentSchemeShardId, {}));
        return;
    }

    if (!TablesManager.GetTabletPathIdOptional()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("TablesManager not ready", TabletID());
        return;
    }

    const auto& tabletSchemeShardLocalPathId = TablesManager.GetTabletPathIdVerified().SchemeShardLocalPathId;
    const TActorContext& ctx = ActorContext();

    if (!LastStats) {
        LastStats = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletID(), tabletSchemeShardLocalPathId.GetRawValue());
    }
    auto newStats = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletID(), tabletSchemeShardLocalPathId.GetRawValue());

    google::protobuf::util::MessageDifferencer differencer;
    differencer.IgnoreField(NKikimrTxDataShard::TEvPeriodicTableStats::descriptor()->FindFieldByName("Round"));
    newStats->Record.CopyFrom(LastStats->Record);
    IExecutor* executor = withExecutor ? Executor() : nullptr;
    FillOlapStats(ctx, newStats, executor);
    FillColumnTableStats(ctx, newStats, executor);

    if (!differencer.Compare(newStats->Record, LastStats->Record)) {
        LastStats->Record.CopyFrom(newStats->Record);
        if (newStats->Record.HasGeneration() && newStats->Record.HasStartTime()) {
            NTabletPipe::SendData(ctx, StatsReportPipe, newStats.release());
            StatsReportRound++;
        }
    }

}

void TColumnShard::Handle(TEvPrivate::TEvReportBaseStatistics::TPtr& /*ev*/) {
    BaseStatsEvInflight--;
    ScheduleBaseStatistics();
    SendPeriodicStats(false);
}

void TColumnShard::Handle(TEvPrivate::TEvReportExecutorStatistics::TPtr& /*ev*/) {
    ExecutorStatsEvInflight--;
    ScheduleExecutorStatistics();
    SendPeriodicStats(true);
}

void TColumnShard::ScheduleBaseStatistics() {
    auto statistics = AppDataVerified().ColumnShardConfig.GetStatistics();
    auto scheduleDuration = TDuration::MilliSeconds(statistics.GetReportBaseStatisticsPeriodMs() + RandomNumber<ui32>(JitterIntervalMS));
    if (!BaseStatsEvInflight) {
        BaseStatsEvInflight++;
        ActorContext().Schedule(scheduleDuration, new TEvPrivate::TEvReportBaseStatistics);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvReportBaseStatistics")("ReportBaseStatisticsPeriodMs", statistics.GetReportBaseStatisticsPeriodMs())("scheduleDuration", scheduleDuration);
    }
}

void TColumnShard::ScheduleExecutorStatistics() {
    auto statistics = AppDataVerified().ColumnShardConfig.GetStatistics();
    auto scheduleDuration = TDuration::MilliSeconds(statistics.GetReportExecutorStatisticsPeriodMs() + RandomNumber<ui32>(JitterIntervalMS));
    if (!ExecutorStatsEvInflight) {
        ExecutorStatsEvInflight++;
        ActorContext().Schedule(scheduleDuration, new TEvPrivate::TEvReportExecutorStatistics);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvReportExecutorStatistics")("ReportExecutorStatisticsPeriodMs", statistics.GetReportExecutorStatisticsPeriodMs())("scheduleDuration", scheduleDuration);
    }
}

}   // namespace NKikimr::NColumnShard
