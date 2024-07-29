#include "columnshard_impl.h"
#include "blobs_reader/actor.h"
#include "hooks/abstract/abstract.h"
#include "resource_subscriber/actor.h"
#include "engines/writer/buffer/actor.h"
#include "engines/column_engine_logs.h"
#include "bg_tasks/manager/manager.h"
#include "counters/aggregation/table_stats.h"

#include <ydb/core/tx/tiering/manager.h>
#include <ydb/core/tx/columnshard/bg_tasks/adapter/adapter.h>
#include <ydb/core/protos/table_stats.pb.h>

namespace NKikimr {

IActor* CreateColumnShard(const TActorId& tablet, TTabletStorageInfo* info) {
    return new NColumnShard::TColumnShard(info, tablet);
}

}

namespace NKikimr::NColumnShard {

void TColumnShard::CleanupActors(const TActorContext& ctx) {
    if (BackgroundSessionsManager) {
        BackgroundSessionsManager->Stop();
    }
    ctx.Send(ResourceSubscribeActor, new TEvents::TEvPoisonPill);
    ctx.Send(BufferizationWriteActorId, new TEvents::TEvPoisonPill);

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

void TColumnShard::SwitchToWork(const TActorContext& ctx) {
    {
        const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("self_id", SelfId());
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "SwitchToWork");

        for (auto&& i : TablesManager.GetTables()) {
            ActivateTiering(i.first, i.second.GetTieringUsage());
        }

        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "SignalTabletActive");
        TryRegisterMediatorTimeCast();
        EnqueueProgressTx(ctx);
    }
    Stats.GetCSCounters().OnIndexMetadataLimit(NOlap::IColumnEngine::GetMetadataLimit());
    EnqueueBackgroundActivities();
    BackgroundSessionsManager->Start();
    ctx.Send(SelfId(), new TEvPrivate::TEvPeriodicWakeup());
    NYDBTest::TControllers::GetColumnShardController()->OnSwitchToWork(TabletID());
    AFL_VERIFY(!!StartInstant);
    CSCounters.Initialization.OnSwitchToWork(TMonotonic::Now() - *StartInstant, TMonotonic::Now() - CreateInstant);
}

void TColumnShard::OnActivateExecutor(const TActorContext& ctx) {
    StartInstant = TMonotonic::Now();
    CSCounters.Initialization.OnActivateExecutor(TMonotonic::Now() - CreateInstant);
    const TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("self_id", SelfId());
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "OnActivateExecutor");
    Executor()->RegisterExternalTabletCounters(TabletCountersHolder.release());

    const auto selfActorId = SelfId();
    StoragesManager->Initialize(Executor()->Generation());
    Tiers = std::make_shared<TTiersManager>(TabletID(), SelfId(),
        [selfActorId](const TActorContext& ctx) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "tiering_new_event");
        ctx.Send(selfActorId, new TEvPrivate::TEvTieringModified);
    });
    Tiers->Start(Tiers);
    if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
        Tiers->TakeConfigs(NYDBTest::TControllers::GetColumnShardController()->GetFallbackTiersSnapshot(), nullptr);
    }
    BackgroundSessionsManager = std::make_shared<NOlap::NBackground::TSessionsManager>(std::make_shared<NBackground::TAdapter>(selfActorId, (NOlap::TTabletId)TabletID(), *this));

    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "initialize_shard")("step", "initialize_tiring_finished");
    auto& icb = *AppData(ctx)->Icb;
    Limits.RegisterControls(icb);
    Settings.RegisterControls(icb);
    ResourceSubscribeActor = ctx.Register(new NOlap::NResourceBroker::NSubscribe::TActor(TabletID(), SelfId()));
    BufferizationWriteActorId = ctx.Register(new NColumnShard::NWriting::TActor(TabletID(), SelfId()));
    Execute(CreateTxInitSchema(), ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvTieringModified::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
    OnTieringModified();
    NYDBTest::TControllers::GetColumnShardController()->OnTieringModified(Tiers);
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    if (clientId == StatsReportPipe) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            LOG_S_DEBUG("Connected to " << tabletId << " at tablet " << TabletID());
        } else {
            LOG_S_INFO("Failed to connect to " << tabletId << " at tablet " << TabletID());
            StatsReportPipe = {};
        }
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
        return;
    }

    PipeClientCache->OnDisconnect(ev);
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&) {
    Y_UNUSED(ev);
    LOG_S_DEBUG("Server pipe connected at tablet " << TabletID());
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&) {
    Y_UNUSED(ev);
    LOG_S_DEBUG("Server pipe reset at tablet " << TabletID());
}

void TColumnShard::Handle(TEvPrivate::TEvScanStats::TPtr& ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);

    Stats.GetTabletCounters().IncCounter(COUNTER_SCANNED_ROWS, ev->Get()->Rows);
    Stats.GetTabletCounters().IncCounter(COUNTER_SCANNED_BYTES, ev->Get()->Bytes);
}

void TColumnShard::Handle(TEvPrivate::TEvReadFinished::TPtr& ev, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    ui64 readCookie = ev->Get()->RequestCookie;
    LOG_S_DEBUG("Finished read cookie: " << readCookie << " at tablet " << TabletID());
    const NOlap::TVersionedIndex* index = nullptr;
    if (HasIndex()) {
        index = &GetIndexAs<NOlap::TColumnEngineForLogs>().GetVersionedIndex();
    }
    InFlightReadsTracker.RemoveInFlightRequest(ev->Get()->RequestCookie, index);

    ui64 txId = ev->Get()->TxId;
    bool success = ev->Get()->Success;
    if (ScanTxInFlight.contains(txId)) {
        TDuration duration = TAppData::TimeProvider->Now() - ScanTxInFlight[txId];
        Stats.GetTabletCounters().IncCounter(COUNTER_SCAN_LATENCY, duration);
        ScanTxInFlight.erase(txId);
        Stats.GetTabletCounters().SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
        Stats.GetTabletCounters().IncCounter(COUNTER_IMMEDIATE_TX_COMPLETED);
        if (success) {
            Stats.GetTabletCounters().IncCounter(COUNTER_READ_SUCCESS);
        } else {
            Stats.GetTabletCounters().IncCounter(COUNTER_READ_FAIL);
        }
    }
}

void TColumnShard::Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Manual) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvPrivate::TEvPeriodicWakeup::MANUAL")("tablet_id", TabletID());
        EnqueueBackgroundActivities();
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TEvPrivate::TEvPeriodicWakeup")("tablet_id", TabletID());
        SendWaitPlanStep(GetOutdatedStep());

        SendPeriodicStats();
        ctx.Schedule(PeriodicWakeupActivationPeriod, new TEvPrivate::TEvPeriodicWakeup());
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

void TColumnShard::UpdateInsertTableCounters() {
    auto& prepared = InsertTable->GetCountersPrepared();
    auto& committed = InsertTable->GetCountersCommitted();

    Stats.GetTabletCounters().SetCounter(COUNTER_PREPARED_RECORDS, prepared.Rows);
    Stats.GetTabletCounters().SetCounter(COUNTER_PREPARED_BYTES, prepared.Bytes);
    Stats.GetTabletCounters().SetCounter(COUNTER_COMMITTED_RECORDS, committed.Rows);
    Stats.GetTabletCounters().SetCounter(COUNTER_COMMITTED_BYTES, committed.Bytes);

    LOG_S_TRACE("InsertTable. Prepared: " << prepared.Bytes << " in " << prepared.Rows
        << " records, committed: " << committed.Bytes << " in " << committed.Rows
        << " records at tablet " << TabletID());
}

void TColumnShard::UpdateIndexCounters() {
    if (!TablesManager.HasPrimaryIndex()) {
        return;
    }

    auto& stats = TablesManager.MutablePrimaryIndex().GetTotalStats();
    const TTabletCountersHandle& counters = Stats.GetTabletCounters();
    counters.SetCounter(COUNTER_INDEX_TABLES, stats.Tables);
    counters.SetCounter(COUNTER_INDEX_COLUMN_RECORDS, stats.ColumnRecords);
    counters.SetCounter(COUNTER_INSERTED_PORTIONS, stats.GetInsertedStats().Portions);
    counters.SetCounter(COUNTER_INSERTED_BLOBS, stats.GetInsertedStats().Blobs);
    counters.SetCounter(COUNTER_INSERTED_ROWS, stats.GetInsertedStats().Rows);
    counters.SetCounter(COUNTER_INSERTED_BYTES, stats.GetInsertedStats().Bytes);
    counters.SetCounter(COUNTER_INSERTED_RAW_BYTES, stats.GetInsertedStats().RawBytes);
    counters.SetCounter(COUNTER_COMPACTED_PORTIONS, stats.GetCompactedStats().Portions);
    counters.SetCounter(COUNTER_COMPACTED_BLOBS, stats.GetCompactedStats().Blobs);
    counters.SetCounter(COUNTER_COMPACTED_ROWS, stats.GetCompactedStats().Rows);
    counters.SetCounter(COUNTER_COMPACTED_BYTES, stats.GetCompactedStats().Bytes);
    counters.SetCounter(COUNTER_COMPACTED_RAW_BYTES, stats.GetCompactedStats().RawBytes);
    counters.SetCounter(COUNTER_SPLIT_COMPACTED_PORTIONS, stats.GetSplitCompactedStats().Portions);
    counters.SetCounter(COUNTER_SPLIT_COMPACTED_BLOBS, stats.GetSplitCompactedStats().Blobs);
    counters.SetCounter(COUNTER_SPLIT_COMPACTED_ROWS, stats.GetSplitCompactedStats().Rows);
    counters.SetCounter(COUNTER_SPLIT_COMPACTED_BYTES, stats.GetSplitCompactedStats().Bytes);
    counters.SetCounter(COUNTER_SPLIT_COMPACTED_RAW_BYTES, stats.GetSplitCompactedStats().RawBytes);
    counters.SetCounter(COUNTER_INACTIVE_PORTIONS, stats.GetInactiveStats().Portions);
    counters.SetCounter(COUNTER_INACTIVE_BLOBS, stats.GetInactiveStats().Blobs);
    counters.SetCounter(COUNTER_INACTIVE_ROWS, stats.GetInactiveStats().Rows);
    counters.SetCounter(COUNTER_INACTIVE_BYTES, stats.GetInactiveStats().Bytes);
    counters.SetCounter(COUNTER_INACTIVE_RAW_BYTES, stats.GetInactiveStats().RawBytes);
    counters.SetCounter(COUNTER_EVICTED_PORTIONS, stats.GetEvictedStats().Portions);
    counters.SetCounter(COUNTER_EVICTED_BLOBS, stats.GetEvictedStats().Blobs);
    counters.SetCounter(COUNTER_EVICTED_ROWS, stats.GetEvictedStats().Rows);
    counters.SetCounter(COUNTER_EVICTED_BYTES, stats.GetEvictedStats().Bytes);
    counters.SetCounter(COUNTER_EVICTED_RAW_BYTES, stats.GetEvictedStats().RawBytes);

    LOG_S_DEBUG("Index: tables " << stats.Tables
        << " inserted " << stats.GetInsertedStats().DebugString()
        << " compacted " << stats.GetCompactedStats().DebugString()
        << " s-compacted " << stats.GetSplitCompactedStats().DebugString()
        << " inactive " << stats.GetInactiveStats().DebugString()
        << " evicted " << stats.GetEvictedStats().DebugString()
        << " column records " << stats.ColumnRecords
        << " at tablet " << TabletID());
}

ui64 TColumnShard::MemoryUsage() const {
    ui64 memory =
        ProgressTxController->GetMemoryUsage() +
        ScanTxInFlight.size() * (sizeof(ui64) + sizeof(TInstant)) +
        LongTxWrites.size() * (sizeof(TWriteId) + sizeof(TLongTxWriteInfo)) +
        LongTxWritesByUniqueId.size() * (sizeof(TULID) + sizeof(void*)) +
        (WaitingScans.size()) * (sizeof(NOlap::TSnapshot) + sizeof(void*)) +
        Stats.GetTabletCounters().GetValue(COUNTER_PREPARED_RECORDS) * sizeof(NOlap::TInsertedData) +
        Stats.GetTabletCounters().GetValue(COUNTER_COMMITTED_RECORDS) * sizeof(NOlap::TInsertedData);
    memory += TablesManager.GetMemoryUsage();
    return memory;
}

void TColumnShard::UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage) {
    auto * metrics = Executor()->GetResourceMetrics();
    if (!metrics) {
        return;
    }

    ui64 storageBytes = Stats.GetTabletCounters().GetValue(COUNTER_PREPARED_BYTES) +
                        Stats.GetTabletCounters().GetValue(COUNTER_COMMITTED_BYTES) +
                        Stats.GetTabletCounters().GetValue(COUNTER_INSERTED_BYTES) +
                        Stats.GetTabletCounters().GetValue(COUNTER_COMPACTED_BYTES) +
                        Stats.GetTabletCounters().GetValue(COUNTER_SPLIT_COMPACTED_BYTES) +
                        Stats.GetTabletCounters().GetValue(COUNTER_INACTIVE_BYTES);

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

void TColumnShard::FillOlapStats(
    const TActorContext& ctx,
    std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev
) {
    ev->Record.SetShardState(2);   // NKikimrTxDataShard.EDatashardState.Ready
    ev->Record.SetGeneration(Executor()->Generation());
    ev->Record.SetRound(StatsReportRound++);
    ev->Record.SetNodeId(ctx.ExecutorThread.ActorSystem->NodeId);
    ev->Record.SetStartTime(StartTime().MilliSeconds());
    if (auto* resourceMetrics = Executor()->GetResourceMetrics()) {
        resourceMetrics->Fill(*ev->Record.MutableTabletMetrics());
    }

    TTableStatsBuilder statsBuilder(*ev->Record.MutableTableStats());
    statsBuilder.FillColumnTableStats(Stats.GetColumnTableCounters());
    statsBuilder.FillTabletStats(Stats.GetTabletCounters());
    statsBuilder.FillBackgroundControllerStats(Stats.GetBackgroundControllerCounters());
    statsBuilder.FillExecutorStats(*Executor());
    statsBuilder.FillTxCompleteLag(GetTxCompleteLag());
    if (TablesManager.HasPrimaryIndex()) {
        statsBuilder.FillColumnEngineStats(TablesManager.MutablePrimaryIndex().GetTotalStats());
    }
}

void TColumnShard::FillColumnTableStats(
    const TActorContext& ctx,
    std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev
) {
    auto tables = TablesManager.GetTables();

    LOG_S_DEBUG("There are stats for " << tables.size() << " tables");
    for (const auto& [pathId, _] : tables) {
        auto* periodicTableStats = ev->Record.AddTables();
        periodicTableStats->SetDatashardId(TabletID());
        periodicTableStats->SetTableLocalId(pathId);

        periodicTableStats->SetShardState(2);   // NKikimrTxDataShard.EDatashardState.Ready
        periodicTableStats->SetGeneration(Executor()->Generation());
        periodicTableStats->SetRound(StatsReportRound++);
        periodicTableStats->SetNodeId(ctx.ExecutorThread.ActorSystem->NodeId);
        periodicTableStats->SetStartTime(StartTime().MilliSeconds());

        if (auto* resourceMetrics = Executor()->GetResourceMetrics()) {
            resourceMetrics->Fill(*periodicTableStats->MutableTabletMetrics());
        }

        TTableStatsBuilder statsBuilder(*periodicTableStats->MutableTableStats());
        statsBuilder.FillColumnTableStats(Stats.GetColumnTableCounters().GetPathIdCounter(pathId));
        statsBuilder.FillTabletStats(Stats.GetTabletCounters());
        statsBuilder.FillBackgroundControllerStats(Stats.GetBackgroundControllerCounters(), pathId);
        statsBuilder.FillExecutorStats(*Executor());
        statsBuilder.FillTxCompleteLag(GetTxCompleteLag());
        if (TablesManager.HasPrimaryIndex()) {
            auto columnEngineStats = TablesManager.GetPrimaryIndexSafe().GetStats().FindPtr(pathId);
            if (columnEngineStats && *columnEngineStats) {
                statsBuilder.FillColumnEngineStats(**columnEngineStats);
            }
        }

        LOG_S_TRACE("Add stats for table, tableLocalID=" << pathId);
    }
}

void TColumnShard::SendPeriodicStats() {
    LOG_S_DEBUG("Send periodic stats.");

    if (!CurrentSchemeShardId || !OwnerPathId) {
        LOG_S_DEBUG("Disabled periodic stats at tablet " << TabletID());
        return;
    }

    const TActorContext& ctx = ActorContext();
    const TInstant now = TAppData::TimeProvider->Now();

    if (LastStatsReport + StatsReportInterval > now) {
        LOG_S_TRACE("Skip send periodic stats: report interavl = " << StatsReportInterval);
        return;
    }
    LastStatsReport = now;

    if (!StatsReportPipe) {
        LOG_S_DEBUG("Create periodic stats pipe to " << CurrentSchemeShardId << " at tablet " << TabletID());
        NTabletPipe::TClientConfig clientConfig;
        StatsReportPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, CurrentSchemeShardId, clientConfig));
    }

    auto ev = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletID(), OwnerPathId);

    FillOlapStats(ctx, ev);
    FillColumnTableStats(ctx, ev);

    NTabletPipe::SendData(ctx, StatsReportPipe, ev.release());
}

}   // namespace NKikimr::NColumnShard
