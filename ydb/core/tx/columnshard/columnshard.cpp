#include "columnshard_impl.h"
#include "blobs_reader/actor.h"
#include "hooks/abstract/abstract.h"
#include "resource_subscriber/actor.h"
#include "engines/writer/buffer/actor.h"
#include "engines/column_engine_logs.h"
#include "bg_tasks/manager/manager.h"

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
    CSCounters.OnIndexMetadataLimit(NOlap::IColumnEngine::GetMetadataLimit());
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
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr.release());

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

    IncCounter(COUNTER_SCANNED_ROWS, ev->Get()->Rows);
    IncCounter(COUNTER_SCANNED_BYTES, ev->Get()->Bytes);
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
        IncCounter(COUNTER_SCAN_LATENCY, duration);
        ScanTxInFlight.erase(txId);
        SetCounter(COUNTER_SCAN_IN_FLY, ScanTxInFlight.size());
        IncCounter(COUNTER_IMMEDIATE_TX_COMPLETED);
        if (success) {
            IncCounter(COUNTER_READ_SUCCESS);
        } else {
            IncCounter(COUNTER_READ_FAIL);
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

    SetCounter(COUNTER_PREPARED_RECORDS, prepared.Rows);
    SetCounter(COUNTER_PREPARED_BYTES, prepared.Bytes);
    SetCounter(COUNTER_COMMITTED_RECORDS, committed.Rows);
    SetCounter(COUNTER_COMMITTED_BYTES, committed.Bytes);

    LOG_S_TRACE("InsertTable. Prepared: " << prepared.Bytes << " in " << prepared.Rows
        << " records, committed: " << committed.Bytes << " in " << committed.Rows
        << " records at tablet " << TabletID());
}

void TColumnShard::UpdateIndexCounters() {
    if (!TablesManager.HasPrimaryIndex()) {
        return;
    }

    auto& stats = TablesManager.MutablePrimaryIndex().GetTotalStats();
    SetCounter(COUNTER_INDEX_TABLES, stats.Tables);
    SetCounter(COUNTER_INDEX_COLUMN_RECORDS, stats.ColumnRecords);
    SetCounter(COUNTER_INSERTED_PORTIONS, stats.GetInsertedStats().Portions);
    SetCounter(COUNTER_INSERTED_BLOBS, stats.GetInsertedStats().Blobs);
    SetCounter(COUNTER_INSERTED_ROWS, stats.GetInsertedStats().Rows);
    SetCounter(COUNTER_INSERTED_BYTES, stats.GetInsertedStats().Bytes);
    SetCounter(COUNTER_INSERTED_RAW_BYTES, stats.GetInsertedStats().RawBytes);
    SetCounter(COUNTER_COMPACTED_PORTIONS, stats.GetCompactedStats().Portions);
    SetCounter(COUNTER_COMPACTED_BLOBS, stats.GetCompactedStats().Blobs);
    SetCounter(COUNTER_COMPACTED_ROWS, stats.GetCompactedStats().Rows);
    SetCounter(COUNTER_COMPACTED_BYTES, stats.GetCompactedStats().Bytes);
    SetCounter(COUNTER_COMPACTED_RAW_BYTES, stats.GetCompactedStats().RawBytes);
    SetCounter(COUNTER_SPLIT_COMPACTED_PORTIONS, stats.GetSplitCompactedStats().Portions);
    SetCounter(COUNTER_SPLIT_COMPACTED_BLOBS, stats.GetSplitCompactedStats().Blobs);
    SetCounter(COUNTER_SPLIT_COMPACTED_ROWS, stats.GetSplitCompactedStats().Rows);
    SetCounter(COUNTER_SPLIT_COMPACTED_BYTES, stats.GetSplitCompactedStats().Bytes);
    SetCounter(COUNTER_SPLIT_COMPACTED_RAW_BYTES, stats.GetSplitCompactedStats().RawBytes);
    SetCounter(COUNTER_INACTIVE_PORTIONS, stats.GetInactiveStats().Portions);
    SetCounter(COUNTER_INACTIVE_BLOBS, stats.GetInactiveStats().Blobs);
    SetCounter(COUNTER_INACTIVE_ROWS, stats.GetInactiveStats().Rows);
    SetCounter(COUNTER_INACTIVE_BYTES, stats.GetInactiveStats().Bytes);
    SetCounter(COUNTER_INACTIVE_RAW_BYTES, stats.GetInactiveStats().RawBytes);
    SetCounter(COUNTER_EVICTED_PORTIONS, stats.GetEvictedStats().Portions);
    SetCounter(COUNTER_EVICTED_BLOBS, stats.GetEvictedStats().Blobs);
    SetCounter(COUNTER_EVICTED_ROWS, stats.GetEvictedStats().Rows);
    SetCounter(COUNTER_EVICTED_BYTES, stats.GetEvictedStats().Bytes);
    SetCounter(COUNTER_EVICTED_RAW_BYTES, stats.GetEvictedStats().RawBytes);

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
        TabletCounters->Simple()[COUNTER_PREPARED_RECORDS].Get() * sizeof(NOlap::TInsertedData) +
        TabletCounters->Simple()[COUNTER_COMMITTED_RECORDS].Get() * sizeof(NOlap::TInsertedData);
    memory += TablesManager.GetMemoryUsage();
    return memory;
}

void TColumnShard::UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage) {
    auto * metrics = Executor()->GetResourceMetrics();
    if (!metrics) {
        return;
    }

    ui64 storageBytes =
        TabletCounters->Simple()[COUNTER_PREPARED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_COMMITTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_INSERTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_COMPACTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_SPLIT_COMPACTED_BYTES].Get() +
        TabletCounters->Simple()[COUNTER_INACTIVE_BYTES].Get();

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

std::optional<TColumnShard::TTableStatsCollection> TColumnShard::CollectTableStats() const {
    if (!TablesManager.HasPrimaryIndex()) {
        return std::nullopt;
    }

    const TMap<ui64, std::shared_ptr<NOlap::TColumnEngineStats>>& columnEngineStats =
        TablesManager.GetPrimaryIndexSafe().GetStats();
    TTableStatsCollection resultStats;

    for (const auto& [pathId, tableInfo] : TablesManager.GetTables()) {
        TColumnTableStats& tableStats = resultStats.StatsByPathId[pathId];
        tableStats.AccessTime = tableInfo.GetLastAccessTime();
        tableStats.UpdateTime = tableInfo.GetLastUpdateTime();
        tableStats.LastFullCompaction = BackgroundController.GetLastCompactionFinishInstant(pathId);

        auto findEngineStats = columnEngineStats.FindPtr(pathId);
        if (findEngineStats && *findEngineStats) {
            NOlap::TColumnEngineStats::TPortionsStats portionsStats =
                (*findEngineStats)->Active(); // data stats excluding inactive and evicted

            if (portionsStats.Rows < 0 || portionsStats.Bytes < 0) {
                LOG_S_WARN(
                    "Negative stats counter. Rows: " << portionsStats.Rows << " Bytes: " << portionsStats.Bytes
                                                     << " Portions: " << portionsStats.Portions
                                                     << " Tablet: " << TabletID()
                );

                portionsStats.Rows = (portionsStats.Rows < 0) ? 0 : portionsStats.Rows;
                portionsStats.Bytes = (portionsStats.Bytes < 0) ? 0 : portionsStats.Bytes;
                portionsStats.Portions = (portionsStats.Portions < 0) ? 0 : portionsStats.Portions;
            }

            // TODO: count rows and bytes of data stored in InsertTable
            // TODO: we need row/dataSize counters for evicted data (managed by tablet but stored outside)
            tableStats.RowCount = portionsStats.Rows;
            tableStats.DataSize = portionsStats.Bytes;
            tableStats.Portions = portionsStats.Portions;
        } else {
            LOG_S_ERROR("CollectTableStats: missing column engine stats for pathId " << pathId);
        }

        if (resultStats.TotalStats.AccessTime < tableStats.AccessTime) {
            resultStats.TotalStats.AccessTime = tableStats.AccessTime;
        }
        if (resultStats.TotalStats.UpdateTime < tableStats.UpdateTime) {
            resultStats.TotalStats.UpdateTime = tableStats.UpdateTime;
        }
        if (resultStats.TotalStats.LastFullCompaction < tableStats.LastFullCompaction) {
            resultStats.TotalStats.LastFullCompaction = tableStats.LastFullCompaction;
        }
        resultStats.TotalStats.RowCount += tableStats.RowCount;
        resultStats.TotalStats.DataSize += tableStats.DataSize;
    }

    return resultStats;
}

void TColumnShard::ConfigureStats(const TColumnTableStats& inputStats, ::NKikimrTableStats::TTableStats* outputStats) {
    Y_ABORT_UNLESS(outputStats);

    outputStats->SetRowCount(inputStats.RowCount);
    outputStats->SetDataSize(inputStats.DataSize);

    // tabletStats->SetIndexSize(...); // Not implemented
    // tabletStats->SetInMemSize(...); // Not implemented

    outputStats->SetLastAccessTime(inputStats.AccessTime.MilliSeconds());
    outputStats->SetLastUpdateTime(inputStats.UpdateTime.MilliSeconds());

    outputStats->SetRowUpdates(TabletCounters->Cumulative()[COUNTER_WRITE_SUCCESS].Get());
    outputStats->SetRowDeletes(0); // manual deletes are not supported
    outputStats->SetRowReads(0); // all reads are range reads
    outputStats->SetRangeReads(TabletCounters->Cumulative()[COUNTER_READ_SUCCESS].Get());
    outputStats->SetRangeReadRows(TabletCounters->Cumulative()[COUNTER_READ_INDEX_ROWS].Get());

    outputStats->SetPartCount(inputStats.Portions);
    // outputStats->SetSearchHeight(...); // Not implemented

    outputStats->SetLastFullCompactionTs(inputStats.LastFullCompaction.Seconds());
    outputStats->SetHasLoanedParts(Executor()->HasLoanedParts());
}

void TColumnShard::FillTxTableStats(::NKikimrTableStats::TTableStats* tableStats) const {
    Y_ABORT_UNLESS(tableStats);
    tableStats->SetImmediateTxCompleted(TabletCounters->Cumulative()[COUNTER_IMMEDIATE_TX_COMPLETED].Get());
    tableStats->SetTxRejectedByOverload(TabletCounters->Cumulative()[COUNTER_WRITE_OVERLOAD].Get());
    tableStats->SetTxRejectedBySpace(TabletCounters->Cumulative()[COUNTER_OUT_OF_SPACE].Get());
    tableStats->SetPlannedTxCompleted(TabletCounters->Cumulative()[COUNTER_PLANNED_TX_COMPLETED].Get());
    tableStats->SetTxCompleteLagMsec(GetTxCompleteLag().MilliSeconds());
    tableStats->SetInFlightTxCount(Executor()->GetStats().TxInFly);
}

void TColumnShard::FillOlapStats(
    const TActorContext& ctx,
    const std::optional<TTableStatsCollection>& tableStats,
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
    auto* outputTableStats = ev->Record.MutableTableStats();
    FillTxTableStats(outputTableStats);
    if (tableStats) {
        ConfigureStats(tableStats->TotalStats, outputTableStats);
    }
}

void TColumnShard::FillColumnTableStats(
    const TActorContext& ctx,
    const std::optional<TTableStatsCollection>& tableStats,
    std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev
) {
    if (!tableStats) {
        return;
    }

    LOG_S_DEBUG("There are stats for " << tableStats->StatsByPathId.size() << " tables");
    for (const auto& [pathId, columnStats] : tableStats->StatsByPathId) {
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

        auto* outputTableStats = periodicTableStats->MutableTableStats();
        FillTxTableStats(outputTableStats);
        ConfigureStats(columnStats, outputTableStats);

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

    std::optional<TTableStatsCollection> aggregatedStats = CollectTableStats();
    auto ev = std::make_unique<TEvDataShard::TEvPeriodicTableStats>(TabletID(), OwnerPathId);

    FillOlapStats(ctx, aggregatedStats, ev);
    FillColumnTableStats(ctx, aggregatedStats, ev);

    NTabletPipe::SendData(ctx, StatsReportPipe, ev.release());
}

}   // namespace NKikimr::NColumnShard
