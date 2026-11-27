#include "columnshard_impl.h"

#include "bg_tasks/manager/manager.h"
#include "blobs_reader/actor.h"
#include "engines/column_engine_logs.h"
#include "engines/writer/buffer/actor2.h"
#include "hooks/abstract/abstract.h"
#include "resource_subscriber/actor.h"
#include "transactions/locks/read_finished.h"

#include <ydb/core/cms/console/events/configs_dispatcher.h>
#include <ydb/core/cms/console/events/console.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/tx/columnshard/bg_tasks/adapter/adapter.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/diagnostics/scan_diagnostics_actor.h>
#include <ydb/core/tx/priorities/usage/service.h>
#include <ydb/core/tx/tiering/manager.h>

#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

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
    StopLocalDbBackup(ctx);

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
        YDB_LOG_INFO("",
            {"event", "skip_switch_to_work"},
            {"reason", "tiering_metadata_not_ready"});
        return;
    }
    if (!IsTxInitFinished) {
        YDB_LOG_INFO("",
            {"event", "skip_switch_to_work"},
            {"reason", "db_reading_not_finished"});
        return;
    }
    YDB_LOG_VERIFY(!WriteGate);
    WriteGate.emplace(*Executor(), TablesManager, *OperationsManager, *SpaceWatcher, Counters, TabletID());
    ProgressTxController->OnTabletInit();
    AbortNotProposedTransactions();
    {
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
            {"tabletId", TabletID()},
            {"selfId", SelfId()},
            {"process", "SwitchToWork"});
        YDB_LOG_INFO("",
            {"event", "initialize_shard"},
            {"step", "SwitchToWork"});
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
        YDB_LOG_INFO("",
            {"event", "initialize_shard"},
            {"step", "SignalTabletActive"});
        TryRegisterMediatorTimeCast();
        EnqueueProgressTx(ctx);
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
    ui64 portionsCount = 0;
    if (TablesManager.HasPrimaryIndex()) {
        for (auto&& [_, granule] : TablesManager.GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetTables()) {
            portionsCount += granule->GetPortions().size();
        }
    }
    StartupTimeline.OnSwitchedToWork(portionsCount, !DeferredPortionLoader.IsEnabled());
    NYDBTest::TControllers::GetColumnShardController()->OnTabletInitCompleted(*this);
    if (DeferredPortionLoader.IsEnabled()) {
        StartDeferredPortionLoading(ctx);
    } else {
        // the request for every portion takes long to build, so it is not part of the start
        ctx.Send(SelfId(), new TEvPrivate::TEvPrefetchPortionsMetadata());
    }
}

void TColumnShard::OnActivateExecutor(const TActorContext& ctx) {
    StartupTimeline.OnExecutorActivated();
    YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
        {"tabletId", TabletID()},
        {"selfId", SelfId()});
    YDB_LOG_INFO("",
        {"event", "initialize_shard"},
        {"step", "OnActivateExecutor"});
    Executor()->RegisterExternalTabletCounters(TabletCountersHolder.release());

    const auto selfActorId = SelfId();
    StoragesManager->Initialize(Executor()->Generation());
    Tiers = std::make_shared<TTiersManager>(TabletID(), SelfId(), [selfActorId](const TActorContext& ctx) {
        YDB_LOG_DEBUG("",
            {"event", "tiering_new_event"});
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

    YDB_LOG_INFO("",
        {"event", "initialize_shard"},
        {"step", "initialize_tiring_finished"});
    auto& icb = *AppData(ctx)->Icb;
    auto* spaceWatcherRawPtr = SpaceWatcher.release();
    SpaceWatcherId = RegisterWithSameMailbox(spaceWatcherRawPtr);
    // Actor System will keep this object
    SpaceWatcher = std::unique_ptr<TSpaceWatcher, std::function<void(TSpaceWatcher*)>>(spaceWatcherRawPtr, [](auto*) {
    });
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

    *ColumnShardConfig = AppData(ctx)->ColumnShardConfig;
    SubscribeToColumnShardConfig();

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

void TColumnShard::Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& /*ev*/) {
    YDB_LOG_DEBUG("",
        {"event", "subscribed_for_columnshard_config"});
}

void TColumnShard::SubscribeToColumnShardConfig() {
    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({ (ui32)NKikimrConsole::TConfigItem::ColumnShardConfigItem }),
        IEventHandle::FlagTrackDelivery);
}

void TColumnShard::Handle(TEvPrivate::TEvRetryConfigSubscription::TPtr& /*ev*/) {
    YDB_LOG_WARN("",
        {"event", "retry_columnshard_config_subscription"});
    SubscribeToColumnShardConfig();
}

void TColumnShard::ApplyColumnShardConfig() {
    if (!HasIndex()) {
        return;
    }
    MutableIndexAs<NOlap::TColumnEngineForLogs>().GetOptimizerRuntimeSettings()->ApplyFromConfig(*ColumnShardConfig);
}

void TColumnShard::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    auto& event = ev->Get()->Record;

    ColumnShardConfig->Swap(event.MutableConfig()->MutableColumnShardConfig());
    ApplyColumnShardConfig();
    YDB_LOG_INFO("",
        {"event", "columnshard_config_updated"},
        {"has_node_portions_count_limit", ColumnShardConfig->HasNodePortionsCountLimit()},
        {"node_portions_count_limit", ColumnShardConfig->HasNodePortionsCountLimit() ? ColumnShardConfig->GetNodePortionsCountLimit() : 0});

    auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
    Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
}

void TColumnShard::HandleInit(TEvPrivate::TEvTieringModified::TPtr& /*ev*/, const TActorContext& ctx) {
    TrySwitchToWork(ctx);
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    if (clientId == StatsReportPipe) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Connected",
                {"toTabletId", tabletId},
                {"fromTabletId", TabletID()});
        } else {
            YDB_LOG_INFO_COMP(TX_COLUMNSHARD, "Failed to connect",
                {"toTabletId", tabletId},
                {"fromTabletId", TabletID()});
            LastStats = {};
            StatsReportPipe = {};
        }
        ExecutorStatsEvInflight++;
        ActorContext().Send(SelfId(), new TEvPrivate::TEvReportExecutorStatistics());
        return;
    }

    if (PipeClientCache->OnConnect(ev)) {
        YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Connected to tablet",
            {"toTabletId", tabletId},
            {"fromTabletId", TabletID()});
        return;
    }

    YDB_LOG_INFO_COMP(TX_COLUMNSHARD, "Failed to connect",
        {"toTabletId", tabletId},
        {"fromTabletId", TabletID()});
}

void TColumnShard::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext&) {
    auto tabletId = ev->Get()->TabletId;
    auto clientId = ev->Get()->ClientId;

    YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Client pipe reset",
        {"toTabletId", tabletId},
        {"fromTabletId", TabletID()});

    if (clientId == StatsReportPipe) {
        StatsReportPipe = {};
        LastStats = {};
        return;
    }

    PipeClientCache->OnDisconnect(ev);
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&) {
    PipeServersInterconnectSessions.emplace(ev->Get()->ServerId, ev->Get()->InterconnectSession);
    YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Server pipe connected at tablet",
        {"tabletId", TabletID()});
}

void TColumnShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx) {
    PipeServersInterconnectSessions.erase(ev->Get()->ServerId);
    ctx.Send(NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        std::make_unique<NOverload::TEvOverloadPipeServerDisconnected>(
            NOverload::TColumnShardInfo{ .ColumnShardId = SelfId(), .TabletId = TabletID() },
            NOverload::TPipeServerInfo{ .PipeServerId = ev->Get()->ServerId, .InterconnectSessionId = {} }));
    YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Server pipe reset at tablet",
        {"tabletId", TabletID()});
}

void TColumnShard::Handle(TEvPrivate::TEvScanStats::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);

    Counters.GetTabletCounters()->IncCounter(COUNTER_SCANNED_ROWS, ev->Get()->Rows);
    Counters.GetTabletCounters()->IncCounter(COUNTER_SCANNED_BYTES, ev->Get()->Bytes);
}

void TColumnShard::Handle(TEvPrivate::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    ui64 readCookie = ev->Get()->RequestCookie;
    YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Finished read at tablet",
        {"cookie", readCookie},
        {"tabletId", TabletID()});
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

void TColumnShard::Handle(TEvPrivate::TEvPrefetchPortionsMetadata::TPtr& /*ev*/) {
    if (TablesManager.HasPrimaryIndex()) {
        TablesManager.MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().PrefetchPortionsMetadata();
    }
}

void TColumnShard::Handle(TEvPrivate::TEvPingSnapshotsUsage::TPtr& /*ev*/, const TActorContext& ctx) {
    const TDuration stalenessLivetime = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStaleness();
    const TDuration stalenessInMem = NYDBTest::TControllers::GetColumnShardController()->GetMaxReadStalenessInMem();
    const TDuration usedLivetime = NYDBTest::TControllers::GetColumnShardController()->GetUsedSnapshotLivetime();
    YDB_LOG_VERIFY(usedLivetime < stalenessInMem || (stalenessInMem == usedLivetime && usedLivetime == TDuration::Zero()),
        {"used", usedLivetime},
        {"staleness", stalenessInMem});
    const TDuration ping = 0.3 * std::min(stalenessInMem - usedLivetime, stalenessLivetime - stalenessInMem);
    if (auto writeTx = InFlightReadsTracker.Ping(this, stalenessInMem, usedLivetime, TInstant::Now())) {
        Execute(writeTx.release(), ctx);
    }
    ctx.Schedule(NYDBTest::TControllers::GetColumnShardController()->GetStalenessLivetimePing(ping), new TEvPrivate::TEvPingSnapshotsUsage());
}

void TColumnShard::Handle(TEvPrivate::TEvPeriodicWakeup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Manual) {
        YDB_LOG_DEBUG("",
            {"event", "TEvPrivate::TEvPeriodicWakeup::MANUAL"},
            {"tabletId", TabletID()});
        EnqueueBackgroundActivities();
    } else {
        YDB_LOG_DEBUG("",
            {"event", "TEvPrivate::TEvPeriodicWakeup"},
            {"tabletId", TabletID()});
        SendWaitPlanStep(GetOutdatedStep());
        EnqueueBackgroundActivities();
        ctx.Schedule(PeriodicWakeupActivationPeriod, new TEvPrivate::TEvPeriodicWakeup());
    }
}

void TColumnShard::Handle(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Tag == 0) {
        YDB_LOG_DEBUG("",
            {"event", "TEvPrivate::TEvPeriodicWakeup::MANUAL"},
            {"tabletId", TabletID()});
        const TMonotonic now = TMonotonic::Now();
        GetProgressTxController().PingTimeouts(now);
        ctx.Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup(0));
    } else if (ev->Get()->Tag == 1) {
        DrainWriteQueue(ctx, true);
    }
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext&) {
    const auto* msg = ev->Get();
    Y_ABORT_UNLESS(msg->TabletId == TabletID());
    MediatorTimeCastEntry = msg->Entry;
    Y_ABORT_UNLESS(MediatorTimeCastEntry);
    YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Registered with mediator time cast at tablet",
        {"tabletId", TabletID()});

    RescheduleWaitingReads();
}

void TColumnShard::Handle(TEvMediatorTimecast::TEvNotifyPlanStep::TPtr& ev, const TActorContext&) {
    const auto* msg = ev->Get();
    Y_ABORT_UNLESS(msg->TabletId == TabletID());

    Y_ABORT_UNLESS(MediatorTimeCastEntry);
    ui64 step = MediatorTimeCastEntry->Get(TabletID());
    YDB_LOG_DEBUG_COMP(TX_COLUMNSHARD, "Notified by mediator time cast at tablet",
        {"planStep", step},
        {"tabletId", TabletID()});

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
    DataStats->UpdateTabletCounters(*Counters.GetTabletCounters());
}

ui64 TColumnShard::MemoryUsage() const {
    ui64 memory = ProgressTxController->GetMemoryUsage() + ScanTxInFlight.size() * (sizeof(ui64) + sizeof(TInstant)) +
                  ScanGate.GetMemoryUsage() + (WriteGate ? WriteGate->GetMemoryUsage() : 0);
    memory += TablesManager.GetMemoryUsage();
    return memory;
}

void TColumnShard::UpdateResourceMetrics(const TActorContext& ctx, const TUsage& usage) {
    auto* metrics = Executor()->GetResourceMetrics();
    if (!metrics) {
        return;
    }

    ui64 blobStorageBytes = DataStats->GetBlobStorageBytes();

    ui64 memory = MemoryUsage();

    TInstant now = TAppData::TimeProvider->Now();
    metrics->CPU.Increment(usage.CPUExecTime, now);
    metrics->Network.Increment(usage.Network, now);
    //metrics->StorageSystem
    metrics->StorageUser.Set(blobStorageBytes);
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

    auto* tableStats = ev->Record.MutableTableStats();
    Counters.FillTotalTableStats(*tableStats);
    if (executor) {
        tableStats->SetInFlightTxCount(executor->GetStats().TxInFly);
        tableStats->SetHasLoanedParts(executor->HasLoanedParts());
    }
    DataStats->FillTotalTableStats(*tableStats);
}

void TColumnShard::FillColumnTableStats(
    const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev, IExecutor* executor) {
    auto tables = TablesManager.GetTables();

    YDB_LOG_DEBUG("There are stats for tables",
        {"tables", tables.size()});
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

            Counters.FillTableStats(internalPathId, *periodicTableStats->MutableTableStats());
            DataStats->FillTableStats(internalPathId, *periodicTableStats->MutableTableStats());
            YDB_LOG_DEBUG("",
                {"event", "Add stats for table"},
                {"tableLocalID", schemeShardLocalPathId});
        }
    }
}

void TColumnShard::SendPeriodicStats(bool withExecutor) {
    if (!CurrentSchemeShardId) {
        YDB_LOG_INFO("",
            {"noCurrentSchemeShardId", TabletID()});
        return;
    }

    if (!StatsReportPipe) {
        YDB_LOG_INFO("StatsReportPipe created",
            {"tabletId", TabletID()});
        StatsReportPipe = ActorContext().Register(NTabletPipe::CreateClient(ActorContext().SelfID, CurrentSchemeShardId, {}));
        return;
    }

    if (!TablesManager.GetTabletPathIdOptional()) {
        YDB_LOG_ERROR("TablesManager not ready",
            {"tabletId", TabletID()});
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
        YDB_LOG_DEBUG("",
            {"event", "TEvReportBaseStatistics"},
            {"reportBaseStatisticsPeriodMs", statistics.GetReportBaseStatisticsPeriodMs()},
            {"scheduleDuration", scheduleDuration});
    }
}

void TColumnShard::ScheduleExecutorStatistics() {
    auto statistics = AppDataVerified().ColumnShardConfig.GetStatistics();
    auto scheduleDuration = TDuration::MilliSeconds(statistics.GetReportExecutorStatisticsPeriodMs() + RandomNumber<ui32>(JitterIntervalMS));
    if (!ExecutorStatsEvInflight) {
        ExecutorStatsEvInflight++;
        ActorContext().Schedule(scheduleDuration, new TEvPrivate::TEvReportExecutorStatistics);
        YDB_LOG_DEBUG("",
            {"event", "TEvReportExecutorStatistics"},
            {"reportExecutorStatisticsPeriodMs", statistics.GetReportExecutorStatisticsPeriodMs()},
            {"scheduleDuration", scheduleDuration});
    }
}

}   // namespace NKikimr::NColumnShard
