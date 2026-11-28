#include "aggregator_impl.h"
#include "analyze_scan_budget.h"

#include <ydb/core/statistics/aggregator/analyze_actor.h>
#include <ydb/core/statistics/database/database.h>
#include <ydb/core/statistics/service/service.h>

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/path.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <algorithm>
#include <limits>
#include <tuple>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

TStatisticsAggregator::TStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
    auto seed = std::random_device{}();
    RandomGenerator.seed(seed);

    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.Get();
}

void TStatisticsAggregator::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TStatisticsAggregator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TStatisticsAggregator::OnActivateExecutor(const TActorContext& ctx) {
    YDB_LOG_INFO("OnActivateExecutor",
        {"tabletId", TabletID()});

    auto appData = AppData(ctx);
    Y_ABORT_UNLESS(appData);
    EnableStatistics = appData->FeatureFlags.GetEnableStatistics();
    EnableColumnStatistics = appData->FeatureFlags.GetEnableColumnStatistics();
    EnableBackgroundAnalyzeChangeRatio = appData->FeatureFlags.GetEnableBackgroundAnalyzeChangeRatio();
    EnableAnalyzeSampling = appData->FeatureFlags.GetEnableAnalyzeSampling();

    InitAnalyzeCounters();
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);

    // The local dispatcher can already have a lower limit than the node's startup config.
    ctx.Send(NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvGetConfigRequest(TVector<ui32>{
            NKikimrConsole::TConfigItem::FeatureFlagsItem, NKikimrConsole::TConfigItem::StatisticsConfigItem}),
        IEventHandle::FlagTrackDelivery);
}

void TStatisticsAggregator::StartRecovery(const NKikimrConfig::TAppConfig& config) {
    ApplyConfig(config);
    ScanBudgetActorId = Register(CreateAnalyzeScanBudget(
        StatisticsConfig.GetAnalyzeMaxTotalScanActorsInFlight(), StatisticsConfig.GetAnalyzeMaxPerNodeScanActorsInFlight()));
    Execute(CreateTxInitSchema(), TActivationContext::AsActorContext());
}

void TStatisticsAggregator::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}

void TStatisticsAggregator::SubscribeForConfigChanges(const TActorContext& ctx) {
    ctx.Send(NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({
            (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
            (ui32)NKikimrConsole::TConfigItem::StatisticsConfigItem,
        }));
}

void TStatisticsAggregator::HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
    YDB_LOG_INFO("Subscribed for config changes",
        {"tabletId", TabletID()});
}

void TStatisticsAggregator::ApplyConfig(const NKikimrConfig::TAppConfig& config) {
    StatisticsConfig = config.HasStatisticsConfig() ? config.GetStatisticsConfig() : AppData()->StatisticsConfig;
    if (config.HasFeatureFlags()) {
        const auto& featureFlags = config.GetFeatureFlags();
        EnableStatistics = featureFlags.GetEnableStatistics();
        EnableColumnStatistics = featureFlags.GetEnableColumnStatistics();
        EnableBackgroundAnalyzeChangeRatio = featureFlags.GetEnableBackgroundAnalyzeChangeRatio();
        EnableAnalyzeSampling = featureFlags.GetEnableAnalyzeSampling();
    }
}

void TStatisticsAggregator::HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    YDB_LOG_DEBUG("EvConfigNotification",
        {"tabletId", TabletID()});

    const auto& record = ev->Get()->Record;
    const bool wasEnabled = EnableColumnStatistics;
    ApplyConfig(record.GetConfig());
    if (!wasEnabled && EnableColumnStatistics) {
        InitializeStatisticsTable();
        StartTraversalScheduler();
    }
    Send(ScanBudgetActorId, new TEvAnalyzeScanBudget::TEvConfigure(
        StatisticsConfig.GetAnalyzeMaxTotalScanActorsInFlight(), StatisticsConfig.GetAnalyzeMaxPerNodeScanActorsInFlight()));
    AnalyzeCountersDirty = true;

    auto response = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(record);
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TStatisticsAggregator::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev) {
    auto pipeServerId = ev->Get()->ServerId;

    YDB_LOG_DEBUG("EvServerConnected",
        {"tabletId", TabletID()},
        {"pipeServerId", pipeServerId});
}

void TStatisticsAggregator::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev) {
    auto pipeServerId = ev->Get()->ServerId;

    YDB_LOG_DEBUG("EvServerDisconnected",
        {"tabletId", TabletID()},
        {"pipeServerId", pipeServerId});

    auto itNodeServer = NodePipes.find(pipeServerId);
    if (itNodeServer != NodePipes.end()) {
        auto nodeId = itNodeServer->second;
        auto itNode = Nodes.find(nodeId);
        if (itNode != Nodes.end()) {
            --itNode->second;
            if (itNode->second == 0) {
                Nodes.erase(itNode);
            }
        }
        NodePipes.erase(itNodeServer);
        return;
    }

    auto itShardServer = SchemeShardPipes.find(pipeServerId);
    if (itShardServer != SchemeShardPipes.end()) {
        auto ssId = itShardServer->second;
        auto itShard = SchemeShards.find(ssId);
        if (itShard != SchemeShards.end()) {
            --itShard->second;
            if (itShard->second == 0) {
                SchemeShards.erase(itShard);
            }
        }
        SchemeShardPipes.erase(itShardServer);
        return;
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvConnectNode::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const TNodeId nodeId = record.GetNodeId();
    auto pipeServerId = ev->Recipient;

    YDB_LOG_DEBUG("EvConnectNode",
        {"tabletId", TabletID()},
        {"pipeServerId", pipeServerId},
        {"nodeId", nodeId},
        {"haveSchemeShardsCount", record.HaveSchemeShardsSize()},
        {"needSchemeShardsCount", record.NeedSchemeShardsSize()});

    if (NodePipes.find(pipeServerId) == NodePipes.end()) {
        NodePipes[pipeServerId] = nodeId;
        ++Nodes[nodeId];
    }

    for (const auto& ssEntry : record.GetHaveSchemeShards()) {
        RequestedSchemeShards.insert(ssEntry.GetSchemeShardId());
    }

    if (!EnableStatistics) {
        auto disabled = std::make_unique<TEvStatistics::TEvStatisticsIsDisabled>();
        Send(NStat::MakeStatServiceID(nodeId), disabled.release());
        return;
    }

    if (!record.NeedSchemeShardsSize()) {
        return;
    }

    std::vector<TSSId> ssIds;
    ssIds.reserve(record.NeedSchemeShardsSize());
    for (const auto& ssId : record.GetNeedSchemeShards()) {
        ssIds.push_back(ssId);
        RequestedSchemeShards.insert(ssId);
    }

    ProcessRequests(nodeId, ssIds);
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvRequestStats::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto nodeId = record.GetNodeId();

    YDB_LOG_DEBUG("EvRequestStats",
        {"tabletId", TabletID()},
        {"nodeId", nodeId},
        {"schemeShardsCount", record.NeedSchemeShardsSize()},
        {"urgent", record.GetUrgent()});

    if (!EnableStatistics) {
        auto disabled = std::make_unique<TEvStatistics::TEvStatisticsIsDisabled>();
        Send(NStat::MakeStatServiceID(nodeId), disabled.release());
        return;
    }

    for (const auto& ssId : record.GetNeedSchemeShards()) {
        RequestedSchemeShards.insert(ssId);
    }

    if (record.GetUrgent()) {
        PendingRequests.push(std::move(ev));
        if (!ProcessUrgentInFlight) {
            Send(SelfId(), new TEvPrivate::TEvProcessUrgent());
            ProcessUrgentInFlight = true;
        }
        return;
    }

    std::vector<TSSId> ssIds;
    ssIds.reserve(record.NeedSchemeShardsSize());
    for (const auto& ssId : record.GetNeedSchemeShards()) {
        ssIds.push_back(ssId);
    }

    ProcessRequests(nodeId, ssIds);
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvConnectSchemeShard::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const TSSId schemeShardId = record.GetSchemeShardId();
    auto pipeServerId = ev->Recipient;

    if (SchemeShardPipes.find(pipeServerId) == SchemeShardPipes.end()) {
        SchemeShardPipes[pipeServerId] = schemeShardId;
        ++SchemeShards[schemeShardId];
    }

    YDB_LOG_DEBUG("EvConnectSchemeShard",
        {"tabletId", TabletID()},
        {"pipeServerId", pipeServerId},
        {"schemeShardId", schemeShardId});
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvFastPropagateCheck::TPtr&) {
    YDB_LOG_DEBUG("EvFastPropagateCheck",
        {"tabletId", TabletID()});

    if (!EnableStatistics) {
        return;
    }

    PropagateFastStatistics();

    FastCheckInFlight = false;
    FastCounter = StatsOptimizeFirstNodesCount;
    FastNodes.clear();
    FastSchemeShards.clear();
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvPropagate::TPtr&) {
    YDB_LOG_TRACE("EvPropagate",
        {"tabletId", TabletID()});

    if (EnableStatistics) {
        PropagateStatistics();
    }

    Schedule(GetPropagateInterval(), new TEvPrivate::TEvPropagate());
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvPropagateStatisticsResponse::TPtr& ev) {
    YDB_LOG_DEBUG("EvPropagateStatisticsResponse,",
        {"tabletId", TabletID()},
        {"cookie", ev->Cookie});

    if (!PropagationInFlight) {
        return;
    }
    if (ev->Cookie != 0 && ev->Cookie != CurPropagationSeq) {
        // Response is not from the current propagation round, ignore.
        // Cookie == 0 may come from an older YDB version, retain the old logic for these events.
        return;
    }
    if (LastSSIndex < PropagationSchemeShards.size()) {
        LastSSIndex = PropagatePart(
            PropagationNodes, PropagationSchemeShards, LastSSIndex, true, CurPropagationSeq);
    } else {
        PropagationInFlight = false;
        PropagationNodes.clear();
        PropagationSchemeShards.clear();
        LastSSIndex = 0;
    }
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvProcessUrgent::TPtr&) {
    YDB_LOG_DEBUG("EvProcessUrgent",
        {"tabletId", TabletID()});

    ProcessUrgentInFlight = false;

    if (PendingRequests.empty()) {
        return;
    }

    TEvStatistics::TEvRequestStats::TPtr ev = std::move(PendingRequests.front());
    PendingRequests.pop();

    if (!PendingRequests.empty()) {
        Send(SelfId(), new TEvPrivate::TEvProcessUrgent());
        ProcessUrgentInFlight = true;
    }

    auto record = ev->Get()->Record;
    const auto nodeId = record.GetNodeId();

    std::vector<TSSId> ssIds;
    ssIds.reserve(record.NeedSchemeShardsSize());
    for (const auto& ssId : record.GetNeedSchemeShards()) {
        ssIds.push_back(ssId);
    }

    SendStatisticsToNode(nodeId, ssIds);
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvPropagateTimeout::TPtr&) {
    YDB_LOG_DEBUG("EvPropagateTimeout",
        {"tabletId", TabletID()});

    if (PropagationInFlight) {
        PropagationInFlight = false;
        PropagationNodes.clear();
        PropagationSchemeShards.clear();
        LastSSIndex = 0;
    }
}

void TStatisticsAggregator::ProcessRequests(TNodeId nodeId, const std::vector<TSSId>& ssIds) {
    if (FastCounter > 0) {
        --FastCounter;
        SendStatisticsToNode(nodeId, ssIds);
    } else {
        FastNodes.insert(nodeId);
        for (const auto& ssId : ssIds) {
            FastSchemeShards.insert(ssId);
        }
    }
    if (!FastCheckInFlight) {
        Schedule(FastCheckInterval, new TEvPrivate::TEvFastPropagateCheck());
        FastCheckInFlight = true;
    }
}

void TStatisticsAggregator::SendStatisticsToNode(TNodeId nodeId, const std::vector<TSSId>& ssIds) {
    YDB_LOG_DEBUG("SendStatisticsToNode()",
        {"tabletId", TabletID()},
        {"nodeId", nodeId},
        {"schemeShardsCount", ssIds.size()});

    std::vector<TNodeId> nodeIds;
    nodeIds.push_back(nodeId);

    PropagatePart(nodeIds, ssIds, 0, false, InvalidPropagationSeq);
}

void TStatisticsAggregator::PropagateStatistics() {
    if (Nodes.empty() || RequestedSchemeShards.empty()) {
        YDB_LOG_TRACE("PropagateStatistics() No data",
            {"tabletId", TabletID()});
        return;
    }

    YDB_LOG_DEBUG("PropagateStatistics()",
        {"tabletId", TabletID()},
        {"nodesCount", Nodes.size()},
        {"schemeShardsCount", RequestedSchemeShards.size()});

    std::vector<TNodeId> nodeIds;
    nodeIds.reserve(Nodes.size());
    for (const auto& [nodeId, _] : Nodes) {
        nodeIds.push_back(nodeId);
    }
    std::shuffle(std::begin(nodeIds), std::end(nodeIds), RandomGenerator);

    std::vector<TSSId> ssIds;
    ssIds.reserve(RequestedSchemeShards.size());
    for (const auto& ssId : RequestedSchemeShards) {
        ssIds.push_back(ssId);
    }

    auto timeout = GetPropagateInterval() * 3 / 5;
    Schedule(timeout, new TEvPrivate::TEvPropagateTimeout);

    ++CurPropagationSeq;
    PropagationInFlight = true;
    PropagationNodes = std::move(nodeIds);
    PropagationSchemeShards = std::move(ssIds);

    LastSSIndex = PropagatePart(PropagationNodes, PropagationSchemeShards, 0, true, CurPropagationSeq);
}

void TStatisticsAggregator::PropagateFastStatistics() {
    YDB_LOG_DEBUG("PropagateFastStatistics()",
        {"tabletId", TabletID()},
        {"nodesCount", FastNodes.size()},
        {"schemeShardsCount", FastSchemeShards.size()});

    if (FastNodes.empty() || FastSchemeShards.empty()) {
        return;
    }

    std::vector<TNodeId> nodeIds;
    nodeIds.reserve(FastNodes.size());
    for (const auto& nodeId : FastNodes) {
        nodeIds.push_back(nodeId);
    }
    std::shuffle(std::begin(nodeIds), std::end(nodeIds), RandomGenerator);

    std::vector<TSSId> ssIds;
    ssIds.reserve(FastSchemeShards.size());
    for (const auto& ssId : FastSchemeShards) {
        ssIds.push_back(ssId);
    }

    PropagatePart(nodeIds, ssIds, 0, false, InvalidPropagationSeq);
}

size_t TStatisticsAggregator::PropagatePart(const std::vector<TNodeId>& nodeIds, const std::vector<TSSId>& ssIds,
    size_t lastSSIndex, bool useSizeLimit, ui64 cookie)
{
    auto propagate = std::make_unique<TEvStatistics::TEvPropagateStatistics>();
    auto* record = propagate->MutableRecord();

    TNodeId leadingNodeId = nodeIds[0];
    record->MutableNodeIds()->Reserve(nodeIds.size() - 1);
    for (size_t i = 1; i < nodeIds.size(); ++i) {
        record->AddNodeIds(nodeIds[i]);
    }

    size_t sizeLimit = useSizeLimit ? StatsSizeLimitBytes : std::numeric_limits<size_t>::max();
    size_t index = lastSSIndex;
    for (size_t size = 0; index < ssIds.size() && size < sizeLimit; ++index) {
        auto ssId = ssIds[index];
        auto* entry = record->AddEntries();
        entry->SetSchemeShardId(ssId);
        auto itStats = BaseStatistics.find(ssId);
        if (itStats != BaseStatistics.end() && itStats->second.Committed) {
            const auto& stats = *itStats->second.Committed;
            entry->SetStats(stats);
            size += stats.size();
        } else {
            entry->SetStats(TString()); // stats are not sent from SS yet
        }
    }

    Send(NStat::MakeStatServiceID(leadingNodeId), propagate.release(), 0, cookie);

    return index;
}

TDuration TStatisticsAggregator::GetPropagateInterval() {
    if (BaseStatistics.size() > 1) {
        // We are in a shared database, switch to a bigger propagation interval.
        return TDuration::Seconds(
            StatisticsConfig.GetBaseStatsPropagateIntervalSecondsServerless());
    } else {
        // Otherwise use the dedicated interval.
        return TDuration::Seconds(
            StatisticsConfig.GetBaseStatsPropagateIntervalSecondsDedicated());
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvStatTableCreationResponse::TPtr&) {
    YDB_LOG_DEBUG("EvStatTableCreationResponse",
        {"tabletId", TabletID()});

    IsStatisticsTableCreated = true;
    ResolveStatisticsTablePathId();
    std::vector<TPathId> waiting;
    for (const auto& [path, traversal] : ActiveTraversals) {
        if (traversal.WaitingToSave || traversal.WaitingToDelete) {
            waiting.push_back(path);
        }
    }
    for (const auto& path : waiting) {
        auto it = ActiveTraversals.find(path);
        if (it != ActiveTraversals.end() && !it->second.Finishing) {
            if (it->second.WaitingToDelete) {
                DeleteStatisticsFromTable(it->second);
            } else {
                SaveStatisticsToTable(it->second);
            }
        }
    }
}

void TStatisticsAggregator::ResolveStatisticsTablePathId() {
    if (StatisticsTablePathId || Database.empty() || !IsStatisticsTableCreated) {
        return;
    }

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    TNavigate::TEntry entry;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Path = SplitPath(Database + "/" + StatisticsTablePath);
    entry.Operation = TNavigate::OpPath;
    entry.ShowPrivatePath = true;

    auto request = std::make_unique<TNavigate>();
    request->DatabaseName = Database;
    request->ResultSet.emplace_back(std::move(entry));

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
}

void TStatisticsAggregator::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    if (StatisticsTablePathId) {
        return;
    }

    auto* request = ev->Get()->Request.Get();
    if (!request || request->ResultSet.empty()) {
        YDB_LOG_WARN("Failed to resolve statistics table path id: empty navigate result",
            {"tabletId", TabletID()});
        return;
    }

    const auto& entry = request->ResultSet[0];
    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok || !entry.TableId.PathId) {
        YDB_LOG_WARN("Failed to resolve statistics table path id",
            {"tabletId", TabletID()},
            {"status", entry.Status});
        return;
    }

    StatisticsTablePathId = entry.TableId.PathId;
    YDB_LOG_DEBUG("Resolved statistics table path id",
        {"tabletId", TabletID()},
        {"pathId", StatisticsTablePathId});
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeStatus::TPtr& ev) {
    const auto& inRecord = ev->Get()->Record;
    const TString operationId = inRecord.GetOperationId();

    auto response = std::make_unique<TEvStatistics::TEvAnalyzeStatusResponse>();
    auto& outRecord = response->Record;
    outRecord.SetOperationId(operationId);

    if (HasActiveOperation(operationId)) {
        outRecord.SetStatus(NKikimrStat::TEvAnalyzeStatusResponse::STATUS_IN_PROGRESS);
    } else {
        auto forceTraversalOperation = ForceTraversalOperation(operationId);
        // Terminal operations are preserved in history but no longer "in-progress"
        const bool isTerminal = forceTraversalOperation &&
            IsTerminalAnalyzeState(forceTraversalOperation->State);
        if (forceTraversalOperation && !isTerminal) {
            outRecord.SetStatus(NKikimrStat::TEvAnalyzeStatusResponse::STATUS_ENQUEUED);
        } else {
            outRecord.SetStatus(NKikimrStat::TEvAnalyzeStatusResponse::STATUS_NO_OPERATION);
        }
    }

    YDB_LOG_DEBUG("Send TEvStatistics::TEvAnalyzeStatusResponse",
        {"tabletId", TabletID()},
        {"status", outRecord.GetStatus()});

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

bool TStatisticsAggregator::HasActiveOperation(const TString& operationId) const {
    return !operationId.empty() && std::any_of(ActiveTraversals.begin(), ActiveTraversals.end(),
        [&](const auto& entry) { return entry.second.OperationId == operationId; });
}

TStatisticsAggregator::TTraversal* TStatisticsAggregator::FindTraversalByActor(
        TActorId actor, TActorId TTraversal::* field) {
    for (auto& [pathId, traversal] : ActiveTraversals) {
        if (traversal.*field == actor && !traversal.Finishing) {
            return &traversal;
        }
    }
    return nullptr;
}

void TStatisticsAggregator::PassAway() {
    for (const auto& [pathId, traversal] : ActiveTraversals) {
        if (traversal.AnalyzeActorId) {
            Send(traversal.AnalyzeActorId, new TEvents::TEvPoison());
        }
    }
    if (ScanBudgetActorId) {
        Send(ScanBudgetActorId, new TEvents::TEvPoison());
    }
    IActor::PassAway();
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev) {
    auto it = ActiveTraversals.find(ev->Get()->PathId);
    if (it == ActiveTraversals.end() || it->second.SaveQueryActorId != ev->Sender || it->second.Finishing) {
        return;
    }
    auto& traversal = it->second;
    YDB_LOG_DEBUG("EvSaveStatisticsQueryResponse",
        {"tabletId", TabletID()},
        {"pathId", traversal.PathId},
        {"success", ev->Get()->Success});

    traversal.SaveQueryActorId = {};
    if (ev->Get()->Success) {
        traversal.Published = true;
        SaveStatisticsToTable(traversal);
    } else {
        YDB_LOG_WARN("SaveStatisticsQuery failed",
            {"tabletId", TabletID()},
            {"pathId", traversal.PathId});

        NYql::TIssue error(TStringBuilder() << "Could not save statistics for table id: " << traversal.PathId.LocalPathId);
        DispatchFinishTraversalTx(traversal, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, {error});
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr& ev) {
    auto* traversal = FindTraversalByActor(ev->Sender, &TTraversal::DeleteQueryActorId);
    if (!traversal) {
        return;
    }
    YDB_LOG_WARN("EvDeleteStatisticsQueryResponse",
        {"tabletId", TabletID()},
        {"pathId", traversal->PathId},
        {"success", ev->Get()->Success});

    traversal->DeleteQueryActorId = {};
    NYql::TIssue error(TStringBuilder() << "Could not find table id: " << traversal->PathId.LocalPathId
        << (ev->Get()->Success ? ", deleted its statistics" : ", statistics cleanup failed"));
    DispatchFinishTraversalTx(*traversal, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, {error});
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeActorResult::TPtr& ev) {
    auto* traversal = FindTraversalByActor(ev->Sender, &TTraversal::AnalyzeActorId);
    if (!traversal) {
        return;
    }
    YDB_LOG_DEBUG("EvAnalyzeActorResult",
        {"tabletId", TabletID()},
        {"pathId", traversal->PathId},
        {"status", static_cast<int>(ev->Get()->Status)},
        {"final", ev->Get()->Final},
        {"statisticsCount", ev->Get()->Statistics.size()});

    if (ev->Get()->Final) {
        traversal->AnalyzeActorId = {};
    }

    using EStatus = TEvStatistics::TEvAnalyzeActorResult::EStatus;
    switch (ev->Get()->Status) {
    case EStatus::Success:
        std::move(
            ev->Get()->Statistics.begin(), ev->Get()->Statistics.end(),
            std::back_inserter(traversal->StatisticsToSave));
        SaveStatisticsToTable(*traversal);
        return;
    case EStatus::TableNotFound:
        DeleteStatisticsFromTable(*traversal);
        return;
    case EStatus::InternalError: {
        const auto* table = ForceTraversalTable(traversal->OperationId, traversal->PathId);
        YDB_LOG_WARN("EvAnalyzeActorResult InternalError",
            {"tabletId", TabletID()},
            {"operationId", traversal->OperationId.Quote()},
            {"database", traversal->Database},
            {"pathId", traversal->PathId},
            {"tablePath", table ? table->Path : TString()},
            {"analyzeActorId", ev->Sender},
            {"issues", ev->Get()->Issues.ToOneLineString()});
        DispatchFinishTraversalTx(*traversal, NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR,
            std::move(ev->Get()->Issues));
        return;
    }
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeCancel::TPtr& ev) {
    const auto& operationId = ev->Get()->Record.GetOperationId();
    if (operationId.empty()) {
        return;
    }
    for (auto& [path, traversal] : ActiveTraversals) {
        if (traversal.OperationId == operationId && !traversal.Finishing) {
            YDB_LOG_DEBUG("Got TEvAnalyzeCancel",
                {"operationId", operationId.Quote()},
                {"pathId", path});
            DispatchFinishTraversalTx(traversal, NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
            return;
        }
    }
    YDB_LOG_NOTICE("Got unexpected TEvAnalyzeCancel, ignoring",
        {"operationId", operationId.Quote()});
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeActorProgress::TPtr& ev) {
    const auto& msg = *ev->Get();
    const auto it = ActiveTraversals.find(msg.PathId);
    if (it == ActiveTraversals.end() || it->second.AnalyzeActorId != ev->Sender ||
            it->second.OperationId != msg.OperationId || it->second.Finishing) {
        return;
    }
    if (auto* table = ForceTraversalTable(msg.OperationId, msg.PathId)) {
        table->ShardsTotal = msg.ShardsTotal;
        table->ShardsDone  = std::max(table->ShardsDone, msg.ShardsDone);
    }
}

void TStatisticsAggregator::InitializeStatisticsTable() {
    if (!EnableColumnStatistics) {
        return;
    }
    if (!Database) {
        return;
    }
    Register(CreateStatisticsTableCreator(
        std::make_unique<TEvStatistics::TEvStatTableCreationResponse>(), Database));
}

void TStatisticsAggregator::SaveStatisticsToTable(TTraversal& traversal) {
    constexpr ui64 MAX_BATCH_SIZE = 30_MB;

    if (!IsStatisticsTableCreated) {
        traversal.WaitingToSave = true;
        return;
    }
    if (traversal.SaveQueryActorId) {
        // Next query will be dispatched when the current SaveQuery actor finishes.
        return;
    }

    traversal.WaitingToSave = false;

    size_t dataSize = 0;
    std::vector<TStatisticsItem> items;
    while (!traversal.StatisticsToSave.empty()) {
        auto& item = traversal.StatisticsToSave.front();
        if (!items.empty() && dataSize + item.Data.size() > MAX_BATCH_SIZE) {
            break;
        }
        dataSize += item.Data.size();
        items.push_back(std::move(item));
        traversal.StatisticsToSave.pop_front();
    };

    if (items.empty() && (traversal.AnalyzeActorId || traversal.Published)) {
        if (!traversal.AnalyzeActorId) {
            DispatchFinishTraversalTx(traversal, NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        }
        return;
    }
    size_t itemsSize = items.size();

    // Even an empty final result advances the write fence for a superseded attempt.
    traversal.SaveQueryActorId = Register(
        CreateSaveStatisticsQuery(SelfId(), Database, traversal.PathId, std::move(items), traversal.Attempt));
    YDB_LOG_DEBUG("Dispatched SaveStatisticsQuery",
        {"tabletId", TabletID()},
        {"actorId", traversal.SaveQueryActorId},
        {"itemsSize", itemsSize},
        {"dataSize", HumanReadableSize(dataSize, ESizeFormat::SF_BYTES)},
        {"itemsLeft", traversal.StatisticsToSave.size()});
}

void TStatisticsAggregator::DeleteStatisticsFromTable(TTraversal& traversal) {
    if (!IsStatisticsTableCreated) {
        traversal.WaitingToDelete = true;
        return;
    }

    traversal.WaitingToDelete = false;

    traversal.DeleteQueryActorId = Register(CreateDeleteStatisticsQuery(SelfId(), Database, traversal.PathId, traversal.Attempt));
}

void TStatisticsAggregator::ScheduleTraversalsForScan(NIceDb::TNiceDb& db, const TActorContext& ctx,
        bool allowBackground, std::vector<std::pair<TPathId, ui64>>& starts) {
    const size_t limit = std::max<ui32>(1, StatisticsConfig.GetAnalyzeMaxTablesInFlight());
    if (ActiveTraversals.size() >= limit) {
        YDB_LOG_TRACE("Analyze table limit reached",
            {"tabletId", TabletID()},
            {"activeTables", ActiveTraversals.size()},
            {"limit", limit});
        return;
    }
    auto reserve = [&](TPathId pathId, const TString& operationId, const TString& database) {
        PendingBackgroundChecks.erase(pathId);
        auto& traversal = ActiveTraversals.try_emplace(pathId).first->second;
        traversal.PathId = pathId;
        traversal.OperationId = operationId;
        traversal.Database = database;
        traversal.StartTime = ctx.Now();
        traversal.Attempt = AllocateTraversalAttempt();
        PersistTraversal(db, traversal);
        starts.emplace_back(pathId, traversal.Attempt);
    };

    for (auto& operation : ForceTraversals) {
        // Terminal operations are history entries with no pending work.
        if (IsTerminalAnalyzeState(operation.State)) {
            continue;
        }
        for (auto& table : operation.Tables) {
            using EStatus = TForceTraversalTable::EStatus;
            if (table.Status != EStatus::None) {
                continue;
            }
            if (ActiveTraversals.contains(table.PathId)) {
                continue;
            }
            if (!IsKnownTable(table.PathId).has_value() && !PendingBackgroundChecks.contains(table.PathId)) {
                YDB_LOG_DEBUG("Waiting for SchemeShard statistics before starting ANALYZE",
                    {"tabletId", TabletID()},
                    {"pathId", table.PathId});
                continue;
            }
            // The keyed reservation is the durable start marker. A previous binary
            // can requeue this table from its legacy None status on downgrade.
            table.Status = EStatus::AnalyzeStarted;
            reserve(table.PathId, operation.OperationId, operation.DatabaseName);
            if (ActiveTraversals.size() == limit) {
                return;
            }
        }
    }

    if (!allowBackground || !StatisticsTablePathId || !StatisticsConfig.GetEnableBackgroundColumnStatsCollection()) {
        return;
    }
    while (ActiveTraversals.size() < limit && !PendingBackgroundChecks.empty()) {
        reserve(*PendingBackgroundChecks.begin(), {}, {});
    }
    while (ActiveTraversals.size() < limit && !ScheduleTraversalsByTime.Empty()) {
        auto* table = ScheduleTraversalsByTime.Top();
        if (IsStatisticsTable(table->PathId) || ActiveTraversals.contains(table->PathId)) {
            // Active paths re-enter this heap on completion.
            ScheduleTraversalsByTime.Remove(table);
            continue;
        }
        // Zero is the persisted marker for a table awaiting its first collection.
        if (table->LastUpdateTime && ctx.Now() < table->LastUpdateTime + ScheduleTraversalPeriod) {
            if (EnableBackgroundAnalyzeChangeRatio) {
                for (const auto* stale : FindStaleTables(limit - ActiveTraversals.size())) {
                    reserve(stale->PathId, {}, {});
                }
            }
            break;
        }
        reserve(table->PathId, {}, {});
    }
}

TStatisticsAggregator::TTraversalReply TStatisticsAggregator::FinishTraversal(
        NIceDb::TNiceDb& db, TTraversal& traversal,
        NKikimrStat::TEvAnalyzeResponse::EStatus status, const NYql::TIssues& issues) {
    const auto pathId = traversal.PathId;
    const TString operationId = traversal.OperationId;
    YDB_LOG_DEBUG("FinishTraversal",
        {"tabletId", TabletID()},
        {"status", static_cast<int>(status)},
        {"pathId", pathId},
        {"operationId", operationId.Quote()});

    const bool succeeded = status == NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS;
    RemoveTraversal(db, traversal, succeeded);
    AnalyzeCountersDirty = true;

    if (operationId.empty()) {
        if (succeeded) {
            BackgroundAnalyzeCompletedCounter->Inc();
        } else {
            BackgroundAnalyzeFailedCounter->Inc();
        }
        // A queued user ANALYZE may require statistics added after this traversal started.
        return {};
    }

    auto* operation = ForceTraversalOperation(operationId);
    if (!operation || IsTerminalAnalyzeState(operation->State)) {
        return {};
    }
    auto state = Ydb::Table::AnalyzeState::STATE_DONE;
    if (succeeded) {
        if (auto* table = ForceTraversalTable(operationId, pathId)) {
            UpdateForceTraversalTableStatus(
                TForceTraversalTable::EStatus::TraversalFinished, operationId, *table, db);
        }
        if (std::any_of(operation->Tables.begin(), operation->Tables.end(),
                [](const auto& t) { return t.Status != TForceTraversalTable::EStatus::TraversalFinished; })) {
            return {};
        }
    } else {
        state = status == NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED
            ? Ydb::Table::AnalyzeState::STATE_CANCELLED : Ydb::Table::AnalyzeState::STATE_FAILED;
        DiscardOperationTraversals(operationId, db);
    }

    TTraversalReply reply{operation->ReplyToActorId, operationId};
    operation->ReplyToActorId = {};
    MarkForceTraversalOperationFinished(operationId, state, TActivationContext::Now(), db, issues);
    return reply;
}

TStatisticsAggregator::TForceTraversalOperation* TStatisticsAggregator::ForceTraversalOperation(const TString& operationId) {
    auto forceTraversalOperation = std::find_if(ForceTraversals.begin(), ForceTraversals.end(),
        [operationId](const TForceTraversalOperation& elem) { return elem.OperationId == operationId;});

    if (forceTraversalOperation == ForceTraversals.end()) {
        return nullptr;
    } else {
        return &*forceTraversalOperation;
    }
}

std::optional<bool> TStatisticsAggregator::IsKnownTable(const TPathId& pathId) const {
    if (ScheduleTraversals.contains(pathId)) {
        return true;
    }
    if (!BaseStatistics.contains(pathId.OwnerId)) {
        // no info from the corresponding schemeshard yet
        return std::nullopt;
    }
    return false;
}

bool TStatisticsAggregator::IsStatisticsTable(const TPathId& pathId) const {
    return StatisticsTablePathId && pathId == StatisticsTablePathId;
}

void TStatisticsAggregator::DeleteForceTraversalOperation(const TString& operationId, NIceDb::TNiceDb& db) {
    YDB_LOG_DEBUG("DeleteForceTraversalOperation",
        {"tabletId", TabletID()},
        {"operationId", operationId.Quote()});

    db.Table<Schema::ForceTraversalOperations>().Key(operationId).Delete();

    auto operation = ForceTraversalOperation(operationId);
    if (operation) {
        for (const TForceTraversalTable& table : operation->Tables) {
            db.Table<Schema::ForceTraversalTables>().Key(operationId, table.PathId.OwnerId, table.PathId.LocalPathId).Delete();
        }
        ForceTraversals.remove_if([operationId](const TForceTraversalOperation& elem) { return elem.OperationId == operationId;});
        RecalcForceTraversalsInflightSizeCounter();
    }
}

size_t TStatisticsAggregator::InflightForceTraversalCount() const {
    size_t n = 0;
    for (const auto& op : ForceTraversals) {
        if (!IsTerminalAnalyzeState(op.State)) {
            ++n;
        }
    }
    return n;
}

void TStatisticsAggregator::RecalcForceTraversalsInflightSizeCounter() {
    TabletCounters->Simple()[COUNTER_FORCE_TRAVERSALS_INFLIGHT_SIZE]
        .Set(InflightForceTraversalCount());
}

void TStatisticsAggregator::RecalcForceTraversalInflightMaxTimeCounter(TInstant now) {
    TDuration time = TDuration::Zero();
    for (const auto& op : ForceTraversals) {
        if (!IsTerminalAnalyzeState(op.State)) {
            time = now - op.CreatedAt;
            break; // first non-terminal is the oldest (list is insertion-ordered)
        }
    }
    TabletCounters->Simple()[COUNTER_FORCE_TRAVERSAL_INFLIGHT_MAX_TIME].Set(time.MicroSeconds());
}

void TStatisticsAggregator::MarkForceTraversalOperationFinished(
    const TString& operationId,
    Ydb::Table::AnalyzeState::State state,
    TInstant endTime,
    NIceDb::TNiceDb& db,
    NYql::TIssues issues)
{
    auto* op = ForceTraversalOperation(operationId);
    if (!op) {
        return;
    }
    if (IsTerminalAnalyzeState(op->State)) {
        return;
    }

    YDB_LOG_DEBUG("MarkForceTraversalOperationFinished",
        {"tabletId", TabletID()},
        {"operationId", operationId.Quote()},
        {"state", static_cast<int>(state)});

    // When the long-running operation API is disabled, fall back to pre-PR behavior:
    // delete the operation on completion instead of retaining it as history.
    if (!AppData()->FeatureFlags.GetEnableAnalyzeLongRunningOperation()) {
        DeleteForceTraversalOperation(operationId, db);
        return;
    }

    op->State = state;
    op->EndTime = endTime;
    if (!issues.Empty()) {
        op->Issues = std::move(issues);
    }

    db.Table<Schema::ForceTraversalOperations>().Key(operationId).Update(
        NIceDb::TUpdate<Schema::ForceTraversalOperations::State>(static_cast<ui64>(state)),
        NIceDb::TUpdate<Schema::ForceTraversalOperations::EndTime>(endTime.GetValue())
    );

    // The op transitions from in-flight to history; the inflight counter shrinks.
    RecalcForceTraversalsInflightSizeCounter();
}

void TStatisticsAggregator::FillAnalyzeOperationProto(
    const TForceTraversalOperation& op,
    NKikimrAnalyzeOp::TAnalyzeOperation& proto) const
{
    proto.SetOperationId(op.OperationId);
    proto.SetDatabaseName(op.DatabaseName);

    {
        auto* ts = proto.MutableCreateTime();
        ts->set_seconds(op.CreatedAt.Seconds());
        ts->set_nanos(op.CreatedAt.NanoSeconds() % 1'000'000'000);
    }

    // Derive state
    Ydb::Table::AnalyzeState::State state;
    if (IsTerminalAnalyzeState(op.State)) {
        state = op.State;
        auto* ts = proto.MutableEndTime();
        ts->set_seconds(op.EndTime.Seconds());
        ts->set_nanos(op.EndTime.NanoSeconds() % 1'000'000'000);
    } else if (HasActiveOperation(op.OperationId)) {
        state = Ydb::Table::AnalyzeState::STATE_IN_PROGRESS;
    } else {
        state = Ydb::Table::AnalyzeState::STATE_ENQUEUED;
    }
    proto.SetState(state);

    for (const auto& issue : op.Issues) {
        NYql::IssueToMessage(issue, proto.AddIssues());
    }

    ui64 shardsTotalSum = 0;
    ui64 shardsDoneSum = 0;
    for (const auto& t : op.Tables) {
        proto.AddPaths(t.Path);
        shardsTotalSum += t.ShardsTotal;
        shardsDoneSum  += t.ShardsDone;
        if (t.Status == TForceTraversalTable::EStatus::TraversalFinished) {
            proto.AddDonePaths(t.Path);
        } else if (t.Status != TForceTraversalTable::EStatus::None &&
                   state == Ydb::Table::AnalyzeState::STATE_IN_PROGRESS) {
            proto.AddInProgressPaths(t.Path);
        }
    }

    if (state == Ydb::Table::AnalyzeState::STATE_DONE) {
        proto.SetProgress(100.0f);
    } else if (state == Ydb::Table::AnalyzeState::STATE_CANCELLED ||
               state == Ydb::Table::AnalyzeState::STATE_FAILED ||
               state == Ydb::Table::AnalyzeState::STATE_ENQUEUED)
    {
        proto.SetProgress(0.0f);
    } else if (shardsTotalSum == 0) {
        proto.SetProgress(0.0f);
    } else {
        // Use double for the intermediate to avoid float32 precision loss for shard counts > 16M.
        proto.SetProgress(static_cast<float>(100.0 * shardsDoneSum / shardsTotalSum));
    }
}

TStatisticsAggregator::TForceTraversalTable* TStatisticsAggregator::ForceTraversalTable(const TString& operationId, const TPathId& pathId) {
    if (operationId.empty()) {
        return nullptr;
    }
    for (TForceTraversalOperation& operation : ForceTraversals) {
        if (operation.OperationId == operationId) {
            for (TForceTraversalTable& operationTable : operation.Tables) {
                if (operationTable.PathId == pathId) {
                    return &operationTable;
                }
            }
        }
    }

    return nullptr;
}

void TStatisticsAggregator::UpdateForceTraversalTableStatus(const TForceTraversalTable::EStatus status, const TString& operationId, TStatisticsAggregator::TForceTraversalTable& table, NIceDb::TNiceDb& db) {
    table.Status = status;
    db.Table<Schema::ForceTraversalTables>().Key(operationId, table.PathId.OwnerId, table.PathId.LocalPathId)
        .Update(NIceDb::TUpdate<Schema::ForceTraversalTables::Status>((ui64)status));
}


void TStatisticsAggregator::PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value) {
    db.Table<Schema::SysParams>().Key(id).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(value));
}

void TStatisticsAggregator::PersistTraversal(NIceDb::TNiceDb& db, const TTraversal& traversal) {
    db.Table<Schema::ActiveTraversals>().Key(traversal.PathId.OwnerId, traversal.PathId.LocalPathId).Update(
        NIceDb::TUpdate<Schema::ActiveTraversals::OperationId>(traversal.OperationId),
        NIceDb::TUpdate<Schema::ActiveTraversals::DatabaseName>(traversal.Database),
        NIceDb::TUpdate<Schema::ActiveTraversals::StartTime>(traversal.StartTime.MicroSeconds()));
}

void TStatisticsAggregator::DeletePersistedTraversal(NIceDb::TNiceDb& db, TPathId pathId) {
    db.Table<Schema::ActiveTraversals>().Key(pathId.OwnerId, pathId.LocalPathId).Delete();
}

ui64 TStatisticsAggregator::AllocateTraversalAttempt() {
    Y_ABORT_UNLESS(AttemptSequence < Max<ui32>());
    return (ui64(Executor()->Generation()) << 32) | ++AttemptSequence;
}

void TStatisticsAggregator::StartTraversal(const TActorContext& ctx, TTraversal& traversal) {
    const auto& operationId = traversal.OperationId;
    const auto& pathId = traversal.PathId;
    // Clamp oversample to [1, 256] and maxStateBytes to (0, MaxStatisticSize].
    const ui32 oversampleFactor = std::max<ui32>(1, std::min<ui32>(
        StatisticsConfig.GetAnalyzeHistogramOversampleFactor(),
        TAnalyzeActor::MaxHistogramOversampleFactor));
    const ui64 maxStateBytes = std::max<ui64>(1, std::min<ui64>(
        StatisticsConfig.GetAnalyzeHistogramMaxStateBytes(),
        TAnalyzeActor::MaxStatisticSize));
    const auto* table = ForceTraversalTable(operationId, pathId);
    const auto scheduled = ScheduleTraversals.find(pathId);
    const auto schemeShardId = scheduled != ScheduleTraversals.end() ? scheduled->second.SchemeShardId : pathId.OwnerId;
    auto analyzeActorConfig = TAnalyzeActor::TConfig{
        .ColumnTableWholeTableScanMaxBytes = StatisticsConfig.GetAnalyzeColumnTableWholeTableScanMaxBytes(),
        .RowTableWholeTableScanMaxBytes = StatisticsConfig.GetAnalyzeRowTableWholeTableScanMaxBytes(),
        .TableBytesSize = GetTableBaseStats(pathId, schemeShardId).BytesSize,
        .CollectPrimaryKeyHistogram = StatisticsConfig.GetAnalyzeCollectPrimaryKeyHistogram(),
        .HistogramOversampleFactor = oversampleFactor,
        .HistogramMaxStateBytes = maxStateBytes,
        .SampleRate = table ? table->SampleRate : 1.0,
    };
    // TAnalyzeActor determines the statistic types from the schema, independently of operation.Types.
    traversal.AnalyzeActorId = ctx.Register(new TAnalyzeActor(
        SelfId(), operationId, traversal.Database, pathId, table ? table->ColumnTags : TVector<ui32>{},
        analyzeActorConfig, ScanBudgetActorId),
        TMailboxType::HTSwap, AppData()->BatchPoolId);
    YDB_LOG_DEBUG("Started analyzing table",
        {"tabletId", TabletID()},
        {"operationId", operationId.Quote()},
        {"pathId", pathId},
        {"analyzeActorId", traversal.AnalyzeActorId});
}

void TStatisticsAggregator::RemoveTraversal(NIceDb::TNiceDb& db, TTraversal& traversal, bool succeeded) {
    const auto path = traversal.PathId;
    if (traversal.AnalyzeActorId) {
        Send(traversal.AnalyzeActorId, new TEvents::TEvPoison());
    }
    if (auto it = ScheduleTraversals.find(path); it != ScheduleTraversals.end()) {
        auto& scheduled = it->second;
        const auto* table = ForceTraversalTable(traversal.OperationId, path);
        // A sample does not refresh the full statistics used by background ANALYZE.
        if (!table || table->SampleRate == 1.0) {
            // Every full scan attempt starts the background cooldown, including a cancelled scan.
            scheduled.LastUpdateTime = traversal.StartTime;
            if (succeeded) {
                const auto current = GetTableBaseStats(path, scheduled.SchemeShardId).Counters;
                // A scan can finish before the first full SchemeShard report arrives.
                // Keep the never-analyzed marker in that case. A baseline of (0, 0)
                // would make the first real report look like a large change.
                if (current.RowCount || current.RowUpdates || current.RowDeletes) {
                    scheduled.LastAnalyzeRowUpdates = current.RowUpdates;
                    scheduled.LastAnalyzeRowDeletes = current.RowDeletes;
                }
            }
            using TSchedule = Schema::ScheduleTraversals;
            db.Table<TSchedule>().Key(path.OwnerId, path.LocalPathId).Update(
                NIceDb::TUpdate<TSchedule::LastUpdateTime>(scheduled.LastUpdateTime.MicroSeconds()),
                NIceDb::TUpdate<TSchedule::LastAnalyzeRowUpdates>(scheduled.LastAnalyzeRowUpdates),
                NIceDb::TUpdate<TSchedule::LastAnalyzeRowDeletes>(scheduled.LastAnalyzeRowDeletes));
            if (ScheduleTraversalsByTime.Has(&scheduled)) {
                ScheduleTraversalsByTime.Update(&scheduled);
            }
        }
        if (!IsStatisticsTable(path) && !ScheduleTraversalsByTime.Has(&scheduled)) {
            ScheduleTraversalsByTime.Add(&scheduled);
        }
    }
    DeletePersistedTraversal(db, path);
    ActiveTraversals.erase(path);
}

void TStatisticsAggregator::DiscardOperationTraversals(const TString& operationId, NIceDb::TNiceDb& db) {
    if (operationId.empty()) {
        return;
    }
    for (auto it = ActiveTraversals.begin(); it != ActiveTraversals.end();) {
        auto& traversal = (it++)->second;
        if (traversal.OperationId == operationId) {
            RemoveTraversal(db, traversal, /*succeeded=*/false);
        }
    }
}

TString TStatisticsAggregator::TForceTraversalTable::GetStatusString() const {
    switch (Status) {
    case EStatus::None:
        return "None";
    case EStatus::AnalyzeStarted:
        return "AnalyzeStarted";
    case EStatus::AnalyzeFinished:
        return "AnalyzeFinished";
    case EStatus::TraversalStarted:
        return "TraversalStarted";
    case EStatus::TraversalFinished:
        return "TraversalFinished";
    }
}

void PrintContainerStart(const auto& container, size_t count, TStringStream& str, auto extractor)
{
    if (container.empty()) {
        return;
    }
    str << "    ";
    size_t i = 0;
    for (const auto& entry : container) {
        if (i) {
            str << ", ";
        }
        str << extractor(entry);
        if (++i == count) {
            break;
        }
    }
    if (container.size() > count) {
        str << " ...";
    }
    str << Endl;
}

bool TStatisticsAggregator::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev,
    const TActorContext& ctx)
{
    if (!ev) {
        return true;
    }

    TStringStream str;
    HTML(str) {
        PRE() {
            str << "---- StatisticsAggregator ----" << Endl << Endl;
            str << "Database: " << Database << Endl;
            str << "BaseStatistics: " << BaseStatistics.size() << Endl;
            str << "SchemeShards: " << SchemeShards.size() << Endl;
            {
                auto extr = [](const auto& x) { return x.first; };
                PrintContainerStart(SchemeShards, 4, str, extr);
            }
            str << "Nodes: " << Nodes.size() << Endl;
            {
                auto extr = [](const auto& x) { return x.first; };
                PrintContainerStart(Nodes, 8, str, extr);
            }
            str << "RequestedSchemeShards: " << RequestedSchemeShards.size() << Endl;
            {
                auto extr = [](const auto& x) { return x; };
                PrintContainerStart(RequestedSchemeShards, 4, str, extr);
            }
            str << "FastCounter: " << FastCounter << Endl;
            str << "FastCheckInFlight: " << FastCheckInFlight << Endl;
            str << "FastSchemeShards: " << FastSchemeShards.size() << Endl;
            {
                auto extr = [](const auto& x) { return x; };
                PrintContainerStart(FastSchemeShards, 4, str, extr);
            }
            str << "FastNodes: " << FastNodes.size() << Endl;
            {
                auto extr = [](const auto& x) { return x; };
                PrintContainerStart(FastNodes, 8, str, extr);
            }
            str << "CurPropagationSeq: " << CurPropagationSeq << Endl;
            str << "PropagationInFlight: " << PropagationInFlight << Endl;
            str << "PropagationSchemeShards: " << PropagationSchemeShards.size() << Endl;
            {
                auto extr = [](const auto& x) { return x; };
                PrintContainerStart(PropagationSchemeShards, 4, str, extr);
            }
            str << "PropagationNodes: " << PropagationNodes.size() << Endl;
            {
                auto extr = [](const auto& x) { return x; };
                PrintContainerStart(FastNodes, 8, str, extr);
            }
            str << "LastSSIndex: " << LastSSIndex << Endl;
            str << "PendingRequests: " << PendingRequests.size() << Endl;
            str << "ProcessUrgentInFlight: " << ProcessUrgentInFlight << Endl << Endl;

            str << "ScheduleTraversalsByTime: " << ScheduleTraversalsByTime.Size() << Endl;
            if (!ScheduleTraversalsByTime.Empty()) {
                auto* oldestTable = ScheduleTraversalsByTime.Top();
                str << "  oldest table: " << oldestTable->PathId
                    << ", update time: " << oldestTable->LastUpdateTime.ToStringUpToSeconds() << Endl;
            }
            str << "ScheduleTraversalsBySchemeShard: " << ScheduleTraversalsBySchemeShard.size() << Endl;
            if (!ScheduleTraversalsBySchemeShard.empty()) {
                str << "    " << ScheduleTraversalsBySchemeShard.begin()->first << Endl;
                auto extr = [](const auto& x) { return x; };
                PrintContainerStart(ScheduleTraversalsBySchemeShard.begin()->second, 2, str, extr);
            }
            str << "ForceTraversals: " << ForceTraversals.size() << Endl;
            if (!ForceTraversals.empty()) {
                auto extr = [](const auto& x) { return x.CreatedAt.ToStringUpToSeconds(); };
                PrintContainerStart(ForceTraversals, 2, str, extr);
            }

            // Per-table change counters (from cached BaseStatistics).
            for (auto& [shardId, serializedStats] : BaseStatistics) {
                const auto& tables = serializedStats.GetTables();
                str << "CachedChangeCounters for SchemeShard " << shardId << ": " << tables.size() << Endl;
                for (const auto& [pathId, table] : tables) {
                    str << "  " << pathId
                        << " RowUpdates=" << table.Counters.RowUpdates
                        << " RowDeletes=" << table.Counters.RowDeletes
                        << " RowCount=" << table.Counters.RowCount << Endl;
                }
            }

            str << Endl;
            str << "ActiveTraversals: " << ActiveTraversals.size() << Endl;
            for (const auto& [path, traversal] : ActiveTraversals) {
                str << "  Path: " << path << " Operation: " << traversal.OperationId.Quote()
                    << " Database: " << traversal.Database << " StartTime: " << traversal.StartTime
                    << " AnalyzeActorId: " << traversal.AnalyzeActorId
                    << " SaveQueryActorId: " << traversal.SaveQueryActorId << Endl;
                if (auto* operation = ForceTraversalOperation(traversal.OperationId)) {
                    str << "    CreatedAt: " << operation->CreatedAt << " ReplyToActorId: " << operation->ReplyToActorId << Endl;
                    if (auto* table = ForceTraversalTable(traversal.OperationId, path)) {
                        str << "    Status: " << table->GetStatusString()
                            << " ColumnTags: " << JoinVectorIntoString(table->ColumnTags, ",") << Endl;
                    }
                }
            }
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

TStatisticsAggregator::TBaseStatisticsTotals TStatisticsAggregator::CalculateBaseStatisticsTotals(
    const NKikimrStat::TSchemeShardStats& stats)
{
    TBaseStatisticsTotals totals;
    for (const auto& entry : stats.GetEntries()) {
        totals.RowCount += entry.GetRowCount();
        totals.BytesSize += entry.GetBytesSize();
    }
    return totals;
}

void TStatisticsAggregator::RebuildBaseStatisticsCounters() {
    BaseStatisticsTotals = {};
    for (auto& [_, serializedStats] : BaseStatistics) {
        serializedStats.CommittedTotals = {};
        if (!serializedStats.Committed) {
            continue;
        }
        NKikimrStat::TSchemeShardStats stats;
        if (stats.ParseFromString(*serializedStats.Committed)) {
            serializedStats.IndexTables(stats);
        }
        serializedStats.CommittedTotals = CalculateBaseStatisticsTotals(stats);
        BaseStatisticsTotals.RowCount += serializedStats.CommittedTotals.RowCount;
        BaseStatisticsTotals.BytesSize += serializedStats.CommittedTotals.BytesSize;
    }
    ReportBaseStatisticsCounters();
}

void TStatisticsAggregator::ReportBaseStatisticsCounters() {
    TabletCounters->Simple()[COUNTER_BASE_STATISTICS_TOTAL_ROW_COUNT].Set(BaseStatisticsTotals.RowCount);
    TabletCounters->Simple()[COUNTER_BASE_STATISTICS_TOTAL_BYTES_SIZE].Set(BaseStatisticsTotals.BytesSize);
}

void TStatisticsAggregator::TSerializedBaseStats::IndexTables(const NKikimrStat::TSchemeShardStats& stats) {
    auto& tables = ParsedTables.emplace();
    tables.reserve(stats.EntriesSize());
    for (const auto& entry : stats.GetEntries()) {
        TTableBaseStats table{
            .Counters = {entry.GetRowUpdates(), entry.GetRowDeletes(), entry.GetRowCount()},
        };
        if (entry.HasBytesSize()) {
            table.BytesSize = entry.GetBytesSize();
        }
        tables.emplace(TPathId::FromProto(entry.GetPathId()), table);
    }
}

const THashMap<TPathId, TStatisticsAggregator::TTableBaseStats>& TStatisticsAggregator::TSerializedBaseStats::GetTables() {
    if (!ParsedTables) {
        NKikimrStat::TSchemeShardStats stats;
        if (Latest && stats.ParseFromString(*Latest)) {
            IndexTables(stats);
        } else {
            ParsedTables.emplace();
        }
    }
    return *ParsedTables;
}

TStatisticsAggregator::TTableBaseStats TStatisticsAggregator::GetTableBaseStats(const TPathId& pathId, TSSId schemeShardId) {
    auto shard = BaseStatistics.find(schemeShardId);
    if (shard == BaseStatistics.end()) {
        return {};
    }
    return shard->second.GetTables().Value(pathId, TTableBaseStats{});
}

bool TStatisticsAggregator::IsChangeRatioAboveThreshold(
    const TChangeCounters& lastAnalyze, const TChangeCounters& current) const
{
    if (!EnableBackgroundAnalyzeChangeRatio) {
        return false;
    }
    if (lastAnalyze.RowUpdates == Max<ui64>() || lastAnalyze.RowDeletes == Max<ui64>()) {
        // Never analyzed — but only treat as stale once SchemeShard has sent
        // real counters. Otherwise FinishTraversal would keep baselining at
        // zero and we would re-analyze on every scheduling tick.
        return current.RowCount > 0 || current.RowUpdates > 0 || current.RowDeletes > 0;
    }
    // RowUpdates and RowDeletes are monotonic cumulative counters, so current
    // should not fall below the snapshot taken at the last ANALYZE. Guard
    // against underflow.
    if (current.RowCount == 0) {
        return false;
    }
    if (current.RowUpdates <= lastAnalyze.RowUpdates && current.RowDeletes <= lastAnalyze.RowDeletes) {
        return false;
    }

    // RowUpdates and RowDeletes are monotonic cumulative counters, so current
    // should not fall below the snapshot taken at the last ANALYZE. Guard
    // against underflow.
    ui64 updatesSinceAnalyze = current.RowUpdates > lastAnalyze.RowUpdates
        ? current.RowUpdates - lastAnalyze.RowUpdates : 0;
    ui64 deletesSinceAnalyze = current.RowDeletes > lastAnalyze.RowDeletes
        ? current.RowDeletes - lastAnalyze.RowDeletes : 0;
    ui64 changesSinceAnalyze = updatesSinceAnalyze + deletesSinceAnalyze;
    auto threshold = StatisticsConfig.GetBackgroundAnalyzeChangeRatioThresholdPercent();


    double changeRatio = static_cast<double>(changesSinceAnalyze) / current.RowCount * 100.0;
    return changeRatio >= static_cast<double>(threshold);
}

std::vector<const TStatisticsAggregator::TScheduleTraversal*> TStatisticsAggregator::FindStaleTables(size_t limit) {
    // The time-ordered heap cannot identify tables above the change-ratio threshold.
    // Examine the candidates and keep the oldest stale tables up to the limit.
    Y_DEBUG_ABORT_UNLESS(limit > 0);
    std::vector<const TScheduleTraversal*> oldest;
    auto older = [](const TScheduleTraversal* l, const TScheduleTraversal* r) {
        return std::tie(l->LastUpdateTime, l->PathId) < std::tie(r->LastUpdateTime, r->PathId);
    };

    for (const auto& [pathId, traversal] : ScheduleTraversals) {
        if (IsStatisticsTable(pathId) || ActiveTraversals.contains(pathId)) {
            continue;
        }
        // The newest selected table is at the front of the bounded heap.
        if (oldest.size() == limit && !older(&traversal, oldest.front())) {
            continue;
        }
        const auto current = GetTableBaseStats(pathId, traversal.SchemeShardId).Counters;
        TChangeCounters lastAnalyze{traversal.LastAnalyzeRowUpdates, traversal.LastAnalyzeRowDeletes, 0};
        if (!IsChangeRatioAboveThreshold(lastAnalyze, current)) {
            continue;
        }
        if (oldest.size() == limit) {
            std::pop_heap(oldest.begin(), oldest.end(), older);
            oldest.pop_back();
        }
        oldest.push_back(&traversal);
        std::push_heap(oldest.begin(), oldest.end(), older);
    }
    std::sort_heap(oldest.begin(), oldest.end(), older);
    return oldest;
}

void TStatisticsAggregator::InitAnalyzeCounters() {
    // Initialize dynamic counters for background ANALYZE monitoring.
    // Uses GetSubgroup to create a single metric with a "status" label:
    //   BackgroundAnalyze{status="pending"}   — gauge: tables needing ANALYZE
    //   BackgroundAnalyze{status="completed"} — cumulative: successful ANALYZE
    //   BackgroundAnalyze{status="failed"}    — cumulative: failed ANALYZE
    auto analyzeCounters = GetServiceCounters(AppData()->Counters, "statistics")
        ->GetSubgroup("subsystem", "background_analyze");
    BackgroundAnalyzePendingCounter = analyzeCounters
        ->GetSubgroup("status", "pending")->GetCounter("BackgroundAnalyze", false);
    BackgroundAnalyzeCompletedCounter = analyzeCounters
        ->GetSubgroup("status", "completed")->GetCounter("BackgroundAnalyze", true);
    BackgroundAnalyzeFailedCounter = analyzeCounters
        ->GetSubgroup("status", "failed")->GetCounter("BackgroundAnalyze", true);
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvReportAnalyzeCounters::TPtr&) {
    if (AnalyzeCountersDirty) {
        ReportAnalyzeCounters();
        AnalyzeCountersDirty = false;
    }
    Schedule(AnalyzeCountersReportPeriod, new TEvPrivate::TEvReportAnalyzeCounters());
}

void TStatisticsAggregator::ReportAnalyzeCounters() {
    if (!EnableBackgroundAnalyzeChangeRatio) {
        *BackgroundAnalyzePendingCounter = 0;
        return;
    }

    ui64 pendingTables = 0;
    for (const auto& [pathId, traversal] : ScheduleTraversals) {
        if (!traversal.IsColumnTable) {
            continue;
        }
        const auto current = GetTableBaseStats(pathId, traversal.SchemeShardId).Counters;
        TChangeCounters lastAnalyze{traversal.LastAnalyzeRowUpdates, traversal.LastAnalyzeRowDeletes, 0};
        if (IsChangeRatioAboveThreshold(lastAnalyze, current)) {
            ++pendingTables;
        }
    }
    *BackgroundAnalyzePendingCounter = pendingTables;
}

} // NKikimr::NStat
