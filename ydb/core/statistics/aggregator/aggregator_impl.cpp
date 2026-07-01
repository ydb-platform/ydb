#include "aggregator_impl.h"

#include <ydb/core/statistics/aggregator/analyze_actor.h>
#include <ydb/core/statistics/database/database.h>
#include <ydb/core/statistics/service/service.h>

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <algorithm>
#include <limits>

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
    StatisticsConfig = appData->StatisticsConfig;

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    Execute(CreateTxInitSchema(), ctx);
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

void TStatisticsAggregator::HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto& config = record.GetConfig();
    if (config.HasFeatureFlags()) {
        const auto& featureFlags = config.GetFeatureFlags();
        EnableStatistics = featureFlags.GetEnableStatistics();

        bool enableColumnStatisticsOld = EnableColumnStatistics;
        EnableColumnStatistics = featureFlags.GetEnableColumnStatistics();
        if (!enableColumnStatisticsOld && EnableColumnStatistics) {
            InitializeStatisticsTable();
        }
    }

    StatisticsConfig = config.GetStatisticsConfig();

    auto response = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(record);
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TStatisticsAggregator::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev) {
    auto pipeServerId = ev->Get()->ServerId;

    YDB_LOG_DEBUG("EvServerConnected pipe server",
        {"tabletId", TabletID()},
        {"id", pipeServerId});
}

void TStatisticsAggregator::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev) {
    auto pipeServerId = ev->Get()->ServerId;

    YDB_LOG_DEBUG("EvServerDisconnected pipe server",
        {"tabletId", TabletID()},
        {"id", pipeServerId});

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

    YDB_LOG_DEBUG("EvConnectNode pipe server node have schemeshards need schemeshards",
        {"tabletId", TabletID()},
        {"id", pipeServerId},
        {"nodeId", nodeId},
        {"count", record.HaveSchemeShardsSize()},
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

    YDB_LOG_DEBUG("EvRequestStats node schemeshard",
        {"tabletId", TabletID()},
        {"id", nodeId},
        {"count", record.NeedSchemeShardsSize()},
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

    YDB_LOG_DEBUG("EvConnectSchemeShard pipe server schemeshard",
        {"tabletId", TabletID()},
        {"id", pipeServerId},
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
    YDB_LOG_DEBUG("SendStatisticsToNode() node schemeshard",
        {"tabletId", TabletID()},
        {"id", nodeId},
        {"count", ssIds.size()});

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

    YDB_LOG_DEBUG("PropagateStatistics() node schemeshard",
        {"tabletId", TabletID()},
        {"count", Nodes.size()},
        {"requestedSchemeShardsCount", RequestedSchemeShards.size()});

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
    YDB_LOG_DEBUG("PropagateFastStatistics() node schemeshard",
        {"tabletId", TabletID()},
        {"count", FastNodes.size()},
        {"fastSchemeShardsCount", FastSchemeShards.size()});

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

void TStatisticsAggregator::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
    if (!TraversalPathId) {
        return;
    }
    auto tabletId = ev->Get()->TabletId;
    if (TraversalIsColumnTable) {
        if (tabletId == HiveId) {
            YDB_LOG_ERROR("TEvDeliveryProblem with",
                {"tabletId", TabletID()},
                {"hiveId", tabletId});
            Schedule(HiveRetryInterval, new TEvPrivate::TEvRequestDistribution);
        } else {
            for (TForceTraversalOperation& operation : ForceTraversals) {
                for (TForceTraversalTable& operationTable : operation.Tables) {
                    for (TAnalyzedShard& shard : operationTable.AnalyzedShards) {
                        if (shard.ShardTabletId == tabletId) {
                            YDB_LOG_ERROR("TEvDeliveryProblem with",
                                {"tabletId", TabletID()},
                                {"columnShard", tabletId});
                            shard.Status = TAnalyzedShard::EStatus::DeliveryProblem;
                            return;
                        }
                    }
                }
            }
            YDB_LOG_CRIT("TEvDeliveryProblem with unexpected tablet",
                {"tabletId", TabletID()},
                {"tabletId", tabletId});
        }
    } else {
        YDB_LOG_ERROR("TEvDeliveryProblem with",
            {"tabletId", TabletID()},
            {"dataShard", tabletId});
        if (DatashardRanges.empty()) {
            return;
        }
        auto& range = DatashardRanges.front();
        if (tabletId != range.DataShardId) {
            return;
        }
        Resolve();
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvStatTableCreationResponse::TPtr&) {
    IsStatisticsTableCreated = true;
    if (PendingSaveStatistics) {
        PendingSaveStatistics = false;
        SaveStatisticsToTable();
    }
    if (PendingDeleteStatistics) {
        PendingDeleteStatistics = false;
        DeleteStatisticsFromTable();
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeStatus::TPtr& ev) {
    const auto& inRecord = ev->Get()->Record;
    const TString operationId = inRecord.GetOperationId();

    auto response = std::make_unique<TEvStatistics::TEvAnalyzeStatusResponse>();
    auto& outRecord = response->Record;
    outRecord.SetOperationId(operationId);

    if (ForceTraversalOperationId == operationId) {
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

    YDB_LOG_DEBUG("Send TEvStatistics::TEvAnalyzeStatusResponse. Status",
        {"tabletId", TabletID()},
        {"status", outRecord.GetStatus()});

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TStatisticsAggregator::PassAway() {
    if (AnalyzeActorId) {
        Send(AnalyzeActorId, new TEvents::TEvPoison());
    }

    IActor::PassAway();
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvResolve::TPtr&) {
    Resolve();
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvRequestDistribution::TPtr&) {
    ++HiveRequestRound;

    auto reqDistribution = std::make_unique<TEvHive::TEvRequestTabletDistribution>();
    reqDistribution->Record.MutableTabletIds()->Add(TabletsForReqDistribution.begin(), TabletsForReqDistribution.end());
    Send(MakePipePerNodeCacheID(false),
        new TEvPipeCache::TEvForward(reqDistribution.release(), HiveId, true));
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAggregateKeepAlive::TPtr& ev) {
    const auto round = ev->Get()->Record.GetRound();
    if (round == GlobalTraversalRound && AggregationRequestBeginTime) {
        TInstant now = AppData(TlsActivationContext->AsActorContext())->TimeProvider->Now();
        TDuration time = now - AggregationRequestBeginTime;
        TabletCounters->Simple()[COUNTER_AGGREGATION_TIME].Set(time.MicroSeconds());
    }

    auto ack = std::make_unique<TEvStatistics::TEvAggregateKeepAliveAck>();
    ack->Record.SetRound(round);
    Send(ev->Sender, ack.release());
    Schedule(KeepAliveTimeout, new TEvPrivate::TEvAckTimeout(++KeepAliveSeqNo));
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr& ev) {
    SaveQueryActorId = {};

    if (ev->Get()->Success) {
        if (!StatisticsToSave.empty()) {
            // There are some more pending statistics items to save.
            SaveStatisticsToTable();
        } else if (!AnalyzeActorId) {
            // If we saved everything and the analyze actor has already finished,
            // finish the traversal.
            DispatchFinishTraversalTx(NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
        }
    } else {
        NYql::TIssue error(TStringBuilder() << "Could not save statistics for "
            "table id: " << TraversalPathId.LocalPathId);
        DispatchFinishTraversalTx(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, {error});
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvDeleteStatisticsQueryResponse::TPtr&) {
    NYql::TIssue error(TStringBuilder() << "Could not find table id: "
        << TraversalPathId.LocalPathId << ", deleted its statistics");
    DispatchFinishTraversalTx(NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, {error});
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeActorResult::TPtr& ev) {
    if (ev->Sender != AnalyzeActorId) {
        return;
    }

    if (ev->Get()->Final) {
        AnalyzeActorId = {};
    }

    using EStatus = TEvStatistics::TEvAnalyzeActorResult::EStatus;
    switch (ev->Get()->Status) {
    case EStatus::Success:
        std::move(
            ev->Get()->Statistics.begin(), ev->Get()->Statistics.end(),
            std::back_inserter(StatisticsToSave));
        SaveStatisticsToTable();
        return;
    case EStatus::TableNotFound:
        DeleteStatisticsFromTable();
        return;
    case EStatus::InternalError:
        DispatchFinishTraversalTx(
            NKikimrStat::TEvAnalyzeResponse::STATUS_ERROR, std::move(ev->Get()->Issues));
        return;
    }
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeCancel::TPtr& ev) {
    const auto& operationId = ev->Get()->Record.GetOperationId();
    if (operationId != ForceTraversalOperationId) {
        YDB_LOG_NOTICE("Got unexpected TEvAnalyzeCancel with ignoring",
            {"operationId", operationId},
            {"expected", ForceTraversalOperationId});
        return;
    }

    YDB_LOG_DEBUG("Got TEvAnalyzeCancel,",
        {"operationId", operationId});
    DispatchFinishTraversalTx(NKikimrStat::TEvAnalyzeResponse::STATUS_CANCELLED);
}

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeActorProgress::TPtr& ev) {
    const auto& msg = *ev->Get();
    auto* op = ForceTraversalOperation(msg.OperationId);
    if (!op || IsTerminalAnalyzeState(op->State)) {
        return;
    }
    auto* table = ForceTraversalTable(msg.OperationId, msg.PathId);
    if (!table) {
        return;
    }
    table->ShardsTotal = msg.ShardsTotal;
    table->ShardsDone  = std::max(table->ShardsDone, msg.ShardsDone);
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

void TStatisticsAggregator::Navigate() {
    Y_ABORT_UNLESS(NavigatePathId);

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    TNavigate::TEntry entry;
    entry.TableId = NavigatePathId;
    entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
    entry.Operation = TNavigate::OpTable;

    auto request = std::make_unique<TNavigate>();
    request->DatabaseName = NavigateDatabase;
    request->ResultSet.emplace_back(entry);

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
}

void TStatisticsAggregator::Resolve() {
    Y_ABORT_UNLESS(NavigatePathId);

    ++ResolveRound;

    TVector<TCell> plusInf;
    TTableRange range(TraversalStartKey.GetCells(), true, plusInf, true, false);
    auto keyDesc = MakeHolder<TKeyDesc>(
        NavigatePathId, range, TKeyDesc::ERowOperation::Read, KeyColumnTypes, Columns);

    auto request = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
    request->DatabaseName = NavigateDatabase;
    request->ResultSet.emplace_back(std::move(keyDesc));

    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.release()));
}

void TStatisticsAggregator::ScanNextDatashardRange() {
    // Datashard traversal is temporary disabled
    Y_FAIL();

    if (DatashardRanges.empty()) {
        for (auto& [tag, sketch] : CountMinSketches) {
            TString strSketch(sketch->AsStringBuf());
            StatisticsToSave.emplace_back(tag, EStatType::COUNT_MIN_SKETCH, std::move(strSketch));
        }
        SaveStatisticsToTable();
        return;
    }

    auto& range = DatashardRanges.front();
    auto request = std::make_unique<NStat::TEvStatistics::TEvStatisticsRequest>();
    auto& record = request->Record;
    auto* path = record.MutableTable()->MutablePathId();
    path->SetOwnerId(TraversalPathId.OwnerId);
    path->SetLocalId(TraversalPathId.LocalPathId);
    record.SetStartKey(TraversalStartKey.GetBuffer());

    Send(MakePipePerNodeCacheID(false),
        new TEvPipeCache::TEvForward(request.release(), range.DataShardId, true),
        IEventHandle::FlagTrackDelivery);
}

void TStatisticsAggregator::SaveStatisticsToTable() {
    constexpr ui64 MAX_BATCH_SIZE = 30_MB;

    if (!IsStatisticsTableCreated) {
        PendingSaveStatistics = true;
        return;
    }
    if (SaveQueryActorId) {
        // Next query will be dispatched when the current SaveQuery actor finishes.
        return;
    }

    PendingSaveStatistics = false;

    size_t dataSize = 0;
    std::vector<TStatisticsItem> items;
    while (!StatisticsToSave.empty()) {
        auto& item = StatisticsToSave.front();
        if (!items.empty() && dataSize + item.Data.size() > MAX_BATCH_SIZE) {
            break;
        }
        dataSize += item.Data.size();
        items.push_back(std::move(item));
        StatisticsToSave.pop_front();
    };

    if (items.empty()) {
        Send(SelfId(), new TEvStatistics::TEvSaveStatisticsQueryResponse(
            Ydb::StatusIds::SUCCESS, {}, TraversalPathId));
        return;
    }
    size_t itemsSize = items.size();

    SaveQueryActorId = Register(
        CreateSaveStatisticsQuery(SelfId(), Database, TraversalPathId, std::move(items)));
    YDB_LOG_DEBUG("Dispatched SaveStatisticsQuery actor items data items",
        {"tabletId", TabletID()},
        {"id", SaveQueryActorId},
        {"size", itemsSize},
        {"humanReadableDataSize", HumanReadableSize(dataSize, ESizeFormat::SF_BYTES)},
        {"left", StatisticsToSave.size()});
}

void TStatisticsAggregator::DeleteStatisticsFromTable() {
    if (!IsStatisticsTableCreated) {
        PendingDeleteStatistics = true;
        return;
    }

    PendingDeleteStatistics = false;

    Register(CreateDeleteStatisticsQuery(SelfId(), Database, TraversalPathId));
}

void TStatisticsAggregator::ScheduleNextAnalyze(NIceDb::TNiceDb& db, const TActorContext& ctx) {
    if (ForceTraversals.empty()) {
        YDB_LOG_TRACE("ScheduleNextAnalyze. Empty ForceTraversals",
            {"tabletId", TabletID()});
        return;
    }
    YDB_LOG_DEBUG("ScheduleNextAnalyze",
        {"tabletId", TabletID()});
    Y_ABORT_UNLESS(!TraversalPathId);
    Y_ABORT_UNLESS(!AnalyzeActorId);

    for (TForceTraversalOperation& operation : ForceTraversals) {
        // Skip terminal operations (history entries) — they have no pending work.
        if (IsTerminalAnalyzeState(operation.State)) {
            continue;
        }

        for (TForceTraversalTable& operationTable : operation.Tables) {
            if (operationTable.Status == TForceTraversalTable::EStatus::None
                || (operationTable.Status == TForceTraversalTable::EStatus::AnalyzeStarted
                    && operation.RequestingActorReattached)) {
                std::optional<bool> isKnown = IsKnownTable(operationTable.PathId);
                if (!isKnown.has_value()) {
                    YDB_LOG_DEBUG("ScheduleNextAnalyze. Don't start analyze for table as there is still no info from its SchemeShard",
                        {"tabletId", TabletID()},
                        {"pathId", operationTable.PathId});
                    continue;
                }

                ForceTraversalOperationId = operation.OperationId;
                PersistSysParam(db, Schema::SysParam_ForceTraversalOperationId, ForceTraversalOperationId);
                TraversalDatabase = operation.DatabaseName;
                TraversalPathId = operationTable.PathId;

                if (!*isKnown) {
                    YDB_LOG_DEBUG("ScheduleNextAnalyze. table was deleted, deleting its statistics",
                        {"tabletId", TabletID()},
                        {"pathId", operationTable.PathId});
                    DeleteStatisticsFromTable();
                    return;
                }

                TraversalStartTime = TInstant::Now();
                LastTraversalWasForce = true;

                UpdateForceTraversalTableStatus(
                    TForceTraversalTable::EStatus::AnalyzeStarted, operation.OperationId, operationTable, db);

                // operation.Types field is not used, TAnalyzeActor will determine suitable
                // statistic types itself.
                auto analyzeActorConfig = TAnalyzeActor::TConfig{
                    .MaxTotalScanActorsInFlight = StatisticsConfig.GetAnalyzeMaxTotalScanActorsInFlight(),
                    .MaxPerNodeScanActorsInFlight = StatisticsConfig.GetAnalyzeMaxPerNodeScanActorsInFlight(),
                };
                AnalyzeActorId = ctx.Register(new TAnalyzeActor(
                    SelfId(), operation.OperationId, operation.DatabaseName, operationTable.PathId,
                    operationTable.ColumnTags, analyzeActorConfig));
                YDB_LOG_DEBUG("ScheduleNextAnalyze. started analyzing",
                    {"tabletId", TabletID()},
                    {"operationId", operation.OperationId},
                    {"table", operationTable.PathId},
                    {"analyzeActorId", AnalyzeActorId});

                return;
            }
        }

        YDB_LOG_DEBUG("ScheduleNextAnalyze. All the force traversal tables sent the requests",
            {"tabletId", TabletID()},
            {"operationId", operation.OperationId});
        continue;
    }

    YDB_LOG_DEBUG("ScheduleNextAnalyze. All the force traversal operations sent the requests",
        {"tabletId", TabletID()});
}

void TStatisticsAggregator::ScheduleNextBackgroundTraversal(NIceDb::TNiceDb& db) {
    YDB_LOG_DEBUG("ScheduleNextBackgroundTraversal",
        {"tabletId", TabletID()});
    Y_ABORT_UNLESS(!TraversalPathId);

    TString databaseName;
    TPathId pathId;

    if (!ScheduleTraversalsByTime.Empty()){
        auto* oldestTable = ScheduleTraversalsByTime.Top();
        if (TInstant::Now() < oldestTable->LastUpdateTime + ScheduleTraversalPeriod) {
            YDB_LOG_TRACE("Background traversal is skipped. The oldest table update time is too fresh",
                {"tabletId", TabletID()},
                {"pathId", oldestTable->PathId},
                {"lastUpdateTime", oldestTable->LastUpdateTime});
            return;
        }

        databaseName = ""; // it's intentional, because ScheduleTraversalsByTime is filled by SchemeShards
        pathId = oldestTable->PathId;
    }

    if (!pathId) {
        YDB_LOG_TRACE("No traversal request to send",
            {"tabletId", TabletID()});
        return;
    }

    TraversalDatabase = databaseName;
    TraversalPathId = pathId;
    TraversalStartTime = TInstant::Now();
    LastTraversalWasForce = false;

    std::optional<bool> isColumnTable = IsColumnTable(pathId);
    if (!isColumnTable){
        DeleteStatisticsFromTable();
        return;
    }

    // Datashard traversal is temporary disabled
    if (!*isColumnTable) {
        YDB_LOG_DEBUG("ScheduleNextBackgroundTraversal. Skip traversal for datashard table",
            {"tabletId", TabletID()},
            {"pathId", pathId});
        DeleteStatisticsFromTable();
        return;
    }

    TraversalIsColumnTable = *isColumnTable;

    YDB_LOG_DEBUG("Start background traversal navigate for path",
        {"tabletId", TabletID()},
        {"pathId", pathId});

    StartTraversal(db);
}

void TStatisticsAggregator::StartTraversal(NIceDb::TNiceDb& db) {
    PersistTraversal(db);

    TraversalStartKey = TSerializedCellVec();
    PersistStartKey(db);

    NavigateDatabase = TraversalDatabase;
    NavigatePathId = TraversalPathId;
    Navigate();
}

void TStatisticsAggregator::FinishTraversal(
    NIceDb::TNiceDb& db,
    std::optional<Ydb::Table::AnalyzeState::State> forceTerminalState,
    NYql::TIssues issues)
{
    auto pathId = TraversalPathId;

    auto pathIt = ScheduleTraversals.find(pathId);
    if (pathIt != ScheduleTraversals.end()) {
        auto& traversalTable = pathIt->second;
        traversalTable.LastUpdateTime = TraversalStartTime;
        db.Table<Schema::ScheduleTraversals>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
            NIceDb::TUpdate<Schema::ScheduleTraversals::LastUpdateTime>(TraversalStartTime.MicroSeconds()));

        if (ScheduleTraversalsByTime.Has(&traversalTable)) {
            ScheduleTraversalsByTime.Update(&traversalTable);
        }
    }

    auto forceTraversalOperation = CurrentForceTraversalOperation();
    if (forceTraversalOperation) {
        if (forceTerminalState.has_value()) {
            MarkForceTraversalOperationFinished(ForceTraversalOperationId,
                *forceTerminalState, TActivationContext::Now(), db, std::move(issues));
        } else {
            auto operationTable = CurrentForceTraversalTable();

            UpdateForceTraversalTableStatus(
                TForceTraversalTable::EStatus::TraversalFinished,
                forceTraversalOperation->OperationId, *operationTable, db);

            bool tablesRemained = std::any_of(forceTraversalOperation->Tables.begin(), forceTraversalOperation->Tables.end(),
            [](const TForceTraversalTable& elem) { return elem.Status != TForceTraversalTable::EStatus::TraversalFinished;});
            if (!tablesRemained) {
                MarkForceTraversalOperationFinished(ForceTraversalOperationId,
                    Ydb::Table::AnalyzeState::STATE_DONE, TActivationContext::Now(), db);
            }
        }
    }

    ResetTraversalState(db);
}

TString TStatisticsAggregator::LastTraversalWasForceString() const {
    return LastTraversalWasForce ? "force" : "background";
}

TStatisticsAggregator::TForceTraversalOperation* TStatisticsAggregator::CurrentForceTraversalOperation() {
    return ForceTraversalOperation(ForceTraversalOperationId);
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

std::optional<bool> TStatisticsAggregator::IsColumnTable(const TPathId& pathId) const {
    auto itPath = ScheduleTraversals.find(pathId);
    if (itPath != ScheduleTraversals.end()) {
        bool ret = itPath->second.IsColumnTable;
        YDB_LOG_DEBUG("IsColumnTable. Path is table",
            {"tabletId", TabletID()},
            {"pathId", pathId},
            {"tableType", (ret ? "column" : "datashard")});
        return ret;
    } else {
        YDB_LOG_ERROR("IsColumnTable. traversal path is not known to schemeshard",
            {"tabletId", TabletID()},
            {"pathId", pathId});
        return {};
    }
}

void TStatisticsAggregator::DeleteForceTraversalOperation(const TString& operationId, NIceDb::TNiceDb& db) {
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
    } else if (ForceTraversalOperationId == op.OperationId) {
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

TStatisticsAggregator::TForceTraversalTable* TStatisticsAggregator::CurrentForceTraversalTable() {
    return ForceTraversalTable(ForceTraversalOperationId, TraversalPathId);
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

void TStatisticsAggregator::PersistTraversal(NIceDb::TNiceDb& db) {
    PersistSysParam(db, Schema::SysParam_TraversalTableDatabase, ToString(TraversalDatabase));
    PersistSysParam(db, Schema::SysParam_TraversalTableOwnerId, ToString(TraversalPathId.OwnerId));
    PersistSysParam(db, Schema::SysParam_TraversalTableLocalPathId, ToString(TraversalPathId.LocalPathId));
    PersistSysParam(db, Schema::SysParam_TraversalStartTime, ToString(TraversalStartTime.MicroSeconds()));
    PersistSysParam(db, Schema::SysParam_TraversalIsColumnTable, ToString(TraversalIsColumnTable));
    PersistSysParam(db, Schema::SysParam_ForceTraversalOperationId, ForceTraversalOperationId);
}

void TStatisticsAggregator::PersistStartKey(NIceDb::TNiceDb& db) {
    PersistSysParam(db, Schema::SysParam_TraversalStartKey, TraversalStartKey.GetBuffer());
}

void TStatisticsAggregator::PersistGlobalTraversalRound(NIceDb::TNiceDb& db) {
    PersistSysParam(db, Schema::SysParam_GlobalTraversalRound, ToString(GlobalTraversalRound));
}

void TStatisticsAggregator::ResetTraversalState(NIceDb::TNiceDb& db) {
    ForceTraversalOperationId.clear();
    TraversalDatabase = "";
    TraversalPathId = {};
    TraversalStartTime = TInstant::MicroSeconds(0);
    if (AnalyzeActorId) {
        Send(AnalyzeActorId, new TEvents::TEvPoison());
        AnalyzeActorId = {};
    }
    SaveQueryActorId = {};
    PersistTraversal(db);

    TraversalStartKey = TSerializedCellVec();
    PersistStartKey(db);

    StatisticsToSave.clear();

    for (auto& [tag, _] : CountMinSketches) {
        db.Table<Schema::ColumnStatistics>().Key(tag).Delete();
    }
    CountMinSketches.clear();

    DatashardRanges.clear();

    KeyColumnTypes.clear();
    Columns.clear();
    ColumnNames.clear();

    TabletsForReqDistribution.clear();

    ResolveRound = 0;
    HiveRequestRound = 0;
    TraversalRound = 0;
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

            str << "Columns: " << Columns.size() << Endl;
            str << "DatashardRanges: " << DatashardRanges.size() << Endl;
            str << "CountMinSketches: " << CountMinSketches.size() << Endl << Endl;

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

            str << Endl;
            str << "NavigatePathId: " << NavigatePathId << Endl;

            str << Endl;
            str << "ForceTraversalOperationId: " << ForceTraversalOperationId.Quote() << Endl;
            if (ForceTraversalOperationId) {
                auto forceTraversal = CurrentForceTraversalOperation();
                str << "  CreatedAt: " << forceTraversal->CreatedAt << Endl;
                str << ", ReplyToActorId: " << forceTraversal->ReplyToActorId << Endl;
                str << ", RequestingActorReattached: " << forceTraversal->RequestingActorReattached << Endl;
                str << ", Types: " << forceTraversal->Types << Endl;
                str << ", Tables size: " << forceTraversal->Tables.size() << Endl;
                str << ", Tables: " << Endl;

                for (size_t i = 0; i < forceTraversal->Tables.size(); ++i) {
                    const TForceTraversalTable& table = forceTraversal->Tables[i];
                    str << "    Table[" << i << "] PathId: " << table.PathId << Endl;
                    str << "        Status: " << table.GetStatusString() << Endl;
                    str << "        AnalyzedShards size: " << table.AnalyzedShards.size() << Endl;
                    str << "        ColumnTags: " << JoinVectorIntoString(table.ColumnTags, ",") << Endl;
                }
            }
            str << "AnalyzeActorId: " << AnalyzeActorId << Endl;

            str << Endl;
            str << "TraversalStartTime: " << TraversalStartTime.ToStringUpToSeconds() << Endl;
            str << "TraversalDatabase: " << TraversalDatabase << Endl;
            str << "TraversalPathId: " << TraversalPathId << Endl;
            str << "TraversalIsColumnTable: " << TraversalIsColumnTable << Endl;
            str << "TraversalStartKey: " << TraversalStartKey.GetBuffer() << Endl;

            str << Endl;
            str << "GlobalTraversalRound: " << GlobalTraversalRound << Endl;
            str << "TraversalRound: " << TraversalRound << Endl;
            str << "HiveRequestRound: " << HiveRequestRound << Endl;
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

 void TStatisticsAggregator::ReportBaseStatisticsCounters() {
    ui64 totalRowCount = 0;
    ui64 totalBytesSize = 0;
    for (const auto& [_, serializedStats] : BaseStatistics) {
        if (!serializedStats.Committed) {
            continue;
        }
        NKikimrStat::TSchemeShardStats stats;
        Y_PROTOBUF_SUPPRESS_NODISCARD stats.ParseFromString(*serializedStats.Committed);
        for (const auto& entry: stats.GetEntries()) {
            totalRowCount += entry.GetRowCount();
            totalBytesSize += entry.GetBytesSize();
        }
    }
    TabletCounters->Simple()[COUNTER_BASE_STATISTICS_TOTAL_ROW_COUNT].Set(totalRowCount);
    TabletCounters->Simple()[COUNTER_BASE_STATISTICS_TOTAL_BYTES_SIZE].Set(totalBytesSize);
 }

} // NKikimr::NStat
