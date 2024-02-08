#include "aggregator_impl.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/statistics/stat_service.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NStat {

TStatisticsAggregator::TStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info, bool forTests)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
    PropagateInterval = forTests ? TDuration::Seconds(5) : TDuration::Minutes(3);

    auto seed = std::random_device{}();
    RandomGenerator.seed(seed);
}

void TStatisticsAggregator::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TStatisticsAggregator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TStatisticsAggregator::OnActivateExecutor(const TActorContext& ctx) {
    SA_LOG_I("[" << TabletID() << "] OnActivateExecutor");

    Execute(CreateTxInitSchema(), ctx);
}

void TStatisticsAggregator::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx);
}

void TStatisticsAggregator::SubscribeForConfigChanges(const TActorContext& ctx) {
    ui32 configKind = (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem;
    ctx.Send(NConsole::MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({configKind}));
}

void TStatisticsAggregator::HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
    SA_LOG_I("[" << TabletID() << "] Subscribed for config changes");
}

void TStatisticsAggregator::HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto& config = record.GetConfig();
    if (config.HasFeatureFlags()) {
        const auto& featureFlags = config.GetFeatureFlags();
        EnableStatistics = featureFlags.GetEnableStatistics();
    }
    auto response = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(record);
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TStatisticsAggregator::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev) {
    auto pipeServerId = ev->Get()->ServerId;

    SA_LOG_D("[" << TabletID() << "] EvServerConnected"
        << ", pipe server id = " << pipeServerId);
}

void TStatisticsAggregator::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev) {
    auto pipeServerId = ev->Get()->ServerId;

    SA_LOG_D("[" << TabletID() << "] EvServerDisconnected"
        << ", pipe server id = " << pipeServerId);

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

    SA_LOG_D("[" << TabletID() << "] EvConnectNode"
        << ", pipe server id = " << pipeServerId
        << ", node id = " << nodeId
        << ", have schemeshards count = " << record.HaveSchemeShardsSize()
        << ", need schemeshards count = " << record.NeedSchemeShardsSize());

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

    if (!IsPropagateInFlight) {
        Schedule(PropagateInterval, new TEvPrivate::TEvPropagate());
        IsPropagateInFlight = true;
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

    SA_LOG_D("[" << TabletID() << "] EvRequestStats"
        << ", node id = " << nodeId
        << ", schemeshard count = " << record.NeedSchemeShardsSize());

    if (!EnableStatistics) {
        auto disabled = std::make_unique<TEvStatistics::TEvStatisticsIsDisabled>();
        Send(NStat::MakeStatServiceID(nodeId), disabled.release());
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

    SA_LOG_D("[" << TabletID() << "] EvConnectSchemeShard"
        << ", pipe server id = " << pipeServerId
        << ", schemeshard id = " << schemeShardId);
}

void TStatisticsAggregator::Handle(TEvPrivate::TEvFastPropagateCheck::TPtr&) {
    SA_LOG_D("[" << TabletID() << "] EvFastPropagateCheck");

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
    SA_LOG_D("[" << TabletID() << "] EvPropagate");

    if (EnableStatistics) {
        PropagateStatistics();
    }

    Schedule(PropagateInterval, new TEvPrivate::TEvPropagate());
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
        Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvFastPropagateCheck());
        FastCheckInFlight = true;
    }
}

void TStatisticsAggregator::SendStatisticsToNode(TNodeId nodeId, const std::vector<TSSId>& ssIds) {
    SA_LOG_D("[" << TabletID() << "] SendStatisticsToNode()"
        << ", node id = " << nodeId
        << ", schemeshard count = " << ssIds.size());

    std::vector<TNodeId> nodeIds;
    nodeIds.push_back(nodeId);

    PropagateStatisticsImpl(nodeIds, ssIds);
}

void TStatisticsAggregator::PropagateStatistics() {
    SA_LOG_D("[" << TabletID() << "] PropagateStatistics()"
        << ", node count = " << Nodes.size()
        << ", schemeshard count = " << RequestedSchemeShards.size());

    if (Nodes.empty() || RequestedSchemeShards.empty()) {
        return;
    }

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

    PropagateStatisticsImpl(nodeIds, ssIds);
}

void TStatisticsAggregator::PropagateFastStatistics() {
    SA_LOG_D("[" << TabletID() << "] PropagateFastStatistics()"
        << ", node count = " << FastNodes.size()
        << ", schemeshard count = " << FastSchemeShards.size());

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

    PropagateStatisticsImpl(nodeIds, ssIds);
}

void TStatisticsAggregator::PropagateStatisticsImpl(
    const std::vector<TNodeId>& nodeIds, const std::vector<TSSId>& ssIds)
{
    if (nodeIds.empty() || ssIds.empty()) {
        return;
    }

    TNodeId leadingNodeId = nodeIds[0];

    for (size_t index = 0; index < ssIds.size(); ) {
        auto propagate = std::make_unique<TEvStatistics::TEvPropagateStatistics>();
        auto* record = propagate->MutableRecord();
        record->MutableNodeIds()->Reserve(nodeIds.size() - 1);
        for (size_t i = 1; i < nodeIds.size(); ++i) {
            record->AddNodeIds(nodeIds[i]);
        }
        for (size_t size = 0; index < ssIds.size(); ++index) {
            auto ssId = ssIds[index];
            auto* entry = record->AddEntries();
            entry->SetSchemeShardId(ssId);
            auto itStats = BaseStats.find(ssId);
            if (itStats != BaseStats.end()) {
                entry->SetStats(itStats->second);
                size += itStats->second.size();
            } else {
                entry->SetStats(TString()); // stats are not sent from SA yet
            }
            if (size >= StatsSizeLimitBytes) {
                ++index;
                break;
            }
        }
        Send(NStat::MakeStatServiceID(leadingNodeId), propagate.release());
    }
}

void TStatisticsAggregator::PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value) {
    db.Table<Schema::SysParams>().Key(id).Update(
        NIceDb::TUpdate<Schema::SysParams::Value>(value));
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
        }
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

} // NKikimr::NStat
