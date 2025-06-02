#include "service.h"
#include "http_request.h"

#include <ydb/core/statistics/common.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/database/database.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/statistics.pb.h>
#include <ydb/core/protos/data_events.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/service/pages/templates.h>


#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

namespace NKikimr {
namespace NStat {

using TEvReadRowsRequest = NGRpcService::TGrpcRequestNoOperationCall<Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

struct TAggregationStatistics {
    using TColumnsStatistics = ::google::protobuf::RepeatedPtrField<::NKikimrStat::TColumnStatistics>;

    TAggregationStatistics(size_t nodesCount)
        : Nodes(nodesCount)
    {}

    struct TFailedTablet {
        using EErrorType = NKikimrStat::TEvAggregateStatisticsResponse::EErrorType;

        ui64 TabletId;
        ui32 NodeId;
        EErrorType Error;

        TFailedTablet(ui64 tabletId, ui32 nodeId, EErrorType error)
            : TabletId(tabletId)
            , NodeId(nodeId)
            , Error(error) {}
    };

    struct TTablets {
        ui32 NodeId;
        std::vector<ui64> Ids;
    };

    struct TNode {
        enum class EStatus: ui8 {
            None,
            Processing,
            Processed,
            Unavailable
        };

        ui64 LastHeartbeat{ 0 };
        std::vector<TTablets> Tablets;
        TActorId Actor;
        EStatus Status{ EStatus::None };
    };

    struct TLocalTablets {
        size_t NextTablet{ 0 };
        ui32 InFlight{ 0 };
        std::vector<ui64> Ids;
        std::unordered_map<ui64, TActorId> TabletsPipes;
    };

    struct ColumnStatistics {
        std::unique_ptr<TCountMinSketch> Statistics;
        ui32 ContainedInResponse{ 0 };
    };

    ui64 Round{ 0 };
    ui64 Cookie{ 0 };
    TPathId PathId;
    ui64 LastAckHeartbeat{ 0 };
    TActorId ParentNode;
    std::vector<TNode> Nodes;
    size_t PprocessedNodes{ 0 };

    std::unordered_map<ui32, ColumnStatistics> CountMinSketches;
    ui32 TotalStatisticsResponse{ 0 };

    std::vector<ui32> ColumnTags;
    TLocalTablets LocalTablets;
    std::vector<TFailedTablet> FailedTablets;

    bool IsCompleted() const noexcept {
        return PprocessedNodes == Nodes.size() && LocalTablets.InFlight == 0;
    }

    TNode* GetProcessingChildNode(ui32 nodeId) {
        for (size_t i = 0; i < Nodes.size(); ++i) {
            if (Nodes[i].Actor.NodeId() == nodeId) {
                return Nodes[i].Status == TAggregationStatistics::TNode::EStatus::Processing
                    ? &Nodes[i] : nullptr;
            }
        }
        SA_LOG_E("Child node with the specified id was not found");
        return nullptr;
    }
};

class TStatService : public TActorBootstrapped<TStatService> {
public:
    using TBase = TActorBootstrapped<TStatService>;

    static constexpr TStringBuf StatisticsTable = "/.metadata/_statistics";

    TStatService(const TStatServiceSettings& settings)
        : Settings(settings)
        , AggregationStatistics(settings.FanOutFactor)
    {}

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE;
    }

    struct TEvPrivate {
        enum EEv {
            EvRequestTimeout = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvDispatchKeepAlive,
            EvKeepAliveTimeout,
            EvKeepAliveAckTimeout,
            EvStatisticsRequestTimeout,

            EvEnd
        };

        struct TEvRequestTimeout : public NActors::TEventLocal<TEvRequestTimeout, EvRequestTimeout> {
            std::unordered_set<ui64> NeedSchemeShards;
            NActors::TActorId PipeClientId;
        };

        struct TEvDispatchKeepAlive: public NActors::TEventLocal<TEvDispatchKeepAlive, EvDispatchKeepAlive> {
            TEvDispatchKeepAlive(ui64 round): Round(round) {}

            ui64 Round;
        };

        struct TEvKeepAliveAckTimeout: public NActors::TEventLocal<TEvKeepAliveAckTimeout, EvKeepAliveAckTimeout> {
            TEvKeepAliveAckTimeout(ui64 round): Round(round) {}

            ui64 Round;
        };

        struct TEvKeepAliveTimeout: public NActors::TEventLocal<TEvKeepAliveTimeout, EvKeepAliveTimeout> {
            TEvKeepAliveTimeout(ui64 round, ui32 nodeId): Round(round), NodeId(nodeId) {}

            ui64 Round;
            ui32 NodeId;
        };

        struct TEvStatisticsRequestTimeout: public NActors::TEventLocal<TEvStatisticsRequestTimeout, EvStatisticsRequestTimeout> {
            TEvStatisticsRequestTimeout(ui64 round, ui64 tabletId): Round(round), TabletId(tabletId) {}

            ui64 Round;
            ui64 TabletId;
        };
    };

    void Bootstrap() {
        EnableStatistics = AppData()->FeatureFlags.GetEnableStatistics();
        EnableColumnStatistics = AppData()->FeatureFlags.GetEnableColumnStatistics();

        ui32 configKind = (ui32) NKikimrConsole::TConfigItem::FeatureFlagsItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({configKind}));

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "statservice", "Statistics service",
                false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        Become(&TStatService::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleConfig);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);

            hFunc(TEvStatistics::TEvGetStatistics, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStatistics::TEvPropagateStatistics, Handle);
            hFunc(TEvStatistics::TEvAggregateStatistics, Handle);
            IgnoreFunc(TEvStatistics::TEvPropagateStatisticsResponse);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvStatistics::TEvStatisticsIsDisabled, Handle);
            hFunc(TEvStatistics::TEvLoadStatisticsQueryResponse, Handle);
            hFunc(TEvPrivate::TEvRequestTimeout, Handle);

            hFunc(TEvStatistics::TEvAggregateKeepAliveAck, Handle);
            hFunc(TEvPrivate::TEvKeepAliveAckTimeout, Handle);
            hFunc(TEvStatistics::TEvAggregateKeepAlive, Handle);
            hFunc(TEvPrivate::TEvDispatchKeepAlive, Handle);
            hFunc(TEvPrivate::TEvKeepAliveTimeout, Handle);
            hFunc(TEvPrivate::TEvStatisticsRequestTimeout, Handle);
            hFunc(TEvStatistics::TEvStatisticsResponse, Handle);
            hFunc(TEvStatistics::TEvAggregateStatisticsResponse, Handle);

            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(NMon::TEvHttpInfoRes, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                SA_LOG_CRIT("NStat::TStatService: unexpected event# " << ev->GetTypeRewrite() << " " << ev->ToString());
        }
    }

private:
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        SA_LOG_I("Subscribed for config changes on node " << SelfId().NodeId());
    }

    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& config = record.GetConfig();
        if (config.HasFeatureFlags()) {
            const auto& featureFlags = config.GetFeatureFlags();
            EnableStatistics = featureFlags.GetEnableStatistics();
            EnableColumnStatistics = featureFlags.GetEnableColumnStatistics();
            if (!EnableStatistics) {
                ReplyAllFailed();
            }
        }
        auto response = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(record);
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    bool IsNotCurrentRound(ui64 round) {
        if (round != AggregationStatistics.Round) {
            SA_LOG_D("Event round " << round << " is different from the current " << AggregationStatistics.Round);
            return true;
        }
        return false;
    }

    void OnAggregateStatisticsFinished() {
        SendAggregateStatisticsResponse();
        ResetAggregationStatistics();
    }

    void SendRequestToNextTablet() {
        auto& localTablets = AggregationStatistics.LocalTablets;
        if (localTablets.NextTablet >= localTablets.Ids.size()) {
            return;
        }

        const auto tabletId = localTablets.Ids[localTablets.NextTablet];
        ++localTablets.NextTablet;
        ++localTablets.InFlight;

        auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
        policy.RetryLimitCount = 2;
        NTabletPipe::TClientConfig pipeConfig{policy};
        pipeConfig.ForceLocal = true;
        localTablets.TabletsPipes[tabletId] = Register(NTabletPipe::CreateClient(SelfId(), tabletId, pipeConfig));
    }

    void ResetAggregationStatistics() {
        const auto& tabletsPipes = AggregationStatistics.LocalTablets.TabletsPipes;
        for (auto it = tabletsPipes.begin(); it != tabletsPipes.end(); ++it) {
            NTabletPipe::CloseClient(SelfId(), it->second);
        }

        TAggregationStatistics aggregationStatistics(Settings.FanOutFactor);
        std::swap(AggregationStatistics, aggregationStatistics);
    }

    void AggregateStatistics(const TAggregationStatistics::TColumnsStatistics& columnsStatistics) {
        ++AggregationStatistics.TotalStatisticsResponse;

        for (const auto& column : columnsStatistics) {
            const auto tag = column.GetTag();

            for (auto& statistic : column.GetStatistics()) {
                if (statistic.GetType() == NKikimr::NStat::COUNT_MIN_SKETCH) {
                    auto data = statistic.GetData().Data();
                    auto sketch = reinterpret_cast<const TCountMinSketch*>(data);
                    auto& current = AggregationStatistics.CountMinSketches[tag];

                    if (current.Statistics == nullptr) {
                        current.Statistics.reset(TCountMinSketch::Create());
                    }

                    ++current.ContainedInResponse;
                    *current.Statistics += *sketch;
                }
            }
        }
    }

    void Handle(TEvStatistics::TEvStatisticsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto tabletId = record.GetShardTabletId();

        SA_LOG_D("Received TEvStatisticsResponse TabletId: " << tabletId);

        const auto round = ev->Cookie;
        if (IsNotCurrentRound(round)) {
            return;
        }

        auto tabletPipe = AggregationStatistics.LocalTablets.TabletsPipes.find(tabletId);
        if (tabletPipe != AggregationStatistics.LocalTablets.TabletsPipes.end()) {
            NTabletPipe::CloseClient(SelfId(), tabletPipe->second);
            AggregationStatistics.LocalTablets.TabletsPipes.erase(tabletPipe);
        }

        AggregateStatistics(record.GetColumns());
        --AggregationStatistics.LocalTablets.InFlight;

        SendRequestToNextTablet();

        if (AggregationStatistics.IsCompleted()) {
            OnAggregateStatisticsFinished();
        }
    }

    void Handle(TEvStatistics::TEvAggregateKeepAliveAck::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto round = record.GetRound();

        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvAggregateKeepAliveAck");
            return;
        }

        AggregationStatistics.LastAckHeartbeat = GetCycleCountFast();
    }

    void Handle(TEvPrivate::TEvKeepAliveAckTimeout::TPtr& ev) {
        const auto round = ev->Get()->Round;
        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvKeepAliveAckTimeout");
            return;
        }

        const auto maxDuration = DurationToCycles(Settings.AggregateKeepAliveAckTimeout);
        const auto deadline = AggregationStatistics.LastAckHeartbeat + maxDuration;
        const auto now = GetCycleCountFast();

        if (deadline >= now) {
            Schedule(Settings.AggregateKeepAliveAckTimeout, new TEvPrivate::TEvKeepAliveAckTimeout(round));
            return;
        }

        // the parent node is unavailable
        // invalidate the subtree with the root in the current node
        SA_LOG_I("Parent node " << AggregationStatistics.ParentNode.NodeId() << " is unavailable");


        ResetAggregationStatistics();
    }

    void Handle(TEvPrivate::TEvDispatchKeepAlive::TPtr& ev) {
        const auto round = ev->Get()->Round;
        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvDispatchKeepAlive");
            return;
        }

        auto keepAlive = std::make_unique<TEvStatistics::TEvAggregateKeepAlive>();
        keepAlive->Record.SetRound(round);
        Send(AggregationStatistics.ParentNode, keepAlive.release());
        Schedule(Settings.AggregateKeepAlivePeriod, new TEvPrivate::TEvDispatchKeepAlive(round));
    }

    void Handle(TEvPrivate::TEvKeepAliveTimeout::TPtr& ev) {
        const auto round = ev->Get()->Round;

        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvKeepAliveTimeout");
            return;
        }

        const auto nodeId = ev->Get()->NodeId;
        auto node = AggregationStatistics.GetProcessingChildNode(nodeId);

        if (node == nullptr) {
            SA_LOG_D("Skip TEvKeepAliveTimeout");
            return;
        }

        const auto maxDuration = DurationToCycles(Settings.AggregateKeepAliveTimeout);
        const auto deadline = node->LastHeartbeat + maxDuration;
        const auto now = GetCycleCountFast();

        if (deadline >= now) {
            Schedule(Settings.AggregateKeepAliveTimeout, new TEvPrivate::TEvKeepAliveTimeout(round, nodeId));
            return;
        }

        node->Status = TAggregationStatistics::TNode::EStatus::Unavailable;
        ++AggregationStatistics.PprocessedNodes;
        SA_LOG_I("Node " << nodeId << " is unavailable");

        if (AggregationStatistics.IsCompleted()) {
            OnAggregateStatisticsFinished();
        }
    }

    void Handle(TEvStatistics::TEvAggregateKeepAlive::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto round = record.GetRound();

        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvAggregateKeepAlive");
            return;
        }

        const auto nodeId = ev->Sender.NodeId();
        auto node = AggregationStatistics.GetProcessingChildNode(nodeId);

        if (node == nullptr) {
            SA_LOG_D( "Skip TEvAggregateKeepAlive");
            return;
        }

        auto response = std::make_unique<TEvStatistics::TEvAggregateKeepAliveAck>();
        response->Record.SetRound(round);
        Send(ev->Sender, response.release());

        node->LastHeartbeat = GetCycleCountFast();
    }

    void Handle(TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev) {
        SA_LOG_D("Received TEvAggregateStatisticsResponse SenderNodeId: " << ev->Sender.NodeId());

        const auto& record = ev->Get()->Record;
        const auto round = record.GetRound();

        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvAggregateStatisticsResponse");
            return;
        }

        const auto nodeId = ev->Sender.NodeId();
        auto node = AggregationStatistics.GetProcessingChildNode(nodeId);

        if (node == nullptr) {
            SA_LOG_D("Skip TEvAggregateStatisticsResponse");
            return;
        }

        node->Status = TAggregationStatistics::TNode::EStatus::Processed;
        ++AggregationStatistics.PprocessedNodes;

        AggregateStatistics(record.GetColumns());

        const auto size = AggregationStatistics.FailedTablets.size();
        AggregationStatistics.FailedTablets.reserve(size + record.GetFailedTablets().size());

        for (const auto& fail : record.GetFailedTablets()) {
            AggregationStatistics.FailedTablets.emplace_back(
                fail.GetTabletId(), fail.GetNodeId(), fail.GetError()
            );
        }

        if (AggregationStatistics.IsCompleted()) {
            OnAggregateStatisticsFinished();
        }
    }

    void AddUnavailableTablets(const TAggregationStatistics::TNode& node,
        NKikimrStat::TEvAggregateStatisticsResponse& response) {
        if (node.Status != TAggregationStatistics::TNode::EStatus::Unavailable) {
            return;
        }

        for (const auto& range : node.Tablets) {
            for (const auto& tabletId : range.Ids) {
                auto failedTablet = response.AddFailedTablets();
                failedTablet->SetNodeId(range.NodeId);
                failedTablet->SetTabletId(tabletId);
                failedTablet->SetError(NKikimrStat::TEvAggregateStatisticsResponse::TYPE_UNAVAILABLE_NODE);
            }
        }
    }

    void SendAggregateStatisticsResponse() {
        SA_LOG_D("Send aggregate statistics response to node: " << AggregationStatistics.ParentNode.NodeId());

        auto response = std::make_unique<TEvStatistics::TEvAggregateStatisticsResponse>();
        auto& record = response->Record;
        record.SetRound(AggregationStatistics.Round);

        const auto& countMinSketches = AggregationStatistics.CountMinSketches;

        for (auto it = countMinSketches.begin(); it != countMinSketches.end(); ++it) {
            if (it->second.ContainedInResponse != AggregationStatistics.TotalStatisticsResponse) {
                continue;
            }

            auto column = record.AddColumns();
            column->SetTag(it->first);

            auto data = it->second.Statistics->AsStringBuf();
            auto statistics = column->AddStatistics();
            statistics->SetType(NKikimr::NStat::COUNT_MIN_SKETCH);
            statistics->SetData(data.Data(), data.Size());
        }

        auto failedTablets = record.MutableFailedTablets();
        failedTablets->Reserve(AggregationStatistics.FailedTablets.size());

        for (const auto& fail : AggregationStatistics.FailedTablets) {
            auto failedTablet = failedTablets->Add();
            failedTablet->SetNodeId(fail.NodeId);
            failedTablet->SetTabletId(fail.TabletId);
            failedTablet->SetError(fail.Error);
        }

        for (auto& node : AggregationStatistics.Nodes) {
            AddUnavailableTablets(node, record);
        }

        Send(AggregationStatistics.ParentNode, response.release(), 0, AggregationStatistics.Cookie);
    }

    void SendRequestToNode(TAggregationStatistics::TNode& node, const NKikimrStat::TEvAggregateStatistics& record) {
        if (node.Tablets.empty()) {
            node.Status = TAggregationStatistics::TNode::EStatus::Processed;
            ++AggregationStatistics.PprocessedNodes;
            return;
        }

        auto request = std::make_unique<TEvStatistics::TEvAggregateStatistics>();
        request->Record.SetRound(AggregationStatistics.Round);
        request->Record.MutableNodes()->Reserve(node.Tablets.size());

        const auto& columnTags = record.GetColumnTags();
        if (!columnTags.empty()) {
            request->Record.MutableColumnTags()->Assign(columnTags.begin(), columnTags.end());
        }

        auto pathId = request->Record.MutablePathId();
        pathId->SetOwnerId(AggregationStatistics.PathId.OwnerId);
        pathId->SetLocalId(AggregationStatistics.PathId.LocalPathId);

        for (const auto& range : node.Tablets) {
            auto recordNode = request->Record.AddNodes();
            recordNode->SetNodeId(range.NodeId);

            auto tabletIds = recordNode->MutableTabletIds();
            tabletIds->Reserve(range.Ids.size());

            for (const auto& tabletId : range.Ids) {
                tabletIds->Add(tabletId);
            }
        }

        // sending the request to the first node of the range
        const auto nodeId = node.Tablets[0].NodeId;
        node.Actor = MakeStatServiceID(nodeId);
        node.Status = TAggregationStatistics::TNode::EStatus::Processing;

        Send(node.Actor, request.release());
        Schedule(Settings.AggregateKeepAliveTimeout,
                new TEvPrivate::TEvKeepAliveTimeout(AggregationStatistics.Round, nodeId));
    }

    void Handle(TEvStatistics::TEvAggregateStatistics::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto round = record.GetRound();

        SA_LOG_D("Received TEvAggregateStatistics from node: " << ev->Sender.NodeId()
            << ", Round: " << round << ", current Round: " << AggregationStatistics.Round);

        // reset previous state
        if (AggregationStatistics.Round != 0) {
            ResetAggregationStatistics();
        }

        AggregationStatistics.Round = round;
        AggregationStatistics.Cookie = ev->Cookie;
        AggregationStatistics.ParentNode = ev->Sender;

        // schedule keep alive with the parent node
        Schedule(Settings.AggregateKeepAlivePeriod, new TEvPrivate::TEvDispatchKeepAlive(round));

        const auto& pathId = record.GetPathId();
        AggregationStatistics.PathId.OwnerId = pathId.GetOwnerId();
        AggregationStatistics.PathId.LocalPathId = pathId.GetLocalId();

        for (const auto tag : record.GetColumnTags()) {
            AggregationStatistics.ColumnTags.emplace_back(tag);
        }

        const auto currentNodeId = ev->Recipient.NodeId();
        const auto& nodes = record.GetNodes();

        // divide the entire range of nodes into two parts,
        // forming the right and left child nodes
        size_t k = 0;
        for (const auto& node : nodes) {
            if (node.GetNodeId() == currentNodeId) {
                AggregationStatistics.LocalTablets.Ids.reserve(node.GetTabletIds().size());

                for (const auto& tabletId : node.GetTabletIds()) {
                    AggregationStatistics.LocalTablets.Ids.push_back(tabletId);
                }
                continue;
            }

            TAggregationStatistics::TTablets nodeTablets;
            nodeTablets.NodeId = node.GetNodeId();
            nodeTablets.Ids.reserve(node.GetTabletIds().size());
            for (const auto& tabletId : node.GetTabletIds()) {
                nodeTablets.Ids.push_back(tabletId);
            }

            AggregationStatistics.Nodes[k % Settings.FanOutFactor].Tablets.push_back(std::move(nodeTablets));
            ++k;
        }

        for (auto& node : AggregationStatistics.Nodes) {
            SendRequestToNode(node, record);
        }

        // to check the locality of the tablets,
        // send requests to receive the IDs of the nodes
        // where the tablets are located
        auto& localTablets = AggregationStatistics.LocalTablets;
        const auto count = std::min(Settings.MaxInFlightTabletRequests,
                                    localTablets.Ids.size());
        for (size_t i = 0; i < count; ++i) {
            SendRequestToNextTablet();
        }
    }

    void LoadStatistics(const TString& database, const TString& tablePath,
                        const TPathId& pathId, ui32 statType, ui32 columnTag, ui64 queryId) {
        SA_LOG_D("[TStatService::LoadStatistics] QueryId[ " << queryId
            << " ], PathId[ " << pathId << " ], " << " StatType[ " << statType
            << " ], ColumnTag[ " << columnTag << " ]");

        auto readRowsRequest = Ydb::Table::ReadRowsRequest();
        readRowsRequest.set_path(tablePath);

        NYdb::TValueBuilder keys_builder;
        keys_builder.BeginList()
            .AddListItem()
                .BeginStruct()
                    .AddMember("owner_id").Uint64(pathId.OwnerId)
                    .AddMember("local_path_id").Uint64(pathId.LocalPathId)
                    .AddMember("stat_type").Uint32(statType)
                    .AddMember("column_tag").Uint32(columnTag)
                .EndStruct()
            .EndList();
        auto keys = keys_builder.Build();
        auto protoKeys = readRowsRequest.mutable_keys();
        *protoKeys->mutable_type() = NYdb::TProtoAccessor::GetProto(keys.GetType());
        *protoKeys->mutable_value() = NYdb::TProtoAccessor::GetProto(keys);

        auto actorSystem = TlsActivationContext->ActorSystem();
        auto rpcFuture = NRpcService::DoLocalRpc<TEvReadRowsRequest>(
            std::move(readRowsRequest), database, Nothing(), TActivationContext::ActorSystem(), true
        );
        rpcFuture.Subscribe([replyTo = SelfId(), queryId, actorSystem](const NThreading::TFuture<Ydb::Table::ReadRowsResponse>& future) mutable {
            const auto& response = future.GetValueSync();
            auto query_response = std::make_unique<TEvStatistics::TEvLoadStatisticsQueryResponse>();

            if (response.status() == Ydb::StatusIds::SUCCESS) {
                NYdb::TResultSetParser parser(response.result_set());
                const auto rowsCount = parser.RowsCount();
                Y_ABORT_UNLESS(rowsCount < 2);

                if (rowsCount == 0) {
                    SA_LOG_E("[TStatService::ReadRowsResponse] QueryId[ "
                        << queryId << " ], RowsCount[ 0 ]");
                }

                query_response->Success = rowsCount > 0;

                while(parser.TryNextRow()) {
                    auto& col = parser.ColumnParser("data");
                    // may be not optional from versions before fix of bug https://github.com/ydb-platform/ydb/issues/15701
                    if (col.GetKind() == NYdb::TTypeParser::ETypeKind::Optional) {
                        if (auto opt = col.GetOptionalString()) {
                            query_response->Data = opt.GetRef();
                        } else {
                            query_response->Data.reset();
                        }
                    } else {
                        query_response->Data = col.GetString();
                    }
                 }
            } else {
                SA_LOG_E("[TStatService::ReadRowsResponse] QueryId[ "
                    << queryId << " ] " << NYql::IssuesFromMessageAsString(response.issues()));
                query_response->Success = false;
            }

            actorSystem->Send(replyTo, query_response.release(), 0, queryId);
        });
    }

    void QueryStatistics(const TString& database, const TString& tablePath, ui64 requestId) {
        SA_LOG_D("[TStatService::QueryStatistics] RequestId[ " << requestId
            << " ], Database[ " << database << " ], TablePath[ " << tablePath << " ]");

        auto it = InFlight.find(requestId);
        if (it == InFlight.end()) {
            SA_LOG_E("[TStatService::QueryStatistics] RequestId[ " << requestId << " ] Not found");
            ReplyFailed(requestId, true);
            return;
        }

        auto& request = it->second;
        request.StatResponses.reserve(request.StatRequests.size());
        ui32 reqIndex = 0;

        for (const auto& req : request.StatRequests) {
            auto& response = request.StatResponses.emplace_back();
            response.Req = req;
            if (!req.ColumnTag) {
                response.Success = false;
                ++reqIndex;
                continue;
            }
            ui64 queryId = NextLoadQueryCookie++;
            LoadQueriesInFlight[queryId] = std::make_pair(requestId, reqIndex);

            LoadStatistics(database, tablePath, req.PathId, request.StatType, *req.ColumnTag, queryId);

            ++request.ReplyCounter;
            ++reqIndex;
        }
    }

    void AddNavigateEntry(TNavigate::TResultSet& items, const TPathId& pathId, bool redirectRequired = false) {
        auto& entry = items.emplace_back();
        entry.TableId = TTableId(pathId.OwnerId, pathId.LocalPathId);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        entry.RedirectRequired = redirectRequired;
        entry.ShowPrivatePath = true;
    }

    void Handle(TEvStatistics::TEvGetStatistics::TPtr& ev) {
        ui64 requestId = NextRequestId++;
        auto& request = InFlight[requestId];
        request.ReplyToActorId = ev->Sender;
        request.EvCookie = ev->Cookie;
        request.StatType = ev->Get()->StatType;
        request.StatRequests.swap(ev->Get()->StatRequests);

        if (!EnableStatistics || IsStatisticsDisabledInSA) {
            ReplyFailed(requestId, true);
            return;
        }

        SA_LOG_D("[TStatService::TEvGetStatistics] RequestId[ " << requestId
            << " ], ReplyToActorId[ " << request.ReplyToActorId
            << "], StatType[ " << static_cast<ui32>(request.StatType)
            << " ], StatRequestsCount[ " << request.StatRequests.size() << " ]");

        auto navigate = std::make_unique<TNavigate>();
        navigate->Cookie = requestId;
        for (const auto& req : request.StatRequests) {
            AddNavigateEntry(navigate->ResultSet, req.PathId, true);
        }

        ui64 cookie = request.StatType == EStatType::COUNT_MIN_SKETCH ? requestId : 0;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()), 0, cookie);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());

        auto requestId = ev->Cookie == 0 ? navigate->Cookie : ev->Cookie;
        SA_LOG_D("[TStatService::TEvNavigateKeySetResult] RequestId[ " << requestId << " ]");

        // Search for the database to query to the statistics table.
        if (ev->Cookie != 0) {
            auto entry = std::find_if(navigate->ResultSet.begin(), navigate->ResultSet.end(), [](const TNavigate::TEntry& entry){
                return entry.Status == TNavigate::EStatus::Ok;
            });

            if (entry == navigate->ResultSet.end()) {
                SA_LOG_E("[TStatService::TEvNavigateKeySetResult] RequestId[ " << requestId << " ] Navigate failed");
                ReplyFailed(requestId, true);
                return;
            }

            if (navigate->Cookie == 0) {
                const auto database = JoinPath(entry->Path);
                const auto tablePath = CanonizePath(database + StatisticsTable);
                QueryStatistics(database, tablePath, requestId);
                return;
            }

            const auto domainInfo = entry->DomainInfo;
            const auto& pathId = domainInfo->IsServerless() ? domainInfo->ResourcesDomainKey : domainInfo->DomainKey;

            SA_LOG_D("[TStatService::TEvNavigateKeySetResult] RequestId[ " << requestId
                << " ] resolve DatabasePath[ " << pathId << " ]");
            auto navigateRequest = std::make_unique<TNavigate>();
            AddNavigateEntry(navigateRequest->ResultSet, pathId);

            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigateRequest.release()), 0, ev->Cookie);
            return;
        }

        // Identification StatisticsAggregator tablet's identifier in the case of serverless.
        if (requestId == ResolveSACookie) {
            Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
            auto& entry = navigate->ResultSet.back();

            if (entry.Status != TNavigate::EStatus::Ok) {
                StatisticsAggregatorId = 0;
            } else if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
                StatisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();
            }
            ResolveSAStage = StatisticsAggregatorId ? RSA_FINISHED : RSA_INITIAL;

            if (StatisticsAggregatorId) {
                ConnectToSA();
                SyncNode();
            } else {
                // In case of StatisticsAggregator tablet could not be found,
                // we need to cancel the current requests. No need to delete CountMinSketch requests.
                for (auto it = InFlight.begin(); it != InFlight.end();) {
                    if (EStatType::COUNT_MIN_SKETCH == it->second.StatType) {
                        ++it;
                        continue;
                    }
                    ReplyFailed(it->first, false);
                    it = InFlight.erase(it);
                }
            }
            return;
        }

        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        if (!EnableStatistics) {
            ReplyFailed(requestId, true);
            return;
        }

        auto entry = std::find_if(navigate->ResultSet.begin(), navigate->ResultSet.end(), [](const TNavigate::TEntry& entry){
            return entry.Status == TNavigate::EStatus::Ok;
        });

        if (entry == navigate->ResultSet.end()) {
            ReplyFailed(requestId, true);
            return;
        }

        const auto domainInfo = entry->DomainInfo;
        request.SchemeShardId = domainInfo->ExtractSchemeShard();

        if (Statistics.find(request.SchemeShardId) != Statistics.end()) {
            ReplySuccess(requestId, true);
            return;
        }

        auto isNewSS = (NeedSchemeShards.find(request.SchemeShardId) == NeedSchemeShards.end());
        if (isNewSS) {
            NeedSchemeShards.insert(request.SchemeShardId);
        }

        auto navigateDomainKey = [this, cookie = ev->Cookie] (const TPathId& domainKey) {
            auto navigateRequest = std::make_unique<TNavigate>();
            AddNavigateEntry(navigateRequest->ResultSet, domainKey);
            navigateRequest->Cookie = ResolveSACookie;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigateRequest.release()));
            ResolveSAStage = RSA_IN_FLIGHT;
        };

        ui64 aggregatorId = domainInfo->Params.GetStatisticsAggregator();

        switch (ResolveSAStage) {
        case RSA_INITIAL:
            if (!domainInfo->IsServerless()) {
                if (aggregatorId) {
                    StatisticsAggregatorId = aggregatorId;
                    ResolveSAStage = RSA_FINISHED;
                } else {
                    navigateDomainKey(domainInfo->DomainKey);
                    return;
                }
            } else {
                navigateDomainKey(domainInfo->ResourcesDomainKey);
                return;
            }
            break;
        case RSA_IN_FLIGHT:
            return;
        default:
            break;
        }

        if (!StatisticsAggregatorId) {
            ReplyFailed(requestId, true);
            return;
        }

        if (!SAPipeClientId) {
            ConnectToSA();
            SyncNode();
        } else if (isNewSS) {
            auto requestStats = std::make_unique<TEvStatistics::TEvRequestStats>();
            requestStats->Record.SetNodeId(SelfId().NodeId());
            requestStats->Record.SetUrgent(false);
            requestStats->Record.AddNeedSchemeShards(request.SchemeShardId);
            NTabletPipe::SendData(SelfId(), SAPipeClientId, requestStats.release());

            auto timeout = std::make_unique<TEvPrivate::TEvRequestTimeout>();
            timeout->NeedSchemeShards.insert(request.SchemeShardId);
            timeout->PipeClientId = SAPipeClientId;
            Schedule(RequestTimeout, timeout.release());
        }
    }

    void Handle(TEvStatistics::TEvPropagateStatistics::TPtr& ev) {
        SA_LOG_D("EvPropagateStatistics, node id = " << SelfId().NodeId());

        Send(ev->Sender, new TEvStatistics::TEvPropagateStatisticsResponse);

        IsStatisticsDisabledInSA = false;

        auto* record = ev->Get()->MutableRecord();
        for (const auto& entry : record->GetEntries()) {
            ui64 schemeShardId = entry.GetSchemeShardId();
            NeedSchemeShards.erase(schemeShardId);
            auto& statisticsState = Statistics[schemeShardId];

            if (entry.GetStats().empty()) {
                continue; // stats are not ready in SA, wait for next cycle
            }

            statisticsState.Map.clear();

            NKikimrStat::TSchemeShardStats statRecord;
            Y_PROTOBUF_SUPPRESS_NODISCARD statRecord.ParseFromString(entry.GetStats());

            for (const auto& pathEntry : statRecord.GetEntries()) {
                TPathId pathId(pathEntry.GetPathId().GetOwnerId(), pathEntry.GetPathId().GetLocalId());
                auto& mapEntry = statisticsState.Map[pathId];
                mapEntry.RowCount = pathEntry.GetRowCount();
                mapEntry.BytesSize = pathEntry.GetBytesSize();
            }
        }

        for (auto itReq = InFlight.begin(); itReq != InFlight.end(); ) {
            auto requestId = itReq->first;
            auto requestState = itReq->second;
            if (requestState.SchemeShardId == 0) {
                ++itReq;
                continue;
            }
            if (Statistics.find(requestState.SchemeShardId) != Statistics.end()) {
                ReplySuccess(requestId, false);
                itReq = InFlight.erase(itReq);
            } else {
                ++itReq;
            }
        }

        if (record->NodeIdsSize() == 0) {
            return;
        }

        std::vector<ui32> nodeIds;
        nodeIds.reserve(record->NodeIdsSize());
        for (const auto nodeId : record->GetNodeIds()) {
            nodeIds.push_back(nodeId);
        }

        size_t step = 0;
        if (nodeIds.size() <= StatFanOut + 1) {
            step = 0;
        } else if (nodeIds.size() <= StatFanOut * (StatFanOut + 1)) {
            step = StatFanOut;
        } else {
            step = nodeIds.size() / StatFanOut;
        }

        auto serialized = std::make_unique<TEvStatistics::TEvPropagateStatistics>();
        serialized->MutableRecord()->MutableEntries()->Swap(record->MutableEntries());
        TString preSerializedStats;
        Y_PROTOBUF_SUPPRESS_NODISCARD serialized->GetRecord().SerializeToString(&preSerializedStats);

        for (size_t i = 0; i < nodeIds.size(); ) {
            ui32 leadingNodeId = nodeIds[i++];

            auto propagate = std::make_unique<TEvStatistics::TEvPropagateStatistics>();
            propagate->MutableRecord()->MutableNodeIds()->Reserve(step);
            for (size_t j = 0; i < nodeIds.size() && j < step; ++i, ++j) {
                propagate->MutableRecord()->AddNodeIds(nodeIds[i]);
            }
            propagate->PreSerializedData = preSerializedStats;
            Send(MakeStatServiceID(leadingNodeId), propagate.release());
        }
    }

    void Handle(TEvPrivate::TEvStatisticsRequestTimeout::TPtr& ev) {
        const auto round = ev->Get()->Round;
        if (IsNotCurrentRound(round)) {
            SA_LOG_D("Skip TEvStatisticsRequestTimeout");
            return;
        }

        const auto tabletId = ev->Get()->TabletId;
        auto tabletPipe = AggregationStatistics.LocalTablets.TabletsPipes.find(tabletId);
        if (tabletPipe == AggregationStatistics.LocalTablets.TabletsPipes.end()) {
            SA_LOG_D("Tablet " << tabletId << " has already been processed");
            return;
        }

        SA_LOG_E("No result was received from the tablet " << tabletId);

        auto clientId = tabletPipe->second;
        OnTabletError(tabletId);
        NTabletPipe::CloseClient(SelfId(), clientId);
    }

    void SendStatisticsRequest(const TActorId& clientId, ui64 tabletId) {
        auto request = std::make_unique<TEvStatistics::TEvStatisticsRequest>();
        auto& record = request->Record;
        record.MutableTypes()->Add(NKikimrStat::TYPE_COUNT_MIN_SKETCH);

        auto* path = record.MutableTable()->MutablePathId();
        path->SetOwnerId(AggregationStatistics.PathId.OwnerId);
        path->SetLocalId(AggregationStatistics.PathId.LocalPathId);

        auto* columnTags = record.MutableTable()->MutableColumnTags();
        for (const auto& tag : AggregationStatistics.ColumnTags) {
            columnTags->Add(tag);
        }

        const auto round = AggregationStatistics.Round;
        NTabletPipe::SendData(SelfId(), clientId, request.release(), round);
        Schedule(Settings.StatisticsRequestTimeout, new TEvPrivate::TEvStatisticsRequestTimeout(round, tabletId));

        SA_LOG_D("TEvStatisticsRequest send"
            << ", client id = " << clientId
            << ", path = " << *path);
    }

    void OnTabletError(ui64 tabletId) {
        SA_LOG_D("Tablet " << tabletId << " is not local.");

        const auto error = NKikimrStat::TEvAggregateStatisticsResponse::TYPE_NON_LOCAL_TABLET;
        AggregationStatistics.FailedTablets.emplace_back(tabletId, 0, error);

        AggregationStatistics.LocalTablets.TabletsPipes.erase(tabletId);
        --AggregationStatistics.LocalTablets.InFlight;
        SendRequestToNextTablet();

        if (AggregationStatistics.IsCompleted()) {
            OnAggregateStatisticsFinished();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;
        const auto& tabletId = ev->Get()->TabletId;

        SA_LOG_D("EvClientConnected"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << clientId
            << ", server id = " << ev->Get()->ServerId
            << ", tablet id = " << tabletId
            << ", status = " << ev->Get()->Status);

        if (clientId == SAPipeClientId) {
            IsStatisticsDisabledInSA = false;
            if (ev->Get()->Status != NKikimrProto::OK) {
                SAPipeClientId = TActorId();
                ConnectToSA();
                SyncNode();
            }
            return;
        }

        const auto& tabletsPipes = AggregationStatistics.LocalTablets.TabletsPipes;
        auto tabletPipe = tabletsPipes.find(tabletId);

        if (tabletPipe != tabletsPipes.end() && clientId == tabletPipe->second) {
            if (ev->Get()->Status == NKikimrProto::OK) {
                SendStatisticsRequest(clientId, tabletId);
            } else {
                OnTabletError(tabletId);
            }
            return;
        }

        SA_LOG_D("Skip EvClientConnected");
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;
        const auto& tabletId = ev->Get()->TabletId;

        SA_LOG_D("EvClientDestroyed"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << clientId
            << ", server id = " << ev->Get()->ServerId
            << ", tablet id = " << tabletId);

        if (clientId == SAPipeClientId) {
            IsStatisticsDisabledInSA = false;
            SAPipeClientId = TActorId();
            ConnectToSA();
            SyncNode();
            return;
        }

        const auto& tabletsPipes = AggregationStatistics.LocalTablets.TabletsPipes;
        auto tabletPipe = tabletsPipes.find(tabletId);

        if (tabletPipe != tabletsPipes.end() && clientId == tabletPipe->second) {
            OnTabletError(tabletId);
            return;
        }

        SA_LOG_D("Skip EvClientDestroyed");
    }

    void Handle(TEvStatistics::TEvStatisticsIsDisabled::TPtr&) {
        IsStatisticsDisabledInSA = true;
        ReplyAllFailed();
    }

    void Handle(TEvStatistics::TEvLoadStatisticsQueryResponse::TPtr& ev) {
        auto itLoadQuery = LoadQueriesInFlight.find(ev->Cookie);
        Y_ABORT_UNLESS(itLoadQuery != LoadQueriesInFlight.end());
        auto [requestId, requestIndex] = itLoadQuery->second;

        SA_LOG_D("TEvLoadStatisticsQueryResponse, request id = " << requestId);

        auto itRequest = InFlight.find(requestId);
        if (InFlight.end() == itRequest) {
            SA_LOG_E("TEvLoadStatisticsQueryResponse, request id = " << requestId
                << ". Request not found in InFlight");
            return;
        }

        auto& request = itRequest->second;

        auto& response = request.StatResponses[requestIndex];
        Y_ABORT_UNLESS(request.StatType == EStatType::COUNT_MIN_SKETCH);

        const auto msg = ev->Get();

        if (msg->Success && msg->Data) {
            response.Success = true;
            response.CountMinSketch.CountMin.reset(TCountMinSketch::FromString(msg->Data->data(), msg->Data->size()));
        } else {
            response.Success = false;
        }

        if (--request.ReplyCounter == 0) {
            auto result = std::make_unique<TEvStatistics::TEvGetStatisticsResult>();
            result->Success = true;
            result->StatResponses.swap(request.StatResponses);

            Send(request.ReplyToActorId, result.release(), 0, request.EvCookie);

            InFlight.erase(requestId);
        }
    }

    void Handle(TEvPrivate::TEvRequestTimeout::TPtr& ev) {
        SA_LOG_D("EvRequestTimeout"
            << ", pipe client id = " << ev->Get()->PipeClientId
            << ", schemeshard count = " << ev->Get()->NeedSchemeShards.size());

        if (SAPipeClientId != ev->Get()->PipeClientId) {
            return;
        }
        auto requestStats = std::make_unique<TEvStatistics::TEvRequestStats>();
        bool hasNeedSchemeShards = false;
        for (auto& ssId : ev->Get()->NeedSchemeShards) {
            if (NeedSchemeShards.find(ssId) != NeedSchemeShards.end()) {
                requestStats->Record.AddNeedSchemeShards(ssId);
                hasNeedSchemeShards = true;
            }
        }
        if (!hasNeedSchemeShards) {
            return;
        }
        requestStats->Record.SetNodeId(SelfId().NodeId());
        requestStats->Record.SetUrgent(true);

        NTabletPipe::SendData(SelfId(), SAPipeClientId, requestStats.release());
    }

    void ConnectToSA() {
        if (SAPipeClientId || !StatisticsAggregatorId) {
            return;
        }
        auto policy = NTabletPipe::TClientRetryPolicy::WithRetries();
        NTabletPipe::TClientConfig pipeConfig{policy};
        SAPipeClientId = Register(NTabletPipe::CreateClient(SelfId(), StatisticsAggregatorId, pipeConfig));

        SA_LOG_D("ConnectToSA(), pipe client id = " << SAPipeClientId);
    }

    void SyncNode() {
        if (!SAPipeClientId || !StatisticsAggregatorId) {
            return;
        }
        auto connect = std::make_unique<TEvStatistics::TEvConnectNode>();
        auto& record = connect->Record;

        auto timeout = std::make_unique<TEvPrivate::TEvRequestTimeout>();
        timeout->PipeClientId = SAPipeClientId;

        record.SetNodeId(SelfId().NodeId());
        for (const auto& [ssId, ssState] : Statistics) {
            auto* entry = record.AddHaveSchemeShards();
            entry->SetSchemeShardId(ssId);
            entry->SetTimestamp(ssState.Timestamp);
        }
        for (const auto& ssId : NeedSchemeShards) {
            record.AddNeedSchemeShards(ssId);
            timeout->NeedSchemeShards.insert(ssId);
        }
        NTabletPipe::SendData(SelfId(), SAPipeClientId, connect.release());

        if (!NeedSchemeShards.empty()) {
            Schedule(RequestTimeout, timeout.release());
        }

        SA_LOG_D("SyncNode(), pipe client id = " << SAPipeClientId);
    }

    void ReplySuccess(ui64 requestId, bool eraseRequest) {
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        SA_LOG_D("ReplySuccess(), request id = " << requestId
            << ", ReplyToActorId = " << request.ReplyToActorId
            << ", StatRequests.size() = " << request.StatRequests.size());

        auto itStatistics = Statistics.find(request.SchemeShardId);
        if (itStatistics == Statistics.end()) {
            return;
        }
        auto& statisticsMap = itStatistics->second.Map;

        auto result = std::make_unique<TEvStatistics::TEvGetStatisticsResult>();
        result->Success = true;

        for (auto& req : request.StatRequests) {
            TResponse rsp;
            rsp.Success = true;
            rsp.Req = req;

            TStatSimple stat;
            auto itStat = statisticsMap.find(req.PathId);
            if (itStat != statisticsMap.end()) {
                stat.RowCount = itStat->second.RowCount;
                stat.BytesSize = itStat->second.BytesSize;
            } else {
                stat.RowCount = 0;
                stat.BytesSize = 0;
            }
            rsp.Simple = stat;

            result->StatResponses.push_back(rsp);
        }

        Send(request.ReplyToActorId, result.release(), 0, request.EvCookie);

        if (eraseRequest) {
            InFlight.erase(requestId);
        }
    }

    void ReplyFailed(ui64 requestId, bool eraseRequest) {
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        SA_LOG_D("ReplyFailed(), request id = " << requestId);

        auto result = std::make_unique<TEvStatistics::TEvGetStatisticsResult>();
        result->Success = false;

        for (auto& req : request.StatRequests) {
            TResponse rsp;
            rsp.Success = false;
            rsp.Req = req;

            TStatSimple stat;
            stat.RowCount = 0;
            stat.BytesSize = 0;
            rsp.Simple = stat;

            result->StatResponses.push_back(rsp);
        }

        Send(request.ReplyToActorId, result.release(), 0, request.EvCookie);

        if (eraseRequest) {
            InFlight.erase(requestId);
        }
    }

    void ReplyAllFailed() {
        for (const auto& [requestId, _] : InFlight) {
            ReplyFailed(requestId, false);
        }
        InFlight.clear();
    }

    void PassAway() {
        if (SAPipeClientId) {
            NTabletPipe::CloseClient(SelfId(), SAPipeClientId);
        }
        TBase::PassAway();
    }

    void PrintStatServiceState(TStringStream& str) {
        HTML(str) {
            PRE() {
            str << "---- StatisticsService ----" << Endl << Endl;
            str << "StatisticsAggregatorId: " << StatisticsAggregatorId << Endl;
            str << "SAPipeClientId: " << SAPipeClientId << Endl;

            str << "InFlight: " << InFlight.size();
            {
                ui32 simple{ 0 };
                ui32 countMin{ 0 };
                for (auto it = InFlight.begin(); it != InFlight.end(); ++it) {
                    if (it->second.StatType == EStatType::SIMPLE) {
                        ++simple;
                    } else if (it->second.StatType == EStatType::COUNT_MIN_SKETCH) {
                        ++countMin;
                    }
                }
                str << "[SIMPLE: " << simple << ", COUNT_MIN_SKETCH: " << countMin << "]" << Endl;
            }
            str << "NextRequestId: " << NextRequestId << Endl;

            str << "LoadQueriesInFlight: " << LoadQueriesInFlight.size() << Endl;
            str << "NextLoadQueryCookie: " << NextLoadQueryCookie << Endl;

            str << "NeedSchemeShards: " << NeedSchemeShards.size() << Endl;
            str << "Statistics: " << Statistics.size() << Endl;

            str << "ResolveSAStage: ";
            if (ResolveSAStage == RSA_INITIAL) {
                str << "RSA_INITIAL";
            } else if (ResolveSAStage == RSA_IN_FLIGHT) {
                str << "RSA_IN_FLIGHT";
            }
            else {
                str << "RSA_FINISHED";
            }
            str << Endl;

            str << "AggregateKeepAlivePeriod: " << Settings.AggregateKeepAlivePeriod << Endl;
            str << "AggregateKeepAliveTimeout: " << Settings.AggregateKeepAliveTimeout << Endl;
            str << "AggregateKeepAliveAckTimeout: " << Settings.AggregateKeepAliveAckTimeout << Endl;
            str << "StatisticsRequestTimeout: " << Settings.StatisticsRequestTimeout << Endl;
            str << "MaxInFlightTabletRequests: " << Settings.MaxInFlightTabletRequests << Endl;
            str << "FanOutFactor: " << Settings.FanOutFactor << Endl;

            str << "---- AggregationStatistics ----" << Endl;
            str << "Round: " << AggregationStatistics.Round << Endl;
            str << "Cookie: " << AggregationStatistics.Cookie << Endl;
            str << "PathId: " << AggregationStatistics.PathId.ToString() << Endl;
            str << "LastAckHeartbeat: " << AggregationStatistics.LastAckHeartbeat << Endl;
            str << "ParentNode: " << AggregationStatistics.ParentNode << Endl;
            str << "PprocessedNodes: " << AggregationStatistics.PprocessedNodes << Endl;
            str << "TotalStatisticsResponse: " << AggregationStatistics.TotalStatisticsResponse << Endl;
            str << "Nodes: " << AggregationStatistics.Nodes.size() << Endl;
            str << "CountMinSketches: " << AggregationStatistics.CountMinSketches.size() << Endl;
            }
        }
    }

    void AddPanel(IOutputStream& str, const TString& title, const std::function<void(IOutputStream&)>& bodyRender) {
        HTML(str) {
            DIV_CLASS("panel panel-default") {
                DIV_CLASS("panel-heading") {
                    H4_CLASS("panel-title") {
                        str << title;
                    }
                }
                DIV_CLASS("panel-body") {
                    bodyRender(str);
                }
            }
        }
    }

    void PrintForm(TStringStream& str) {
        HTML(str) {
            AddPanel(str, "Analyze table", [](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "path") {
                                str << "Path";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='path' name='path' class='form-control' placeholder='/full/path'>";
                            }
                            str << "<input type=\"hidden\" name=\"action\" value=\"analyze\"/>";
                            DIV_CLASS("col-sm-2") {
                                str << "<input class=\"btn btn-default\" type=\"submit\" value=\"Analyze\"/>";
                            }
                        }
                    }
                }
            });
            AddPanel(str, "Get operation status", [](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "path") {
                                str << "Path";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='path' name='path' class='form-control' placeholder='/full/path'>";
                            }
                        }
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "operation") {
                                str << "OperationId";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='operation' name='operation' class='form-control' placeholder='operation id'>";
                            }
                            str << "<input type=\"hidden\" name=\"action\" value=\"status\"/>";
                            DIV_CLASS("col-sm-2") {
                                str << "<input class=\"btn btn-default\" type=\"submit\" value=\"GetStatus\"/>";
                            }
                        }
                    }
                }
            });
            AddPanel(str, "Probe count-min sketch", [](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "database") {
                                str << "Database";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='database' name='database' class='form-control' placeholder='/full/database/path'>";
                            }
                        }
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "path") {
                                str << "Path";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='path' name='path' class='form-control' placeholder='/full/path'>";
                            }
                        }
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "column") {
                                str << "ColumnName";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='column' name='column' class='form-control' placeholder='column name'>";
                            }
                        }
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "cell") {
                                str << "Value";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='cell' name='cell' class='form-control' placeholder='value'>";
                            }

                            str << "<input type=\"hidden\" name=\"action\" value=\"probe\"/>";
                            DIV_CLASS("col-sm-2") {
                                str << "<input class=\"btn btn-default\" type=\"submit\" value=\"Probe\"/>";
                            }
                        }
                    }
                }
            });

            PrintStatServiceState(str);
        }
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr& ev) {
        if (HttpRequestActorId != ev->Sender) {
            return;
        }

        HttpRequestActorId = TActorId();

        const auto* msg = ev->CastAsLocal<NMon::TEvHttpInfoRes>();
        if (msg != nullptr) {
            ReplyToMonitoring(msg->Answer);
        }
    }

    void ReplyToMonitoring(const TString& description) {
        TStringStream str;

        if (!description.empty()) {
            HTML(str) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12 alert alert-info") {
                        str << description;
                    }
                }
            }
        }

        PrintForm(str);
        Send(MonitoringActorId, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        if (!EnableColumnStatistics) {
            Send(ev->Sender, new NMon::TEvHttpInfoRes("Column statistics is disabled"));
            return;
        }

        HttpRequestActorId = TActorId();
        MonitoringActorId = ev->Sender;

        const auto& request = ev->Get()->Request;
        const auto& params = request.GetParams();

        auto getRequestParam = [&params](const TString& name){
            auto it = params.find(name);
            return it != params.end() ? it->second : TString();
        };

        const auto action = getRequestParam("action");
        if (action.empty()) {
            ReplyToMonitoring("");
            return;
        }

        const auto path = getRequestParam("path");
        if (path.empty()) {
            ReplyToMonitoring("'Path' parameter is required");
            return;
        }

        if (action == "analyze") {
            HttpRequestActorId = Register(new THttpRequest(THttpRequest::ERequestType::ANALYZE, {
                { THttpRequest::EParamType::PATH, path }
            }, SelfId()));
        } else if (action == "status") {
            const auto operationId = getRequestParam("operation");
            if (operationId.empty()) {
                ReplyToMonitoring("'OperationId' parameter is required");
                return;
            }

            HttpRequestActorId = Register(new THttpRequest(THttpRequest::ERequestType::STATUS, {
                { THttpRequest::EParamType::PATH, path },
                { THttpRequest::EParamType::OPERATION_ID, operationId }
            }, SelfId()));
        } else if (action == "probe") {
            const auto column = getRequestParam("column");
            if (column.empty()) {
                ReplyToMonitoring("'ColumnName' parameter is required");
                return;
            }

            const auto cell = getRequestParam("cell");
            if (cell.empty()) {
                ReplyToMonitoring("'Value' parameter is required");
                return;
            }

            const auto database = getRequestParam("database");
            if (database.empty()) {
                ReplyToMonitoring("'Database' parameter is required");
                return;
            }

            HttpRequestActorId = Register(new THttpRequest(THttpRequest::ERequestType::COUNT_MIN_SKETCH_PROBE, {
                { THttpRequest::EParamType::DATABASE, database },
                { THttpRequest::EParamType::PATH, path },
                { THttpRequest::EParamType::COLUMN_NAME, column },
                { THttpRequest::EParamType::CELL_VALUE, cell }
            }, SelfId()));
        } else {
            ReplyToMonitoring("Wrong 'action' parameter value");
        }
    }

private:
    TStatServiceSettings Settings;
    TAggregationStatistics AggregationStatistics;

    bool EnableStatistics = false;
    bool EnableColumnStatistics = false;
    bool IsStatisticsDisabledInSA = false;

    static constexpr size_t StatFanOut = 10;

    struct TRequestState {
        NActors::TActorId ReplyToActorId;
        ui64 EvCookie = 0;
        ui64 SchemeShardId = 0;
        EStatType StatType = EStatType::SIMPLE;
        std::vector<TRequest> StatRequests;
        std::vector<TResponse> StatResponses;
        size_t ReplyCounter = 0;
    };
    std::unordered_map<ui64, TRequestState> InFlight; // request id -> state
    ui64 NextRequestId = 1;

    std::unordered_map<ui64, std::pair<ui64, ui32>> LoadQueriesInFlight; // load cookie -> req id, req index
    ui64 NextLoadQueryCookie = 1;

    std::unordered_set<ui64> NeedSchemeShards;

    struct TStatEntry {
        ui64 RowCount = 0;
        ui64 BytesSize = 0;
    };
    typedef std::unordered_map<TPathId, TStatEntry> TStatisticsMap;
    struct TStatisticsState {
        TStatisticsMap Map;
        ui64 Timestamp = 0;
    };
    std::unordered_map<ui64, TStatisticsState> Statistics; // ss id -> stats

    ui64 StatisticsAggregatorId = 0;
    TActorId SAPipeClientId;

    static const ui64 ResolveSACookie = std::numeric_limits<ui64>::max();
    enum EResolveSAStage {
        RSA_INITIAL,
        RSA_IN_FLIGHT,
        RSA_FINISHED
    };
    EResolveSAStage ResolveSAStage = RSA_INITIAL;

    static constexpr TDuration RequestTimeout = TDuration::MilliSeconds(100);

    TActorId HttpRequestActorId;
    TActorId MonitoringActorId;
};

THolder<IActor> CreateStatService(const TStatServiceSettings& settings) {
    return MakeHolder<TStatService>(settings);
}


} // NStat
} // NKikimr
