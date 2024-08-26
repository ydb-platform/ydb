#include "service.h"
#include "http_request.h"

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

namespace NKikimr {
namespace NStat {

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
        LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Child node with the specified id was not found");
        return nullptr;
    }
};

class TStatService : public TActorBootstrapped<TStatService> {
public:
    using TBase = TActorBootstrapped<TStatService>;

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
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleConfig)
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig)
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
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                    "NStat::TStatService: unexpected event# " << ev->GetTypeRewrite() << " " << ev->ToString());
        }
    }

private:
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Subscribed for config changes on node " << SelfId().NodeId());
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
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Event round " << round << " is different from the current " << AggregationStatistics.Round);
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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Received TEvStatisticsResponse TabletId: " << tabletId);

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
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvAggregateKeepAliveAck");
            return;
        }

        AggregationStatistics.LastAckHeartbeat = GetCycleCountFast();
    }

    void Handle(TEvPrivate::TEvKeepAliveAckTimeout::TPtr& ev) {
        const auto round = ev->Get()->Round;
        if (IsNotCurrentRound(round)) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvKeepAliveAckTimeout");
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
        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Parent node " << AggregationStatistics.ParentNode.NodeId() << " is unavailable");


        ResetAggregationStatistics();
    }

    void Handle(TEvPrivate::TEvDispatchKeepAlive::TPtr& ev) {
        const auto round = ev->Get()->Round;
        if (IsNotCurrentRound(round)) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvDispatchKeepAlive");
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
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvKeepAliveTimeout");
            return;
        }

        const auto nodeId = ev->Get()->NodeId;
        auto node = AggregationStatistics.GetProcessingChildNode(nodeId);

        if (node == nullptr) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvKeepAliveTimeout");
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
        LOG_INFO_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Node " << nodeId << " is unavailable");

        if (AggregationStatistics.IsCompleted()) {
            OnAggregateStatisticsFinished();
        }
    }

    void Handle(TEvStatistics::TEvAggregateKeepAlive::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto round = record.GetRound();

        if (IsNotCurrentRound(round)) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvAggregateKeepAlive");
            return;
        }

        const auto nodeId = ev->Sender.NodeId();
        auto node = AggregationStatistics.GetProcessingChildNode(nodeId);

        if (node == nullptr) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvAggregateKeepAlive");
            return;
        }

        auto response = std::make_unique<TEvStatistics::TEvAggregateKeepAliveAck>();
        response->Record.SetRound(round);
        Send(ev->Sender, response.release());

        node->LastHeartbeat = GetCycleCountFast();
    }

    void Handle(TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Received TEvAggregateStatisticsResponse SenderNodeId: " << ev->Sender.NodeId());

        const auto& record = ev->Get()->Record;
        const auto round = record.GetRound();

        if (IsNotCurrentRound(round)) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvAggregateStatisticsResponse");
            return;
        }

        const auto nodeId = ev->Sender.NodeId();
        auto node = AggregationStatistics.GetProcessingChildNode(nodeId);

        if (node == nullptr) {
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvAggregateStatisticsResponse");
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
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Send aggregate statistics response to node: " << AggregationStatistics.ParentNode.NodeId());

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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Received TEvAggregateStatistics from node: " << ev->Sender.NodeId()
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

    void Handle(TEvStatistics::TEvGetStatistics::TPtr& ev) {
        ui64 requestId = NextRequestId++;

        auto& request = InFlight[requestId];
        request.ReplyToActorId = ev->Sender;
        request.EvCookie = ev->Cookie;
        request.StatType = ev->Get()->StatType;
        request.StatRequests.swap(ev->Get()->StatRequests);

        if (!EnableStatistics) {
            ReplyFailed(requestId, true);
            return;
        }

        if (request.StatType == EStatType::COUNT_MIN_SKETCH) {
            request.StatResponses.reserve(request.StatRequests.size());
            ui32 reqIndex = 0;
            for (const auto& req : request.StatRequests) {
                auto& response = request.StatResponses.emplace_back();
                response.Req = req;
                ui64 loadCookie = NextLoadQueryCookie++;
                LoadQueriesInFlight[loadCookie] = std::make_pair(requestId, reqIndex);
                Register(CreateLoadStatisticsQuery(req.PathId, request.StatType,
                    *req.ColumnTag, loadCookie));
                ++request.ReplyCounter;
                ++reqIndex;
            }
            return;
        }

        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = std::make_unique<TNavigate>();
        for (const auto& req : request.StatRequests) {
            auto& entry = navigate->ResultSet.emplace_back();
            entry.TableId = TTableId(req.PathId.OwnerId, req.PathId.LocalPathId);
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        }
        navigate->Cookie = requestId;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());

        auto cookie = navigate->Cookie;

        if (cookie == ResolveSACookie) {
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
                ReplyAllFailed();
            }
            return;
        }

        ui64 requestId = cookie;
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        if (!EnableStatistics) {
            ReplyFailed(requestId, true);
            return;
        }

        std::unordered_set<ui64> ssIds;
        bool isServerless = false;
        ui64 aggregatorId = 0;
        TPathId domainKey, resourcesDomainKey;
        for (const auto& entry : navigate->ResultSet) {
            if (entry.Status != TNavigate::EStatus::Ok) {
                continue;
            }
            auto& domainInfo = entry.DomainInfo;
            ssIds.insert(domainInfo->ExtractSchemeShard());
            aggregatorId = domainInfo->Params.GetStatisticsAggregator();
            isServerless = domainInfo->IsServerless();
            domainKey = domainInfo->DomainKey;
            resourcesDomainKey = domainInfo->ResourcesDomainKey;
        }
        if (ssIds.size() != 1) {
            ReplyFailed(requestId, true);
            return;
        }
        request.SchemeShardId = *ssIds.begin();

        if (Statistics.find(request.SchemeShardId) != Statistics.end()) {
            ReplySuccess(requestId, true);
            return;
        }

        bool isNewSS = (NeedSchemeShards.find(request.SchemeShardId) == NeedSchemeShards.end());
        if (isNewSS) {
            NeedSchemeShards.insert(request.SchemeShardId);
        }

        auto navigateDomainKey = [this] (TPathId domainKey) {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            auto navigate = std::make_unique<TNavigate>();
            auto& entry = navigate->ResultSet.emplace_back();
            entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.RedirectRequired = false;
            navigate->Cookie = ResolveSACookie;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
            ResolveSAStage = RSA_IN_FLIGHT;
        };

        switch (ResolveSAStage) {
        case RSA_INITIAL:
            if (!isServerless) {
                if (aggregatorId) {
                    StatisticsAggregatorId = aggregatorId;
                    ResolveSAStage = RSA_FINISHED;
                } else {
                    navigateDomainKey(domainKey);
                    return;
                }
            } else {
                navigateDomainKey(resourcesDomainKey);
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
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "EvPropagateStatistics, node id = " << SelfId().NodeId());

        Send(ev->Sender, new TEvStatistics::TEvPropagateStatisticsResponse);

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
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "Skip TEvStatisticsRequestTimeout");
            return;
        }

        const auto tabletId = ev->Get()->TabletId;
        auto tabletPipe = AggregationStatistics.LocalTablets.TabletsPipes.find(tabletId);
        if (tabletPipe == AggregationStatistics.LocalTablets.TabletsPipes.end()) {
            return;
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                "No result was received from the tablet " << tabletId);

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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "TEvStatisticsRequest send"
            << ", client id = " << clientId
            << ", path = " << *path);
    }

    void OnTabletError(ui64 tabletId) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Tablet " << tabletId << " is not local.");

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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "EvClientConnected"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << clientId
            << ", server id = " << ev->Get()->ServerId
            << ", tablet id = " << tabletId
            << ", status = " << ev->Get()->Status);

        if (clientId == SAPipeClientId) {
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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Skip EvClientConnected");
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;
        const auto& tabletId = ev->Get()->TabletId;

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "EvClientDestroyed"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << clientId
            << ", server id = " << ev->Get()->ServerId
            << ", tablet id = " << tabletId);

        if (clientId == SAPipeClientId) {
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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "Skip EvClientDestroyed");
    }

    void Handle(TEvStatistics::TEvStatisticsIsDisabled::TPtr&) {
        ReplyAllFailed();
    }

    void Handle(TEvStatistics::TEvLoadStatisticsQueryResponse::TPtr& ev) {
        ui64 cookie = ev->Get()->Cookie;

        auto itLoadQuery = LoadQueriesInFlight.find(cookie);
        Y_ABORT_UNLESS(itLoadQuery != LoadQueriesInFlight.end());
        auto [requestId, requestIndex] = itLoadQuery->second;

        auto itRequest = InFlight.find(requestId);
        Y_ABORT_UNLESS(itRequest != InFlight.end());
        auto& request = itRequest->second;

        auto& response = request.StatResponses[requestIndex];
        Y_ABORT_UNLESS(request.StatType == EStatType::COUNT_MIN_SKETCH);

        if (ev->Get()->Success) {
            response.Success = true;
            auto& data = ev->Get()->Data;
            Y_ABORT_UNLESS(data);
            response.CountMinSketch.CountMin.reset(TCountMinSketch::FromString(data->Data(), data->Size()));
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
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "EvRequestTimeout"
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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ConnectToSA(), pipe client id = " << SAPipeClientId);
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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "SyncNode(), pipe client id = " << SAPipeClientId);
    }

    void ReplySuccess(ui64 requestId, bool eraseRequest) {
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ReplySuccess(), request id = " << requestId);

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

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "ReplyFailed(), request id = " << requestId);

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

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        auto& request = ev->Get()->Request;

        if (!EnableColumnStatistics) {
            Send(ev->Sender, new NMon::TEvHttpInfoRes("Column statistics is disabled"));
            return;
        }

        auto method = request.GetMethod();
        if (method == HTTP_METHOD_POST) {
            auto& params = request.GetPostParams();
            auto itAction = params.find("action");
            if (itAction == params.end()) {
                Send(ev->Sender, new NMon::TEvHttpInfoRes("'action' parameter is required"));
                return;
            }
            if (itAction->second != "analyze") {
                Send(ev->Sender, new NMon::TEvHttpInfoRes("Unknown 'action' parameter"));
                return;
            }
            auto itPath = params.find("path");
            if (itPath == params.end()) {
                Send(ev->Sender, new NMon::TEvHttpInfoRes("'path' parameter is required"));
                return;
            }
            Register(new THttpRequest(THttpRequest::EType::ANALYZE, itPath->second, ev->Sender));
            return;

        } else if (method == HTTP_METHOD_GET) {
            auto& params = request.GetParams();
            auto itAction = params.find("action");
            if (itAction == params.end()) {
                Send(ev->Sender, new NMon::TEvHttpInfoRes("'action' parameter is required"));
                return;
            }
            if (itAction->second != "status") {
                Send(ev->Sender, new NMon::TEvHttpInfoRes("Unknown 'action' parameter"));
                return;
            }
            auto itPath = params.find("path");
            if (itPath == params.end()) {
                Send(ev->Sender, new NMon::TEvHttpInfoRes("'path' parameter is required"));
                return;
            }
            Register(new THttpRequest(THttpRequest::EType::STATUS, itPath->second, ev->Sender));
            return;
        }

        TStringStream str;
        HTML(str) {
            str << "<form method=\"post\" id=\"analyzePath\" name=\"analyzePath\" class=\"form-group\">" << Endl;
            str << "<input type=\"hidden\" name=\"action\" value=\"analyze\"/>";
            DIV() {
                str << "<input type=\"text\" class=\"form-control\" id=\"path\" name=\"path\"/>";
            }
            DIV() {
                str << "<input class=\"btn btn-default\" type=\"submit\" value=\"Analyze\"/>";
            }
            str << "</form>" << Endl;
            str << "<form method=\"get\" id=\"pathStatus\" name=\"pathStatus\" class=\"form-group\">" << Endl;
            str << "<input type=\"hidden\" name=\"action\" value=\"status\"/>";
            DIV() {
                str << "<input type=\"text\" class=\"form-control\" id=\"path\" name=\"path\"/>";
            }
            DIV() {
                str << "<input class=\"btn btn-default\" type=\"submit\" value=\"Status\"/>";
            }
            str << "</form>" << Endl;
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    TStatServiceSettings Settings;
    TAggregationStatistics AggregationStatistics;

    bool EnableStatistics = false;
    bool EnableColumnStatistics = false;

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
};

THolder<IActor> CreateStatService(const TStatServiceSettings& settings) {
    return MakeHolder<TStatService>(settings);
}


} // NStat
} // NKikimr
