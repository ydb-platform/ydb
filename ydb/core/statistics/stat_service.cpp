#include "stat_service.h"
#include "events.h"
#include "save_load_stats.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
namespace NStat {

class THttpRequest : public TActorBootstrapped<THttpRequest> {
public:
    using TBase = TActorBootstrapped<THttpRequest>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE_HTTP_REQUEST;
    }

    void Bootstrap() {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        auto navigate = std::make_unique<TNavigate>();
        auto& entry = navigate->ResultSet.emplace_back();
        entry.Path = SplitPath(Path);
        entry.Operation = TNavigate::EOp::OpTable;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        navigate->Cookie = FirstRoundCookie;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));

        Become(&THttpRequest::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStatistics::TEvScanTableAccepted, Handle);
            hFunc(TEvStatistics::TEvGetScanStatusResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            IgnoreFunc(TEvStatistics::TEvScanTableResponse);
            default:
                LOG_CRIT_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                    "NStat::THttpRequest: unexpected event# " << ev->GetTypeRewrite());
        }
    }

    enum EType {
        ANALYZE,
        STATUS
    };

    THttpRequest(EType type, const TString& path, TActorId replyToActorId)
        : Type(type)
        , Path(path)
        , ReplyToActorId(replyToActorId)
    {}

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());
        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
        auto& entry = navigate->ResultSet.front();

        if (navigate->Cookie == SecondRoundCookie) {
            if (entry.Status != TNavigate::EStatus::Ok) {
                HttpReply("Internal error");
                return;
            }
            if (entry.DomainInfo->Params.HasStatisticsAggregator()) {
                StatisticsAggregatorId = entry.DomainInfo->Params.GetStatisticsAggregator();
            }
            ResolveSuccess();
            return;
        }

        if (entry.Status != TNavigate::EStatus::Ok) {
            switch (entry.Status) {
            case TNavigate::EStatus::PathErrorUnknown:
                HttpReply("Path does not exist");
                return;
            case TNavigate::EStatus::PathNotPath:
                HttpReply("Invalid path");
                return;
            case TNavigate::EStatus::PathNotTable:
                HttpReply("Path is not a table");
                return;
            default:
                HttpReply("Internal error");
                return;
            }
        }

        PathId = entry.TableId.PathId;

        auto& domainInfo = entry.DomainInfo;
        ui64 aggregatorId = 0;
        if (domainInfo->Params.HasStatisticsAggregator()) {
            aggregatorId = domainInfo->Params.GetStatisticsAggregator();
        }
        bool isServerless = domainInfo->IsServerless();
        TPathId domainKey = domainInfo->DomainKey;
        TPathId resourcesDomainKey = domainInfo->ResourcesDomainKey;

        auto navigateDomainKey = [this] (TPathId domainKey) {
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            auto navigate = std::make_unique<TNavigate>();
            auto& entry = navigate->ResultSet.emplace_back();
            entry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.RedirectRequired = false;
            navigate->Cookie = SecondRoundCookie;

            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()));
        };

        if (!isServerless) {
            if (aggregatorId) {
                StatisticsAggregatorId = aggregatorId;
                ResolveSuccess();
            } else {
                navigateDomainKey(domainKey);
            }
        } else {
            navigateDomainKey(resourcesDomainKey);
        }
    }

    void Handle(TEvStatistics::TEvScanTableAccepted::TPtr&) {
        HttpReply("Scan accepted");
    }

    void Handle(TEvStatistics::TEvGetScanStatusResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        switch (record.GetStatus()) {
        case NKikimrStat::TEvGetScanStatusResponse::NO_OPERATION:
            HttpReply("No scan operation");
            break;
        case NKikimrStat::TEvGetScanStatusResponse::ENQUEUED:
            HttpReply("Scan is enqueued");
            break;
        case NKikimrStat::TEvGetScanStatusResponse::IN_PROGRESS:
            HttpReply("Scan is in progress");
            break;
        }
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&) {
        HttpReply("Delivery problem");
    }

    void ResolveSuccess() {
        if (StatisticsAggregatorId == 0) {
            HttpReply("No statistics aggregator");
            return;
        }

        if (Type == ANALYZE) {
            auto scanTable = std::make_unique<TEvStatistics::TEvScanTable>();
            auto& record = scanTable->Record;
            PathIdFromPathId(PathId, record.MutablePathId());

            Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(scanTable.release(), StatisticsAggregatorId, true));
        } else {
            auto getStatus = std::make_unique<TEvStatistics::TEvGetScanStatus>();
            auto& record = getStatus->Record;
            PathIdFromPathId(PathId, record.MutablePathId());

            Send(MakePipePerNodeCacheID(false),
                new TEvPipeCache::TEvForward(getStatus.release(), StatisticsAggregatorId, true));
        }
    }

    void HttpReply(const TString& msg) {
        Send(ReplyToActorId, new NMon::TEvHttpInfoRes(msg));
        PassAway();
    }

    void PassAway() {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));
        TBase::PassAway();
    }

private:
    const EType Type;
    const TString Path;
    const TActorId ReplyToActorId;

    TPathId PathId;
    ui64 StatisticsAggregatorId = 0;

    static const ui64 FirstRoundCookie = 1;
    static const ui64 SecondRoundCookie = 2;
};


class TStatService : public TActorBootstrapped<TStatService> {
public:
    using TBase = TActorBootstrapped<TStatService>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE;
    }

    struct TEvPrivate {
        enum EEv {
            EvRequestTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),

            EvEnd
        };

        struct TEvRequestTimeout : public TEventLocal<TEvRequestTimeout, EvRequestTimeout> {
            std::unordered_set<ui64> NeedSchemeShards;
            TActorId PipeClientId;
        };
    };

    void Bootstrap() {
        EnableStatistics = AppData()->FeatureFlags.GetEnableStatistics();

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
            IgnoreFunc(TEvStatistics::TEvPropagateStatisticsResponse);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvStatistics::TEvStatisticsIsDisabled, Handle);
            hFunc(TEvStatistics::TEvLoadStatisticsQueryResponse, Handle);
            hFunc(TEvPrivate::TEvRequestTimeout, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
                    "NStat::TStatService: unexpected event# " << ev->GetTypeRewrite());
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
            if (!EnableStatistics) {
                ReplyAllFailed();
            }
        }
        auto response = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationResponse>(record);
        Send(ev->Sender, response.release(), 0, ev->Cookie);
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
                if (!req.ColumnName) {
                    response.Success = false;
                    ++reqIndex;
                    continue;
                }
                ui64 loadCookie = NextLoadQueryCookie++;
                LoadQueriesInFlight[loadCookie] = std::make_pair(requestId, reqIndex);
                Register(CreateLoadStatisticsQuery(req.PathId, request.StatType,
                    *req.ColumnName, loadCookie));
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

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "EvClientConnected"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << ev->Get()->ClientId
            << ", server id = " << ev->Get()->ServerId
            << ", status = " << ev->Get()->Status);

        if (ev->Get()->Status != NKikimrProto::OK) {
            SAPipeClientId = TActorId();
            ConnectToSA();
            SyncNode();
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::STATISTICS,
            "EvClientDestroyed"
            << ", node id = " << ev->Get()->ClientId.NodeId()
            << ", client id = " << ev->Get()->ClientId
            << ", server id = " << ev->Get()->ServerId);

        SAPipeClientId = TActorId();
        ConnectToSA();
        SyncNode();
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
    bool EnableStatistics = false;

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

THolder<IActor> CreateStatService() {
    return MakeHolder<TStatService>();
}

} // NStat
} // NKikimr
