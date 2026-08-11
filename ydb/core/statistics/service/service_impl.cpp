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
#include <ydb/core/protos/feature_flags.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr {
namespace NStat {

using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TStatService : public TActorBootstrapped<TStatService> {
public:
    using TBase = TActorBootstrapped<TStatService>;

    TStatService() = default;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::STAT_SERVICE;
    }

    struct TEvPrivate {
        enum EEv {
            EvRequestTimeout = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

            EvEnd
        };

        struct TEvRequestTimeout : public NActors::TEventLocal<TEvRequestTimeout, EvRequestTimeout> {
            std::unordered_set<ui64> NeedSchemeShards;
            NActors::TActorId PipeClientId;
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
                false, TActivationContext::ActorSystem(), SelfId());
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
            IgnoreFunc(TEvStatistics::TEvPropagateStatisticsResponse);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvStatistics::TEvStatisticsIsDisabled, Handle);
            hFunc(TEvStatistics::TEvLoadStatisticsQueryResponse, Handle);
            hFunc(TEvPrivate::TEvRequestTimeout, Handle);

            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(NMon::TEvHttpInfoRes, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                YDB_LOG_CRIT("NStat::TStatService: unexpected event",
                    {"eventType", ev->GetTypeRewrite()},
                    {"eventString", ev->ToString()});
        }
    }

private:
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        YDB_LOG_INFO("Subscribed for config changes on node",
            {"nodeId", SelfId().NodeId()});
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

    void QueryStatistics(const TString& database, ui64 requestId) {
        YDB_LOG_DEBUG("[TStatService::QueryStatistics]",
            {"requestId", requestId},
            {"database", database});

        auto it = InFlight.find(requestId);
        if (it == InFlight.end()) {

            YDB_LOG_ERROR("[TStatService::QueryStatistics] Request not found",
                {"requestId", requestId});
            ReplyFailed(requestId, true);
            return;
        }

        auto& request = it->second;
        request.StatResponses.reserve(request.StatRequests.size());
        ui32 reqIndex = 0;

        for (const auto& req : request.StatRequests) {
            auto& response = request.StatResponses.emplace_back();
            response.Req = req;
            ui64 queryId = NextLoadQueryCookie++;
            LoadQueriesInFlight[queryId] = std::make_pair(requestId, reqIndex);

            DispatchLoadStatisticsQuery(
                SelfId(), queryId, database, req.PathId, request.StatType, req.ColumnTags);

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
        request.Database = ev->Get()->Database;
        request.StatRequests.swap(ev->Get()->StatRequests);

        if (!EnableStatistics || IsStatisticsDisabledInSA) {
            ReplyFailed(requestId, true);
            return;
        }

        YDB_LOG_DEBUG("[TStatService::TEvGetStatistics]",
            {"requestId", requestId},
            {"replyToActorId", request.ReplyToActorId},
            {"statType", static_cast<ui32>(request.StatType)},
            {"statRequestsCount", request.StatRequests.size()});

        // SIMPLE stats use the propagation path (cookie=0, navigate->Cookie=requestId):
        //   ev->Cookie == 0 -> SA-resolution -> ReplySuccess reads from the Statistics map.
        // Column stats use the load path (cookie=requestId, navigate->Cookie=0):
        //   ev->Cookie != 0 -> navigate->Cookie == 0 -> QueryStatistics loads pre-computed stats.
        bool isSimple = (request.StatType == EStatType::SIMPLE);

        auto navigate = std::make_unique<TNavigate>();
        navigate->DatabaseName = ev->Get()->Database;
        navigate->Cookie = isSimple ? requestId : 0;
        for (const auto& req : request.StatRequests) {
            AddNavigateEntry(navigate->ResultSet, req.PathId, true);
        }

        ui64 cookie = isSimple ? 0 : requestId;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()), 0, cookie);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());

        auto requestId = ev->Cookie == 0 ? navigate->Cookie : ev->Cookie;

        YDB_LOG_DEBUG("[TStatService::TEvNavigateKeySetResult]",
            {"requestId", requestId});

        // Column stats: navigate succeeded, now load pre-computed stats from the
        // statistics table. The statistics table lives at the database level
        // (.metadata/statistics_v2). The database path must be resolved from the
        // table's domain, because the table may be nested in subdirectories
        // (e.g. /Root/Database/subdir/Table1 -> database is /Root/Database, not
        // /Root/Database/subdir). For serverless databases the statistics table
        // is in the shared database, so we resolve ResourcesDomainKey; for
        // non-serverless databases we resolve DomainKey. Both cases require a
        // second navigate round to obtain the absolute database path.
        if (ev->Cookie != 0 && ev->Cookie != ResolveDatabaseCookie) {
            auto entry = std::find_if(navigate->ResultSet.begin(), navigate->ResultSet.end(), [](const TNavigate::TEntry& entry){
                return entry.Status == TNavigate::EStatus::Ok;
            });

            if (entry == navigate->ResultSet.end()) {
                YDB_LOG_ERROR("[TStatService::TEvNavigateKeySetResult] Navigate failed",
                    {"requestId", requestId});
                ReplyFailed(requestId, true);
                return;
            }

            // If the request already has a Database set, use it directly.
            auto itRequest = InFlight.find(requestId);
            if (itRequest != InFlight.end() && !itRequest->second.Database.empty()) {
                QueryStatistics(itRequest->second.Database, requestId);
                return;
            }

            // Resolve the database path via a second navigate round.
            // For serverless databases, the statistics table is in the shared
            // database, so resolve ResourcesDomainKey. For non-serverless
            // databases, resolve DomainKey. This correctly handles tables
            // nested in subdirectories, where stripping path components would
            // yield the wrong database path.
            const auto domainInfo = entry->DomainInfo;
            const auto& domainKey = domainInfo->IsServerless()
                ? domainInfo->ResourcesDomainKey
                : domainInfo->DomainKey;

            auto resolveNavigate = std::make_unique<TNavigate>();
            resolveNavigate->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
            auto& resolveEntry = resolveNavigate->ResultSet.emplace_back();
            resolveEntry.TableId = TTableId(domainKey.OwnerId, domainKey.LocalPathId);
            resolveEntry.Operation = TNavigate::EOp::OpPath;
            resolveEntry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            resolveEntry.RedirectRequired = false;

            ui64 resolveCookie = NextResolveDatabaseCookie++;
            resolveNavigate->Cookie = resolveCookie;
            ResolveDatabaseInFlight[resolveCookie] = requestId;
            Send(MakeSchemeCacheID(),
                new TEvTxProxySchemeCache::TEvNavigateKeySet(resolveNavigate.release()),
                0, ResolveDatabaseCookie);
            return;
        }

        // Second navigate round: resolve the database path (DomainKey for
        // non-serverless, ResourcesDomainKey for serverless) to an absolute path.
        if (ev->Cookie == ResolveDatabaseCookie) {
            auto itResolve = ResolveDatabaseInFlight.find(navigate->Cookie);
            if (itResolve == ResolveDatabaseInFlight.end()) {
                return;
            }
            ui64 originalRequestId = itResolve->second;
            ResolveDatabaseInFlight.erase(itResolve);

            auto entry = std::find_if(navigate->ResultSet.begin(), navigate->ResultSet.end(), [](const TNavigate::TEntry& entry){
                return entry.Status == TNavigate::EStatus::Ok;
            });

            if (entry == navigate->ResultSet.end()) {
                YDB_LOG_ERROR("[TStatService::TEvNavigateKeySetResult] Resolve database navigate failed",
                    {"requestId", originalRequestId});
                ReplyFailed(originalRequestId, true);
                return;
            }

            // CanonizePath yields an absolute path (e.g. "/Root/Shared"),
            // which is required by DoLocalRpc downstream.
            const auto database = CanonizePath(entry->Path);
            QueryStatistics(database, originalRequestId);
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
                // we need to cancel the current requests. No need to delete column statistic requests.
                for (auto it = InFlight.begin(); it != InFlight.end();) {
                    if (it->second.StatType != EStatType::SIMPLE) {
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
            navigateRequest->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
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
        YDB_LOG_DEBUG("EvPropagateStatistics",
            {"nodeId", SelfId().NodeId()},
            {"cookie", ev->Cookie});

        Send(ev->Sender, new TEvStatistics::TEvPropagateStatisticsResponse, 0, ev->Cookie);

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

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;

        YDB_LOG_DEBUG("EvClientConnected",
            {"nodeId", ev->Get()->ClientId.NodeId()},
            {"clientId", clientId},
            {"serverId", ev->Get()->ServerId},
            {"tabletId", ev->Get()->TabletId},
            {"status", ev->Get()->Status});

        if (clientId == SAPipeClientId) {
            IsStatisticsDisabledInSA = false;
            if (ev->Get()->Status != NKikimrProto::OK) {
                SAPipeClientId = TActorId();
                ConnectToSA();
                SyncNode();
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto& clientId = ev->Get()->ClientId;

        YDB_LOG_DEBUG("EvClientDestroyed",
            {"nodeId", ev->Get()->ClientId.NodeId()},
            {"clientId", clientId},
            {"serverId", ev->Get()->ServerId},
            {"tabletId", ev->Get()->TabletId});

        if (clientId == SAPipeClientId) {
            IsStatisticsDisabledInSA = false;
            SAPipeClientId = TActorId();
            ConnectToSA();
            SyncNode();
        }
    }

    void Handle(TEvStatistics::TEvStatisticsIsDisabled::TPtr&) {
        IsStatisticsDisabledInSA = true;
        ReplyAllFailed();
    }

    void Handle(TEvStatistics::TEvLoadStatisticsQueryResponse::TPtr& ev) {
        auto itLoadQuery = LoadQueriesInFlight.find(ev->Cookie);
        Y_ABORT_UNLESS(itLoadQuery != LoadQueriesInFlight.end());
        auto [requestId, requestIndex] = itLoadQuery->second;

        YDB_LOG_DEBUG("TEvLoadStatisticsQueryResponse",
            {"requestId", requestId});

        auto itRequest = InFlight.find(requestId);
        if (InFlight.end() == itRequest) {
            YDB_LOG_ERROR("TEvLoadStatisticsQueryResponse, Request not found in InFlight",
                {"requestId", requestId});
            return;
        }
        auto& request = itRequest->second;
        auto& response = request.StatResponses[requestIndex];

        const auto msg = ev->Get();
        if (msg->Success && msg->Data) {
            switch (request.StatType) {
            case EStatType::SIMPLE_COLUMN: {
                NKikimrStat::TSimpleColumnStatistics data;
                response.Success = data.ParseFromString(*msg->Data);
                if (response.Success) {
                    response.SimpleColumn.Data = std::move(data);
                }
                break;
            }
            case EStatType::COUNT_MIN_SKETCH:
                response.Success = true;
                response.CountMinSketch.CountMin.reset(
                    TCountMinSketch::FromString(msg->Data->data(), msg->Data->size()));
                break;
            case EStatType::EQ_WIDTH_HISTOGRAM:
                response.Success = true;
                response.EqWidthHistogram.Data =
                    std::make_shared<TEqWidthHistogram>(msg->Data->data(), msg->Data->size());
                break;
            case EStatType::TABLE_SUMMARY: {
                NKikimrStat::TTableSummaryStatistics data;
                response.Success = data.ParseFromString(*msg->Data);
                if (response.Success) {
                    response.TableSummary.Data = std::move(data);
                }
                break;
            }
            default:
                YDB_LOG_ERROR("TEvLoadStatisticsQueryResponse, unexpected stat type",
                    {"requestId", requestId},
                    {"statType", static_cast<int>(request.StatType)});
                response.Success = false;
                break;
            }
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
        YDB_LOG_DEBUG("EvRequestTimeout",
            {"pipeClientId", ev->Get()->PipeClientId},
            {"schemeShardsCount", ev->Get()->NeedSchemeShards.size()});

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
        NTabletPipe::TClientConfig pipeConfig{.RetryPolicy = policy};
        SAPipeClientId = Register(NTabletPipe::CreateClient(SelfId(), StatisticsAggregatorId, pipeConfig));

        YDB_LOG_DEBUG("ConnectToSA()",
            {"pipeClientId", SAPipeClientId});
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

        YDB_LOG_DEBUG("SyncNode()",
            {"pipeClientId", SAPipeClientId});
    }

    void ReplySuccess(ui64 requestId, bool eraseRequest) {
        auto itRequest = InFlight.find(requestId);
        if (itRequest == InFlight.end()) {
            return;
        }
        auto& request = itRequest->second;

        YDB_LOG_DEBUG("ReplySuccess()",
            {"requestId", requestId},
            {"replyToActorId", request.ReplyToActorId},
            {"statRequestsCount", request.StatRequests.size()});

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

        YDB_LOG_DEBUG("ReplyFailed()",
            {"requestId", requestId});

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
                std::unordered_map<EStatType, size_t> counts;
                for (const auto& [id, req] : InFlight) {
                    ++counts[req.StatType];
                }
                str << "[SIMPLE: " << counts[EStatType::SIMPLE]
                    << ", SIMPLE_COLUMN: " << counts[EStatType::SIMPLE_COLUMN]
                    << ", COUNT_MIN_SKETCH: " << counts[EStatType::COUNT_MIN_SKETCH]
                    << ", EQ_WIDTH_HISTOGRAM: " << counts[EStatType::EQ_WIDTH_HISTOGRAM]
                    << ", TABLE_SUMMARY: " << counts[EStatType::TABLE_SUMMARY]
                    << "]" << Endl;
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
            AddPanel(str, "Probe base statistics", [](IOutputStream& str) {
                HTML(str) {
                    FORM_CLASS("form-horizontal") {
                        DIV_CLASS("form-group") {
                            LABEL_CLASS_FOR("col-sm-2 control-label", "path") {
                                str << "Path";
                            }
                            DIV_CLASS("col-sm-8") {
                                str << "<input type='text' id='path' name='path' class='form-control' placeholder='/full/path'>";
                            }

                            str << "<input type=\"hidden\" name=\"action\" value=\"probe_base_stats\"/>";
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
            if (msg->ContentType == NMon::IEvHttpInfoRes::Html) {
                ReplyToMonitoring(msg->Answer);
            } else {
                Send(MonitoringActorId, ev->Release());
            }
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
        HttpRequestActorId = TActorId();
        MonitoringActorId = ev->Sender;

        const auto& request = ev->Get()->Request;
        const auto& params = request.GetParams();

        auto getRequestParam = [&params](const TStringBuf name) {
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
            if (!EnableColumnStatistics) {
                ReplyToMonitoring("Column statistics is disabled");
                return;
            }

            HttpRequestActorId = Register(new THttpRequest(THttpRequest::ERequestType::ANALYZE, {
                { THttpRequest::EParamType::PATH, path }
                },
                THttpRequest::EResponseContentType::HTML,
                SelfId()));
        } else if (action == "status") {
            if (!EnableColumnStatistics) {
                ReplyToMonitoring("Column statistics is disabled");
                return;
            }

            const auto operationId = getRequestParam("operation");
            if (operationId.empty()) {
                ReplyToMonitoring("'OperationId' parameter is required");
                return;
            }

            HttpRequestActorId = Register(new THttpRequest(THttpRequest::ERequestType::STATUS, {
                { THttpRequest::EParamType::PATH, path },
                { THttpRequest::EParamType::OPERATION_ID, operationId }
                },
                THttpRequest::EResponseContentType::HTML,
                SelfId()));
        } else if (action == "probe") {
            if (!EnableColumnStatistics) {
                ReplyToMonitoring("Column statistics is disabled");
                return;
            }

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

            HttpRequestActorId = Register(new THttpRequest(THttpRequest::ERequestType::PROBE_COUNT_MIN_SKETCH, {
                { THttpRequest::EParamType::PATH, path },
                { THttpRequest::EParamType::COLUMN_NAME, column },
                { THttpRequest::EParamType::CELL_VALUE, cell }
                },
                THttpRequest::EResponseContentType::HTML,
                SelfId()));
        } else if (action == "probe_base_stats") {
            if (!EnableStatistics) {
                ReplyToMonitoring("Base statistics is disabled");
                return;
            }

            auto respContentType = THttpRequest::EResponseContentType::HTML;
            if (params.Has("json")) {
                ui32 json = 0;
                if (!TryFromString(params.Get("json"), json)) {
                    return ReplyToMonitoring("Failed to parse json parameter -- must be an integer");
                }
                if (json) {
                    respContentType = THttpRequest::EResponseContentType::JSON;
                }
            }

            HttpRequestActorId = Register(new THttpRequest(
                THttpRequest::ERequestType::PROBE_BASE_STATS, {
                    { THttpRequest::EParamType::PATH, path },
                },
                respContentType,
                SelfId()));
        } else {
            ReplyToMonitoring("Wrong 'action' parameter value");
        }
    }

private:
    bool EnableStatistics = false;
    bool EnableColumnStatistics = false;
    bool IsStatisticsDisabledInSA = false;

    static constexpr size_t StatFanOut = 10;

    struct TRequestState {
        NActors::TActorId ReplyToActorId;
        ui64 EvCookie = 0;
        ui64 SchemeShardId = 0;
        EStatType StatType = EStatType::SIMPLE;
        TString Database;
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
    static const ui64 ResolveDatabaseCookie = std::numeric_limits<ui64>::max() - 1;
    enum EResolveSAStage {
        RSA_INITIAL,
        RSA_IN_FLIGHT,
        RSA_FINISHED
    };
    EResolveSAStage ResolveSAStage = RSA_INITIAL;

    // Maps ResolveDatabaseCookie -> original requestId for the second
    // navigate round (serverless: resolve shared database path).
    std::unordered_map<ui64, ui64> ResolveDatabaseInFlight;
    ui64 NextResolveDatabaseCookie = 1;

    static constexpr TDuration RequestTimeout = TDuration::MilliSeconds(100);

    TActorId HttpRequestActorId;
    TActorId MonitoringActorId;
};

THolder<IActor> CreateStatService(const TStatServiceSettings&) {
    return MakeHolder<TStatService>();
}


} // NStat
} // NKikimr
