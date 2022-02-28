#include "msgbus_server_persqueue.h"
#include "msgbus_server_pq_metacache.h"

#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/base/counters.h>

#include <ydb/core/base/appdata.h>

namespace NKikimr::NMsgBusProxy {

using namespace NYdb::NTable;

namespace NPqMetaCacheV2 {

IActor* CreateSchemeCache(NActors::TActorSystem* ActorSystem, TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    auto appData = ActorSystem->AppData<TAppData>();
    auto cacheCounters = GetServiceCounters(counters, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, cacheCounters);
    return CreateSchemeBoardSchemeCache(cacheConfig.Get());
}

class TPersQueueMetaCacheActor : public TActorBootstrapped<TPersQueueMetaCacheActor> {
    using TBase = TActorBootstrapped<TPersQueueMetaCacheActor>;
public:
    TPersQueueMetaCacheActor(TPersQueueMetaCacheActor&&) = default;
    TPersQueueMetaCacheActor& operator=(TPersQueueMetaCacheActor&&) = default;

    TPersQueueMetaCacheActor(ui64 grpcPort,
                             const NMonitoring::TDynamicCounterPtr& counters,
                             const TDuration& versionCheckInterval)
        : Counters(counters)
        , ClientWrapper(std::move(std::make_unique<TClientWrapper>(grpcPort)))
        , VersionCheckInterval(versionCheckInterval)
        , Generation(std::make_shared<TAtomicCounter>())
    {
    }

    TPersQueueMetaCacheActor(const NMonitoring::TDynamicCounterPtr& counters,
                             const TDuration& versionCheckInterval)
        : Counters(counters)
        , VersionCheckInterval(versionCheckInterval)
        , Generation(std::make_shared<TAtomicCounter>())
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        if (ClientWrapper == nullptr) {
            auto* driver = AppData(ctx)->YdbDriver;
            if (driver == nullptr) {
                LOG_WARN_S(
                        ctx, NKikimrServices::PQ_METACACHE,
                        "Initialized without valid YDB driver - suppose misconfiguration. Refuse to work. Die."
                );
                Die(ctx);
                return;
            }
            ClientWrapper.reset(new TClientWrapper(driver));
        }
        SkipVersionCheck = !AppData(ctx)->PQConfig.GetMetaCacheSkipVersionCheck();
        PathPrefix = TopicPrefix(ctx);
        TopicsQuery = TStringBuilder() << "--!syntax_v1\n"
                                       << "DECLARE $Path as Utf8; DECLARE $Cluster as Utf8; "
                                       << "SELECT path, dc from `" << PathPrefix << "Config/V2/Topics` "
                                       << "WHERE path > $Path OR (path = $Path AND dc > $Cluster);";

        VersionQuery = TStringBuilder() << "--!syntax_v1\nSELECT version FROM `" << PathPrefix << "Config/V2/Versions` "
                                        << "WHERE name = 'Topics';";
        PathPrefixParts = NKikimr::SplitPath(PathPrefix);
        Become(&TPersQueueMetaCacheActor::StateFunc);

        if (!SchemeCacheId) {
            SchemeCacheId = Register(CreateSchemeCache(GetActorSystem(), Counters));
        }
        ActorSystem = GetActorSystem();
        Reset(ctx);
    }

    ~TPersQueueMetaCacheActor() {
        Generation->Inc();
        if (ClientWrapper)
            ClientWrapper->Stop();
    }

private:
    template<class TEventType, class TFutureType, class... TArgs>
    void SubscribeEvent(const TFutureType& future, TArgs... args) {
        std::weak_ptr<TAtomicCounter> weakGeneration(Generation);
        future.Subscribe(
            [
                id = SelfId(),
                originalGen = Generation->Val(),
                weakGen = weakGeneration,
                as = ActorSystem,
                ... args = std::forward<TArgs>(args)
            ](const auto&) mutable {
                auto currentGen = weakGen.lock();
                if (currentGen && originalGen == currentGen->Val()) {
                    as->Send(id, new TEventType(args...));
                }
            }
        );
    }

    void Reset(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Metacache: reset");
        Y_VERIFY(ClientWrapper);
        ClientWrapper->Reset();
        Generation->Inc();
        YdbSession = Nothing();
        PreparedTopicsQuery = Nothing();
        LastTopicKey = {};
        ctx.Schedule(QueryRetryInterval, new NActors::TEvents::TEvWakeup());
    }

    void StartSession(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Start new session");
        SessionFuture = ClientWrapper->GetClient()->GetSession();
        SubscribeEvent<TEvPqNewMetaCache::TEvSessionStarted>(SessionFuture);
    }

    void HandleSessionStarted(const TActorContext& ctx) {
        auto& value = SessionFuture.GetValue();
        if (!value.IsSuccess()) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_METACACHE, "Session start failed: " << value.GetIssues().ToString());
            return Reset(ctx);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Session started");
        YdbSession = value.GetSession();
        PrepareTopicsQuery(ctx);
    }

    void PrepareTopicsQuery(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        TopicsQueryFuture = YdbSession->PrepareDataQuery(TopicsQuery);
        SubscribeEvent<TEvPqNewMetaCache::TEvQueryPrepared>(TopicsQueryFuture);
    }

    void HandleTopicsQueryPrepared(const TActorContext& ctx) {
        auto& value = TopicsQueryFuture.GetValue();
        if (!value.IsSuccess()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Topics query prepare failed: " << value.GetIssues().ToString());
            return Reset(ctx);
        }
        PreparedTopicsQuery = value.GetQuery();
        if (NewTopicsVersion > CurrentTopicsVersion) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Start topics rescan");
            RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            Y_VERIFY(NewTopicsVersion == CurrentTopicsVersion);
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Check version");
            RunQuery(EQueryType::ECheckVersion, ctx);
        }
    }

    void RunQuery(EQueryType type, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Y_VERIFY(YdbSession);
        if (type == EQueryType::ECheckVersion) {
            AsyncQueryResult = YdbSession->ExecuteDataQuery(VersionQuery, TTxControl::BeginTx().CommitTx());
        } else {
            Y_VERIFY(PreparedTopicsQuery);
            auto builder = PreparedTopicsQuery->GetParamsBuilder();
            {
                auto& param = builder.AddParam("$Path");
                param.Utf8(LastTopicKey.Path);
                param.Build();
            }
            {
                auto& param = builder.AddParam("$Cluster");
                param.Utf8(LastTopicKey.Cluster);
                param.Build();
            }
            AsyncQueryResult = PreparedTopicsQuery->Execute(TTxControl::BeginTx().CommitTx(), builder.Build());
        }
        SubscribeEvent<TEvPqNewMetaCache::TEvQueryComplete>(AsyncQueryResult, type);
    }

    void HandleCheckVersionResult(const TActorContext& ctx) {
        auto result = AsyncQueryResult.GetValue();
        if (!result.IsSuccess()) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_METACACHE,
                        "Got error trying to update config version: " << result.GetIssues().ToString());
            ctx.Schedule(QueryRetryInterval, new TEvPqNewMetaCache::TEvRestartQuery(EQueryType::ECheckVersion));
            return;
        }
        Y_VERIFY(result.GetResultSets().size() == 1);
        ui64 newVersion = 0;
        {
            auto versionQueryResult = result.GetResultSetParser(0);
            while (versionQueryResult.TryNextRow()) {
                newVersion = versionQueryResult.ColumnParser("version").GetOptionalInt64().GetOrElse(0);
            }
        }
        LastVersionUpdate = ctx.Now();
        if (newVersion > CurrentTopicsVersion || CurrentTopicsVersion == 0 || SkipVersionCheck) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got config version: " << newVersion);
            NewTopicsVersion = newVersion;
            RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            ctx.Schedule(VersionCheckInterval, new TEvPqNewMetaCache::TEvRestartQuery(EQueryType::ECheckVersion));
        }
    }

    void HandleGetTopicsResult(const TActorContext& ctx) {
        auto result = AsyncQueryResult.GetValue();
        if (!result.IsSuccess()) {
            LOG_INFO_S(ctx, NKikimrServices::PQ_METACACHE,
                       "Got error trying to get topics: " << result.GetIssues().ToString());
            return Reset(ctx);
        }
        Y_VERIFY(result.GetResultSets().size() == 1);
        auto versionQueryResult = result.GetResultSetParser(0);
        TString path, dc;
        while (versionQueryResult.TryNextRow()) {
            path = *versionQueryResult.ColumnParser("path").GetOptionalUtf8();
            dc = *versionQueryResult.ColumnParser("dc").GetOptionalUtf8();
            NewTopics.emplace_back(NPersQueue::BuildFullTopicName(path, dc));
        }
        if (result.GetResultSet(0).Truncated()) {
            LastTopicKey = {path, dc};
            return RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            LastTopicKey = {};
            CurrentTopics = std::move(NewTopics);
            NewTopics.clear();
            EverGotTopics = true;
            CurrentTopicsVersion = NewTopicsVersion;
            FullTopicsCacheOutdated = true;
            FullTopicsCache = nullptr;
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "reset topics cache");
            while (!ListTopicsWaiters.empty()) {
                auto& waiter = ListTopicsWaiters.front();
                ProcessDescribeAllTopics(waiter, ctx);
                ListTopicsWaiters.pop();
            }
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE,
                        "Updated topics list with : " << CurrentTopics.size() << " topics");
            ctx.Schedule(VersionCheckInterval, new TEvPqNewMetaCache::TEvRestartQuery(EQueryType::ECheckVersion));
        }
    }


    void HandleQueryComplete(TEvPqNewMetaCache::TEvQueryComplete::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->Type) {
            case EQueryType::ECheckVersion:
                return HandleCheckVersionResult(ctx);
            case EQueryType::EGetTopics:
                return HandleGetTopicsResult(ctx);
            default:
                Y_FAIL();
        }
    }

    void HandleRestartQuery(TEvPqNewMetaCache::TEvRestartQuery::TPtr& ev, const TActorContext& ctx) {
        Y_VERIFY(ev->Get()->Type == EQueryType::ECheckVersion);
        RunQuery(ev->Get()->Type, ctx);
    }

    void HandleGetVersion(TEvPqNewMetaCache::TEvGetVersionRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send version response: " << CurrentTopicsVersion);

        ctx.Send(ev->Sender, new TEvPqNewMetaCache::TEvGetVersionResponse{CurrentTopicsVersion});
    }

    void HandleDescribeTopics(TEvPqNewMetaCache::TEvDescribeTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe topics");

        SendSchemeCacheRequest(ev->Get()->Topics, !ev->Get()->PathPrefix.empty(), false, ctx);
        auto inserted = DescribeTopicsWaiters.insert(std::make_pair(
                RequestId,
                TWaiter{ev->Sender, std::move(ev->Get()->Topics)}
        )).second;
        Y_VERIFY(inserted);
    }

    void HandleDescribeAllTopics(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe all topics");
        if (ev->Get()->PathPrefix && ev->Get()->PathPrefix != PathPrefix) {
            auto* response = new TEvPqNewMetaCache::TEvDescribeAllTopicsResponse(ev->Get()->PathPrefix);
            response->Success = false;
            ctx.Send(ev->Sender, response);
            return;
        }
        if (!EverGotTopics) {
            ListTopicsWaiters.push(ev->Sender);
            return;
        }
        return ProcessDescribeAllTopics(ev->Sender, ctx);
    }

    void ProcessDescribeAllTopics(const TActorId& waiter, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Process describe all topics");
        if (EverGotTopics && CurrentTopics.empty()) {
            SendDescribeAllTopicsResponse(waiter, ctx, true);
            return;
        }
        if (FullTopicsCache && !FullTopicsCacheOutdated) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Respond from cache");
            return SendDescribeAllTopicsResponse(waiter, ctx);
        }
        if (DescribeAllTopicsWaiters.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Make full list SC request");
            SendSchemeCacheRequest(CurrentTopics, true, true, ctx);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Store waiter");
        DescribeAllTopicsWaiters.push(waiter);
        FullTopicsCacheOutdated = false;
        FullTopicsCache = nullptr;
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        auto& result = ev->Get()->Request;
        if (result->Instant == 0) {
            for (const auto& entry : result->ResultSet) {
                if (!entry.PQGroupInfo) {
                    continue;
                }

                const auto& desc = entry.PQGroupInfo->Description;
                if (desc.HasBalancerTabletID() && desc.GetBalancerTabletID() != 0) {
                    continue;
                }
                FullTopicsCacheOutdated = true;
            }

            FullTopicsCache.reset(result.Release());
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Updated topics cache with " << FullTopicsCache->ResultSet.size());
            while (!DescribeAllTopicsWaiters.empty()) {
                SendDescribeAllTopicsResponse(DescribeAllTopicsWaiters.front(), ctx);
                DescribeAllTopicsWaiters.pop();
            }
        } else {
            auto waiterIter = DescribeTopicsWaiters.find(result->Instant);
            Y_VERIFY(!waiterIter.IsEnd());
            auto& waiter = waiterIter->second;

            Y_VERIFY(waiter.Topics.size() == result->ResultSet.size());
            auto *response = new TEvPqNewMetaCache::TEvDescribeTopicsResponse{
                    std::move(waiter.Topics), result.Release()
            };
            ctx.Send(waiter.WaiterId, response);
            DescribeTopicsWaiters.erase(waiterIter);
        }
    }


    void SendSchemeCacheRequest(
            const TVector<TString>& topics, bool addDefaultPathPrefix, bool isFullListingRequest, const TActorContext& ctx
    ) {
        auto instant = isFullListingRequest ? 0 : ++RequestId;
        auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>(instant);
        for (const auto& path : topics) {
            auto split = NKikimr::SplitPath(path);
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            if (addDefaultPathPrefix) {
                entry.Path.insert(entry.Path.end(), PathPrefixParts.begin(), PathPrefixParts.end());
            }

            entry.Path.insert(entry.Path.end(), split.begin(), split.end());
            entry.SyncVersion = !isFullListingRequest;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTopic;
            schemeCacheRequest->ResultSet.emplace_back(std::move(entry));
        }
        ctx.Send(SchemeCacheId, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.Release()));
    }

    void SendDescribeAllTopicsResponse(const TActorId& recipient, const TActorContext& ctx, bool empty = false) {
        if (empty) {
            ctx.Send(
                    recipient,
                    new TEvPqNewMetaCache::TEvDescribeAllTopicsResponse(
                            PathPrefix, std::make_shared<NSchemeCache::TSchemeCacheNavigate>()
                    )
            );
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send empty describe all topics response");
            return;
        } else {
            ctx.Send(
                    recipient,
                    new TEvPqNewMetaCache::TEvDescribeAllTopicsResponse(
                            PathPrefix, FullTopicsCache
                    )
            );
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send describe all topics response with " << FullTopicsCache->ResultSet.size() << " topics");

        }
    }

    static NActors::TActorSystem* GetActorSystem() {
        return TActivationContext::ActorSystem();
    }

public:
    void Die(const TActorContext& ctx) {
        Generation->Inc();
        TBase::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
                  SFunc(NActors::TEvents::TEvWakeup, StartSession)
                          SFunc(TEvPqNewMetaCache::TEvSessionStarted, HandleSessionStarted)
                          SFunc(TEvPqNewMetaCache::TEvQueryPrepared, HandleTopicsQueryPrepared)
                          HFunc(TEvPqNewMetaCache::TEvQueryComplete, HandleQueryComplete)
                          HFunc(TEvPqNewMetaCache::TEvRestartQuery, HandleRestartQuery)

                          HFunc(TEvPqNewMetaCache::TEvGetVersionRequest, HandleGetVersion)
                          HFunc(TEvPqNewMetaCache::TEvDescribeTopicsRequest, HandleDescribeTopics)
                          HFunc(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest, HandleDescribeAllTopics)
                          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse)
    )

private:
    enum class EWaiterType {
        ListTopics,
        DescribeAllTopics,
        DescribeCustomTopics
    };

    struct TWaiter {
        TActorId WaiterId;
        TVector<TString> Topics;
    };

    struct TTopicKey {
        TString Path;
        TString Cluster;
    };


    class TClientWrapper {
    public:
        TClientWrapper(const TClientWrapper&) = delete;
        TClientWrapper& operator=(const TClientWrapper&) = delete;
        TClientWrapper(TClientWrapper&&) = default;
        TClientWrapper& operator=(TClientWrapper&&) = default;

        TClientWrapper(ui64 driverPort)
            : DriverPort(driverPort)
        {}

        TClientWrapper(NYdb::TDriver* driver)
            : Driver(driver)
        {}

        void Reset() {
            if (DriverPort.Defined()) { // Own driver
                if (DriverHolder == nullptr) {
                    TString endpoint = TStringBuilder() << "localhost:" << *DriverPort;
                    auto driverConfig = NYdb::TDriverConfig().SetEndpoint(endpoint);
                    DriverHolder.Reset(new NYdb::TDriver(driverConfig));
                    Driver = DriverHolder.Get();
                    TableClient.reset(new NYdb::NTable::TTableClient(*Driver));
                }
            } else if (Driver != nullptr) {
                TableClient.reset(new NYdb::NTable::TTableClient(*Driver));
            }
        }

        NYdb::NTable::TTableClient* GetClient() {
            Y_VERIFY(TableClient);
            return TableClient.get();
        }

        void Stop() {
            if (DriverHolder != nullptr) {
                TableClient->Stop();
                DriverHolder->Stop();
            } else if (Driver != nullptr) {
                TableClient->Stop();
            }
        }

    private:
        THolder<NYdb::TDriver> DriverHolder;
        NYdb::TDriver* Driver = nullptr;
        TMaybe<ui64> DriverPort;
        std::unique_ptr<NYdb::NTable::TTableClient> TableClient;
    };

    NMonitoring::TDynamicCounterPtr Counters;
    NActors::TActorSystem* ActorSystem;
    TString VersionQuery;
    TString TopicsQuery;

    std::unique_ptr<TClientWrapper> ClientWrapper;
    TAsyncCreateSessionResult SessionFuture;
    TMaybe<TSession> YdbSession;
    TAsyncPrepareQueryResult TopicsQueryFuture;
    TMaybe<TDataQuery> PreparedTopicsQuery;
    TAsyncDataQueryResult AsyncQueryResult;
    ui64 CurrentTopicsVersion = 0;
    ui64 NewTopicsVersion = 0;
    TTopicKey LastTopicKey = TTopicKey{};
    TVector<TString> NewTopics;
    TVector<TString> CurrentTopics;
    bool EverGotTopics = false;
    TDuration QueryRetryInterval = TDuration::Seconds(2);
    TDuration VersionCheckInterval = TDuration::Seconds(1);
    TInstant LastVersionUpdate = TInstant::Zero();

    TQueue<TActorId> ListTopicsWaiters;
    TQueue<TActorId> DescribeAllTopicsWaiters;
    THashMap<ui64, TWaiter> DescribeTopicsWaiters;
    ui64 RequestId = 1;

    std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> FullTopicsCache;
    bool FullTopicsCacheOutdated = false;
    NActors::TActorId SchemeCacheId;
    TString PathPrefix;
    TVector<TString> PathPrefixParts;
    std::shared_ptr<TAtomicCounter> Generation;
    bool SkipVersionCheck = false;
};

IActor* CreatePQMetaCache(const NMonitoring::TDynamicCounterPtr& counters, const TDuration& versionCheckInterval) {
    return new TPersQueueMetaCacheActor(counters, versionCheckInterval);
}

IActor* CreatePQMetaCache(ui64 grpcPort, const NMonitoring::TDynamicCounterPtr& counters, const TDuration& versionCheckInterval) {
    return new TPersQueueMetaCacheActor(grpcPort, counters, versionCheckInterval);
}

} // namespace NPqMetaCacheV2

} // namespace NKikimr::NMsgBusProxy
