#include "msgbus_server_persqueue.h"
#include "msgbus_server_pq_metacache.h"

#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/pq_database.h>


namespace NKikimr::NMsgBusProxy {

using namespace NYdb::NTable;

namespace NPqMetaCacheV2 {



IActor* CreateSchemeCache(const TActorContext& ctx, TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    auto appData = AppData(ctx);
    auto cacheCounters = GetServiceCounters(counters, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, cacheCounters);
    return CreateSchemeBoardSchemeCache(cacheConfig.Get());
}

class TPersQueueMetaCacheActor : public TActorBootstrapped<TPersQueueMetaCacheActor> {
    using TBase = TActorBootstrapped<TPersQueueMetaCacheActor>;
public:
    TPersQueueMetaCacheActor(TPersQueueMetaCacheActor&&) = default;
    TPersQueueMetaCacheActor& operator=(TPersQueueMetaCacheActor&&) = default;

    TPersQueueMetaCacheActor(const NMonitoring::TDynamicCounterPtr& counters,
                             const TDuration& versionCheckInterval)
        : Counters(counters)
        , VersionCheckInterval(versionCheckInterval)
        , Generation(std::make_shared<TAtomicCounter>(100))
    {
    }

    TPersQueueMetaCacheActor(
            const NActors::TActorId& schemeBoardCacheId,
            const TDuration& versionCheckInterval
    )
            : VersionCheckInterval(versionCheckInterval)
            , SchemeCacheId(schemeBoardCacheId)
            , Generation(std::make_shared<TAtomicCounter>())
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TPersQueueMetaCacheActor::StateFunc);

        if (!SchemeCacheId) {
            SchemeCacheId = Register(CreateSchemeCache(ctx, Counters));
        }

        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen())
            return;

        SkipVersionCheck = AppData(ctx)->PQConfig.GetMetaCacheSkipVersionCheck();

        PathPrefix = TopicPrefix(ctx);
        TopicsQuery = TStringBuilder() << "--!syntax_v1\n"
                                       << "DECLARE $Path as Utf8; DECLARE $Cluster as Utf8; "
                                       << "SELECT path, dc from `" << PathPrefix << "Config/V2/Topics` "
                                       << "WHERE path > $Path OR (path = $Path AND dc > $Cluster);";

        VersionQuery = TStringBuilder() << "--!syntax_v1\nSELECT version FROM `" << PathPrefix << "Config/V2/Versions` "
                                        << "WHERE name = 'Topics';";
        PathPrefixParts = NKikimr::SplitPath(PathPrefix);

        Reset(ctx);
    }

    ~TPersQueueMetaCacheActor() {
    }

private:

    void Reset(const TActorContext& ctx, bool error = true) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Metacache: reset");
        Generation->Inc();
        LastTopicKey = {};
        Type = EQueryType::ECheckVersion;
        ctx.Schedule(error ? QueryRetryInterval : VersionCheckInterval, new NActors::TEvents::TEvWakeup());
    }

    void RunQuery(EQueryType type, const TActorContext& ctx) {

        auto req = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        req->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        req->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        req->Record.MutableRequest()->SetKeepSession(false);
        req->Record.MutableRequest()->SetDatabase(NKikimr::NPQ::GetDatabaseFromConfig(AppData(ctx)->PQConfig));

        req->Record.MutableRequest()->MutableQueryCachePolicy()->set_keep_in_cache(true);
        req->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        req->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);

        Type = type;

        if (type == EQueryType::ECheckVersion) {
            req->Record.MutableRequest()->SetQuery(VersionQuery);
        } else {
            req->Record.MutableRequest()->SetQuery(TopicsQuery);
            NClient::TParameters params;
            params["$Path"] = LastTopicKey.Path;
            params["$Cluster"] = LastTopicKey.Cluster;
            req->Record.MutableRequest()->MutableParameters()->Swap(&params);
        }
        Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), req.Release(), 0, Generation->Val());
    }

    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        if (ev->Cookie != (ui64)Generation->Val()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "stale response with generation " << ev->Cookie << ", actual is " << Generation->Val());
            return;
        }
        const auto& record = ev->Get()->Record.GetRef();

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            LOG_ERROR_S(ctx, NKikimrServices::PQ_METACACHE,
                        "Got error trying to perform request: " << record);
            Reset(ctx);
            return;
        }

        switch (Type) {
            case EQueryType::ECheckVersion:
                return HandleCheckVersionResult(ev, ctx);
            case EQueryType::EGetTopics:
                return HandleGetTopicsResult(ev, ctx);
            default:
                Y_FAIL();
        }
    }

    void HandleQueryResponse(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        LOG_ERROR_S(ctx, NKikimrServices::PQ_METACACHE, "failed to list topics: " << record);

        Reset(ctx);
    }

    void HandleCheckVersionResult(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {

        const auto& record = ev->Get()->Record.GetRef();

        Y_VERIFY(record.GetResponse().GetResults().size() == 1);
        const auto& rr = record.GetResponse().GetResults(0).GetValue().GetStruct(0);
        ui64 newVersion = rr.ListSize() == 0 ? 0 : rr.GetList(0).GetStruct(0).GetOptional().GetInt64();

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got config version: " << newVersion);

        LastVersionUpdate = ctx.Now();
        if (newVersion > CurrentTopicsVersion || CurrentTopicsVersion == 0 || SkipVersionCheck) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got config version: " << newVersion);
            NewTopicsVersion = newVersion;
            RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            Reset(ctx, false);
        }
    }

    void HandleGetTopicsResult(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {

        const auto& record = ev->Get()->Record.GetRef();

        Y_VERIFY(record.GetResponse().GetResults().size() == 1);

        TString path, dc;
        const auto& rr = record.GetResponse().GetResults(0).GetValue().GetStruct(0);
        for (const auto& row : rr.GetList()) {

            path = row.GetStruct(0).GetOptional().GetText();
            dc = row.GetStruct(1).GetOptional().GetText();

            NewTopics.emplace_back(NPersQueue::BuildFullTopicName(path, dc));
        }
        if (rr.ListSize() > 0) {
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
            while (!ListTopicsWaiters.empty()) {
                auto& waiter = ListTopicsWaiters.front();
                ProcessDescribeAllTopics(waiter, ctx);
                ListTopicsWaiters.pop();
            }
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE,
                        "Updated topics list with : " << CurrentTopics.size() << " topics");
            Reset(ctx, false);
        }
    }


    void HandleGetVersion(TEvPqNewMetaCache::TEvGetVersionRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send version response: " << CurrentTopicsVersion);

        ctx.Send(ev->Sender, new TEvPqNewMetaCache::TEvGetVersionResponse{CurrentTopicsVersion});
    }

    void HandleDescribeTopics(TEvPqNewMetaCache::TEvDescribeTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe topics");
        const auto& msg = *ev->Get();

        SendSchemeCacheRequest(msg.Topics, !msg.PathPrefix.empty(), false, msg.SyncVersion, msg.ShowPrivate, ctx);
        auto inserted = DescribeTopicsWaiters.insert(std::make_pair(
                RequestId,
                TWaiter{ev->Sender, std::move(msg.Topics)}
        )).second;
        Y_VERIFY(inserted);
    }

    void HandleDescribeAllTopics(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() || ev->Get()->PathPrefix && ev->Get()->PathPrefix != PathPrefix) {
            auto* response = new TEvPqNewMetaCache::TEvDescribeAllTopicsResponse(ev->Get()->PathPrefix);
            response->Success = false;
            ctx.Send(ev->Sender, response);
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe all topics");

        if (!EverGotTopics) {
            ListTopicsWaiters.push(ev->Sender);
            return;
        }
        return ProcessDescribeAllTopics(ev->Sender, ctx);
    }

    void ProcessDescribeAllTopics(const TActorId& waiter, const TActorContext& ctx) {
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
            SendSchemeCacheRequest(CurrentTopics, true, true, false, false, ctx);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Store waiter");
        DescribeAllTopicsWaiters.push(waiter);
        FullTopicsCacheOutdated = false;
        FullTopicsCache = nullptr;
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        auto& result = ev->Get()->Request;
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle SchemeCache response"
            << ": result# " << result->ToString(*AppData()->TypeRegistry));

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

    void SendSchemeCacheRequest(const TVector<TString>& topics,
            bool addDefaultPathPrefix, bool isFullListingRequest, bool syncVersion, bool showPrivate,
            const TActorContext& ctx)
    {
        auto instant = isFullListingRequest ? 0 : ++RequestId;
        auto schemeCacheRequest = MakeHolder<NSchemeCache::TSchemeCacheNavigate>(instant);
        for (const auto& path : topics) {
            auto split = NKikimr::SplitPath(path);
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            if (addDefaultPathPrefix) {
                entry.Path.insert(entry.Path.end(), PathPrefixParts.begin(), PathPrefixParts.end());
            }

            entry.Path.insert(entry.Path.end(), split.begin(), split.end());
            entry.SyncVersion = syncVersion;
            entry.ShowPrivatePath = showPrivate;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
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

    void StartQuery(const TActorContext& ctx) {
        if (NewTopicsVersion > CurrentTopicsVersion) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Start topics rescan");
            RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            Y_VERIFY(NewTopicsVersion == CurrentTopicsVersion);
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Check version rescan");
            RunQuery(EQueryType::ECheckVersion, ctx);
        }
    }

public:
    void Die(const TActorContext& ctx) {
        TBase::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
          SFunc(NActors::TEvents::TEvWakeup, StartQuery)
          HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
          HFunc(NKqp::TEvKqp::TEvProcessResponse, HandleQueryResponse);
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


    NMonitoring::TDynamicCounterPtr Counters;
    TString VersionQuery;
    TString TopicsQuery;

    ui64 CurrentTopicsVersion = 0;
    ui64 NewTopicsVersion = 0;
    TTopicKey LastTopicKey = TTopicKey{};
    EQueryType Type = EQueryType::ECheckVersion;
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

IActor* CreatePQMetaCache(
        const NActors::TActorId& schemeBoardCacheId,
        const TDuration& versionCheckInterval
) {
    return new TPersQueueMetaCacheActor(schemeBoardCacheId, versionCheckInterval);
}

} // namespace NPqMetaCacheV2

} // namespace NKikimr::NMsgBusProxy
