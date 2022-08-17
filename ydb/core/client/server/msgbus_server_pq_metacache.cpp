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
#include <ydb/core/persqueue/cluster_tracker.h>


namespace NKikimr::NMsgBusProxy {

using namespace NYdb::NTable;

namespace NPqMetaCacheV2 {

using namespace NSchemeCache;


IActor* CreateSchemeCache(const TActorContext& ctx, TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    auto appData = AppData(ctx);
    auto cacheCounters = GetServiceCounters(counters, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<TSchemeCacheConfig>(appData, cacheCounters);
    return CreateSchemeBoardSchemeCache(cacheConfig.Get());
}

void CheckEntrySetHasTopicPath(auto* scNavigate) {
    for (auto& entry : scNavigate->ResultSet) {
        if (entry.PQGroupInfo && entry.PQGroupInfo->Description.HasPQTabletConfig()) {
            if (entry.PQGroupInfo->Description.GetPQTabletConfig().GetTopicPath().empty()) {
                auto* newGroupInfo = new NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo(*entry.PQGroupInfo);
                newGroupInfo->Description.MutablePQTabletConfig()->SetTopicPath(NKikimr::JoinPath(entry.Path));
                entry.PQGroupInfo.Reset(newGroupInfo);
            }
        }
    }
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

        auto& metaCacheConfig = AppData(ctx)->PQConfig.GetPQDiscoveryConfig();
        UseLbAccountAlias = metaCacheConfig.GetUseLbAccountAlias();
        DbRoot = metaCacheConfig.GetLbUserDatabaseRoot();
        if (!UseLbAccountAlias) {
            Y_FAIL("Not supported");
        }

        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            return;
        }
        SubscribeToClustersUpdate(ctx);
        if (!metaCacheConfig.GetLBFrontEnabled()) {
            return;
        }
        SkipVersionCheck = metaCacheConfig.GetCacheSkipVersionCheck();
        TStringBuf root = AppData(ctx)->PQConfig.GetRoot();
        root.SkipPrefix("/");
        root.ChopSuffix("/");
        TopicsQuery = TStringBuilder() << "--!syntax_v1\n"
                                       << "DECLARE $Path as Utf8; DECLARE $Cluster as Utf8; "
                                       << "SELECT path, dc from `/" << root << "/Config/V2/Topics` "
                                       << "WHERE path > $Path OR (path = $Path AND dc > $Cluster);";

        VersionQuery = TStringBuilder() << "--!syntax_v1\n"
                                           "SELECT version FROM `/" << root << "/Config/V2/Versions` "
                                           "WHERE name = 'Topics';";

    }

    ~TPersQueueMetaCacheActor() {
    }

private:

    void Reset(const TActorContext& ctx, bool error = true) {
        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen() || !AppData(ctx)->PQConfig.GetPQDiscoveryConfig().GetLBFrontEnabled()) {
            return;
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Metacache: reset");
        Generation->Inc();
        LastTopicKey = {};
        Type = EQueryType::ECheckVersion;
        ctx.Schedule(error ? QueryRetryInterval : VersionCheckInterval, new NActors::TEvents::TEvWakeup());
    }

    void SubscribeToClustersUpdate(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Subscribe to cluster tracker");
        Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe);
    }

    void HandleClustersUpdate(
            NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev,
            const TActorContext& ctx
    ) {
        if (!LocalCluster.empty()) {
            return;
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got clusters update");
        for (const auto& cluster : ev->Get()->ClustersList->Clusters) {
            if (cluster.IsLocal) {
                LocalCluster = cluster.Name;
                break;
            }
        }
       ConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
               AppData(ctx)->PQConfig, LocalCluster
       );
       Reset(ctx);
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

            NewTopics.emplace_back(decltype(NewTopics)::value_type{path, dc});
        }
        if (rr.ListSize() > 0) {
            LastTopicKey = {path, dc};
            return RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            LastTopicKey = {};
            CurrentTopics.clear();
            for (const auto& [path_, dc_] : NewTopics) {
                CurrentTopics.push_back(ConverterFactory->MakeDiscoveryConverter(path_, false, dc_));
            }
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
        ctx.Send(ev->Sender, new TEvPqNewMetaCache::TEvGetVersionResponse{CurrentTopicsVersion});
    }

private:
    enum class EWaiterType {
        DescribeAllTopics,
        DescribeCustomTopics
    };

    struct TWaiter {
        TActorId WaiterId;
        TString DbRoot;
        TVector<NPersQueue::TDiscoveryConverterPtr> Topics;
        TVector<ui64> SecondTryTopics;
        std::shared_ptr<TSchemeCacheNavigate> Result;
        EWaiterType Type;
        bool FirstRequestDone = false;
        bool SyncVersion;
        bool ShowPrivate;

        TWaiter(const TActorId& waiterId, const TString& dbRoot, bool syncVersion, bool showPrivate,
                const TVector<NPersQueue::TDiscoveryConverterPtr>& topics, EWaiterType type)

            : WaiterId(waiterId)
            , DbRoot(dbRoot)
            , Topics(topics)
            , Type(type)
            , SyncVersion(syncVersion)
            , ShowPrivate(showPrivate)
        {}

        bool ApplyResult(std::shared_ptr<TSchemeCacheNavigate>& result) {
            if (FirstRequestDone) {
                Y_VERIFY(Result != nullptr);
                Y_VERIFY(!SecondTryTopics.empty());
                ui64 i = 0;
                Y_VERIFY(result->ResultSet.size() == SecondTryTopics.size());
                for (ui64 index : SecondTryTopics) {
                    Y_VERIFY(Result->ResultSet[index].Status != TSchemeCacheNavigate::EStatus::Ok);
                    Result->ResultSet[index] = result->ResultSet[i++];
                }
                return true;
            }
            Y_VERIFY(Topics.size() == result->ResultSet.size());
            Y_VERIFY(Result == nullptr);
            FirstRequestDone = true;
            ui64 index = 0;
            Result = std::move(result);
            for (const auto& entry : Result->ResultSet) {
                if (DbRoot.empty()) {
                    continue;
                }
                if (entry.Status == TSchemeCacheNavigate::EStatus::PathErrorUnknown ||
                    entry.Status == TSchemeCacheNavigate::EStatus::RootUnknown
                ) {
                    auto account = Topics[index]->GetAccount_();
                    if (!account.Defined() || account->empty()) {
                        continue;
                    }
                    Topics[index]->SetDatabase(NKikimr::JoinPath({DbRoot, *account}));
                    if (!Topics[index]->GetSecondaryPath("").Defined()) {
                        continue;
                    }
                    SecondTryTopics.push_back(index);
                }
                index++;
            }
            //return true;
            return SecondTryTopics.empty(); //ToDo - second try topics
        }
        std::shared_ptr<TSchemeCacheNavigate>& GetResult() {
            Y_VERIFY(Result != nullptr);
            return Result;
        };

        TVector<TString> GetTopics() const {
            TVector<TString> ret;
            if (FirstRequestDone) {
                for (auto i: SecondTryTopics) {
                    auto account = Topics[i]->GetAccount_();
                    if (!account.Defined() || account->empty()) {
                        continue;
                    } else if (DbRoot.empty()) {
                        continue;
                    }
                    Topics[i]->SetDatabase(NKikimr::JoinPath({DbRoot, *account}));
                    auto second = Topics[i]->GetSecondaryPath("");
                    if (!second.Defined()) {
                        continue;
                    }
                    ret.push_back(*second);
                }
            } else {
                for (auto& t : Topics) {
                    ret.push_back(t->GetPrimaryPath());
                }
            }
            return ret;
        }
    };

private:
    void HandleDescribeTopics(TEvPqNewMetaCache::TEvDescribeTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe topics");
        const auto& msg = *ev->Get();

        for (auto& t : ev->Get()->Topics) {
            Y_VERIFY(t != nullptr);
        }
        SendSchemeCacheRequest(
                std::make_shared<TWaiter>(ev->Sender, DbRoot, msg.SyncVersion, msg.ShowPrivate, ev->Get()->Topics,
                                          EWaiterType::DescribeCustomTopics),
                ctx
        );
    }

    void HandleDescribeAllTopics(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe all topics");
        if (!EverGotTopics) {
            ListTopicsWaiters.push(ev->Sender);
            return;
        }
        return ProcessDescribeAllTopics(ev->Sender, ctx);
    }

    void ProcessDescribeAllTopics(const TActorId& waiter, const TActorContext& ctx) {
        if (EverGotTopics && CurrentTopics.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Describe all topics - send empty response");
            SendDescribeAllTopicsResponse(waiter, {}, ctx, true);
            return;
        }
        if (FullTopicsCache && !FullTopicsCacheOutdated) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Respond from cache");
            return SendDescribeAllTopicsResponse(waiter, CurrentTopics, ctx);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Describe all topics - send request");
        SendSchemeCacheRequest(
                std::make_shared<TWaiter>(waiter, DbRoot, false, false, CurrentTopics, EWaiterType::DescribeAllTopics),
                ctx
        );
        FullTopicsCacheOutdated = false;
        FullTopicsCache = nullptr;
    }

    void SendSchemeCacheRequest(std::shared_ptr<TWaiter> waiter, const TActorContext& ctx) {
        if (waiter->Type == EWaiterType::DescribeAllTopics && !waiter->FirstRequestDone) {
            DescribeAllTopicsWaiters.push(waiter);
            if (HaveDescribeAllTopicsInflight) {
                return;
            } else {
                HaveDescribeAllTopicsInflight = true;
            }
        }
        auto reqId = ++RequestId;
        auto schemeCacheRequest = std::make_unique<TSchemeCacheNavigate>(reqId);
        auto inserted = DescribeTopicsWaiters.insert(std::make_pair(reqId, waiter)).second;
        Y_VERIFY(inserted);
        for (const auto& path : waiter->GetTopics()) {
            auto split = NKikimr::SplitPath(path);
            Y_VERIFY(!split.empty());
            TSchemeCacheNavigate::TEntry entry;
            entry.Path.insert(entry.Path.end(), split.begin(), split.end());

            entry.SyncVersion = waiter->SyncVersion;
            entry.ShowPrivatePath = waiter->ShowPrivate;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
            schemeCacheRequest->ResultSet.emplace_back(std::move(entry));
        }

        ctx.Send(SchemeCacheId, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.release()));
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        std::shared_ptr<TSchemeCacheNavigate> result(ev->Get()->Request.Release());
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle SchemeCache response"
            << ": result# " << result->ToString(*AppData()->TypeRegistry));
        auto waiterIter = DescribeTopicsWaiters.find(result->Instant);
        Y_VERIFY(!waiterIter.IsEnd());
        auto& waiter = waiterIter->second;
        auto res = waiter->ApplyResult(result);
        if (!res) {
            // First attempt topics failed
            SendSchemeCacheRequest(waiter, ctx);
        } else if (waiter->Type == EWaiterType::DescribeAllTopics) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got describe all topics SC response");

            Y_VERIFY(HaveDescribeAllTopicsInflight);
            FullTopicsCacheOutdated = false;
            HaveDescribeAllTopicsInflight = false;
            for (const auto& entry : waiter->Result->ResultSet) {
                if (!entry.PQGroupInfo) {
                    continue;
                }

                const auto& desc = entry.PQGroupInfo->Description;
                if (desc.HasBalancerTabletID() && desc.GetBalancerTabletID() != 0) {
                    continue;
                }
                FullTopicsCacheOutdated = true;
            }
            FullTopicsCache = waiter->GetResult();
            CheckEntrySetHasTopicPath(FullTopicsCache.get());
 
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Updated topics cache with " << FullTopicsCache->ResultSet.size());
            while (DescribeAllTopicsWaiters) {
                SendDescribeAllTopicsResponse(DescribeAllTopicsWaiters.front()->WaiterId, waiter->Topics, ctx);
                DescribeAllTopicsWaiters.pop();
            }
        } else {
            auto& navigate = waiter->GetResult();
            Y_VERIFY(!waiterIter.IsEnd());

            Y_VERIFY(waiterIter->second->Topics.size() == navigate->ResultSet.size());
            CheckEntrySetHasTopicPath(navigate.get());
            auto *response = new TEvPqNewMetaCache::TEvDescribeTopicsResponse{
                    std::move(waiter->Topics), navigate
            };
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got describe topics SC response");
            ctx.Send(waiter->WaiterId, response);
            DescribeTopicsWaiters.erase(waiterIter);
        }
    }

    void SendDescribeAllTopicsResponse(const TActorId& recipient, TVector<NPersQueue::TDiscoveryConverterPtr> topics,
                                       const TActorContext& ctx, bool empty = false
    ) {
        TSchemeCacheNavigate* scResponse;
        if (empty) {
            scResponse = new TSchemeCacheNavigate();
        } else {
            scResponse = new TSchemeCacheNavigate(*FullTopicsCache);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send describe all topics response with " << scResponse->ResultSet.size() << " topics");
        auto* response = new TEvPqNewMetaCache::TEvDescribeAllTopicsResponse(
                AppData(ctx)->PQConfig.GetRoot(), std::move(topics), scResponse
        );
        ctx.Send(recipient, response);
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
          HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, HandleClustersUpdate)
          HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
          HFunc(NKqp::TEvKqp::TEvProcessResponse, HandleQueryResponse);
          HFunc(TEvPqNewMetaCache::TEvGetVersionRequest, HandleGetVersion)
          HFunc(TEvPqNewMetaCache::TEvDescribeTopicsRequest, HandleDescribeTopics)
          HFunc(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest, HandleDescribeAllTopics)
          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse)
    )

private:
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
    TVector<TTopicKey> NewTopics;
    TVector<NPersQueue::TDiscoveryConverterPtr> CurrentTopics;
    bool EverGotTopics = false;
    TDuration QueryRetryInterval = TDuration::Seconds(2);
    TDuration VersionCheckInterval = TDuration::Seconds(1);
    TInstant LastVersionUpdate = TInstant::Zero();

    TQueue<TActorId> ListTopicsWaiters;
    THashMap<ui64, std::shared_ptr<TWaiter>> DescribeTopicsWaiters;
    TQueue<std::shared_ptr<TWaiter>> DescribeAllTopicsWaiters;
    bool HaveDescribeAllTopicsInflight = false;
    ui64 RequestId = 1;

    std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> FullTopicsCache;
    bool FullTopicsCacheOutdated = false;
    NActors::TActorId SchemeCacheId;
    std::shared_ptr<TAtomicCounter> Generation;
    bool SkipVersionCheck = false;
    bool UseLbAccountAlias = false;
    TString LocalCluster;
    TString DbRoot;
    NPersQueue::TConverterFactoryPtr ConverterFactory;

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
