#include "msgbus_server_persqueue.h"
#include "msgbus_server_pq_metacache.h"

#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/pq_database.h>
#include <ydb/core/persqueue/public/cluster_tracker/cluster_tracker.h>

#include <ydb/library/actors/protos/actors.pb.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_reader.h>

namespace NKikimr::NMsgBusProxy {

using namespace NYdb::NTable;

namespace NPqMetaCacheV2 {

using namespace NSchemeCache;


IActor* CreateSchemeCache(const TActorContext& ctx, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
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
                newGroupInfo->Description.MutablePQTabletConfig()->SetTopicPath("/" + NKikimr::JoinPath(entry.Path));
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

    TPersQueueMetaCacheActor(const ::NMonitoring::TDynamicCounterPtr& counters)
        : Counters(counters)
        , Generation(std::make_shared<TAtomicCounter>(100))
    {
    }

    TPersQueueMetaCacheActor(const NActors::TActorId& schemeBoardCacheId)
            : SchemeCacheId(schemeBoardCacheId)
            , Generation(std::make_shared<TAtomicCounter>())
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PQ_META_CACHE;
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
            Y_ABORT("Not supported");
        }

        if (AppData(ctx)->PQConfig.GetTopicsAreFirstClassCitizen()) {
            ConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
                AppData(ctx)->PQConfig, TString{});
                return;
        }
        SubscribeToClustersUpdate(ctx);
        if (!metaCacheConfig.GetLBFrontEnabled()) {
            return;
        }
        if (metaCacheConfig.GetUseDynNodesMapping()) {
            TStringBuf tenant = AppData(ctx)->TenantName;
            tenant.SkipPrefix("/");
            tenant.ChopSuffix("/");
            if (tenant != "Root") {
                LOG_NOTICE_S(ctx, NKikimrServices::PQ_METACACHE, "Started on tenant = '" << tenant << "', will not request hive");
                OnDynNode = true;
            } else {
                StartHivePipe(ctx);
                ProcessNodesInfoWork(ctx);
            }
        }
    }

    ~TPersQueueMetaCacheActor() {
    }

private:
    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
        ProcessNodesInfoWork(ctx);
    }

    void SubscribeToClustersUpdate(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Subscribe to cluster tracker");
        Send(NPQ::NClusterTracker::MakeClusterTrackerID(), new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe);
    }

    void HandleClustersUpdate(
            NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate::TPtr& ev,
            const TActorContext& ctx
    ) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "HandleClustersUpdate");
        if (!LocalCluster.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "HandleClustersUpdate LocalCluster !LocalCluster.empty()");
            return;
        }
        for (const auto& cluster : ev->Get()->ClustersList->Clusters) {
            if (cluster.IsLocal) {
                LocalCluster = cluster.Name;
                break;
            }
        }
        ConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
               AppData(ctx)->PQConfig, LocalCluster
        );
        for (auto& ev : PendingTopicsRequests) {
            ProcessTopicsByNameRequest(ev);
        }
        PendingTopicsRequests.clear();
    }

    void StartHivePipe(const TActorContext& ctx) {
        auto hiveTabletId = GetHiveTabletId(ctx);
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Start pipe to hive tablet: " << hiveTabletId);
        auto pipeRetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeRetryPolicy.MaxRetryTime = TDuration::Seconds(1);
        NTabletPipe::TClientConfig pipeConfig{.RetryPolicy = pipeRetryPolicy};
        HivePipeClient = ctx.RegisterWithSameMailbox(
                NTabletPipe::CreateClient(ctx.SelfID, hiveTabletId, pipeConfig)
        );
    }

    void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        switch (ev->Get()->Status) {
            case NKikimrProto::EReplyStatus::OK:
            case NKikimrProto::EReplyStatus::ALREADY:
                break;
            default:
                return HandlePipeDestroyed(ctx);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Hive pipe connected");
        ProcessNodesInfoWork(ctx);
    }

    void HandlePipeDestroyed(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Hive pipe destroyed");
        NTabletPipe::CloseClient(ctx, HivePipeClient);
        HivePipeClient = TActorId();
        StartHivePipe(ctx);
        ResetHiveRequestState(ctx);
    }

private:
    struct TWaiter {
        TActorId WaiterId;
        TString DbRoot;
        TVector<NPersQueue::TDiscoveryConverterPtr> Topics;
        TVector<ui64> SecondTryTopics;
        std::shared_ptr<TSchemeCacheNavigate> Result;
        bool FirstRequestDone = false;
        bool SyncVersion;
        bool ShowPrivate;
        NWilson::TSpan Span;

        TWaiter(const TActorId& waiterId, const TString& dbRoot, bool syncVersion, bool showPrivate,
                const TVector<NPersQueue::TDiscoveryConverterPtr>& topics, NWilson::TSpan span = {})

            : WaiterId(waiterId)
            , DbRoot(dbRoot)
            , Topics(topics)
            , SyncVersion(syncVersion)
            , ShowPrivate(showPrivate)
            , Span(std::move(span))
        {}

        bool ApplyResult(std::shared_ptr<TSchemeCacheNavigate>& result) {
            if (FirstRequestDone) {
                Y_ABORT_UNLESS(Result != nullptr);
                Y_ABORT_UNLESS(!SecondTryTopics.empty());
                ui64 i = 0;
                Y_ABORT_UNLESS(result->ResultSet.size() == SecondTryTopics.size());
                for (ui64 index : SecondTryTopics) {
                    Y_ABORT_UNLESS(Result->ResultSet[index].Status != TSchemeCacheNavigate::EStatus::Ok);
                    Result->ResultSet[index] = result->ResultSet[i++];
                }
                return true;
            }
            Y_ABORT_UNLESS(Topics.size() == result->ResultSet.size());
            Y_ABORT_UNLESS(Result == nullptr);
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
            Y_ABORT_UNLESS(Result != nullptr);
            return Result;
        };

        TVector<std::pair<TString, TString>> GetTopics() const {
            TVector<std::pair<TString, TString>> ret;
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
                    ret.push_back(std::make_pair(*second, Topics[i]->GetDatabase().GetOrElse("")));
                }
            } else {
                for (auto& t : Topics) {
                    ret.push_back(std::make_pair(t->GetPrimaryPath(), t->GetDatabase().GetOrElse("")));
                }
            }
            return ret;
        }
    };

    enum class EWakeupTag {
        WakeForQuery = 1,
        WakeForHive = 2
    };
private:
    static ui64 GetHiveTabletId(const TActorContext& ctx) {
        return AppData(ctx)->DomainsInfo->GetHive();
    }

    void HandleDescribeTopics(TEvPqNewMetaCache::TEvDescribeTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe topics");
        const auto& msg = *ev->Get();

        for (auto& t : ev->Get()->Topics) {
            Y_ABORT_UNLESS(t != nullptr);
        }
        SendSchemeCacheRequest(
                std::make_shared<TWaiter>(ev->Sender, DbRoot, msg.SyncVersion, msg.ShowPrivate, ev->Get()->Topics,
                                          NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId(ev->TraceId), "Topic.SchemeCacheRequest", NWilson::EFlags::AUTO_END)));
    }

    void HandleDescribeTopicsByName(TEvPqNewMetaCache::TEvDescribeTopicsByNameRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle describe topics by name");

        if (!ConverterFactory) {
            PendingTopicsRequests.emplace_back(ev);
            return;
        }
        ProcessTopicsByNameRequest(ev);
    }

    void ProcessTopicsByNameRequest(TEvPqNewMetaCache::TEvDescribeTopicsByNameRequest::TPtr& ev) {
        TVector<NPersQueue::TDiscoveryConverterPtr> topics;
        topics.reserve(ev->Get()->Topics.size());
        for (auto& t : ev->Get()->Topics) {
            topics.emplace_back(ConverterFactory->MakeDiscoveryConverter(t, {}));
        }
        SendSchemeCacheRequest(
            std::make_shared<TWaiter>(
                ev->Sender, DbRoot, ev->Get()->SyncVersion, false, topics,
                NWilson::TSpan(
                    TWilsonTopic::TopicDetailed, NWilson::TTraceId(ev->TraceId), "Topic.SchemeCacheRequest",
                    NWilson::EFlags::AUTO_END
                )
            )
        );
    }

    void HandleGetNodesMapping(TEvPqNewMetaCache::TEvGetNodesMappingRequest::TPtr& ev, const TActorContext& ctx) {
        NodesMappingWaiters.emplace(std::move(ev->Sender));
        ProcessNodesInfoWork(ctx);
    }


    void SendSchemeCacheRequest(std::shared_ptr<TWaiter> waiter) {
        const auto& ctx = ActorContext();
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "SendSchemeCacheRequest");

        auto reqId = ++RequestId;
        auto schemeCacheRequest = std::make_unique<TSchemeCacheNavigate>(reqId);

        auto inserted = DescribeTopicsWaiters.insert(std::make_pair(reqId, waiter)).second;
        Y_ABORT_UNLESS(inserted);

        TMaybe<TString> db = {};

        for (const auto& [path, database] : waiter->GetTopics()) {
            if (!db) db = database;
            if (*db != database) db = "";
            auto split = NKikimr::SplitPath(path);
            TSchemeCacheNavigate::TEntry entry;
            if (!split.empty()) {
                entry.Path.insert(entry.Path.end(), split.begin(), split.end());
            }

            entry.SyncVersion = waiter->SyncVersion;
            entry.ShowPrivatePath = waiter->ShowPrivate;
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

            schemeCacheRequest->ResultSet.emplace_back(std::move(entry));
        }
        if (db) schemeCacheRequest->DatabaseName = *db;
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "send request for "
                             << waiter->GetTopics().size() << " topics, got " << DescribeTopicsWaiters.size() << " requests infly, db = \"" << db << "\"");

        ctx.Send(SchemeCacheId, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.release()), 0, 0, waiter->Span.GetTraceId());
    }

    void HandleSchemeCacheResponse(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        std::shared_ptr<TSchemeCacheNavigate> result(ev->Get()->Request.Release());
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Handle SchemeCache response" << ": result# " << result->ToString(*AppData()->TypeRegistry));
        auto waiterIter = DescribeTopicsWaiters.find(result->Instant);
        Y_ABORT_UNLESS(!waiterIter.IsEnd());
        auto waiter = waiterIter->second; //copy shared ptr
        auto res = waiter->ApplyResult(result);
        DescribeTopicsWaiters.erase(waiterIter);

        if (!res) {
            // First attempt topics failed
            SendSchemeCacheRequest(waiter);
        } else {
            auto& navigate = waiter->GetResult();

            Y_ABORT_UNLESS(waiter->Topics.size() == navigate->ResultSet.size());
            for (auto& entry : navigate->ResultSet) {
                if (entry.Status == TSchemeCacheNavigate::EStatus::Ok && entry.Kind == TSchemeCacheNavigate::KindTopic) {
                    Y_ABORT_UNLESS(entry.PQGroupInfo);
                }
            }
            CheckEntrySetHasTopicPath(navigate.get());
            auto* response = new TEvPqNewMetaCache::TEvDescribeTopicsResponse{
                    std::move(waiter->Topics), navigate
            };
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got describe topics SC response");
            ctx.Send(waiter->WaiterId, response);
        }
    }

    void ResetHiveRequestState(const TActorContext& ctx) {
        if (NextHiveRequestDeadline == TInstant::Zero()) {
            NextHiveRequestDeadline = ctx.Now() + TDuration::Seconds(5);
        }
        RequestedNodesInfo = false;
        ctx.Schedule(
                TDuration::Seconds(5),
                new NActors::TEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::WakeForHive))
        );
    }

    void ProcessNodesInfoWork(const TActorContext& ctx) {
        if (OnDynNode) {
            ProcessNodesInfoWaitersQueue(false, ctx);
            return;
        }
        if (DynamicNodesMapping != nullptr && LastNodesInfoUpdate != TInstant::Zero()) {
            const auto nextNodesUpdateTs = LastNodesInfoUpdate + TDuration::MilliSeconds(
                    AppData(ctx)->PQConfig.GetPQDiscoveryConfig().GetNodesMappingRescanIntervalMilliSeconds()
            );
            if (ctx.Now() < nextNodesUpdateTs)
                return ProcessNodesInfoWaitersQueue(true, ctx);
        }
        if (RequestedNodesInfo)
            return;

        if (NextHiveRequestDeadline != TInstant::Zero() && ctx.Now() < NextHiveRequestDeadline) {
            ResetHiveRequestState(ctx);
            return;
        }
        NextHiveRequestDeadline = ctx.Now() + TDuration::Seconds(5);
        RequestedNodesInfo = true;

        NActorsProto::TRemoteHttpInfo info;
        {
            auto* param = info.AddQueryParams();
            param->SetKey("page");
            param->SetValue("MemStateNodes");
        }
        {
            auto* param = info.AddQueryParams();
            param->SetKey("format");
            param->SetValue("json");
        }
        info.SetPath("/app");
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send Hive nodes state request");
        NTabletPipe::SendData(ctx, HivePipeClient, new NActors::NMon::TEvRemoteHttpInfo(info));
    }

    void HandleHiveMonResponse(NMon::TEvRemoteJsonInfoRes::TPtr& ev, const TActorContext& ctx) {
        ResetHiveRequestState(ctx);
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got Hive landing data response: '" << ev->Get()->Json << "'");
        TStringInput input(ev->Get()->Json);
        auto jsonValue = NJson::ReadJsonTree(&input, true);
        const auto& rootMap = jsonValue.GetMap();
        ui32 aliveNodes = rootMap.find("AliveNodes")->second.GetUInteger();
        if (!aliveNodes) {
            return;
        }
        const auto& nodes = rootMap.find("Nodes")->second.GetArray();
        TSet<ui32> staticNodeIds;
        TVector<ui32> dynamicNodes;
        ui64 maxStaticNodeId = 0;
        for (const auto& node : nodes) {
            const auto& nodeMap = node.GetMap();
            ui64 nodeId = nodeMap.find("Id")->second.GetUInteger();
            if (nodeMap.find("Domain")->second.GetString() == "/Root") {
                maxStaticNodeId = std::max(maxStaticNodeId, nodeId);
                if (nodeMap.find("Alive")->second.GetBoolean() && !nodeMap.find("Down")->second.GetBoolean()) {
                    staticNodeIds.insert(nodeId);
                }
            } else {
                dynamicNodes.push_back(nodeId);
            }
        }
        if (staticNodeIds.empty()) {
            return;
        }
        DynamicNodesMapping.reset(new THashMap<ui32, ui32>());
        for (auto& dynNodeId : dynamicNodes) {
            ui32 hash_ = dynNodeId % (maxStaticNodeId + 1);
            auto iter = staticNodeIds.lower_bound(hash_);
            DynamicNodesMapping->insert(std::make_pair(
                    dynNodeId,
                    iter == staticNodeIds.end() ? *staticNodeIds.begin() : *iter
            ));
        }
        LastNodesInfoUpdate = ctx.Now();
        ProcessNodesInfoWaitersQueue(true, ctx);
    }

    void ProcessNodesInfoWaitersQueue(bool status, const TActorContext& ctx) {
        if (DynamicNodesMapping == nullptr) {
            Y_ABORT_UNLESS(!status);
            DynamicNodesMapping.reset(new THashMap<ui32, ui32>);
        }
        while(!NodesMappingWaiters.empty()) {
            ctx.Send(NodesMappingWaiters.front(),
                     new TEvPqNewMetaCache::TEvGetNodesMappingResponse(DynamicNodesMapping, status));
            NodesMappingWaiters.pop();
        }
    }

public:
    void Die(const TActorContext& ctx) {
        TBase::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
          HFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
          HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, HandleClustersUpdate)
          HFunc(TEvPqNewMetaCache::TEvDescribeTopicsRequest, HandleDescribeTopics)
          HFunc(TEvPqNewMetaCache::TEvDescribeTopicsByNameRequest, HandleDescribeTopicsByName)
          HFunc(TEvPqNewMetaCache::TEvGetNodesMappingRequest, HandleGetNodesMapping)
          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse)

          HFunc(NMon::TEvRemoteJsonInfoRes, HandleHiveMonResponse)
          SFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed)
          HFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected)
    )

private:

    ::NMonitoring::TDynamicCounterPtr Counters;

    TQueue<TActorId> NodesMappingWaiters;
    THashMap<ui64, std::shared_ptr<TWaiter>> DescribeTopicsWaiters;
    TQueue<std::shared_ptr<TWaiter>> DescribeAllTopicsWaiters;
    ui64 RequestId = 1;

    NActors::TActorId SchemeCacheId;
    std::shared_ptr<TAtomicCounter> Generation;
    bool UseLbAccountAlias = false;
    TString LocalCluster;
    TString DbRoot;
    NPersQueue::TConverterFactoryPtr ConverterFactory;

    TActorId HivePipeClient;
    bool RequestedNodesInfo = false;
    TInstant NextHiveRequestDeadline = TInstant::Zero();
    TInstant LastNodesInfoUpdate = TInstant::Now();
    bool OnDynNode = false;

    std::shared_ptr<THashMap<ui32, ui32>> DynamicNodesMapping;
    TVector<TEvPqNewMetaCache::TEvDescribeTopicsByNameRequest::TPtr> PendingTopicsRequests;

};

IActor* CreatePQMetaCache(const NMonitoring::TDynamicCounterPtr& counters) {
    return new TPersQueueMetaCacheActor(counters);
}

IActor* CreatePQMetaCache(const NActors::TActorId& schemeBoardCacheId) {
    return new TPersQueueMetaCacheActor(schemeBoardCacheId);
}

} // namespace NPqMetaCacheV2

} // namespace NKikimr::NMsgBusProxy
