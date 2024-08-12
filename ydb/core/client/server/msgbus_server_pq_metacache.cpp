#include "msgbus_server_persqueue.h"
#include "msgbus_server_pq_metacache.h"

#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/cluster_tracker.h>

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

    TPersQueueMetaCacheActor(const ::NMonitoring::TDynamicCounterPtr& counters,
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
        //TODO: on start there will be additional delay for VersionCheckInterval
        ctx.Schedule(
                error ? QueryRetryInterval : VersionCheckInterval,
                new NActors::TEvents::TEvWakeup(static_cast<ui32>(EWakeupTag::WakeForQuery))
        );
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        auto tag = static_cast<EWakeupTag>(ev->Get()->Tag);
        switch (tag) {
            case EWakeupTag::WakeForQuery:
                return StartQuery(ctx);
            case EWakeupTag::WakeForHive:
                return ProcessNodesInfoWork(ctx);
        }
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
       Reset(ctx);
    }

    void StartHivePipe(const TActorContext& ctx) {
        auto hiveTabletId = GetHiveTabletId(ctx);
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Start pipe to hive tablet: " << hiveTabletId);
        auto pipeRetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        pipeRetryPolicy.MaxRetryTime = TDuration::Seconds(1);
        NTabletPipe::TClientConfig pipeConfig{pipeRetryPolicy};
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

            NYdb::TParams params = NYdb::TParamsBuilder()
                .AddParam("$Path")
                    .Utf8(LastTopicKey.Path)
                    .Build()
                .AddParam("$Cluster")
                    .Utf8(LastTopicKey.Cluster)
                    .Build()
                .Build();

            req->Record.MutableRequest()->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(params)));
        }
        Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), req.Release(), 0, Generation->Val());
    }

    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::PQ_METACACHE, "HandleQueryResponse TEvQueryResponse");

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
                Y_ABORT();
        }
    }

    void HandleCheckVersionResult(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {

        const auto& record = ev->Get()->Record.GetRef();

        Y_ABORT_UNLESS(record.GetResponse().GetResults().size() == 1);
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
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "HandleGetTopicsResult");

        const auto& record = ev->Get()->Record.GetRef();

        Y_ABORT_UNLESS(record.GetResponse().GetResults().size() == 1);
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
            CurrentTopicsFullConverters.clear();
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
                                          EWaiterType::DescribeCustomTopics),
                ctx
        );
    }

    void HandleDescribeAllTopics(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest::TPtr& ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "HandleDescribeAllTopics");
        if (!EverGotTopics) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "HandleDescribeAllTopics return due to !EverGotTopics");
            ListTopicsWaiters.push(ev->Sender);
            return;
        }
        return ProcessDescribeAllTopics(ev->Sender, ctx);
    }

    void HandleGetNodesMapping(TEvPqNewMetaCache::TEvGetNodesMappingRequest::TPtr& ev, const TActorContext& ctx) {
        NodesMappingWaiters.emplace(std::move(ev->Sender));
        ProcessNodesInfoWork(ctx);
    }

    void ProcessDescribeAllTopics(const TActorId& waiter, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "ProcessDescribeAllTopics");
        if (EverGotTopics && CurrentTopics.empty()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Describe all topics - send empty response");
            SendDescribeAllTopicsResponse(waiter, {}, ctx, true);
            return;
        }
        if (FullTopicsCache && !FullTopicsCacheOutdated) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Respond from cache");
            return SendDescribeAllTopicsResponse(waiter, CurrentTopicsFullConverters, ctx);
        }

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "ProcessDescribeAllTopics SendSchemeCacheRequest");
        SendSchemeCacheRequest(
                std::make_shared<TWaiter>(waiter, DbRoot, false, false, CurrentTopics, EWaiterType::DescribeAllTopics),
                ctx
        );
        FullTopicsCacheOutdated = false;
        FullTopicsCache = nullptr;
        CurrentTopicsFullConverters.clear();
    }

    void SendSchemeCacheRequest(std::shared_ptr<TWaiter> waiter, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "SendSchemeCacheRequest");
        if (waiter->Type == EWaiterType::DescribeAllTopics && !waiter->FirstRequestDone) {
            DescribeAllTopicsWaiters.push(waiter);
            if (HaveDescribeAllTopicsInflight) {
                LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "SendSchemeCacheRequest returns due to HaveDescribeAllTopicsInflight");
                return;
            } else {
                HaveDescribeAllTopicsInflight = true;
            }
        }
        auto reqId = ++RequestId;
        auto schemeCacheRequest = std::make_unique<TSchemeCacheNavigate>(reqId);

        auto inserted = DescribeTopicsWaiters.insert(std::make_pair(reqId, waiter)).second;
        Y_ABORT_UNLESS(inserted);

        for (const auto& [path, database] : waiter->GetTopics()) {
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

        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "send request for " << (waiter->Type == EWaiterType::DescribeAllTopics ? " all " : "") << waiter->GetTopics().size() << " topics, got " << DescribeTopicsWaiters.size() << " requests infly");

        ctx.Send(SchemeCacheId, new TEvTxProxySchemeCache::TEvNavigateKeySet(schemeCacheRequest.release()));
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
            SendSchemeCacheRequest(waiter, ctx);
        } else if (waiter->Type == EWaiterType::DescribeAllTopics) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got describe all topics SC response");

            Y_ABORT_UNLESS(HaveDescribeAllTopicsInflight);
            FullTopicsCacheOutdated = false;
            HaveDescribeAllTopicsInflight = false;
            for (const auto& entry : waiter->Result->ResultSet) {
                if (!entry.PQGroupInfo) {
                    FullTopicsCacheOutdated = true;
                    break;
                }

                const auto& desc = entry.PQGroupInfo->Description;
                if (!desc.HasBalancerTabletID() || desc.GetBalancerTabletID() == 0) {
                    FullTopicsCacheOutdated = true;
                    break;
                }
            }
            FullTopicsCache = waiter->GetResult();

            CheckEntrySetHasTopicPath(FullTopicsCache.get());
            auto factory = NPersQueue::TTopicNamesConverterFactory(AppData(ctx)->PQConfig, {});

            for (auto& entry : FullTopicsCache->ResultSet) {
                if (!entry.PQGroupInfo) {
                    CurrentTopicsFullConverters.push_back(nullptr);
                } else {

                    auto converter = factory.MakeTopicConverter(
                                                    entry.PQGroupInfo->Description.GetPQTabletConfig()
                                                );
                    CurrentTopicsFullConverters.push_back(converter);
                }
            }

            Y_ABORT_UNLESS(CurrentTopicsFullConverters.size() == FullTopicsCache->ResultSet.size());

            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Updated topics cache with " << FullTopicsCache->ResultSet.size());
            while (!DescribeAllTopicsWaiters.empty()) {
                SendDescribeAllTopicsResponse(DescribeAllTopicsWaiters.front()->WaiterId, CurrentTopicsFullConverters, ctx);
                DescribeAllTopicsWaiters.pop();
            }
        } else {
            auto& navigate = waiter->GetResult();

            Y_ABORT_UNLESS(waiter->Topics.size() == navigate->ResultSet.size());
            for (auto& entry : navigate->ResultSet) {
                if (entry.Status == TSchemeCacheNavigate::EStatus::Ok && entry.Kind == TSchemeCacheNavigate::KindTopic) {
                    Y_ABORT_UNLESS(entry.PQGroupInfo);
                }
            }
            CheckEntrySetHasTopicPath(navigate.get());
            auto *response = new TEvPqNewMetaCache::TEvDescribeTopicsResponse{
                    std::move(waiter->Topics), navigate
            };
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Got describe topics SC response");
            ctx.Send(waiter->WaiterId, response);
        }
    }

    void SendDescribeAllTopicsResponse(const TActorId& recipient, TVector<NPersQueue::TTopicConverterPtr> topics,
                                       const TActorContext& ctx, bool empty = false
    ) {
        std::shared_ptr<TSchemeCacheNavigate> scResponse;
        if (empty) {
            scResponse.reset(new TSchemeCacheNavigate());
        } else {
            scResponse = FullTopicsCache;
        }
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Send describe all topics response with " << scResponse->ResultSet.size() << " topics");
        auto* response = new TEvPqNewMetaCache::TEvDescribeAllTopicsResponse(
                AppData(ctx)->PQConfig.GetRoot(), std::move(topics), scResponse
        );
        ctx.Send(recipient, response);
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

    void StartQuery(const TActorContext& ctx) {
        if (NewTopicsVersion > CurrentTopicsVersion) {
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Start topics rescan");
            RunQuery(EQueryType::EGetTopics, ctx);
        } else {
            Y_ABORT_UNLESS(NewTopicsVersion == CurrentTopicsVersion);
            LOG_DEBUG_S(ctx, NKikimrServices::PQ_METACACHE, "Check version rescan");
            RunQuery(EQueryType::ECheckVersion, ctx);
        }
    }

public:
    void Die(const TActorContext& ctx) {
        TBase::Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
          HFunc(NActors::TEvents::TEvWakeup, HandleWakeup)
          HFunc(NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate, HandleClustersUpdate)
          HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
          HFunc(TEvPqNewMetaCache::TEvGetVersionRequest, HandleGetVersion)
          HFunc(TEvPqNewMetaCache::TEvDescribeTopicsRequest, HandleDescribeTopics)
          HFunc(TEvPqNewMetaCache::TEvDescribeAllTopicsRequest, HandleDescribeAllTopics)
          HFunc(TEvPqNewMetaCache::TEvGetNodesMappingRequest, HandleGetNodesMapping)
          HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleSchemeCacheResponse)

          HFunc(NMon::TEvRemoteJsonInfoRes, HandleHiveMonResponse)
          SFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed)
          HFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected)
    )

private:
    struct TTopicKey {
        TString Path;
        TString Cluster;
    };

    ::NMonitoring::TDynamicCounterPtr Counters;
    TString VersionQuery;
    TString TopicsQuery;

    ui64 CurrentTopicsVersion = 0;
    ui64 NewTopicsVersion = 0;
    TTopicKey LastTopicKey = TTopicKey{};
    EQueryType Type = EQueryType::ECheckVersion;
    TVector<TTopicKey> NewTopics;
    TVector<NPersQueue::TDiscoveryConverterPtr> CurrentTopics;
    TVector<NPersQueue::TTopicConverterPtr> CurrentTopicsFullConverters;
    bool EverGotTopics = false;
    TDuration QueryRetryInterval = TDuration::Seconds(2);
    TDuration VersionCheckInterval = TDuration::Seconds(1);
    TInstant LastVersionUpdate = TInstant::Zero();

    TQueue<TActorId> ListTopicsWaiters;
    TQueue<TActorId> NodesMappingWaiters;
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

    TActorId HivePipeClient;
    bool RequestedNodesInfo = false;
    TInstant NextHiveRequestDeadline = TInstant::Zero();
    TInstant LastNodesInfoUpdate = TInstant::Now();
    bool OnDynNode = false;

    std::shared_ptr<THashMap<ui32, ui32>> DynamicNodesMapping;

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
