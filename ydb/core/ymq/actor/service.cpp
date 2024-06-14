#include "service.h"

#include "auth_factory.h"
#include "cfg.h"
#include "executor.h"
#include "garbage_collector.h"
#include "local_rate_limiter_allocator.h"
#include "monitoring.h"
#include "params.h"
#include "proxy_service.h"
#include "queue_leader.h"
#include "queues_list_reader.h"
#include "user_settings_names.h"
#include "user_settings_reader.h"
#include "index_events_processor.h"
#include "node_tracker.h"
#include "cleanup_queue_data.h"

#include <ydb/public/lib/value/value.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/probes.h>
#include <ydb/core/ymq/base/secure_protobuf_printer.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/counters.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/logger/global/global.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/hostname.h>

LWTRACE_USING(SQS_PROVIDER);

template <>
struct THash<NKikimr::NSQS::TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr> : THash<const NActors::TEventHandle<NKikimr::NSQS::TSqsEvents::TEvGetLeaderNodeForQueueRequest>*> {
    using TParent = THash<const NActors::TEventHandle<NKikimr::NSQS::TSqsEvents::TEvGetLeaderNodeForQueueRequest>*>;
    using TParent::operator();
    size_t operator()(const NKikimr::NSQS::TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ptr) const {
        return TParent::operator()(ptr.Get());
    }
};

namespace NKikimr::NSQS {

using NKikimr::NClient::TValue;

const TString LEADER_CREATE_REASON_USER_REQUEST = "UserRequestOnNode";
const TString LEADER_CREATE_REASON_LOCAL_TABLET = "LocalTablet";
const TString LEADER_DESTROY_REASON_LAST_REF = "LastReference";
const TString LEADER_DESTROY_REASON_TABLET_ON_ANOTHER_NODE = "LeaderTabletOnAnotherNode";
const TString LEADER_DESTROY_REASON_REMOVE_INFO = "RemoveQueueInfo";

constexpr ui64 LIST_USERS_WAKEUP_TAG = 1;
constexpr ui64 LIST_QUEUES_WAKEUP_TAG = 2;
constexpr ui64 CONNECT_TIMEOUT_TO_LEADER_WAKEUP_TAG = 3;

constexpr size_t EARLY_REQUEST_USERS_LIST_MAX_BUDGET = 10;
constexpr i64 EARLY_REQUEST_QUEUES_LIST_MAX_BUDGET = 5; // per user

bool IsInternalFolder(const TString& folder) {
    return folder.StartsWith(".sys");
}

struct TSqsService::TQueueInfo : public TAtomicRefCount<TQueueInfo> {
    TQueueInfo(
            TString userName, TString queueName, TString rootUrl, ui64 leaderTabletId, bool isFifo, TString customName,
            TString folderId, ui32 tablesFormat, ui64 version, ui64 shardsCount, const TIntrusivePtr<TUserCounters>& userCounters,
            const TIntrusivePtr<TFolderCounters>& folderCounters,
            const TActorId& schemeCache, TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> quoterResourcesForUser,
            bool insertCounters, bool useLeaderCPUOptimization
    )
        : UserName_(std::move(userName))
        , QueueName_(std::move(queueName))
        , CustomName_(std::move(customName))
        , FolderId_(std::move(folderId))
        , TablesFormat_(tablesFormat)
        , Version_(version)
        , ShardsCount_(shardsCount)
        , RootUrl_(std::move(rootUrl))
        , LeaderTabletId_(leaderTabletId)
        , IsFifo_(isFifo)
        , Counters_(userCounters->CreateQueueCounters(QueueName_, FolderId_, insertCounters))
        , UserCounters_(userCounters)
        , FolderCounters_(folderCounters)
        , SchemeCache_(schemeCache)
        , QuoterResourcesForUser_(std::move(quoterResourcesForUser))
        , UseLeaderCPUOptimization(useLeaderCPUOptimization)
    {
    }

    bool LeaderMustBeOnCurrentNode() const {
        return LeaderNodeId_ && LeaderNodeId_.value() == SelfId().NodeId();
    }
    bool NeedStartLocalLeader() const {
        return !LocalLeader_ && (LocalLeaderRefCount_ > 0 || LeaderMustBeOnCurrentNode());
    }

    bool NeedStopLocalLeader() const {
        return LocalLeader_ && LocalLeaderRefCount_ == 0 && !LeaderMustBeOnCurrentNode();
    }

    void SetLeaderNodeId(ui32 nodeId) {
        LeaderNodeId_ = nodeId;
    }

    void LocalLeaderWayMoved() const {
        if (LocalLeader_) {
            TActivationContext::Send(new IEventHandle(LocalLeader_, SelfId(), new TSqsEvents::TEvForceReloadState()));
        }
    }

    void StartLocalLeader(const TString& reason) {
        Y_ABORT_UNLESS(!LocalLeader_);
        Counters_ = Counters_->GetCountersForLeaderNode();
        LWPROBE(CreateLeader, UserName_, QueueName_, reason);
        LocalLeader_ = TActivationContext::Register(new TQueueLeader(
            UserName_, QueueName_, FolderId_, RootUrl_, Counters_, UserCounters_,
            SchemeCache_, QuoterResourcesForUser_, UseLeaderCPUOptimization
        ));
        LOG_SQS_INFO("Start local leader [" << UserName_ << "/" << QueueName_ << "] actor " << LocalLeader_);

        if (FolderId_) {
            Y_ABORT_UNLESS(FolderCounters_);
            FolderCounters_->InitCounters();
            INC_COUNTER(FolderCounters_, total_count);
        }

        for (auto ev : GetConfigurationRequests_) {
            TActivationContext::Send(ev->Forward(LocalLeader_));
        }
        GetConfigurationRequests_.clear();
    }

    void StopLocalLeader(const TString& reason) {
        Y_ABORT_UNLESS(LocalLeader_);
        Counters_ = Counters_->GetCountersForNotLeaderNode();
        LWPROBE(DestroyLeader, UserName_, QueueName_, reason);
        LOG_SQS_INFO("Stop local leader [" << UserName_ << "/" << QueueName_ << "] actor " << LocalLeader_);
        TActivationContext::Send(new IEventHandle(LocalLeader_, SelfId(), new TEvPoisonPill()));
        LocalLeader_ = TActorId();
        if (FolderId_) {
            Y_ABORT_UNLESS(FolderCounters_);
            DEC_COUNTER(FolderCounters_, total_count);
        }
    }

    void IncLocalLeaderRef() {
        ++LocalLeaderRefCount_;
    }

    void DecLocalLeaderRef() {
        Y_ABORT_UNLESS(LocalLeaderRefCount_ > 0);
        --LocalLeaderRefCount_;
    }


    TActorIdentity SelfId() const {
        return TActorIdentity(TActivationContext::AsActorContext().SelfID);
    }

    TString UserName_;
    TString QueueName_;
    TString CustomName_;
    TString FolderId_;
    ui32 TablesFormat_;
    ui64 Version_;
    ui64 ShardsCount_;
    TString RootUrl_;
    ui64 LeaderTabletId_ = 0;
    bool IsFifo_ = false;
    TIntrusivePtr<TQueueCounters> Counters_;
    TIntrusivePtr<TUserCounters> UserCounters_;
    TIntrusivePtr<TFolderCounters> FolderCounters_;
    std::optional<ui32> LeaderNodeId_;
    ui64 NodeTrackingSubscriptionId = 0;

    TActorId LocalLeader_;
    TActorId SchemeCache_;
    ui64 LocalLeaderRefCount_ = 0;
    TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> QuoterResourcesForUser_;
    bool UseLeaderCPUOptimization;

    // State machine
    THashSet<TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr> GetLeaderNodeRequests_;
    TVector<TSqsEvents::TEvGetConfiguration::TPtr> GetConfigurationRequests_;
    TInstant NodeUnknownSince_ = TInstant::Now();

};

class TSqsService::TLocalLeaderManager {
public:
    TLocalLeaderManager(TIntrusivePtr<TMonitoringCounters> counters)
        : MaxInflight(Cfg().GetStartLocalLeaderInflightMax())
        , Counters(counters)
    {
    }
    void QueueRemoved(TQueueInfoPtr queue);
    void SetLeaderNodeId(TQueueInfoPtr queue, ui32 nodeId, TInstant now);
    void IncLocalLeaderRef(TQueueInfoPtr queue, const TString& reason, TInstant now);
    void DecLocalLeaderRef(TQueueInfoPtr queue, const TString& reason);
    void LocalLeaderStarted(TInstant now);
private:
    struct TAwaitingQueueInfo {
        TAwaitingQueueInfo(TQueueInfoPtr queue, TInstant since, const TString& reason)
            : Queue(queue)
            , Since(since)
            , Reason(reason)
        {}
        TQueueInfoPtr Queue;
        TInstant Since;
        const TString& Reason;
    };
private:
    void TryStartLocalLeader(TQueueInfoPtr queue, const TString& reason, TInstant waitSince, TInstant now);
    void ProcessAwaiting(TInstant now);

private:
    const ui64 MaxInflight;
    ui64 Inflight = 0;
    TDeque<TAwaitingQueueInfo> Awaiting;
    THashSet<TQueueInfoPtr> AlreadyAwaiting;
    TIntrusivePtr<TMonitoringCounters> Counters;
};


void TSqsService::TLocalLeaderManager::QueueRemoved(TQueueInfoPtr queue) {
    queue->LeaderNodeId_.reset();
    if (queue->NeedStopLocalLeader()) {
        queue->StopLocalLeader(LEADER_DESTROY_REASON_REMOVE_INFO);
    }
}

void TSqsService::TLocalLeaderManager::SetLeaderNodeId(TQueueInfoPtr queue, ui32 nodeId, TInstant now) {
    queue->SetLeaderNodeId(nodeId);
    if (queue->NeedStartLocalLeader()) {
        TryStartLocalLeader(queue, LEADER_CREATE_REASON_LOCAL_TABLET, now, now);
    } else if (queue->NeedStopLocalLeader()) {
        queue->StopLocalLeader(LEADER_DESTROY_REASON_TABLET_ON_ANOTHER_NODE);
    }
}

void TSqsService::TLocalLeaderManager::IncLocalLeaderRef(TQueueInfoPtr queue, const TString& reason, TInstant now) {
    queue->IncLocalLeaderRef();
    TryStartLocalLeader(queue, reason, now, now);
}

void TSqsService::TLocalLeaderManager::TryStartLocalLeader(TQueueInfoPtr queue, const TString& reason, TInstant waitSince, TInstant now) {
    if (queue->NeedStartLocalLeader()) {
        if (MaxInflight != 0 && Inflight >= MaxInflight) {
            if (!AlreadyAwaiting.count(queue)) {
                LOG_SQS_DEBUG("Queue [" << queue->UserName_ << "/" << queue->QueueName_ << "] is waiting for the leader to start, inflight=" << Inflight);
                Awaiting.emplace_back(queue, waitSince, reason);
                AlreadyAwaiting.insert(queue);
            }
        } else {
            ++Inflight;
            queue->StartLocalLeader(reason);
            Counters->LocalLeaderStartAwaitMs->Collect((now - waitSince).MilliSeconds());
        }
    }
    *Counters->LocalLeaderStartInflight = Inflight;
    *Counters->LocalLeaderStartQueue = Awaiting.size();
}

void TSqsService::TLocalLeaderManager::DecLocalLeaderRef(TQueueInfoPtr queue, const TString& reason) {
    queue->DecLocalLeaderRef();
    if (queue->NeedStopLocalLeader()) {
        queue->StopLocalLeader(reason);
    }
}

void TSqsService::TLocalLeaderManager::LocalLeaderStarted(TInstant now) {
    Y_ABORT_UNLESS(Inflight > 0);
    --Inflight;

    ProcessAwaiting(now);

    *Counters->LocalLeaderStartInflight = Inflight;
    *Counters->LocalLeaderStartQueue = Awaiting.size();
}

void TSqsService::TLocalLeaderManager::ProcessAwaiting(TInstant now) {
    while (!Awaiting.empty() && (MaxInflight == 0 || Inflight < MaxInflight)) {
        auto info = Awaiting.front();
        Awaiting.pop_front();
        AlreadyAwaiting.erase(info.Queue);
        TryStartLocalLeader(info.Queue, info.Reason, info.Since, now);
    }
}

struct TSqsService::TUserInfo : public TAtomicRefCount<TUserInfo> {
    TUserInfo(TString userName, TIntrusivePtr<TUserCounters> userCounters)
        : UserName_(std::move(userName))
        , Counters_(std::move(userCounters))
    {
    }

    void InitQuoterResources() {
        const auto& cfg = Cfg().GetQuotingConfig();
        if (cfg.GetEnableQuoting()) {
            Y_ABORT_UNLESS(cfg.HasLocalRateLimiterConfig() != cfg.HasKesusQuoterConfig()); // exactly one must be set
            if (cfg.HasLocalRateLimiterConfig()) { // the only one that is fully supported
                const auto& rates = cfg.GetLocalRateLimiterConfig().GetRates();
                // allocate resources
                CreateObjectsQuoterResource_ = TLocalRateLimiterResource(rates.GetCreateObjectsRate());
                DeleteObjectsQuoterResource_ = TLocalRateLimiterResource(rates.GetDeleteObjectsRate());
                OtherActionsQuoterResource_ = TLocalRateLimiterResource(rates.GetOtherRequestsRate());
                // fill map
                QuoterResources_ = new TSqsEvents::TQuoterResourcesForActions();
                {
                    TSqsEvents::TQuoterResourcesForActions::TResourceDescription res{TEvQuota::TResourceLeaf::QuoterSystem, CreateObjectsQuoterResource_};
                    QuoterResources_->ActionsResources.emplace(EAction::CreateUser, res);

                    // SQS-620
                    QuoterResources_->CreateQueueAction = res;
                }
                {
                    TSqsEvents::TQuoterResourcesForActions::TResourceDescription res{TEvQuota::TResourceLeaf::QuoterSystem, DeleteObjectsQuoterResource_};
                    QuoterResources_->ActionsResources.emplace(EAction::DeleteQueue, res);
                    QuoterResources_->ActionsResources.emplace(EAction::DeleteQueueBatch, res);
                    QuoterResources_->ActionsResources.emplace(EAction::DeleteUser, res);
                }
                QuoterResources_->OtherActions.QuoterId = TEvQuota::TResourceLeaf::QuoterSystem;
                QuoterResources_->OtherActions.ResourceId = OtherActionsQuoterResource_;
            }
        }
    }

    size_t CountQueuesInFolder(const TString& folderId) const {
        if (!folderId) {
            return QueueByNameAndFolder_.size(); // for YaSQS
        }

        return std::count_if(QueueByNameAndFolder_.begin(), QueueByNameAndFolder_.end(), [&folderId](const auto& p) { return p.first.second == folderId; });
    }

    TString UserName_;
    std::shared_ptr<const std::map<TString, TString>> Settings_ = std::make_shared<const std::map<TString, TString>>();
    TIntrusivePtr<TUserCounters> Counters_;
    std::map<TString, TSqsService::TQueueInfoPtr> Queues_;
    std::map<TString, TIntrusivePtr<TFolderCounters>> FolderCounters_;
    THashMap<std::pair<TString, TString>, TSqsService::TQueueInfoPtr> QueueByNameAndFolder_; // <custom name, folder id> -> queue info
    TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> QuoterResources_;
    TLocalRateLimiterResource CreateObjectsQuoterResource_;
    TLocalRateLimiterResource DeleteObjectsQuoterResource_;
    TLocalRateLimiterResource OtherActionsQuoterResource_;
    i64 EarlyRequestQueuesListBudget_ = EARLY_REQUEST_QUEUES_LIST_MAX_BUDGET; // Defence from continuously requesting queues list.
    bool UseLeaderCPUOptimization = true;

    // State machine
    THashMultiMap<TString, TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr> GetLeaderNodeRequests_; // queue name -> request
    THashMultiMap<TString, TSqsEvents::TEvGetConfiguration::TPtr> GetConfigurationRequests_; // queue name -> request
    THashMultiMap<std::pair<TString, TString>, TSqsEvents::TEvGetQueueId::TPtr> GetQueueIdRequests_; // <queue custom name, folder id> -> request
    THashMultiMap<TString, TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr> GetQueueFolderIdAndCustomNameRequests_; // queue name -> request
    THashMultiMap<TString, TSqsEvents::TEvCountQueues::TPtr> CountQueuesRequests_; // folder id -> request
};

static TString GetEndpoint(const NKikimrConfig::TSqsConfig& config) {
    const TString& endpoint = config.GetEndpoint();
    if (endpoint) {
        return endpoint;
    } else {
        return TStringBuilder() << "http://" << FQDNHostName() << ":" << config.GetHttpServerConfig().GetPort();
    }
}

void TSqsService::Bootstrap() {
    LOG_SQS_INFO("Start SQS service actor");
    LOG_SQS_DEBUG("SQS service config: " << Cfg());
    Become(&TSqsService::StateFunc);

    EarlyRequestUsersListBudget_ = EARLY_REQUEST_USERS_LIST_MAX_BUDGET;

    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(SQS_PROVIDER));

    RootUrl_ = GetEndpoint(Cfg());

    // Counters.
    SqsCoreCounters_ = GetSqsServiceCounters(AppData()->Counters, "core");
    YmqRootCounters_ = GetYmqPublicCounters(AppData()->Counters);
    AllocPoolCounters_ = std::make_shared<TAlignedPagePoolCounters>(AppData()->Counters, "sqs");
    AggregatedUserCounters_ = MakeIntrusive<TUserCounters>(
        Cfg(), SqsCoreCounters_, nullptr, AllocPoolCounters_, TOTAL_COUNTER_LABEL, nullptr, true
    );
    AggregatedUserCounters_->ShowDetailedCounters(TInstant::Max());
    MonitoringCounters_ = MakeIntrusive<TMonitoringCounters>(
        Cfg(), GetServiceCounters(AppData()->Counters, "sqs")->GetSubgroup("subsystem", "monitoring")
    );

    InitSchemeCache();
    NodeTrackerActor_ = Register(new TNodeTrackerActor(SchemeCache_));

    LocalLeaderManager = MakeHolder<TLocalLeaderManager>(MonitoringCounters_);

    Register(new TCleanupQueueDataActor(MonitoringCounters_));
    Register(new TMonitoringActor(MonitoringCounters_));

    Register(new TUserSettingsReader(AggregatedUserCounters_->GetTransactionCounters()));
    QueuesListReader_ = Register(new TQueuesListReader(AggregatedUserCounters_->GetTransactionCounters()));

    Register(CreateGarbageCollector(SchemeCache_, QueuesListReader_));

    RequestSqsUsersList();
    RequestSqsQueuesList();

    if (Cfg().HasYcSearchEventsConfig()) {
        auto& ycSearchCfg = Cfg().GetYcSearchEventsConfig();
        YcSearchEventsConfig.Enabled = ycSearchCfg.GetEnableYcSearch();

        YcSearchEventsConfig.ReindexInterval = TDuration::Seconds(ycSearchCfg.GetReindexIntervalSeconds());
        YcSearchEventsConfig.RescanInterval = TDuration::Seconds(ycSearchCfg.GetRescanIntervalSeconds());

        if (ycSearchCfg.HasTenantMode() && ycSearchCfg.GetTenantMode()) {
            YcSearchEventsConfig.Database = Cfg().GetRoot();
            YcSearchEventsConfig.TenantMode = true;
        }

        auto factory = AppData()->SqsAuthFactory;
        Y_ABORT_UNLESS(factory);

        MakeAndRegisterYcEventsProcessor();
    }
}

STATEFN(TSqsService::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        // Interface events
        hFunc(TSqsEvents::TEvGetLeaderNodeForQueueRequest, HandleGetLeaderNodeForQueueRequest);
        hFunc(TSqsEvents::TEvQueueLeaderDecRef, HandleQueueLeaderDecRef);
        hFunc(TSqsEvents::TEvGetQueueId, HandleGetQueueId);
        hFunc(TSqsEvents::TEvGetQueueFolderIdAndCustomName, HandleGetQueueFolderIdAndCustomName);
        hFunc(TSqsEvents::TEvCountQueues, HandleCountQueues);

        // Details
        hFunc(TEvWakeup, HandleWakeup);
        hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleDescribeSchemeResult);
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        hFunc(TSqsEvents::TEvReloadStateRequest, HandleReloadStateRequest);
        hFunc(TSqsEvents::TEvNodeTrackerSubscriptionStatus, HandleNodeTrackingSubscriptionStatus);
        hFunc(TSqsEvents::TEvGetConfiguration, HandleGetConfiguration);
        hFunc(TSqsEvents::TEvSqsRequest, HandleSqsRequest);
        hFunc(TSqsEvents::TEvInsertQueueCounters, HandleInsertQueueCounters);
        hFunc(TSqsEvents::TEvUserSettingsChanged, HandleUserSettingsChanged);
        hFunc(TSqsEvents::TEvLeaderStarted, HandleLeaderStarted);

        hFunc(TSqsEvents::TEvQueuesList, HandleQueuesList);
    default:
        LOG_SQS_ERROR("Unknown type of event came to SQS service actor: " << ev->Type << " (" << ev->GetTypeName() << "), sender: " << ev->Sender);
    }
}

void TSqsService::InitSchemeCache() {
    LOG_SQS_DEBUG("Enable scheme board scheme cache");
    auto cacheCounters = GetServiceCounters(AppData()->Counters, "sqs")->GetSubgroup("subsystem", "schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(AppData(), cacheCounters);
    SchemeCache_ = Register(CreateSchemeBoardSchemeCache(cacheConfig.Get()));
}

void TSqsService::ScheduleRequestSqsUsersList() {
    if (!ScheduledRequestingUsersList_) {
        ScheduledRequestingUsersList_ = true;
        const TInstant now = TActivationContext::Now();
        const TInstant whenToRequest = Max(LastRequestUsersListTime_ + TDuration::MilliSeconds(GetLeadersDescriberUpdateTimeMs()), now);
        Schedule(whenToRequest - now, new TEvWakeup(LIST_USERS_WAKEUP_TAG));
    }
}

void TSqsService::RequestSqsUsersList() {
    if (RequestingUsersList_) {
        return;
    }
    RequestingUsersList_ = true;
    LOG_SQS_INFO("Request SQS users list");
    THolder<TEvTxUserProxy::TEvNavigate> navigateRequest(new TEvTxUserProxy::TEvNavigate());
    NKikimrSchemeOp::TDescribePath* record = navigateRequest->Record.MutableDescribePath();
    record->SetPath(Cfg().GetRoot());
    Send(MakeTxProxyID(), navigateRequest.Release());
}

void TSqsService::ScheduleRequestSqsQueuesList() {
    if (!ScheduledRequestingQueuesList_) {
        ScheduledRequestingQueuesList_ = true;
        const TInstant now = TActivationContext::Now();
        const TInstant whenToRequest = Max(LastRequestQueuesListTime_ + TDuration::MilliSeconds(GetLeadersDescriberUpdateTimeMs()), now);
        Schedule(whenToRequest - now, new TEvWakeup(LIST_QUEUES_WAKEUP_TAG));
    }
}

void TSqsService::RequestSqsQueuesList() {
    if (!RequestingQueuesList_) {
        RequestingQueuesList_ = true;
        LOG_SQS_DEBUG("Request SQS queues list");
        Send(QueuesListReader_, new TSqsEvents::TEvReadQueuesList());
    }
}

Y_WARN_UNUSED_RESULT bool TSqsService::RequestQueueListForUser(const TUserInfoPtr& user, const TString& reqId) {
    if (RequestingQueuesList_) {
        return true;
    }
    const i64 budget = Min(user->EarlyRequestQueuesListBudget_, EarlyRequestQueuesListMinBudget_ + EARLY_REQUEST_QUEUES_LIST_MAX_BUDGET);
    if (budget <= EarlyRequestQueuesListMinBudget_) {
        RLOG_SQS_REQ_WARN(reqId, "No budget to request queues list for user [" << user->UserName_ << "]. Min budget: " << EarlyRequestQueuesListMinBudget_ << ". User's budget: " << user->EarlyRequestQueuesListBudget_);
        return false; // no budget
    }

    RLOG_SQS_REQ_DEBUG(reqId, "Using budget to request queues list for user [" << user->UserName_ << "]. Current budget: " << budget << ". Min budget: " << EarlyRequestQueuesListMinBudget_);
    user->EarlyRequestQueuesListBudget_ = budget - 1;
    RequestSqsQueuesList();
    return true;
}

void TSqsService::HandleGetLeaderNodeForQueueRequest(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev) {
    TUserInfoPtr user = GetUserOrWait(ev);
    if (!user) {
        return;
    }

    const TString& reqId = ev->Get()->RequestId;
    const TString& userName = ev->Get()->UserName;
    const TString& queueName = ev->Get()->QueueName;

    const auto queueIt = user->Queues_.find(queueName);
    if (queueIt == user->Queues_.end()) {
        LWPROBE(QueueRequestCacheMiss, userName, queueName, reqId, ev->Get()->ToStringHeader());
        if (RequestQueueListForUser(user, reqId)) {
            RLOG_SQS_REQ_DEBUG(reqId, "Queue [" << userName << "/" << queueName << "] was not found in sqs service list. Requesting queues list");
            user->GetLeaderNodeRequests_.emplace(queueName, std::move(ev));
        } else {
            AnswerThrottled(ev);
        }
        return;
    }

    auto queuePtr = queueIt->second;
    if (!queuePtr->LeaderNodeId_) {
        LWPROBE(QueueRequestCacheMiss, userName, queueName, reqId, ev->Get()->ToStringHeader());
        RLOG_SQS_REQ_DEBUG(reqId, "Queue [" << userName << "/" << queueName << "] is waiting for connection to leader tablet.");

        queuePtr->GetLeaderNodeRequests_.emplace(std::move(ev));
        if (QueuesWithGetNodeWaitingRequests.empty()) {
            Schedule(
                TDuration::MilliSeconds(Cfg().GetLeaderConnectTimeoutMs()),
                new TEvWakeup(CONNECT_TIMEOUT_TO_LEADER_WAKEUP_TAG)
            );
        }
        QueuesWithGetNodeWaitingRequests.insert(queuePtr);
        return;
    }

    const ui32 nodeId = queuePtr->LeaderNodeId_.value();
    RLOG_SQS_REQ_DEBUG(reqId, "Leader node for queue [" << userName << "/" << queueName << "] is " << nodeId);
    Send(ev->Sender, new TSqsEvents::TEvGetLeaderNodeForQueueResponse(reqId, userName, queueName, nodeId));
}

void TSqsService::HandleGetConfiguration(TSqsEvents::TEvGetConfiguration::TPtr& ev) {
    TUserInfoPtr user = GetUserOrWait(ev);
    if (!user) {
        return;
    }

    const TString& reqId = ev->Get()->RequestId;
    const TString& userName = ev->Get()->UserName;
    const TString& queueName = ev->Get()->QueueName;
    if (!queueName) { // common user configuration
        RLOG_SQS_REQ_DEBUG(reqId, "Asked common user [" << userName << "] configuration");
        AnswerNotExists(ev, user); // exists = false, but all configuration details are present
        return;
    }

    const auto queueIt = user->Queues_.find(queueName);
    if (queueIt != user->Queues_.end()) {
        ProcessConfigurationRequestForQueue(ev, user, queueIt->second);
        return;
    } else if (ev->Get()->FolderId) {
        const auto byNameAndFolderIt = user->QueueByNameAndFolder_ .find(
                std::make_pair(ev->Get()->QueueName, ev->Get()->FolderId)
        );
        if (byNameAndFolderIt != user->QueueByNameAndFolder_.end()) {
            ProcessConfigurationRequestForQueue(ev, user, byNameAndFolderIt->second);
            return;
        }
    }

    if (RequestQueueListForUser(user, reqId)) {
        LWPROBE(QueueRequestCacheMiss, userName, queueName, reqId, ev->Get()->ToStringHeader());
        RLOG_SQS_REQ_DEBUG(reqId, "Queue [" << userName << "/" << queueName << "] was not found in sqs service list. Requesting queues list");
        user->GetConfigurationRequests_.emplace(queueName, std::move(ev));
    } else if (ev->Get()->EnableThrottling) {
        AnswerThrottled(ev);
    } else {
        AnswerNotExists(ev, user);
    }
}

void TSqsService::AnswerNotExists(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo) {
    if (ev->Get()->UserName && ev->Get()->QueueName) {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No queue [" << ev->Get()->QueueName << "] found in user [" << ev->Get()->UserName << "] record");
    }
    auto answer = MakeHolder<TSqsEvents::TEvConfiguration>();
    answer->UserExists = userInfo != nullptr;
    answer->QueueExists = false;
    answer->RootUrl = RootUrl_;
    answer->SqsCoreCounters = SqsCoreCounters_;
    answer->UserCounters = userInfo ? userInfo->Counters_ : nullptr;
    answer->Fail = false;
    answer->SchemeCache = SchemeCache_;
    answer->QuoterResources = userInfo ? userInfo->QuoterResources_ : nullptr;
    Send(ev->Sender, answer.Release());
}

void TSqsService::AnswerNotExists(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev, const TUserInfoPtr& userInfo) {
    const TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus status = userInfo ? TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoQueue : TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoUser;
    if (status == TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoUser) {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No user [" << ev->Get()->UserName << "] found");
    } else {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No queue [" << ev->Get()->QueueName << "] found for user [" << ev->Get()->UserName << "]");
    }
    Send(ev->Sender,
        new TSqsEvents::TEvGetLeaderNodeForQueueResponse(ev->Get()->RequestId,
                                                         ev->Get()->UserName,
                                                         ev->Get()->QueueName,
                                                         status));
}

void TSqsService::AnswerNotExists(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev, const TUserInfoPtr& userInfo) {
    if (userInfo) {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No queue [" << ev->Get()->QueueName << "] found for user [" << ev->Get()->UserName << "] while getting queue folder id");
    } else {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No user [" << ev->Get()->UserName << "] found while getting queue folder id");
    }
    Send(ev->Sender, new TSqsEvents::TEvQueueFolderIdAndCustomName());
}

void TSqsService::AnswerNotExists(TSqsEvents::TEvGetQueueId::TPtr& ev, const TUserInfoPtr& userInfo) {
    if (userInfo) {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No queue with custom name [" << ev->Get()->CustomQueueName << "] and folder id [" << ev->Get()->FolderId << "] found for user [" << ev->Get()->UserName << "]");
    } else {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No user [" << ev->Get()->UserName << "] found while getting queue id");
    }
    Send(ev->Sender, new TSqsEvents::TEvQueueId());
}

void TSqsService::AnswerNotExists(TSqsEvents::TEvCountQueues::TPtr& ev, const TUserInfoPtr&) {
    RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "No user [" << ev->Get()->UserName << "] found while counting queues");
    Send(ev->Sender, new TSqsEvents::TEvCountQueuesResponse(false));
}

void TSqsService::AnswerFailed(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev, const TUserInfoPtr&) {
    Send(ev->Sender, new TSqsEvents::TEvGetLeaderNodeForQueueResponse(ev->Get()->RequestId, ev->Get()->UserName, ev->Get()->QueueName, TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::Error));
}

void TSqsService::AnswerFailed(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo) {
    auto answer = MakeHolder<TSqsEvents::TEvConfiguration>();
    answer->RootUrl = RootUrl_;
    answer->SqsCoreCounters = SqsCoreCounters_;
    answer->UserCounters = userInfo ? userInfo->Counters_ : nullptr;
    answer->Fail = true;
    answer->SchemeCache = SchemeCache_;
    answer->QuoterResources = userInfo ? userInfo->QuoterResources_ : nullptr;
    Send(ev->Sender, answer.Release());
}

void TSqsService::AnswerFailed(TSqsEvents::TEvGetQueueId::TPtr& ev, const TUserInfoPtr&) {
    Send(ev->Sender, new TSqsEvents::TEvQueueId(true));
}

void TSqsService::AnswerFailed(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev, const TUserInfoPtr&) {
    Send(ev->Sender, new TSqsEvents::TEvQueueFolderIdAndCustomName(true));
}

void TSqsService::AnswerFailed(TSqsEvents::TEvCountQueues::TPtr& ev, const TUserInfoPtr&) {
    Send(ev->Sender, new TSqsEvents::TEvCountQueuesResponse(true));
}

void TSqsService::AnswerThrottled(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr& ev) {
    const TString& reqId = ev->Get()->RequestId;
    const TString& userName = ev->Get()->UserName;
    const TString& queueName = ev->Get()->QueueName;
    RLOG_SQS_REQ_DEBUG(reqId, "Throttled because of too many requests for nonexistent queue [" << queueName << "] for user [" << userName << "] while getting leader node");
    Send(ev->Sender, new TSqsEvents::TEvGetLeaderNodeForQueueResponse(reqId, userName, queueName, TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::Throttled));
}

void TSqsService::AnswerThrottled(TSqsEvents::TEvGetConfiguration::TPtr& ev) {
    RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Throttled because of too many requests for nonexistent queue [" << ev->Get()->QueueName << "] for user [" << ev->Get()->UserName << "] while getting configuration");
    auto answer = MakeHolder<TSqsEvents::TEvConfiguration>();
    answer->Throttled = true;
    answer->SchemeCache = SchemeCache_;
    Send(ev->Sender, answer.Release());
}

void TSqsService::AnswerThrottled(TSqsEvents::TEvGetQueueId::TPtr& ev) {
    RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Throttled because of too many requests for nonexistent queue [" << ev->Get()->CustomQueueName << "] for user [" << ev->Get()->UserName << "] while getting queue id");
    Send(ev->Sender, new TSqsEvents::TEvQueueId(false, true));
}

void TSqsService::AnswerThrottled(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev) {
    RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Throttled because of too many requests for nonexistent queue [" << ev->Get()->QueueName << "] for user [" << ev->Get()->UserName << "] while getting folder id and custom name");
    Send(ev->Sender, new TSqsEvents::TEvQueueFolderIdAndCustomName(false, true));
}

void TSqsService::Answer(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev, const TQueueInfoPtr& queueInfo) {
    Send(ev->Sender, new TSqsEvents::TEvQueueFolderIdAndCustomName(queueInfo->FolderId_, queueInfo->CustomName_));
}

void TSqsService::AnswerLeaderlessConfiguration(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo, const TQueueInfoPtr& queueInfo) {
    auto answer = MakeHolder<TSqsEvents::TEvConfiguration>();
    answer->UserExists = true;
    answer->QueueExists = true;
    answer->RootUrl = RootUrl_;
    answer->SqsCoreCounters = SqsCoreCounters_;
    answer->QueueCounters = queueInfo->Counters_;
    answer->TablesFormat = queueInfo->TablesFormat_;
    answer->QueueVersion = queueInfo->Version_;
    answer->UserCounters = userInfo->Counters_;
    answer->Fail = false;
    answer->SchemeCache = SchemeCache_;
    answer->QuoterResources = queueInfo ? queueInfo->QuoterResourcesForUser_ : nullptr;
    Send(ev->Sender, answer.Release());
}

void TSqsService::ProcessConfigurationRequestForQueue(TSqsEvents::TEvGetConfiguration::TPtr& ev, const TUserInfoPtr& userInfo, const TQueueInfoPtr& queueInfo) {
    if (ev->Get()->Flags & TSqsEvents::TEvGetConfiguration::EFlags::NeedQueueLeader) {
        IncLocalLeaderRef(ev->Sender, queueInfo, LEADER_CREATE_REASON_USER_REQUEST);
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Forward configuration request to queue [" << queueInfo->UserName_ << "/" << queueInfo->QueueName_ << "] leader");
        if (queueInfo->LocalLeader_) {
            TActivationContext::Send(ev->Forward(queueInfo->LocalLeader_));
        } else {
            queueInfo->GetConfigurationRequests_.emplace_back(std::move(ev));
        }
    } else {
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Answer configuration for queue [" << queueInfo->UserName_ << "/" << queueInfo->QueueName_ << "] without leader");
        AnswerLeaderlessConfiguration(ev, userInfo, queueInfo);
    }
}

void TSqsService::HandleDescribeSchemeResult(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
    RequestingUsersList_ = false;
    LastRequestUsersListTime_ = TActivationContext::Now();
    const auto& record = ev->Get()->GetRecord();
    const auto& desc = record.GetPathDescription();

    LOG_SQS_DEBUG("Got info for main folder (user list): " << record);
    if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
        LOG_SQS_WARN("Failed to get user list: " << record);
        AnswerErrorToRequests();

        ScheduleRequestSqsUsersList();
        return;
    }

    THashSet<TString> usersNotProcessed;
    usersNotProcessed.reserve(Users_.size());
    for (const auto& [userName, userInfo] : Users_) {
        usersNotProcessed.insert(userName);
    }

    for (const auto& child : desc.children()) {
        if (child.GetPathType() == NKikimrSchemeOp::EPathTypeDir) {
            bool moved = false;
            TUserInfoPtr user = MutableUser(child.GetName(), true, &moved);
            usersNotProcessed.erase(child.GetName());
            if (moved) {
                if (RequestQueueListForUser(user, "")) {
                } else {
                    AnswerNoQueueToRequests(user);
                    AnswerCountQueuesRequests(user);
                }
            }
        }
    }
    AnswerNoUserToRequests();

    for (const TString& userName : usersNotProcessed) {
        RemoveUser(userName);
    }

    ScheduleRequestSqsUsersList();
}

void TSqsService::HandleQueueLeaderDecRef(TSqsEvents::TEvQueueLeaderDecRef::TPtr& ev) {
    DecLocalLeaderRef(ev->Sender, LEADER_DESTROY_REASON_LAST_REF);
}

void TSqsService::HandleGetQueueId(TSqsEvents::TEvGetQueueId::TPtr& ev) {
    TUserInfoPtr user = GetUserOrWait(ev);
    if (!user) {
        return;
    }

    const TString& reqId = ev->Get()->RequestId;
    const TString& userName = ev->Get()->UserName;
    const auto queueIt = user->QueueByNameAndFolder_.find(std::make_pair(ev->Get()->CustomQueueName, ev->Get()->FolderId));
    if (queueIt == user->QueueByNameAndFolder_.end()) {
        if (RequestQueueListForUser(user, reqId)) {
            RLOG_SQS_REQ_DEBUG(reqId,
                                "Queue with custom name [" << ev->Get()->CustomQueueName << "] and folder id ["
                                << ev->Get()->FolderId << "] was not found in sqs service list for user ["
                                << userName << "]. Requesting queues list");
            user->GetQueueIdRequests_.emplace(std::make_pair(ev->Get()->CustomQueueName, ev->Get()->FolderId), std::move(ev));
        } else
            AnswerThrottled(ev);
        return;
    }

    const auto& info = *queueIt->second;
    RLOG_SQS_REQ_DEBUG(
        reqId,
        "Queue id is " << info.QueueName_ << " and version is " << info.Version_
            << " with shards count: " << info.ShardsCount_ << " tables format: " << info.TablesFormat_
    );
    Send(
        ev->Sender,
        new TSqsEvents::TEvQueueId(info.QueueName_, info.Version_, info.ShardsCount_, info.TablesFormat_)
    );
}

void TSqsService::HandleGetQueueFolderIdAndCustomName(
    TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr& ev) {
    TUserInfoPtr user = GetUserOrWait(ev);
    if (!user) {
        return;
    }

    const TString& reqId = ev->Get()->RequestId;
    const TString& userName = ev->Get()->UserName;
    const TString& queueName = ev->Get()->QueueName;
    const auto queueIt = user->Queues_.find(queueName);
    if (queueIt == user->Queues_.end()) {
        if (RequestQueueListForUser(user, reqId)) {
            LWPROBE(QueueRequestCacheMiss, userName, queueName, reqId, ev->Get()->ToStringHeader());
            RLOG_SQS_REQ_DEBUG(reqId, "Queue [" << userName << "/" << queueName << "] was not found in sqs service list. Requesting queues list");
            user->GetQueueFolderIdAndCustomNameRequests_.emplace(queueName, std::move(ev));
        } else {
            AnswerThrottled(ev);
        }
        return;
    }

    Answer(ev, queueIt->second);
}

void TSqsService::HandleCountQueues(TSqsEvents::TEvCountQueues::TPtr& ev) {
    TUserInfoPtr user = GetUserOrWait(ev);
    if (!user) {
        return;
    }

    Send(ev->Sender, new TSqsEvents::TEvCountQueuesResponse(false, true, user->CountQueuesInFolder(ev->Get()->FolderId)));
}

template <class TEvent>
TSqsService::TUserInfoPtr TSqsService::GetUserOrWait(TAutoPtr<TEvent>& ev) {
    const TString& reqId = ev->Get()->RequestId;
    const TString& userName = ev->Get()->UserName;
    if (!userName) { // common configuration
        RLOG_SQS_REQ_DEBUG(reqId, "Asked common request " << ev->Get()->ToStringHeader());
        AnswerNotExists(ev, nullptr);
        return nullptr;
    }

    const auto userIt = Users_.find(userName);
    if (userIt == Users_.end()) {
        if (!RequestingUsersList_) {
            RLOG_SQS_REQ_DEBUG(reqId, "User [" << userName << "] was not found in sqs service list. EarlyRequestUsersListBudget: " << EarlyRequestUsersListBudget_);
            if (EarlyRequestUsersListBudget_ > 0) {
                --EarlyRequestUsersListBudget_;
                RequestSqsUsersList();
            }
        }
        if (RequestingUsersList_) {
            LWPROBE(QueueRequestCacheMiss, userName, "", reqId, ev->Get()->ToStringHeader());
            RLOG_SQS_REQ_DEBUG(reqId, "User [" << userName << "] was not found in sqs service list. Wait for user list answer");
            InsertWaitingRequest(std::move(ev));
        } else {
            RLOG_SQS_REQ_DEBUG(reqId, "User [" << userName << "] was not found in sqs service list");
            AnswerNotExists(ev, nullptr);
        }
        return nullptr;
    }
    return userIt->second;
}

void TSqsService::HandleReloadStateRequest(TSqsEvents::TEvReloadStateRequest::TPtr& ev) {
    const auto userIt = Users_.find(ev->Get()->Record.GetTarget().GetUserName());
    if (userIt != Users_.end()) {
        auto queueIt = userIt->second->Queues_.find(ev->Get()->Record.GetTarget().GetQueueName());
        if (queueIt != userIt->second->Queues_.end()) {
            if (queueIt->second->LocalLeader_) {
                Send(ev->Forward(queueIt->second->LocalLeader_));
                return;
            }
        }
    }
}

void TSqsService::HandleNodeTrackingSubscriptionStatus(TSqsEvents::TEvNodeTrackerSubscriptionStatus::TPtr& ev) {
    ui64 subscriptionId = ev->Get()->SubscriptionId;
    auto it = QueuePerNodeTrackingSubscription.find(subscriptionId);
    if (it == QueuePerNodeTrackingSubscription.end()) {
        LOG_SQS_WARN("Get node tracking status for unknown subscription id: " << subscriptionId);
        Send(NodeTrackerActor_, new TSqsEvents::TEvNodeTrackerUnsubscribeRequest(subscriptionId));
        return;
    }
    auto queuePtr = it->second;
    auto& queue = *queuePtr;
    auto nodeId = ev->Get()->NodeId;
    bool disconnected = ev->Get()->Disconnected;
    LocalLeaderManager->SetLeaderNodeId(queuePtr, nodeId, TActivationContext::Now());
    if (disconnected) {
        queue.LocalLeaderWayMoved();
    }
    LOG_SQS_DEBUG(
        "Got node leader for queue [" << queue.UserName_ << "/" << queue.QueueName_
        << "]. Node: " << nodeId << " subscription id: " << subscriptionId
    );
    for (auto& req : queue.GetLeaderNodeRequests_) {
        RLOG_SQS_REQ_DEBUG(req->Get()->RequestId, "Got node leader. Node id: " << nodeId);
        Send(req->Sender, new TSqsEvents::TEvGetLeaderNodeForQueueResponse(req->Get()->RequestId, req->Get()->UserName, req->Get()->QueueName, nodeId));
    }
    queue.GetLeaderNodeRequests_.clear();
    QueuesWithGetNodeWaitingRequests.erase(queuePtr);
}

void TSqsService::HandleLeaderStarted(TSqsEvents::TEvLeaderStarted::TPtr&) {
    LocalLeaderManager->LocalLeaderStarted(TActivationContext::Now());
}

void TSqsService::HandleQueuesList(TSqsEvents::TEvQueuesList::TPtr& ev) {
    RequestingQueuesList_ = false;
    LastRequestQueuesListTime_ = TActivationContext::Now();
    ScheduleRequestSqsQueuesList();
    if (ev->Get()->Success) {
        auto newListIt = ev->Get()->SortedQueues.begin();
        auto usersIt = Users_.begin();
        while (newListIt != ev->Get()->SortedQueues.end() || usersIt != Users_.end()) {
            if (usersIt == Users_.end() || newListIt != ev->Get()->SortedQueues.end() && newListIt->UserName < usersIt->second->UserName_) {
                usersIt = MutableUserIter(newListIt->UserName); // insert new user
            }
            const TUserInfoPtr user = usersIt->second;
            auto oldListIt = user->Queues_.begin();
            while (oldListIt != user->Queues_.end() && newListIt != ev->Get()->SortedQueues.end() && newListIt->UserName == user->UserName_) {
                if (oldListIt->first == newListIt->QueueName) { // the same queue
                    if (oldListIt->second->Version_ != newListIt->Version) {
                        LOG_SQS_WARN("Queue version for queue " << oldListIt->first << " has been changed from "
                                   << oldListIt->second->Version_ << " to " << newListIt->Version << " (queue was recreated)");
                        THashSet<TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr> oldQueueRequests;
                        oldQueueRequests.swap(oldListIt->second->GetLeaderNodeRequests_);

                        RemoveQueue(user->UserName_, newListIt->QueueName);
                        oldListIt = AddQueue(user->UserName_,
                                           newListIt->QueueName,
                                           newListIt->LeaderTabletId,
                                           newListIt->CustomName,
                                           newListIt->FolderId,
                                           newListIt->TablesFormat,
                                           newListIt->Version,
                                           newListIt->ShardsCount,
                                           newListIt->CreatedTimestamp,
                                           newListIt->IsFifo);
                        oldQueueRequests.swap(oldListIt->second->GetLeaderNodeRequests_);
                        if (!oldListIt->second->GetLeaderNodeRequests_.empty()) {
                            QueuesWithGetNodeWaitingRequests.insert(oldListIt->second);
                        }
                    }
                    ++oldListIt;
                    ++newListIt;
                } else if (oldListIt->first < newListIt->QueueName) {
                    const TString name = oldListIt->first;
                    ++oldListIt;
                    RemoveQueue(user->UserName_, name);
                } else {
                    oldListIt = AddQueue(user->UserName_,
                                       newListIt->QueueName,
                                       newListIt->LeaderTabletId,
                                       newListIt->CustomName,
                                       newListIt->FolderId,
                                       newListIt->TablesFormat,
                                       newListIt->Version,
                                       newListIt->ShardsCount,
                                       newListIt->CreatedTimestamp,
                                       newListIt->IsFifo);
                    ++oldListIt;
                    ++newListIt;
                }
            }
            while (oldListIt != user->Queues_.end()) {
                TString name = oldListIt->first;
                ++oldListIt;
                RemoveQueue(user->UserName_, name);
            }
            while (newListIt != ev->Get()->SortedQueues.end() && newListIt->UserName == user->UserName_) {
                AddQueue(user->UserName_,
                         newListIt->QueueName,
                         newListIt->LeaderTabletId,
                         newListIt->CustomName,
                         newListIt->FolderId,
                         newListIt->TablesFormat,
                         newListIt->Version,
                         newListIt->ShardsCount,
                         newListIt->CreatedTimestamp,
                         newListIt->IsFifo);
                ++newListIt;
            }

            // answer to all CountQueues requests
            AnswerCountQueuesRequests(user);
            AnswerNoQueueToRequests(user);

            if (usersIt != Users_.end()) {
                ++usersIt;
            }
        }

        NotifyLocalDeadLetterQueuesLeaders(ev->Get()->SortedQueues);
    } else {
        for (const auto& [userName, user] : Users_) {
            AnswerErrorToRequests(user);
        }
    }
}

void TSqsService::NotifyLocalDeadLetterQueuesLeaders(const std::vector<TSqsEvents::TEvQueuesList::TQueueRecord>& sortedQueues) const {
    using TKnownDeadLetterQueues = THashMap<TString, THashSet<std::pair<TString, TString>>>;

    TKnownDeadLetterQueues knownDlqs;
    for (const auto& queueRecord : sortedQueues) {
        if (queueRecord.DlqName) {
            knownDlqs[queueRecord.UserName].insert(std::make_pair(queueRecord.DlqName, queueRecord.FolderId)); // account -> custom name + folder id pair
        }
    }

    for (const auto& [account, dlqs] : knownDlqs) {
        auto accountIt = Users_.find(account);
        if (accountIt != Users_.end()) {
            for (const auto& customNameAndFolderPair : dlqs) {
                auto queueInfoIt = accountIt->second->QueueByNameAndFolder_.find(customNameAndFolderPair);
                if (queueInfoIt != accountIt->second->QueueByNameAndFolder_.end()) {
                    const auto& queueInfo = *queueInfoIt->second;
                    if (queueInfo.LocalLeader_) {
                        Send(queueInfo.LocalLeader_, new TSqsEvents::TEvDeadLetterQueueNotification);
                    }
                }
            }
        }
    }
}

void TSqsService::AnswerCountQueuesRequests(const TUserInfoPtr& user) {
    while (!user->CountQueuesRequests_.empty()) {
        const TString folderId = user->CountQueuesRequests_.begin()->first;
        const auto queuesCount = user->CountQueuesInFolder(folderId);

        auto requests = user->CountQueuesRequests_.equal_range(folderId);

        for (auto i = requests.first; i != requests.second; ++i) {
            auto& req = i->second;
            Send(req->Sender, new TSqsEvents::TEvCountQueuesResponse(false, true, queuesCount));
        }

        user->CountQueuesRequests_.erase(requests.first, requests.second);
    }
}

void TSqsService::HandleUserSettingsChanged(TSqsEvents::TEvUserSettingsChanged::TPtr& ev) {
    LOG_SQS_TRACE("User [" << ev->Get()->UserName << "] settings changed. Changed " << ev->Get()->Diff->size() << " items");
    auto user = MutableUser(ev->Get()->UserName, false);
    const auto& diff = ev->Get()->Diff;
    const auto& newSettings = ev->Get()->Settings;
    if (IsIn(*diff, USER_SETTING_DISABLE_COUNTERS)) {
        const auto value = newSettings->find(USER_SETTING_DISABLE_COUNTERS);
        Y_ABORT_UNLESS(value != newSettings->end());
        const bool disableCounters = FromStringWithDefault(value->second, false);
        user->Counters_->DisableCounters(disableCounters);
    }

    if (IsIn(*diff, USER_SETTING_SHOW_DETAILED_COUNTERS_DEADLINE_MS)) {
        const auto value = newSettings->find(USER_SETTING_SHOW_DETAILED_COUNTERS_DEADLINE_MS);
        Y_ABORT_UNLESS(value != newSettings->end());
        const ui64 deadline = FromStringWithDefault(value->second, 0ULL);
        user->Counters_->ShowDetailedCounters(TInstant::MilliSeconds(deadline));
    }

    if (IsIn(*diff, USER_SETTING_EXPORT_TRANSACTION_COUNTERS)) {
        const auto value = newSettings->find(USER_SETTING_EXPORT_TRANSACTION_COUNTERS);
        Y_ABORT_UNLESS(value != newSettings->end());
        const bool needExport = FromStringWithDefault(value->second, false);
        user->Counters_->ExportTransactionCounters(needExport);
    }

    if (IsIn(*diff, USE_CPU_LEADER_OPTIMIZATION)) {
        const auto value = newSettings->find(USE_CPU_LEADER_OPTIMIZATION);
        Y_ABORT_UNLESS(value != newSettings->end());
        const bool use = FromStringWithDefault(value->second, false);
        user->UseLeaderCPUOptimization = use;
        for (auto queue : user->Queues_) {
            queue.second->UseLeaderCPUOptimization = use;
        }
    }
}

TSqsService::TUserInfoPtr TSqsService::MutableUser(const TString& userName, bool moveUserRequestsToUserRecord, bool* requestsWereMoved) {
    return MutableUserIter(userName, moveUserRequestsToUserRecord, requestsWereMoved)->second;
}

TSqsService::TUsersMap::iterator TSqsService::MutableUserIter(const TString& userName, bool moveUserRequestsToUserRecord, bool* requestsWereMoved) {
    auto userIt = Users_.find(userName);
    if (userIt == Users_.end()) {
        LOG_SQS_INFO("Creating user info record for user [" << userName << "]");
        bool isInternal = IsInternalFolder(userName);
        if (isInternal) {
            LOG_SQS_INFO("[" << userName << "] is considered and internal service folder, will not create YMQ counters");
        }
        TUserInfoPtr user = new TUserInfo(
                userName,
                new TUserCounters(
                        Cfg(), SqsCoreCounters_,
                        isInternal ? nullptr : YmqRootCounters_,
                        AllocPoolCounters_, userName, AggregatedUserCounters_, false
                )
        );
        user->InitQuoterResources();
        userIt = Users_.emplace(userName, user).first;

        if (moveUserRequestsToUserRecord) {
            // move user's requests to user info
            size_t moved = 0;
            moved += MoveUserRequests(user, GetLeaderNodeRequests_);
            moved += MoveUserRequests(user, GetConfigurationRequests_);
            moved += MoveUserRequests(user, GetQueueIdRequests_);
            moved += MoveUserRequests(user, GetQueueFolderIdAndCustomNameRequests_);
            moved += MoveUserRequests(user, CountQueuesRequests_);

            if (requestsWereMoved) {
                *requestsWereMoved = moved != 0;
            }
        }
    }
    return userIt;
}

void TSqsService::RemoveUser(const TString& userName) {
    const auto userIt = Users_.find(userName);
    if (userIt == Users_.end()) {
        return;
    }

    LOG_SQS_INFO("Removing user info record for user [" << userName << "]");
    const auto user = userIt->second;
    while (!user->Queues_.empty()) {
        TString queueName = user->Queues_.begin()->first;
        RemoveQueue(userName, queueName);
    }

    AnswerNoQueueToRequests(user);
    for (auto&& [folderId, req] : user->CountQueuesRequests_) {
        Send(req->Sender, new TSqsEvents::TEvCountQueuesResponse(false));
    }
    user->CountQueuesRequests_.clear();

    user->Counters_->RemoveCounters();
    Users_.erase(userIt);
}

void TSqsService::RemoveQueue(const TString& userName, const TString& queue) {
    LOG_SQS_INFO("Removing queue record for queue [" << userName << "/" << queue << "]");
    const auto userIt = Users_.find(userName);
    if (userIt == Users_.end()) {
        LOG_SQS_WARN("Attempt to remove queue record for queue [" << userName << "/" << queue << "], but there is no user record");
        return;
    }
    const auto queueIt = userIt->second->Queues_.find(queue);
    if (queueIt == userIt->second->Queues_.end()) {
        LOG_SQS_WARN("Attempt to remove queue record for queue [" << userName << "/" << queue << "], but there is no queue record");
        return;
    }

    auto queuePtr = queueIt->second;
    CancleNodeTrackingSubscription(queuePtr);
    for (auto& req : queuePtr->GetLeaderNodeRequests_) {
        RLOG_SQS_REQ_DEBUG(req->Get()->RequestId, "Removing queue [" << req->Get()->UserName << "/" << req->Get()->QueueName << "] from sqs service info");
        Send(req->Sender, new TSqsEvents::TEvGetLeaderNodeForQueueResponse(req->Get()->RequestId, req->Get()->UserName, req->Get()->QueueName, TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::NoQueue));
    }
    queuePtr->GetLeaderNodeRequests_.clear();
    userIt->second->QueueByNameAndFolder_.erase(std::make_pair(queuePtr->CustomName_, queuePtr->FolderId_));
    auto queuesCount = userIt->second->CountQueuesInFolder(queuePtr->FolderId_);
    if (!queuesCount) {
        userIt->second->FolderCounters_.erase(queuePtr->FolderId_);
    }
    userIt->second->Queues_.erase(queueIt);
    queuePtr->Counters_->RemoveCounters();
}

std::map<TString, TSqsService::TQueueInfoPtr>::iterator TSqsService::AddQueue(const TString& userName,
                                                                              const TString& queue,
                                                                              ui64 leaderTabletId,
                                                                              const TString& customName,
                                                                              const TString& folderId,
                                                                              const ui32 tablesFormat,
                                                                              const ui64 version,
                                                                              const ui64 shardsCount,
                                                                              const TInstant createdTimestamp,
                                                                              bool isFifo) {
    auto user = MutableUser(userName, false); // don't move requests because they are already moved in our caller
    const TInstant now = TActivationContext::Now();
    const TInstant timeToInsertCounters = createdTimestamp + TDuration::MilliSeconds(Cfg().GetQueueCountersExportDelayMs());
    const bool insertCounters = now >= timeToInsertCounters;
    auto folderCntrIter = user->FolderCounters_.find(folderId);
    if (folderCntrIter == user->FolderCounters_.end()) {
        folderCntrIter = user->FolderCounters_.insert(std::make_pair(folderId, user->Counters_->CreateFolderCounters(folderId, true))).first;
    }
    if (!insertCounters) {
        Schedule(timeToInsertCounters - now, new TSqsEvents::TEvInsertQueueCounters(userName, queue, version));
    }

    auto ret = user->Queues_.insert(std::make_pair(queue, TQueueInfoPtr(new TQueueInfo(
            userName, queue, RootUrl_, leaderTabletId, isFifo, customName, folderId, tablesFormat, version, shardsCount,
            user->Counters_, folderCntrIter->second, SchemeCache_, user->QuoterResources_, insertCounters, user->UseLeaderCPUOptimization)))
    ).first;

    auto queueInfo = ret->second;
    user->QueueByNameAndFolder_.emplace(std::make_pair(customName, folderId), queueInfo);

    {
        auto requests = user->GetLeaderNodeRequests_.equal_range(queue);
        for (auto i = requests.first; i != requests.second; ++i) {
            auto& req = i->second;
            RLOG_SQS_REQ_DEBUG(req->Get()->RequestId, "Adding queue [" << req->Get()->UserName << "/" << req->Get()->QueueName << "] to sqs service. Move get leader node request to queue info");
            queueInfo->GetLeaderNodeRequests_.emplace(std::move(req));
        }
        user->GetLeaderNodeRequests_.erase(requests.first, requests.second);
    }

    {
        auto requests = user->GetConfigurationRequests_.equal_range(queue);
        for (auto i = requests.first; i != requests.second; ++i) {
            auto& req = i->second;
            ProcessConfigurationRequestForQueue(req, user, queueInfo);
        }
        user->GetConfigurationRequests_.erase(requests.first, requests.second);
    }

    {
        auto requests = user->GetQueueIdRequests_.equal_range(std::make_pair(customName, folderId));
        for (auto i = requests.first; i != requests.second; ++i) {
            auto& req = i->second;
            Send(
                req->Sender,
                new TSqsEvents::TEvQueueId(
                    queueInfo->QueueName_,
                    queueInfo->Version_,
                    queueInfo->ShardsCount_,
                    queueInfo->TablesFormat_
                )
            );
        }
        user->GetQueueIdRequests_.erase(requests.first, requests.second);
    }

    {
        auto requests = user->GetQueueFolderIdAndCustomNameRequests_.equal_range(queue);
        for (auto i = requests.first; i != requests.second; ++i) {
            auto& req = i->second;
            Answer(req, queueInfo);
        }
        user->GetQueueFolderIdAndCustomNameRequests_.erase(requests.first, requests.second);
    }

    CreateNodeTrackingSubscription(queueInfo);
    LOG_SQS_DEBUG("Created queue record. Queue: [" << queue << "]. QueueIdNumber: " << queueInfo->Version_ << ". Leader tablet id: [" << leaderTabletId << "]. Node tracker subscription: " << queueInfo->NodeTrackingSubscriptionId);
    return ret;
}

void TSqsService::CreateNodeTrackingSubscription(TQueueInfoPtr queueInfo) {
    Y_ABORT_UNLESS(!queueInfo->NodeTrackingSubscriptionId);
    queueInfo->NodeTrackingSubscriptionId = ++MaxNodeTrackingSubscriptionId;
    LOG_SQS_DEBUG("Create node tracking subscription queue_id_number=" << queueInfo->Version_
        << " tables_format=" << queueInfo->TablesFormat_ << " subscription_id=" << queueInfo->NodeTrackingSubscriptionId
    );

    QueuePerNodeTrackingSubscription[queueInfo->NodeTrackingSubscriptionId] = queueInfo;

    std::optional<ui64> fixedLeaderTabletId;
    if (queueInfo->TablesFormat_ == 0) {
        fixedLeaderTabletId = queueInfo->LeaderTabletId_;
    }
    Send(
        NodeTrackerActor_,
        new TSqsEvents::TEvNodeTrackerSubscribeRequest(
            queueInfo->NodeTrackingSubscriptionId,
            queueInfo->Version_,
            queueInfo->IsFifo_,
            fixedLeaderTabletId
        )
    );
}

void TSqsService::CancleNodeTrackingSubscription(TQueueInfoPtr queueInfo) {
    LOG_SQS_DEBUG("Cancle node tracking subscription queue_id_number=" << queueInfo->Version_
        << " tables_format=" << queueInfo->TablesFormat_ << " subscription_id=" << queueInfo->NodeTrackingSubscriptionId
    );
    Y_ABORT_UNLESS(queueInfo->NodeTrackingSubscriptionId);
    auto id = queueInfo->NodeTrackingSubscriptionId;
    queueInfo->NodeTrackingSubscriptionId = 0;

    QueuePerNodeTrackingSubscription.erase(id);
    LocalLeaderManager->QueueRemoved(queueInfo);

    Send(
        NodeTrackerActor_,
        new TSqsEvents::TEvNodeTrackerUnsubscribeRequest(id)
    );
}

void TSqsService::AnswerNoUserToRequests() {
    AnswerNoUserToRequests(GetLeaderNodeRequests_);
    AnswerNoUserToRequests(GetConfigurationRequests_);
    AnswerNoUserToRequests(GetQueueIdRequests_);
    AnswerNoUserToRequests(GetQueueFolderIdAndCustomNameRequests_);
    AnswerNoUserToRequests(CountQueuesRequests_);
}

void TSqsService::AnswerNoQueueToRequests(const TUserInfoPtr& user) {
    AnswerNoQueueToRequests(user, user->GetLeaderNodeRequests_);
    AnswerNoQueueToRequests(user, user->GetConfigurationRequests_);
    AnswerNoQueueToRequests(user, user->GetQueueIdRequests_);
    AnswerNoQueueToRequests(user, user->GetQueueFolderIdAndCustomNameRequests_);
}

void TSqsService::AnswerThrottledToRequests(const TUserInfoPtr& user) {
    AnswerThrottledToRequests(user->GetLeaderNodeRequests_);
    AnswerThrottledToRequests(user->GetConfigurationRequests_);
    AnswerThrottledToRequests(user->GetQueueIdRequests_);
    AnswerThrottledToRequests(user->GetQueueFolderIdAndCustomNameRequests_);
}

void TSqsService::AnswerErrorToRequests() {
    AnswerErrorToRequests(nullptr, GetLeaderNodeRequests_);
    AnswerErrorToRequests(nullptr, GetConfigurationRequests_);
    AnswerErrorToRequests(nullptr, GetQueueIdRequests_);
    AnswerErrorToRequests(nullptr, GetQueueFolderIdAndCustomNameRequests_);
    AnswerErrorToRequests(nullptr, CountQueuesRequests_);
}

void TSqsService::AnswerErrorToRequests(const TUserInfoPtr& user) {
    AnswerErrorToRequests(user, user->GetLeaderNodeRequests_);
    AnswerErrorToRequests(user, user->GetConfigurationRequests_);
    AnswerErrorToRequests(user, user->GetQueueIdRequests_);
    AnswerErrorToRequests(user, user->GetQueueFolderIdAndCustomNameRequests_);
    AnswerErrorToRequests(user, user->CountQueuesRequests_);
}

void TSqsService::ProcessConnectTimeoutToLeader() {
    TDuration nextRunAfter = TDuration::Max();
    TDuration leaderConnectTimeout = TDuration::MilliSeconds(Cfg().GetLeaderConnectTimeoutMs());
    auto it = QueuesWithGetNodeWaitingRequests.begin();
    while(it != QueuesWithGetNodeWaitingRequests.end()) {
        auto& queue = **it;
        auto nodeUnknownTime = TActivationContext::Now() - queue.NodeUnknownSince_;
        auto timeLeft = leaderConnectTimeout - nodeUnknownTime;
        if (timeLeft == TDuration::Zero()) {
            for (auto& req : queue.GetLeaderNodeRequests_) {
                RLOG_SQS_REQ_WARN(req->Get()->RequestId, "Can't connect to leader tablet for " << nodeUnknownTime);
                Send(req->Sender, new TSqsEvents::TEvGetLeaderNodeForQueueResponse(req->Get()->RequestId, req->Get()->UserName, req->Get()->QueueName, TSqsEvents::TEvGetLeaderNodeForQueueResponse::EStatus::FailedToConnectToLeader));
            }
            queue.GetLeaderNodeRequests_.clear();
            auto toRemoveIt = it++;
            QueuesWithGetNodeWaitingRequests.erase(toRemoveIt);
        } else {
            nextRunAfter = Min(nextRunAfter, timeLeft);
            ++it;
        }
    }
    if (nextRunAfter != TDuration::Max()) {
        Schedule(nextRunAfter, new TEvWakeup(CONNECT_TIMEOUT_TO_LEADER_WAKEUP_TAG));
    }
}

void TSqsService::HandleWakeup(TEvWakeup::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->Tag != 0);
    switch (ev->Get()->Tag) {
    case LIST_USERS_WAKEUP_TAG:
        ScheduledRequestingUsersList_ = false;
        if (TActivationContext::Now() < LastRequestUsersListTime_ + TDuration::MilliSeconds(GetLeadersDescriberUpdateTimeMs())) {
            ScheduleRequestSqsUsersList();
        } else {
            EarlyRequestUsersListBudget_ = Min(EarlyRequestUsersListBudget_ + 1, EARLY_REQUEST_USERS_LIST_MAX_BUDGET);
            RequestSqsUsersList();
        }
        break;
    case LIST_QUEUES_WAKEUP_TAG:
        ScheduledRequestingQueuesList_ = false;
        if (TActivationContext::Now() < LastRequestQueuesListTime_ + TDuration::MilliSeconds(GetLeadersDescriberUpdateTimeMs())) {
            ScheduleRequestSqsQueuesList();
        } else {
            --EarlyRequestQueuesListMinBudget_;
            RequestSqsQueuesList();
        }
        break;
    case CONNECT_TIMEOUT_TO_LEADER_WAKEUP_TAG:
        ProcessConnectTimeoutToLeader();
        break;
    }
}

void TSqsService::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TSqsService::HandleSqsRequest(TSqsEvents::TEvSqsRequest::TPtr& ev) {
    LOG_SQS_TRACE("HandleSqsRequest " << SecureShortUtf8DebugString(ev->Get()->Record));
    auto replier = MakeHolder<TReplierToSenderActorCallback>(ev);
    const auto& request = replier->Request->Get()->Record;
    Register(CreateActionActor(request, std::move(replier)));
}

void TSqsService::HandleInsertQueueCounters(TSqsEvents::TEvInsertQueueCounters::TPtr& ev) {
    const auto userIt = Users_.find(ev->Get()->User);
    if (userIt == Users_.end()) {
        LOG_SQS_WARN("No user [" << ev->Get()->User << "]. Don't insert queue [" << ev->Get()->Queue << "] counters");
        return;
    }
    const auto& user = userIt->second;
    const auto queueIt = user->Queues_.find(ev->Get()->Queue);
    if (queueIt == user->Queues_.end()) {
        LOG_SQS_WARN("Don't insert queue [" << ev->Get()->Queue << "] counters: no queue");
        return;
    }
    const auto& queue = queueIt->second;
    if (queue->Version_ != ev->Get()->Version) {
        LOG_SQS_WARN("Don't insert queue [" << ev->Get()->Queue << "] counters: queue version is not as expected. Expected: "
                   << ev->Get()->Version << ". Real: " << queue->Version_);
        return;
    }

    queue->Counters_->InsertCounters();
}

void TSqsService::IncLocalLeaderRef(const TActorId& referer, const TQueueInfoPtr& queueInfo, const TString& reason) {
    LWPROBE(IncLeaderRef, queueInfo->UserName_, queueInfo->QueueName_, referer.ToString());
    const auto [iter, inserted] = LocalLeaderRefs_.emplace(referer, queueInfo);
    if (inserted) {
        LOG_SQS_TRACE("Inc local leader ref for actor " << referer);
        LocalLeaderManager->IncLocalLeaderRef(queueInfo, reason, TActivationContext::Now());
    } else {
        LWPROBE(IncLeaderRefAlreadyHasRef, queueInfo->UserName_, queueInfo->QueueName_, referer.ToString());
        LOG_SQS_WARN("Inc local leader ref for actor " << referer << ". Ignore because this actor already presents in referers set");
    }
}

void TSqsService::DecLocalLeaderRef(const TActorId& referer, const TString& reason) {
    LWPROBE(DecLeaderRef, referer.ToString());
    const auto iter = LocalLeaderRefs_.find(referer);
    LOG_SQS_TRACE("Dec local leader ref for actor " << referer << ". Found: " << (iter != LocalLeaderRefs_.end()));
    if (iter != LocalLeaderRefs_.end()) {
        auto queueInfo = iter->second;
        LocalLeaderManager->DecLocalLeaderRef(queueInfo, reason);
        LocalLeaderRefs_.erase(iter);
    } else {
        LWPROBE(DecLeaderRefNotInRefSet, referer.ToString());
    }
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetQueueId::TPtr&& ev) {
    GetQueueIdRequests_.emplace(ev->Get()->UserName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr&& ev) {
    GetQueueFolderIdAndCustomNameRequests_.emplace(ev->Get()->UserName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetConfiguration::TPtr&& ev) {
    GetConfigurationRequests_.emplace(ev->Get()->UserName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr&& ev) {
    GetLeaderNodeRequests_.emplace(ev->Get()->UserName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvCountQueues::TPtr&& ev) {
    CountQueuesRequests_.emplace(ev->Get()->UserName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetQueueId::TPtr&& ev, const TUserInfoPtr& userInfo) {
    userInfo->GetQueueIdRequests_.emplace(std::make_pair(ev->Get()->CustomQueueName, ev->Get()->FolderId), std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetQueueFolderIdAndCustomName::TPtr&& ev, const TUserInfoPtr& userInfo) {
    userInfo->GetQueueFolderIdAndCustomNameRequests_.emplace(ev->Get()->QueueName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetConfiguration::TPtr&& ev, const TUserInfoPtr& userInfo) {
    userInfo->GetConfigurationRequests_.emplace(ev->Get()->QueueName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvGetLeaderNodeForQueueRequest::TPtr&& ev, const TUserInfoPtr& userInfo) {
    userInfo->GetLeaderNodeRequests_.emplace(ev->Get()->QueueName, std::move(ev));
}

void TSqsService::InsertWaitingRequest(TSqsEvents::TEvCountQueues::TPtr&& ev, const TUserInfoPtr& userInfo) {
    userInfo->CountQueuesRequests_.emplace(ev->Get()->FolderId, std::move(ev));
}

template <class TMultimap>
size_t TSqsService::MoveUserRequests(const TUserInfoPtr& userInfo, TMultimap& map) {
    size_t moved = 0;
    auto requests = map.equal_range(userInfo->UserName_);
    for (auto i = requests.first; i != requests.second; ++i) {
        RLOG_SQS_REQ_DEBUG(i->second->Get()->RequestId, "Got user in sqs service. Move request " << i->second->Get()->ToStringHeader() << " to user info");
        InsertWaitingRequest(std::move(i->second), userInfo);
        ++moved;
    }
    if (moved) {
        map.erase(requests.first, requests.second);
    }
    return moved;
}

template <class TMultimap>
void TSqsService::AnswerNoUserToRequests(TMultimap& map) {
    for (auto& userToRequest : map) {
        AnswerNotExists(userToRequest.second, nullptr);
    }
    map.clear();
}

template <class TMultimap>
void TSqsService::AnswerNoQueueToRequests(const TUserInfoPtr& user, TMultimap& map) {
    for (auto& queueToRequest : map) {
        auto& req = queueToRequest.second;
        AnswerNotExists(req, user);
    }
    map.clear();
}

template <class TMultimap>
void TSqsService::AnswerThrottledToRequests(TMultimap& map) {
    for (auto& queueToRequest : map) {
        auto& req = queueToRequest.second;
        AnswerThrottled(req);
    }
    map.clear();
}

template <class TMultimap>
void TSqsService::AnswerErrorToRequests(const TUserInfoPtr& user, TMultimap& map) {
    for (auto& queueToRequest : map) {
        auto& req = queueToRequest.second;
        if (user) {
            RLOG_SQS_REQ_ERROR(req->Get()->RequestId, "Error in sqs service for user [" << user->UserName_ << "]. Request " << req->Get()->ToStringHeader());
        } else {
            RLOG_SQS_REQ_ERROR(req->Get()->RequestId, "Error in sqs service. Request " << req->Get()->ToStringHeader());
        }
        AnswerFailed(req, user);
    }
    map.clear();
}

void TSqsService::MakeAndRegisterYcEventsProcessor() {
    if (!YcSearchEventsConfig.Enabled)
        return;

    auto root = YcSearchEventsConfig.TenantMode ? TString() : Cfg().GetRoot();

    auto factory = AppData()->SqsEventsWriterFactory;
    Y_ABORT_UNLESS(factory);
    Register(new TSearchEventsProcessor(
            root, YcSearchEventsConfig.ReindexInterval, YcSearchEventsConfig.RescanInterval,
            YcSearchEventsConfig.Database,
            factory->CreateEventsWriter(Cfg(), GetSqsServiceCounters(AppData()->Counters, "yc_unified_agent"))
    ));
}

IActor* CreateSqsService() {
    return new TSqsService();
}

} // namespace NKikimr::NSQS
