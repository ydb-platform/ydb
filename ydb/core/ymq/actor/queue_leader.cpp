#include "queue_leader.h"
#include "fifo_cleanup.h"
#include "executor.h"
#include "log.h"
#include "purge.h"
#include "retention.h"

#include <ydb/public/lib/value/value.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/core/ymq/base/constants.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/probes.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/ymq/queues/common/queries.h>
#include <ydb/core/ymq/queues/common/key_hashes.h>
#include <ydb/core/ymq/queues/common/db_queries_maker.h>
#include <ydb/core/ymq/queues/fifo/queries.h>
#include <ydb/core/ymq/queues/std/queries.h>

#include <ydb/library/actors/core/hfunc.h>

#include <util/random/random.h>
#include <util/random/shuffle.h>
#include <util/system/yassert.h>
#include <util/string/ascii.h>

LWTRACE_USING(SQS_PROVIDER);


namespace NKikimr::NSQS {

constexpr ui64 UPDATE_COUNTERS_TAG = 0;
constexpr ui64 UPDATE_MESSAGES_METRICS_TAG = 1;
constexpr ui64 REQUEST_CONFIGURATION_TAG = 2;
constexpr ui64 RELOAD_INFLY_TAG = 1000;

const TString INFLY_INVALIDATION_REASON_VERSION_CHANGED = "InflyVersionChanged";
const TString INFLY_INVALIDATION_REASON_DEADLINE_CHANGED = "MessageDeadlineChanged";
const TString INFLY_INVALIDATION_REASON_DELETED = "MessageDeleted";

constexpr TDuration STATE_UPDATE_PERIOD_MIN = TDuration::MilliSeconds(200);
constexpr TDuration STATE_UPDATE_PERIOD_MAX = TDuration::Minutes(1);

TDuration GetNextReloadStateWaitPeriod(TDuration current = TDuration::Zero()) {
    if (current == STATE_UPDATE_PERIOD_MAX) {
        return TDuration::Max();
    }
    auto nextWaitPeriod = 2 * Max(current, STATE_UPDATE_PERIOD_MIN);
    nextWaitPeriod += TDuration::MilliSeconds(RandomNumber<ui64>(nextWaitPeriod.MilliSeconds() / 2));
    return Max(STATE_UPDATE_PERIOD_MAX, nextWaitPeriod);
}

TQueueLeader::TQueueLeader(
    TString userName,
    TString queueName,
    TString folderId,
    TString rootUrl,
    TIntrusivePtr<TQueueCounters> counters,
    TIntrusivePtr<TUserCounters> userCounters,
    const TActorId& schemeCache,
    const TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions>& quoterResourcesForUser,
    bool useCPUOptimization
)
    : UserName_(std::move(userName))
    , QueueName_(std::move(queueName))
    , FolderId_(std::move(folderId))
    , RootUrl_(std::move(rootUrl))
    , SchemeCache_(schemeCache)
    , Counters_(std::move(counters))
    , UserCounters_(std::move(userCounters))
    , UseCPUOptimization(useCPUOptimization)
{
    if (quoterResourcesForUser) {
        QuoterResources_ = new TSqsEvents::TQuoterResourcesForActions(*quoterResourcesForUser);
    }
    if (UseCPUOptimization) {
        LOG_SQS_INFO("Use CPU optimization for queue " << TLogQueueName(UserName_, QueueName_));
    }
}

void TQueueLeader::Bootstrap() {
    Become(&TQueueLeader::StateInit);
    QueueAttributesCacheTime_ = TDuration::MilliSeconds(Cfg().GetQueueAttributesCacheTimeMs());
    RequestConfiguration();
}

void TQueueLeader::BecomeWorking() {
    Become(&TQueueLeader::StateWorking);
    const auto& cfg = Cfg();
    const ui64 randomTimeToWait = RandomNumber<ui64>(cfg.GetBackgroundMetricsUpdateTimeMs() / 4); // Don't start all such operations at one moment
    if (UseCPUOptimization) {
        Schedule(TDuration::MilliSeconds(randomTimeToWait), new TSqsEvents::TEvForceReloadState(GetNextReloadStateWaitPeriod()));
    } else {
        Schedule(TDuration::MilliSeconds(randomTimeToWait), new TEvWakeup(UPDATE_COUNTERS_TAG));
    }
    Schedule(TDuration::Seconds(1), new TEvWakeup(UPDATE_MESSAGES_METRICS_TAG));

    std::vector<TSqsEvents::TEvExecute::TPtr> requests;
    requests.swap(ExecuteRequests_);
    for (auto& req : requests) {
        HandleExecuteWhileWorking(req);
    }

    for (auto&& [reqId, reqInfo] : SendMessageRequests_) {
        ProcessSendMessageBatch(reqInfo);
    }

    for (auto&& [reqId, reqInfo] : ReceiveMessageRequests_) {
        ProcessReceiveMessageBatch(reqInfo);
    }

    for (auto&& [reqIdAndShard, reqInfo] : DeleteMessageRequests_) {
        ProcessDeleteMessageBatch(reqInfo);
    }

    for (auto&& [reqIdAndShard, reqInfo] : ChangeMessageVisibilityRequests_) {
        ProcessChangeMessageVisibilityBatch(reqInfo);
    }

    Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvLeaderStarted());
}

STATEFN(TQueueLeader::StateInit) {
    switch (ev->GetTypeRewrite()) {
        // interface
        cFunc(TEvPoisonPill::EventType, PassAway); // from service
        hFunc(TSqsEvents::TEvGetConfiguration, HandleGetConfigurationWhileIniting); // from action actors
        hFunc(TSqsEvents::TEvActionCounterChanged, HandleActionCounterChanged);
        hFunc(TSqsEvents::TEvLocalCounterChanged, HandleLocalCounterChanged);
        hFunc(TSqsEvents::TEvExecute, HandleExecuteWhileIniting); // from action actors
        hFunc(TSqsEvents::TEvClearQueueAttributesCache, HandleClearQueueAttributesCache); // from set queue attributes
        hFunc(TSqsEvents::TEvPurgeQueue, HandlePurgeQueue); // from purge queue actor
        hFunc(TSqsEvents::TEvSendMessageBatch, HandleSendMessageBatchWhileIniting); // from send message action actor
        hFunc(TSqsEvents::TEvReceiveMessageBatch, HandleReceiveMessageBatchWhileIniting); // from receive message action actor
        hFunc(TSqsEvents::TEvDeleteMessageBatch, HandleDeleteMessageBatchWhileIniting); // from delete message action actor
        hFunc(TSqsEvents::TEvChangeMessageVisibilityBatch, HandleChangeMessageVisibilityBatchWhileIniting); // from change message visibility action actor
        hFunc(TSqsEvents::TEvGetRuntimeQueueAttributes, HandleGetRuntimeQueueAttributesWhileIniting); // from get queue attributes action actor
        hFunc(TSqsEvents::TEvDeadLetterQueueNotification, HandleDeadLetterQueueNotification); // service periodically notifies active dead letter queues
        hFunc(TSqsEvents::TEvForceReloadState, HandleForceReloadState);
        hFunc(TSqsEvents::TEvReloadStateRequest, HandleReloadStateRequest);

        // internal
        hFunc(TSqsEvents::TEvQueueId, HandleQueueId); // discover dlq id and version
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted); // from executor
        hFunc(TEvWakeup, HandleWakeup);
    default:
        LOG_SQS_ERROR("Unknown type of event came to SQS background queue " << TLogQueueName(UserName_, QueueName_) << " leader actor: " << ev->Type << " (" << ev->GetTypeName() << "), sender: " << ev->Sender);
    }
}

STATEFN(TQueueLeader::StateWorking) {
    switch (ev->GetTypeRewrite()) {
        // interface
        cFunc(TEvPoisonPill::EventType, PassAway); // from service
        hFunc(TSqsEvents::TEvGetConfiguration, HandleGetConfigurationWhileWorking); // from action actors
        hFunc(TSqsEvents::TEvActionCounterChanged, HandleActionCounterChanged);
        hFunc(TSqsEvents::TEvLocalCounterChanged, HandleLocalCounterChanged);
        hFunc(TSqsEvents::TEvExecute, HandleExecuteWhileWorking); // from action actors
        hFunc(TSqsEvents::TEvClearQueueAttributesCache, HandleClearQueueAttributesCache); // from set queue attributes
        hFunc(TSqsEvents::TEvPurgeQueue, HandlePurgeQueue); // from purge queue actor
        hFunc(TSqsEvents::TEvSendMessageBatch, HandleSendMessageBatchWhileWorking); // from send message action actor
        hFunc(TSqsEvents::TEvReceiveMessageBatch, HandleReceiveMessageBatchWhileWorking); // from receive message action actor
        hFunc(TSqsEvents::TEvDeleteMessageBatch, HandleDeleteMessageBatchWhileWorking); // from delete message action actor
        hFunc(TSqsEvents::TEvChangeMessageVisibilityBatch, HandleChangeMessageVisibilityBatchWhileWorking); // from change message visibility action actor
        hFunc(TSqsEvents::TEvGetRuntimeQueueAttributes, HandleGetRuntimeQueueAttributesWhileWorking); // from get queue attributes action actor
        hFunc(TSqsEvents::TEvDeadLetterQueueNotification, HandleDeadLetterQueueNotification); // service periodically notifies active dead letter queues
        hFunc(TSqsEvents::TEvForceReloadState, HandleForceReloadState);
        hFunc(TSqsEvents::TEvReloadStateRequest, HandleReloadStateRequest);

        // internal
        hFunc(TSqsEvents::TEvQueueId, HandleQueueId); // discover dlq id and version
        hFunc(TSqsEvents::TEvExecuted, HandleExecuted); // from executor
        hFunc(TEvWakeup, HandleWakeup);
        hFunc(TSqsEvents::TEvInflyIsPurgingNotification, HandleInflyIsPurgingNotification);
        hFunc(TSqsEvents::TEvQueuePurgedNotification, HandleQueuePurgedNotification);
    default:
        LOG_SQS_ERROR("Unknown type of event came to SQS background queue " << TLogQueueName(UserName_, QueueName_) << " leader actor: " << ev->Type << " (" << ev->GetTypeName() << "), sender: " << ev->Sender);
    }
}

void TQueueLeader::PassAway() {
    LOG_SQS_INFO("Queue " << TLogQueueName(UserName_, QueueName_) << " leader is dying");

    if (CurrentStateFunc() != &TThis::StateWorking) {
        Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvLeaderStarted());  
    }

    for (auto& req : GetConfigurationRequests_) {
        AnswerFailed(req);
    }
    GetConfigurationRequests_.clear();

    Y_ABORT_UNLESS(ExecuteRequests_.empty() || CurrentStateFunc() == &TThis::StateInit);

    if (DeduplicationCleanupActor_) {
        Send(DeduplicationCleanupActor_, new TEvPoisonPill());
    }
    if (ReadsCleanupActor_) {
        Send(ReadsCleanupActor_, new TEvPoisonPill());
    }
    if (RetentionActor_) {
        Send(RetentionActor_, new TEvPoisonPill());
    }
    if (PurgeActor_) {
        Send(PurgeActor_, new TEvPoisonPill());
    }

    // Explicitly set absolute counters to zero for proper counting aggregated parent counters:
    SET_COUNTER_COUPLE(Counters_, MessagesCount, stored_count, 0);
    SET_COUNTER_COUPLE(Counters_, InflyMessagesCount, inflight_count, 0);
    SET_COUNTER_COUPLE(Counters_, OldestMessageAgeSeconds, oldest_age_milliseconds, 0);

    TActorBootstrapped::PassAway();
}

void TQueueLeader::HandleWakeup(TEvWakeup::TPtr& ev) {
    if (ev->Get()->Tag >= RELOAD_INFLY_TAG && ev->Get()->Tag < RELOAD_INFLY_TAG + MAX_SHARDS_COUNT) {
        StartLoadingInfly(ev->Get()->Tag - RELOAD_INFLY_TAG, true); // reload infly after failure while loading infly
        return;
    }

    switch (ev->Get()->Tag) {
    case UPDATE_COUNTERS_TAG: {
        StartGatheringMetrics();
        break;
    }
    case UPDATE_MESSAGES_METRICS_TAG: {
        CheckStillDLQ();
        PlanningRetentionWakeup();
        ReportOldestTimestampMetricsIfReady();
        ReportMessagesCountMetricsIfReady();
        Schedule(TDuration::Seconds(1), new TEvWakeup(UPDATE_MESSAGES_METRICS_TAG));
        break;
    }
    case REQUEST_CONFIGURATION_TAG: {
        RequestConfiguration();
        break;
    }
    default:
        Y_ABORT("Unknown wakeup tag: %lu", ev->Get()->Tag);
    }
}


void TQueueLeader::HandleReloadStateRequest(TSqsEvents::TEvReloadStateRequest::TPtr& ev) {
    ReloadStateRequestedFromNodes.insert(ev->Sender.NodeId());
    ForceReloadState();
}

void TQueueLeader::HandleForceReloadState(TSqsEvents::TEvForceReloadState::TPtr& ev) {
    if (!UseCPUOptimization) {
        return;
    }
    auto nextTryAfter = ev->Get()->NextTryAfter;
    if (nextTryAfter != TDuration::Max()) {
        Schedule(nextTryAfter, new TSqsEvents::TEvForceReloadState(GetNextReloadStateWaitPeriod(nextTryAfter)));
    }
    ForceReloadState();
}

void TQueueLeader::ForceReloadState() {
    if (UpdateStateRequestStartedAt) {
        LOG_SQS_DEBUG("Update state request already in process for queue " << TLogQueueName(UserName_, QueueName_));
        return;
    }
    UpdateStateRequestStartedAt = TActivationContext::Now();
    LOG_SQS_DEBUG("Start update state request for queue " << TLogQueueName(UserName_, QueueName_));
    TExecutorBuilder(SelfId(), "")
        .User(UserName_)
        .Queue(QueueName_)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(GET_STATE_ID)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .RetryOnTimeout()
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { HandleState(ev); })
        .Counters(Counters_)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
        .ParentBuilder().Start();
    AskQueueAttributes();
}

void TQueueLeader::HandleState(const TSqsEvents::TEvExecuted::TRecord& reply) {
    LOG_SQS_DEBUG("Handle state for " << TLogQueueName(UserName_, QueueName_));
    Y_ABORT_UNLESS(UpdateStateRequestStartedAt != TInstant::Zero());

    if (reply.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const TValue shardStates = val["state"];

        for (size_t i = 0; i < shardStates.Size(); ++i) {
            const auto& state = shardStates[i];
            const ui64 shard = IsFifoQueue_ ? 0 : (TablesFormat_ == 1 ? ui32(state["Shard"]) : ui64(state["State"]));
            auto& shardInfo = Shards_[shard];
            
            SetMessagesCount(shard, state["MessageCount"]);
            SetInflyMessagesCount(shard, state["InflyCount"]);
            
            const TValue createdTimestamp = state["CreatedTimestamp"];
            if (!createdTimestamp.IsNull()) {
                shardInfo.CreatedTimestamp = TInstant::MilliSeconds(ui64(createdTimestamp));
            }
            
            const TValue inflyVersion = state["InflyVersion"];
            if (!inflyVersion.IsNull() && shardInfo.InflyVersion != ui64(inflyVersion)) {
                shardInfo.NeedInflyReload = true;
            }

            if (shardInfo.MessagesCount > 0) {
                RequestOldestTimestampMetrics(shard);
            }

            shardInfo.MessagesCountWasGot = true;
            ProcessGetRuntimeQueueAttributes(shard);
            for (auto nodeId : ReloadStateRequestedFromNodes) {
                Send(
                    MakeSqsProxyServiceID(nodeId),
                    new TSqsEvents::TEvReloadStateResponse(
                        UserName_,
                        QueueName_,
                        UpdateStateRequestStartedAt
                    )
                );
            }
            ReloadStateRequestedFromNodes.clear();
        }
    } else {
        LOG_SQS_ERROR("Failed to update state for " << TLogQueueName(UserName_, QueueName_) << ": " << reply);
    }

    UpdateStateRequestStartedAt = TInstant::Zero();
}

void TQueueLeader::HandleGetConfigurationWhileIniting(TSqsEvents::TEvGetConfiguration::TPtr& ev) {
    GetConfigurationRequests_.emplace_back(ev);
}

void TQueueLeader::HandleGetConfigurationWhileWorking(TSqsEvents::TEvGetConfiguration::TPtr& ev) {
    if (ev->Get()->NeedQueueAttributes && TActivationContext::Now() <= AttributesUpdateTime_ + QueueAttributesCacheTime_ && QueueAttributes_) {
        AnswerGetConfiguration(ev);
    } else {
        LWPROBE(QueueAttributesCacheMiss, ev->Get()->UserName, ev->Get()->QueueName, ev->Get()->RequestId);
        GetConfigurationRequests_.emplace_back(ev);
        AskQueueAttributes();
    }
}

void TQueueLeader::HandleActionCounterChanged(TSqsEvents::TEvActionCounterChanged::TPtr& ev) {
    auto actionNumber = ev->Get()->Record.GetAction();
    if (actionNumber > EAction::ActionsArraySize) {
        LOG_SQS_DEBUG("Action with number " << actionNumber << " doesn't exist.");
        return;
    }
    EAction action = static_cast<EAction>(actionNumber);
    if (!IsActionForMessage(action) && !Counters_->NeedToShowDetailedCounters()) {
        return;
    }
    ui32 errorsCount = ev->Get()->Record.GetErrorsCount();
    TCountersCouple<TActionCounters*> actionCountersCouple{nullptr, nullptr};
    if (IsActionForQueue(action)) {
        actionCountersCouple.SqsCounters = &Counters_->SqsActionCounters[action];
        if (errorsCount > 0) {
            ADD_COUNTER(actionCountersCouple.SqsCounters, Errors, errorsCount);
        } else {
            INC_COUNTER(actionCountersCouple.SqsCounters, Success);
        }
    }
    if (IsActionForQueueYMQ(action)) {
        actionCountersCouple.YmqCounters = &Counters_->YmqActionCounters[action];
        if (errorsCount > 0) {
            ADD_COUNTER(actionCountersCouple.YmqCounters, Errors, errorsCount);
        } else {
            INC_COUNTER(actionCountersCouple.YmqCounters, Success);
        }
    }
    if (actionCountersCouple.Defined()) {
        COLLECT_HISTOGRAM_COUNTER_COUPLE(actionCountersCouple, Duration, ev->Get()->Record.GetDurationMs());
        auto workingDuration = ev->Get()->Record.GetWorkingDurationMs();
        LOG_SQS_DEBUG("Request " << action << " working duration: " << workingDuration << "ms");
        COLLECT_HISTOGRAM_COUNTER_COUPLE(actionCountersCouple, WorkingDuration, workingDuration);
    }
}

void TQueueLeader::HandleLocalCounterChanged(TSqsEvents::TEvLocalCounterChanged::TPtr& ev) {
    switch (ev->Get()->CounterType) {
        case TSqsEvents::TEvLocalCounterChanged::ECounterType::ReceiveMessageImmediateDuration:
            if (auto* detailedCounters = Counters_ ? Counters_->GetDetailedCounters() : nullptr) {
                COLLECT_HISTOGRAM_COUNTER(detailedCounters, ReceiveMessageImmediate_Duration, ev->Get()->Value);
            }
            break;
        case TSqsEvents::TEvLocalCounterChanged::ECounterType::ReceiveMessageEmptyCount:
            INC_COUNTER_COUPLE(Counters_, ReceiveMessage_EmptyCount, empty_receive_attempts_count_per_second);
            break;
        case TSqsEvents::TEvLocalCounterChanged::ECounterType::MessagesPurged:
            ADD_COUNTER_COUPLE(Counters_, MessagesPurged, purged_count_per_second, ev->Get()->Value);
            break;
        case TSqsEvents::TEvLocalCounterChanged::ECounterType::ClientMessageProcessingDuration:
            COLLECT_HISTOGRAM_COUNTER(Counters_, ClientMessageProcessing_Duration, ev->Get()->Value);
            COLLECT_HISTOGRAM_COUNTER(Counters_, client_processing_duration_milliseconds, ev->Get()->Value);
            break;
    }
}

void TQueueLeader::HandleClearQueueAttributesCache([[maybe_unused]] TSqsEvents::TEvClearQueueAttributesCache::TPtr& ev) {
    AttributesUpdateTime_ = TInstant::Zero();
    QueueAttributes_ = Nothing();
    AskQueueAttributes();
}

void TQueueLeader::HandleExecuteWhileIniting(TSqsEvents::TEvExecute::TPtr& ev) {
    ExecuteRequests_.emplace_back(ev);
}

void TQueueLeader::HandleExecuteWhileWorking(TSqsEvents::TEvExecute::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Get()->QueryIdx < QUERY_VECTOR_SIZE);
    Y_ABORT_UNLESS(ev->Get()->Shard < ShardsCount_);
    auto& query = Shards_[ev->Get()->Shard].Queries[ev->Get()->QueryIdx];

    switch (query.State) {
    case EQueryState::Empty:
        query.State = EQueryState::Preparing;
        Prepare(ev);
        break;
    case EQueryState::Preparing:
        RLOG_SQS_REQ_DEBUG(ev->Get()->RequestId, "Waiting query(idx=" << ev->Get()->QueryIdx << ") compilation");
        query.Deferred.push_back(ev);
        break;
    case EQueryState::Cached:
        ExecuteRequest(ev, query.Compiled);
        break;
    }
}

void TQueueLeader::Prepare(TSqsEvents::TEvExecute::TPtr& ev) {
    const TSqsEvents::TEvExecute& req = *ev->Get();
    RLOG_SQS_REQ_DEBUG(req.RequestId, "Preparing query(idx=" << req.QueryIdx << ")");

    TExecutorBuilder(SelfId(), req.RequestId)
        .User(UserName_)
        .Queue(QueueName_)
        .Shard(req.Shard)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .TablesFormat(TablesFormat_)
        .Mode(NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE)
        .QueryId(req.QueryIdx)
        .RetryOnTimeout(req.RetryOnTimeout)
        .Counters(Counters_)
        .OnExecuted([this, ev](const TSqsEvents::TEvExecuted::TRecord& record) mutable { OnQueryPrepared(ev, record); })
        .StartExecutorActor();
}

void TQueueLeader::OnQueryPrepared(TSqsEvents::TEvExecute::TPtr& ev, const TSqsEvents::TEvExecuted::TRecord& record) {
    const TSqsEvents::TEvExecute& req = *ev->Get();
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(record.GetStatus());
    auto& query = Shards_[req.Shard].Queries[req.QueryIdx];

    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        RLOG_SQS_REQ_DEBUG(req.RequestId, "Query(idx=" << req.QueryIdx << ") has been prepared");

        query.Compiled = record.GetMiniKQLCompileResults().GetCompiledProgram();
        query.State = EQueryState::Cached;

        std::vector<TSqsEvents::TEvExecute::TPtr> requests;
        requests.swap(query.Deferred);
        HandleExecuteWhileWorking(ev);
        for (auto& r : requests) {
            HandleExecuteWhileWorking(r);
        }

    } else {
        RLOG_SQS_REQ_WARN(req.RequestId, "Request preparation error: "
                           << "status=" << status << ", "
                           << "record=" << record);
        Send(req.Sender, MakeHolder<TSqsEvents::TEvExecuted>(record, req.Cb, req.Shard));

        for (const auto& def : query.Deferred) {
            RLOG_SQS_REQ_WARN(def->Get()->RequestId, "Request preparation error: "
                                                         << "status=" << status << ", "
                                                         << "record=" << record);
            Send(def->Get()->Sender, MakeHolder<TSqsEvents::TEvExecuted>(record, def->Get()->Cb, def->Get()->Shard));
        }
        query.Deferred.clear();

        if (!NTxProxy::TResultStatus::IsSoftErrorWithoutSideEffects(status)) {
            RemoveCachedRequest(req.Shard, req.QueryIdx);
        }
    }
}

void TQueueLeader::RemoveCachedRequest(size_t shard, size_t idx) {
    TQuery& query = Shards_[shard].Queries[idx];
    if (query.State == EQueryState::Cached) {
        LOG_SQS_INFO("Remove cached compiled query(idx=" << idx << ") for queue " << TLogQueueName(UserName_, QueueName_, shard));

        query.State = EQueryState::Empty;
        query.Compiled = TString();
    } else if (query.State == EQueryState::Preparing) {
        LOG_SQS_INFO("Clear compiling state for query(idx=" << idx << ") for queue " << TLogQueueName(UserName_, QueueName_, shard));
        Y_ABORT_UNLESS(query.Deferred.empty());

        query.State = EQueryState::Empty;
        query.Compiled = TString();
    }
}

void TQueueLeader::ExecuteRequest(TSqsEvents::TEvExecute::TPtr& ev, const TString& compiled) {
    const TSqsEvents::TEvExecute& req = *ev->Get();
    RLOG_SQS_REQ_DEBUG(req.RequestId, "Executing compiled query(idx=" << req.QueryIdx << ")");
    TExecutorBuilder builder(SelfId(), req.RequestId);
    builder
        .User(UserName_)
        .Queue(QueueName_)
        .Shard(req.Shard)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .TablesFormat(TablesFormat_)
        .QueryId(req.QueryIdx)
        .Bin(compiled)
        .RetryOnTimeout(req.RetryOnTimeout)
        .Counters(Counters_)
        .OnExecuted([this, ev](const TSqsEvents::TEvExecuted::TRecord& record) mutable { OnQueryExecuted(ev, record); });

    builder.Request().Record.MutableTransaction()->MutableMiniKQLTransaction()->MutableParams()->MutableProto()->CopyFrom(req.Params);
    builder.StartExecutorActor();
}

void TQueueLeader::OnQueryExecuted(TSqsEvents::TEvExecute::TPtr& ev, const TSqsEvents::TEvExecuted::TRecord& record) {
    const TSqsEvents::TEvExecute& req = *ev->Get();
    bool retried = false;
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(record.GetStatus());
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        CreateBackgroundActors();
    } else {
        RLOG_SQS_REQ_WARN(req.RequestId, "Query(idx=" << req.QueryIdx << ") execution error. Queue: [" << UserName_ << "/" << QueueName_ << "]: " << record);

        if (!NTxProxy::TResultStatus::IsSoftErrorWithoutSideEffects(status)) {
            TQuery& query = Shards_[req.Shard].Queries[req.QueryIdx];
            if (query.State != EQueryState::Preparing) { // if query is preparing, there is a concurrent process that has cleared our cache
                RemoveCachedRequest(req.Shard, req.QueryIdx);
            }
            if (TSqsEvents::TEvExecuted::IsResolvingError(record)) {
                retried = true;
                RLOG_SQS_REQ_DEBUG(req.RequestId, "Trying to recompile and execute query second time");
                HandleExecuteWhileWorking(ev);
            }
        }
    }

    if (!retried) {
        RLOG_SQS_REQ_DEBUG(req.RequestId, "Sending executed reply");
        Send(req.Sender, MakeHolder<TSqsEvents::TEvExecuted>(record, req.Cb, req.Shard));
    }
}

void TQueueLeader::HandleSendMessageBatchWhileIniting(TSqsEvents::TEvSendMessageBatch::TPtr& ev) {
    TString reqId = ev->Get()->RequestId;
    Y_ABORT_UNLESS(SendMessageRequests_.emplace(std::move(reqId), std::move(ev)).second);
}

void TQueueLeader::HandleSendMessageBatchWhileWorking(TSqsEvents::TEvSendMessageBatch::TPtr& ev) {
    TString reqId = ev->Get()->RequestId;
    auto [reqIter, inserted] = SendMessageRequests_.emplace(std::move(reqId), std::move(ev));
    Y_ABORT_UNLESS(inserted);
    ProcessSendMessageBatch(reqIter->second);
}

void TQueueLeader::ProcessSendMessageBatch(TSendMessageBatchRequestProcessing& reqInfo) {
    reqInfo.Init(ShardsCount_); // init if not inited
    if (!IncActiveMessageRequests(reqInfo.Shard, reqInfo.Event->Get()->RequestId)) {
        return;
    }

    auto& shardInfo = Shards_[reqInfo.Shard];
    shardInfo.SendBatchingState.AddRequest(reqInfo);
    shardInfo.SendBatchingState.TryExecute(this);
}

void TQueueLeader::OnMessageSent(const TString& requestId, size_t index, const TSqsEvents::TEvExecuted::TRecord& reply, const NKikimr::NClient::TValue* messageRecord) {
    auto reqInfoIt = SendMessageRequests_.find(requestId);
    Y_ABORT_UNLESS(reqInfoIt != SendMessageRequests_.end());
    auto& reqInfo = reqInfoIt->second;
    const ui64 shard = reqInfo.Shard;
    auto& messageStatus = reqInfo.Statuses[index];
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(reply.GetStatus());
    RLOG_SQS_REQ_TRACE(reqInfo.Event->Get()->RequestId, "Received reply from DB: " << status);
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        if (!(*messageRecord)["dedupCond"]) {
            // A message with same deduplication id
            // has already been sent.
            if (IsFifoQueue_) {
                messageStatus.SequenceNumber = (*messageRecord)["dedupSelect"]["Offset"];
            }
            messageStatus.MessageId = (*messageRecord)["dedupSelect"]["MessageId"];
            messageStatus.Status = TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::AlreadySent;
        } else {
            if (IsFifoQueue_) {
                messageStatus.SequenceNumber = (*messageRecord)["offset"];
            }
            messageStatus.MessageId = reqInfo.Event->Get()->Messages[index].MessageId;
            messageStatus.Status = TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::OK;
        }
    } else {
        messageStatus.Status = TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::Failed;
    }

    ++reqInfo.AnswersGot;
    if (reqInfo.AnswersGot == reqInfo.Statuses.size()) {
        auto answer = MakeHolder<TSqsEvents::TEvSendMessageBatchResponse>();
        answer->Statuses.swap(reqInfo.Statuses);
        ui64 bytesWritten = 0;
        for (auto& message : reqInfo.Event->Get()->Messages) {
            bytesWritten += message.Body.Size();
        }

        INC_COUNTER_COUPLE(Counters_, SendMessage_Count, sent_count_per_second);
        ADD_COUNTER_COUPLE(Counters_, SendMessage_BytesWritten, sent_bytes_per_second, bytesWritten);
        if (messageStatus.Status == TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::AlreadySent) {
            INC_COUNTER_COUPLE(Counters_, SendMessage_DeduplicationCount, deduplicated_count_per_second);
        }

        Send(reqInfo.Event->Sender, answer.Release());
        SendMessageRequests_.erase(reqInfo.Event->Get()->RequestId);
        DecActiveMessageRequests(shard);
    }
}

void TQueueLeader::OnSendBatchExecuted(ui64 shard, ui64 batchId, const TSqsEvents::TEvExecuted::TRecord& reply) {
    auto& shardInfo = Shards_[shard];
    auto& batchingState = shardInfo.SendBatchingState;
    auto batchIt = batchingState.BatchesExecuting.find(batchId);
    Y_ABORT_UNLESS(batchIt != batchingState.BatchesExecuting.end());
    auto batch = batchIt->second;
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(reply.GetStatus());
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const TValue result(val["result"]);
        Y_ABORT_UNLESS(result.Size() == batch->Size());
        for (size_t i = 0; i < batch->Size(); ++i) {
            const TSendBatchEntry& entry = batch->Entries[i];
            auto messageResult = result[i];
            OnMessageSent(entry.RequestId, entry.IndexInRequest, reply, &messageResult);
            if (entry.Message.Delay) {
                DelayStatistics_.AddDelayedMessage(batch->TransactionStartedTime + entry.Message.Delay, batch->TransactionStartedTime);
            }
        }
        SetMessagesCount(shard, val["newMessagesCount"]);
    } else {
        const TString* prevRequestId = nullptr;
        for (size_t i = 0; i < batch->Size(); ++i) {
            const TSendBatchEntry& entry = batch->Entries[i];
            if (!prevRequestId || *prevRequestId != entry.RequestId) {
                prevRequestId = &entry.RequestId;
                RLOG_SQS_REQ_ERROR(entry.RequestId, "Batch transaction failed: " << reply << ". BatchId: " << batch->BatchId);
            }
            OnMessageSent(entry.RequestId, entry.IndexInRequest, reply, nullptr);
        }
    }
    batchingState.BatchesExecuting.erase(batchId);
    batchingState.TryExecute(this);
}

void TQueueLeader::HandleReceiveMessageBatchWhileIniting(TSqsEvents::TEvReceiveMessageBatch::TPtr& ev) {
    TString reqId = ev->Get()->RequestId;
    Y_ABORT_UNLESS(ReceiveMessageRequests_.emplace(std::move(reqId), std::move(ev)).second);
}

void TQueueLeader::HandleReceiveMessageBatchWhileWorking(TSqsEvents::TEvReceiveMessageBatch::TPtr& ev) {
    TString reqId = ev->Get()->RequestId;
    auto [reqIter, inserted] = ReceiveMessageRequests_.emplace(std::move(reqId), std::move(ev));
    Y_ABORT_UNLESS(inserted);
    ProcessReceiveMessageBatch(reqIter->second);
}

void TQueueLeader::ProcessReceiveMessageBatch(TReceiveMessageBatchRequestProcessing& reqInfo) {
    reqInfo.Init(ShardsCount_); // init if not inited

    if (reqInfo.WaitingAddMessagesToInfly) {
        return;
    }

    if (!IncActiveMessageRequests(reqInfo.GetCurrentShard(), reqInfo.Event->Get()->RequestId)) {
        return;
    }
    if (IsFifoQueue_) {
        reqInfo.LockedFifoMessages.reserve(reqInfo.Event->Get()->MaxMessagesCount);
        LockFifoGroup(reqInfo);
    } else {
        GetMessagesFromInfly(reqInfo);
    }
}

void TQueueLeader::LockFifoGroup(TReceiveMessageBatchRequestProcessing& reqInfo) {
    reqInfo.LockSendTs = TActivationContext::Now();
    auto onExecuted = [this, requestId = reqInfo.Event->Get()->RequestId] (const TSqsEvents::TEvExecuted::TRecord& ev) {
        OnFifoGroupLocked(requestId, ev);
    };

    TExecutorBuilder(SelfId(), reqInfo.Event->Get()->RequestId)
        .User(UserName_)
        .Queue(QueueName_)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(LOCK_GROUP_ID)
        .Counters(Counters_)
        .RetryOnTimeout()
        .OnExecuted(onExecuted)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
            .Uint64("NOW", reqInfo.LockSendTs.MilliSeconds())
            .Utf8("ATTEMPT_ID", reqInfo.Event->Get()->ReceiveAttemptId)
            .Uint64("COUNT", reqInfo.Event->Get()->MaxMessagesCount - reqInfo.LockedFifoMessages.size())
            .Uint64("VISIBILITY_TIMEOUT", reqInfo.Event->Get()->VisibilityTimeout.MilliSeconds())
            .Uint64("GROUPS_READ_ATTEMPT_IDS_PERIOD", Cfg().GetGroupsReadAttemptIdsPeriodMs())
            .String("FROM_GROUP", reqInfo.FromGroup)
            .Uint64("BATCH_SIZE", Cfg().GetGroupSelectionBatchSize())
        .ParentBuilder().Start();
}

void TQueueLeader::OnFifoGroupLocked(const TString& requestId, const TSqsEvents::TEvExecuted::TRecord& ev) {
    auto reqInfoIt = ReceiveMessageRequests_.find(requestId);
    Y_ABORT_UNLESS(reqInfoIt != ReceiveMessageRequests_.end());
    auto& reqInfo = reqInfoIt->second;

    if (ev.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(ev.GetExecutionEngineEvaluatedResponse()));
        const TValue offsets(val["offsets"]);
        const bool appliedOldAttemptId = val["sameCond"];
        const bool truncated = val["truncated"];
        if (truncated) {
            const TValue lastProcessedGroup = val["lastProcessedGroup"];
            reqInfo.FromGroup = lastProcessedGroup["GroupId"];
        }

        for (size_t i = 0; i < offsets.Size(); ++i) {
            reqInfo.LockedFifoMessages.emplace_back();
            auto& msg = reqInfo.LockedFifoMessages.back();
            msg.RandomId = offsets[i]["RandomId"];
            msg.Offset = offsets[i]["Head"];
            msg.GroupId = offsets[i]["GroupId"];
        }

        if (truncated) {
            if (reqInfo.LockedFifoMessages.empty() || appliedOldAttemptId && reqInfo.Event->Get()->MaxMessagesCount > reqInfo.LockedFifoMessages.size()) {
                LockFifoGroup(reqInfo);
            } else {
                ReadFifoMessages(reqInfo);
            }
        } else {
            if (reqInfo.LockedFifoMessages.empty()) {
                Reply(reqInfo);
            } else {
                ReadFifoMessages(reqInfo);
            }
        }
    } else {
        reqInfo.Answer->Failed = true;
        Reply(reqInfo);
    }
}

void TQueueLeader::ReadFifoMessages(TReceiveMessageBatchRequestProcessing& reqInfo) {
    ui32 maxReceiveCount = 0; // not set
    if (Cfg().GetEnableDeadLetterQueues() && DlqInfo_) {
        const auto& dlqInfo(*DlqInfo_);
        if (dlqInfo.DlqName && dlqInfo.QueueId) {
            // dlq is set and resolved
            maxReceiveCount = dlqInfo.MaxReceiveCount;
        }
    }

    TExecutorBuilder builder(SelfId(), reqInfo.Event->Get()->RequestId);
    builder
        .User(UserName_)
        .Queue(QueueName_)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .Counters(Counters_)
        .RetryOnTimeout();

    NClient::TWriteValue params = builder.ParamsValue();
    params["QUEUE_ID_NUMBER"] = QueueVersion_;
    params["QUEUE_ID_NUMBER_HASH"] = GetKeysHash(QueueVersion_);
    params["NOW"] = ui64(TActivationContext::Now().MilliSeconds());
    ui64 index = 0;

    THashSet<TString> usedGroups; // mitigates extremely rare bug with duplicated groups during locking
    for (const auto& msg : reqInfo.LockedFifoMessages) {
        if (usedGroups.insert(msg.GroupId).second) {
            auto key = params["KEYS"].AddListItem();

            key["RandomId"] = msg.RandomId;
            key["Offset"]   = msg.Offset;

            if (maxReceiveCount) {
                key["GroupId"].Bytes(msg.GroupId);
                key["Index"] = index++;
            }
        }
    }

    if (maxReceiveCount) {
        // perform heavy read and move transaction (DLQ)
        Y_ABORT_UNLESS(DlqInfo_);

        builder
            .DlqName(DlqInfo_->QueueId)
            .DlqVersion(DlqInfo_->QueueVersion)
            .DlqTablesFormat(DlqInfo_->TablesFormat)
            .CreateExecutorActor(true)
            .QueryId(READ_OR_REDRIVE_MESSAGE_ID)
            .Params()
                .Uint64("DLQ_ID_NUMBER", DlqInfo_->QueueVersion)
                .Uint64("DLQ_ID_NUMBER_HASH", GetKeysHash(DlqInfo_->QueueVersion))
                .Uint32("MAX_RECEIVE_COUNT", maxReceiveCount)
                .Uint64("RANDOM_ID",  RandomNumber<ui64>());
    } else {
        builder
            .QueryId(READ_MESSAGE_ID);
    }

    const bool usedDLQ = maxReceiveCount > 0;

    builder.OnExecuted([this, requestId = reqInfo.Event->Get()->RequestId, usedDLQ] (const TSqsEvents::TEvExecuted::TRecord& ev) {
        OnFifoMessagesRead(requestId, ev, usedDLQ);
    });

    builder.Start();
}

void TQueueLeader::OnFifoMessagesReadSuccess(const NKikimr::NClient::TValue& value, TReceiveMessageBatchRequestProcessing& reqInfo) {
    const NKikimr::NClient::TValue list(value["result"]);

    if (const ui64 movedMessagesCount = value["movedMessagesCount"]) {
        ADD_COUNTER(Counters_, MessagesMovedToDLQ, movedMessagesCount);

        SetMessagesCount(0, value["newMessagesCount"]);
        
        const auto& moved = value["movedMessages"]; // TODO return only moved offsets
        for (size_t i = 0; i < moved.Size(); ++i) {
            const ui64 offset = moved[i]["SourceOffset"];
            if (offset == Shards_.front().OldestMessageOffset) {
                RequestOldestTimestampMetrics(0);
            }
        }
        SendReloadStateRequestToDLQ();
    }

    reqInfo.Answer->Messages.resize(list.Size());
    for (size_t i = 0; i < list.Size(); ++i) {
        const NKikimr::NClient::TValue& data = list[i]["SourceDataFieldsRead"];
        const NKikimr::NClient::TValue& msg  = list[i]["SourceMessageFieldsRead"];
        const ui64 receiveTimestamp = msg["FirstReceiveTimestamp"];
        auto& msgAnswer = reqInfo.Answer->Messages[i];

        msgAnswer.FirstReceiveTimestamp = (receiveTimestamp ? TInstant::MilliSeconds(receiveTimestamp) : reqInfo.LockSendTs);
        msgAnswer.ReceiveCount = ui32(msg["ReceiveCount"]) + 1; // since the query returns old receive count value
        msgAnswer.MessageId = data["MessageId"];
        msgAnswer.MessageDeduplicationId = data["DedupId"];
        msgAnswer.MessageGroupId = msg["GroupId"];
        msgAnswer.Data = data["Data"];
        msgAnswer.SentTimestamp = TInstant::MilliSeconds(ui64(msg["SentTimestamp"]));
        msgAnswer.SequenceNumber = msg["Offset"];

        msgAnswer.ReceiptHandle.SetMessageGroupId(TString(msg["GroupId"]));
        msgAnswer.ReceiptHandle.SetOffset(msgAnswer.SequenceNumber);
        msgAnswer.ReceiptHandle.SetReceiveRequestAttemptId(reqInfo.Event->Get()->ReceiveAttemptId);
        msgAnswer.ReceiptHandle.SetLockTimestamp(reqInfo.LockSendTs.MilliSeconds());
        msgAnswer.ReceiptHandle.SetShard(0);

        const NKikimr::NClient::TValue senderIdValue = data["SenderId"];
        if (senderIdValue.HaveValue()) {
            if (const TString senderId = TString(senderIdValue)) {
                msgAnswer.SenderId = senderId;
            }
        }

        const NKikimr::NClient::TValue attributesValue = data["Attributes"];
        if (attributesValue.HaveValue()) {
            msgAnswer.MessageAttributes = attributesValue;
        }
    }
}

void TQueueLeader::OnFifoMessagesRead(const TString& requestId, const TSqsEvents::TEvExecuted::TRecord& ev, const bool usedDLQ) {
    auto reqInfoIt = ReceiveMessageRequests_.find(requestId);
    Y_ABORT_UNLESS(reqInfoIt != ReceiveMessageRequests_.end());
    auto& reqInfo = reqInfoIt->second;

    bool dlqExists = true;
    bool success = false;
    if (ev.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        const NKikimr::NClient::TValue value = NKikimr::NClient::TValue::Create(ev.GetExecutionEngineEvaluatedResponse());
        dlqExists = value["dlqExists"];
        if (dlqExists) {
            success = true;
            OnFifoMessagesReadSuccess(value, reqInfo);
        }
    }

    if (!success) {
        const auto errStatus = NKikimr::NTxProxy::TResultStatus::EStatus(ev.GetStatus());
        if (usedDLQ && (!dlqExists || !NTxProxy::TResultStatus::IsSoftErrorWithoutSideEffects(errStatus))) {
            // it's possible that DLQ was removed, hence it'd be wise to refresh corresponding info
            DlqInfo_.Clear();
            reqInfo.Answer->Failed = false;
            reqInfo.Answer->Messages.clear();
        } else {
            reqInfo.Answer->Failed = true;
        }
    }

    Reply(reqInfo);
}

void TQueueLeader::GetMessagesFromInfly(TReceiveMessageBatchRequestProcessing& reqInfo) {
    reqInfo.LockSendTs = TActivationContext::Now();
    Y_ABORT_UNLESS(reqInfo.GetCurrentShard() < Shards_.size());
    const ui64 shard = reqInfo.GetCurrentShard();
    auto& shardInfo = Shards_[shard];
    reqInfo.ReceiveCandidates = shardInfo.Infly->Receive(reqInfo.Event->Get()->MaxMessagesCount, reqInfo.LockSendTs);
    if (reqInfo.ReceiveCandidates) {
        LoadStdMessages(reqInfo);
    } else {
        RLOG_SQS_REQ_DEBUG(reqInfo.Event->Get()->RequestId, "Received empty result from shard " << shard << " infly. Infly capacity: " << shardInfo.Infly->GetCapacity()
                            << ". Messages count: " << shardInfo.MessagesCount);
        if (shardInfo.Infly->GetCapacity() >= INFLY_LIMIT / ShardsCount_) {
            reqInfo.Answer->OverLimit = true;
            Reply(reqInfo);
        } else {
            WaitAddMessagesToInflyOrTryAnotherShard(reqInfo);
        }
    }
}

void TQueueLeader::LoadStdMessages(TReceiveMessageBatchRequestProcessing& reqInfo) {
    const ui64 shard = reqInfo.GetCurrentShard();
    auto& shardInfo = Shards_[shard];
    RLOG_SQS_REQ_DEBUG(reqInfo.Event->Get()->RequestId, "Reading messages. Shard: " << shard);
    shardInfo.LoadBatchingState.AddRequest(reqInfo);
    shardInfo.LoadBatchingState.TryExecute(this);
    for (auto i = reqInfo.ReceiveCandidates.Begin(), end = reqInfo.ReceiveCandidates.End(); i != end; ++i) {
        ++reqInfo.LoadAnswersLeft; // these iterators doesn't support difference_type for std::distance
    }
}

void TQueueLeader::OnLoadStdMessageResult(const TString& requestId, const ui64 offset, bool success, const NKikimr::NClient::TValue* messageRecord, const bool ignoreMessageLoadingErrors) {
    auto reqInfoIt = ReceiveMessageRequests_.find(requestId);
    Y_ABORT_UNLESS(reqInfoIt != ReceiveMessageRequests_.end());
    auto& reqInfo = reqInfoIt->second;

    --reqInfo.LoadAnswersLeft;
    if (success) {
        bool deleted = true;
        bool deadlineChanged = true;
        const bool exists = (*messageRecord)["Exists"];
        const auto wasDeadLetterValue = (*messageRecord)["IsDeadLetter"];
        const bool wasDeadLetter = wasDeadLetterValue.HaveValue() ? bool(wasDeadLetterValue) : false;

        const bool valid = (*messageRecord)["Valid"];
        if (exists && !wasDeadLetter) {
            const ui64 visibilityDeadlineMs = (*messageRecord)["VisibilityDeadline"];
            const ui32 receiveCount = (*messageRecord)["ReceiveCount"];
            const TInstant visibilityDeadline = TInstant::MilliSeconds(visibilityDeadlineMs);
            // Update actual visibility deadline and receive count even if this message won't be given to user in this request.
            // It prevents such synchronization errors later.
            reqInfo.ReceiveCandidates.SetVisibilityDeadlineAndReceiveCount(offset, visibilityDeadline, receiveCount);

            if (valid && reqInfo.ReceiveCandidates.Has(offset)) { // there may be concurrent successful delete message request (purge)
                reqInfo.Answer->Messages.emplace_back();
                auto& msgAnswer = reqInfo.Answer->Messages.back();

                msgAnswer.ReceiptHandle.SetOffset(offset);
                msgAnswer.ReceiptHandle.SetLockTimestamp(ui64((*messageRecord)["LockTimestamp"]));
                msgAnswer.ReceiptHandle.SetShard(reqInfo.GetCurrentShard());

                msgAnswer.FirstReceiveTimestamp = TInstant::MilliSeconds(ui64((*messageRecord)["FirstReceiveTimestamp"]));
                msgAnswer.ReceiveCount = receiveCount;
                msgAnswer.MessageId = (*messageRecord)["MessageId"];
                msgAnswer.Data = TString((*messageRecord)["Data"]);
                msgAnswer.SentTimestamp = TInstant::MilliSeconds(ui64((*messageRecord)["SentTimestamp"]));

                const NKikimr::NClient::TValue senderIdValue = (*messageRecord)["SenderId"];
                if (senderIdValue.HaveValue()) {
                    if (const TString senderId = TString(senderIdValue)) {
                        msgAnswer.SenderId = std::move(senderId);
                    }
                }

                const NKikimr::NClient::TValue attributesValue = (*messageRecord)["Attributes"];
                if (attributesValue.HaveValue()) {
                    msgAnswer.MessageAttributes = attributesValue;
                }
            } else {
                deadlineChanged = true;
                RLOG_SQS_REQ_WARN(requestId, "Attempted to receive message that was received by another leader's request. Shard: " << reqInfo.GetCurrentShard()
                                   << ". Offset: " << offset << ". Visibility deadline: " << visibilityDeadline);
            }
        } else {
            if (exists) { // dlq
                deadlineChanged = !valid;
            }
            if (reqInfo.ReceiveCandidates.Delete(offset)) {
                if (wasDeadLetter) {
                    deleted = false; // Success, not invalidated
                } else {
                    RLOG_SQS_REQ_WARN(requestId, "Attempted to receive message that was deleted. Shard: " << reqInfo.GetCurrentShard() << ". Offset: " << offset);
                    deleted = true;
                }
            } // else there was concurrent delete (purge) by this leader, => OK
        }
        const bool invalidated = deleted || deadlineChanged;
        if (invalidated) {
            auto* detailedCounters = Counters_->GetDetailedCounters();
            INC_COUNTER(detailedCounters, ReceiveMessage_KeysInvalidated);
            const TString& reason = deleted ? INFLY_INVALIDATION_REASON_DELETED : INFLY_INVALIDATION_REASON_DEADLINE_CHANGED;
            MarkInflyReloading(reqInfo.GetCurrentShard(), 1, reason);
        }
    } else {
        reqInfo.LoadError = !ignoreMessageLoadingErrors;
        // there may be other successful loads
    }

    if (reqInfo.LoadAnswersLeft == 0) {
        if (reqInfo.Answer->Messages.empty() && reqInfo.LoadError) {
            reqInfo.Answer->Failed = true;
        }
        Reply(reqInfo);
    }
}

void TQueueLeader::SendReloadStateRequestToDLQ() {
    if (DlqInfo_) {
        Send(MakeSqsProxyServiceID(SelfId().NodeId()), new TSqsEvents::TEvReloadStateRequest(UserName_, DlqInfo_->QueueId));
    } else {
        LOG_SQS_ERROR("Leader for " << TLogQueueName(UserName_, QueueName_) << " don't know about dlq, but messages moved");
    }
}

void TQueueLeader::OnLoadStdMessagesBatchSuccess(const NKikimr::NClient::TValue& value, ui64 shard, TShardInfo& shardInfo, TIntrusivePtr<TLoadBatch> batch) {
    const NKikimr::NClient::TValue list(value["result"]);
    Y_ABORT_UNLESS(list.Size() == batch->Size());

    if (const ui64 movedMessagesCount = value["movedMessagesCount"]) {
        ADD_COUNTER(Counters_, MessagesMovedToDLQ, movedMessagesCount);

        SetMessagesCount(shard, value["newMessagesCount"]);
        SendReloadStateRequestToDLQ();
    }

    THashMap<ui64, const TLoadBatchEntry*> offset2entry;
    offset2entry.reserve(batch->Entries.size());
    for (const TLoadBatchEntry& entry : batch->Entries) {
        offset2entry.emplace(entry.Offset, &entry);
    }

    for (size_t i = 0; i < list.Size(); ++i) {
        auto msg = list[i];
        const ui64 offset = msg["Offset"];
        
        const bool exists = msg["Exists"];
        const auto wasDeadLetterValue = msg["IsDeadLetter"];
        const bool wasDeadLetter = wasDeadLetterValue.HaveValue() ? bool(wasDeadLetterValue) : false;
        const bool valid = msg["Valid"];
        if (exists && wasDeadLetter && valid && shardInfo.OldestMessageOffset == offset) {
            RequestOldestTimestampMetrics(shard);
        }
        const auto entry = offset2entry.find(offset);
        Y_ABORT_UNLESS(entry != offset2entry.end());
        OnLoadStdMessageResult(entry->second->RequestId, offset, true, &msg, false);
    }
}

void TQueueLeader::OnLoadStdMessagesBatchExecuted(ui64 shard, ui64 batchId, const bool usedDLQ, const TSqsEvents::TEvExecuted::TRecord& reply) {
    auto& shardInfo = Shards_[shard];
    auto& batchingState = shardInfo.LoadBatchingState;
    auto batchIt = batchingState.BatchesExecuting.find(batchId);
    Y_ABORT_UNLESS(batchIt != batchingState.BatchesExecuting.end());
    auto batch = batchIt->second;
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(reply.GetStatus());

    bool dlqExists = true;
    bool success = false;
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        const NKikimr::NClient::TValue value = NKikimr::NClient::TValue::Create(reply.GetExecutionEngineEvaluatedResponse());
        dlqExists = value["dlqExists"];
        if (dlqExists) {
            success = true;
            OnLoadStdMessagesBatchSuccess(value, shard, shardInfo, batch);
        }
    }

    if (!success) {
        const auto errStatus = NKikimr::NTxProxy::TResultStatus::EStatus(reply.GetStatus());
        bool ignoreMessageLoadingErrors = false;
        if (usedDLQ && (!dlqExists || !NTxProxy::TResultStatus::IsSoftErrorWithoutSideEffects(errStatus))) {
            // it's possible that DLQ was removed, hence it'd be wise to refresh corresponding info
            DlqInfo_.Clear();
            ignoreMessageLoadingErrors = true;
        }

        const TString* prevRequestId = nullptr;
        for (size_t i = 0; i < batch->Size(); ++i) {
            const TLoadBatchEntry& entry = batch->Entries[i];
            if (!prevRequestId || *prevRequestId != entry.RequestId) {
                prevRequestId = &entry.RequestId;
                RLOG_SQS_REQ_ERROR(entry.RequestId,
                    "Batch transaction failed: " << reply << ". DlqExists=" << dlqExists << ". BatchId: " << batch->BatchId
                );
            }
            OnLoadStdMessageResult(entry.RequestId, entry.Offset, success, nullptr, ignoreMessageLoadingErrors);
        }
    }
    batchingState.BatchesExecuting.erase(batchId);
    batchingState.TryExecute(this);
}

void TQueueLeader::TryReceiveAnotherShard(TReceiveMessageBatchRequestProcessing& reqInfo) {
    const TString& requestId = reqInfo.Event->Get()->RequestId;
    const TInstant waitDeadline = reqInfo.Event->Get()->WaitDeadline;
    const TInstant now = TActivationContext::Now();
    if (!Cfg().GetCheckAllShardsInReceiveMessage() && now >= waitDeadline) {
        if (waitDeadline) {
            RLOG_SQS_REQ_DEBUG(requestId, "Wait time expired. Overworked " << (now - waitDeadline).MilliSeconds() << "ms");
        }
    } else if (reqInfo.CurrentShardIndex + 1 < reqInfo.Shards.size()) {
        DecActiveMessageRequests(reqInfo.GetCurrentShard());
        ++reqInfo.CurrentShardIndex;
        RLOG_SQS_REQ_DEBUG(requestId, "Trying another shard: " << reqInfo.GetCurrentShard());
        reqInfo.LockCount = 0;
        reqInfo.TriedAddMessagesToInfly = false;
        reqInfo.Answer->Retried = true;
        ProcessReceiveMessageBatch(reqInfo);
        return;
    }
    Reply(reqInfo);
}

void TQueueLeader::WaitAddMessagesToInflyOrTryAnotherShard(TReceiveMessageBatchRequestProcessing& reqInfo) {
    const ui64 shard = reqInfo.GetCurrentShard();
    auto& shardInfo = Shards_[shard];
    const TString& requestId = reqInfo.Event->Get()->RequestId;
    const TInstant waitDeadline = reqInfo.Event->Get()->WaitDeadline;
    const TInstant now = TActivationContext::Now();
    if (!Cfg().GetCheckAllShardsInReceiveMessage() && waitDeadline != TInstant::Zero() && now >= waitDeadline) {
        RLOG_SQS_REQ_DEBUG(requestId, "Wait time expired. Overworked " << (now - waitDeadline).MilliSeconds() << "ms");
        Reply(reqInfo);
    } else {
        if (!IsDlqQueue_ && !shardInfo.HasMessagesToAddToInfly() && !shardInfo.NeedAddMessagesToInflyCheckInDatabase()) {
            RLOG_SQS_REQ_DEBUG(requestId, "No known messages in this shard. Skip attempt to add messages to infly");
            ++shardInfo.AddMessagesToInflyCheckAttempts;
            reqInfo.TriedAddMessagesToInfly = true;
        }

        if (reqInfo.TriedAddMessagesToInfly) {
            RLOG_SQS_REQ_DEBUG(requestId, "Already tried to add messages to infly");
            TryReceiveAnotherShard(reqInfo);
            return;
        }

        reqInfo.TriedAddMessagesToInfly = true;
        reqInfo.WaitingAddMessagesToInfly = true;
        DecActiveMessageRequests(reqInfo.GetCurrentShard());
        RLOG_SQS_REQ_DEBUG(requestId, "Waiting for adding messages to infly. AddingMessagesToInfly: " << shardInfo.AddingMessagesToInfly << ". NeedInflyReload: " << shardInfo.NeedInflyReload);
        if (shardInfo.AddingMessagesToInfly) {
            return;
        }
        if (shardInfo.NeedInflyReload) {
            shardInfo.NeedAddingMessagesToInfly = true;
            StartLoadingInfly(shard);
        } else {
            AddMessagesToInfly(shard);
        }
    }
}

void TQueueLeader::Reply(TReceiveMessageBatchRequestProcessing& reqInfo) {
    const ui64 shard = reqInfo.GetCurrentShard();
    if (!reqInfo.Answer->Failed && !reqInfo.Answer->OverLimit) {
        int messageCount = 0;
        ui64 bytesRead = 0;

        for (auto& message : reqInfo.Answer->Messages) {
            COLLECT_HISTOGRAM_COUNTER(Counters_, MessageReceiveAttempts, message.ReceiveCount);
            COLLECT_HISTOGRAM_COUNTER(Counters_, receive_attempts_count_rate, message.ReceiveCount);

            messageCount++;
            bytesRead += message.Data.size();

            const TDuration messageResideDuration = TActivationContext::Now() - message.SentTimestamp;
            COLLECT_HISTOGRAM_COUNTER(Counters_, MessageReside_Duration, messageResideDuration.MilliSeconds());
            COLLECT_HISTOGRAM_COUNTER(Counters_, reside_duration_milliseconds, messageResideDuration.MilliSeconds());
        }

        if (messageCount > 0) {
            ADD_COUNTER_COUPLE(Counters_, ReceiveMessage_Count, received_count_per_second, messageCount);
            ADD_COUNTER_COUPLE(Counters_, ReceiveMessage_BytesRead, received_bytes_per_second, bytesRead);
        }
    }
    Send(reqInfo.Event->Sender, std::move(reqInfo.Answer));
    ReceiveMessageRequests_.erase(reqInfo.Event->Get()->RequestId);
    DecActiveMessageRequests(shard);
}

void TQueueLeader::HandleDeleteMessageBatchWhileIniting(TSqsEvents::TEvDeleteMessageBatch::TPtr& ev) {
    auto key = std::make_pair(ev->Get()->RequestId, ev->Get()->Shard);
    Y_ABORT_UNLESS(DeleteMessageRequests_.emplace(std::move(key), std::move(ev)).second);
}

void TQueueLeader::HandleDeleteMessageBatchWhileWorking(TSqsEvents::TEvDeleteMessageBatch::TPtr& ev) {
    auto key = std::make_pair(ev->Get()->RequestId, ev->Get()->Shard);
    auto [reqIter, inserted] = DeleteMessageRequests_.emplace(std::move(key), std::move(ev));
    Y_ABORT_UNLESS(inserted);
    ProcessDeleteMessageBatch(reqIter->second);
}

void TQueueLeader::ProcessDeleteMessageBatch(TDeleteMessageBatchRequestProcessing& reqInfo) {
    auto& req = reqInfo.Event;
    if (!IncActiveMessageRequests(req->Get()->Shard, req->Get()->RequestId)) {
        return;
    }

    if (!IsFifoQueue_) {
        for (const auto& messageReq : req->Get()->Messages) {
            THolder<TInflyMessage> inflyMessage = Shards_[req->Get()->Shard].Infly->Delete(messageReq.Offset);
            if (inflyMessage) {
                reqInfo.InflyMessages.emplace_back(std::move(inflyMessage));
            } else {
                reqInfo.InflyMessages.emplace_back(); // nullptr
                RLOG_SQS_REQ_WARN(req->Get()->RequestId, "Message with offset " << messageReq.Offset << " was not found in infly");
            }
        }
    }

    auto& shardInfo = Shards_[reqInfo.Event->Get()->Shard];
    shardInfo.DeleteBatchingState.AddRequest(reqInfo);
    shardInfo.DeleteBatchingState.TryExecute(this);
}

void TQueueLeader::OnMessageDeleted(const TString& requestId, ui64 shard, size_t index, const TSqsEvents::TEvExecuted::TRecord& reply, const NKikimr::NClient::TValue* messageRecord) {
    auto key = std::make_pair(requestId, shard);
    auto reqIt = DeleteMessageRequests_.find(key);
    Y_ABORT_UNLESS(reqIt != DeleteMessageRequests_.end());
    auto& reqInfo = reqIt->second;
    auto& req = reqInfo.Event;
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(reply.GetStatus());
    RLOG_SQS_REQ_TRACE(req->Get()->RequestId, "Received reply from DB: " << status);
    ++reqInfo.AnswersGot;
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        reqInfo.Answer->Statuses[index].Status = messageRecord ?
            TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::OK
            : TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::NotFound;

        if (!IsFifoQueue_ && !reqInfo.InflyMessages[index]) { // concurrent receives & change visibilities
            const ui64 offset = reqInfo.Event->Get()->Messages[index].Offset;
            if (!Shards_[shard].Infly->Delete(offset)) {
                bool deleted = false;
                // search in receive requests
                for (auto& [receiveRequestId, receiveRequestInfo] : ReceiveMessageRequests_) {
                    if (receiveRequestInfo.CurrentShardIndex < receiveRequestInfo.Shards.size()
                        && receiveRequestInfo.Shards[receiveRequestInfo.CurrentShardIndex] == shard
                        && receiveRequestInfo.ReceiveCandidates
                        && receiveRequestInfo.ReceiveCandidates.Delete(offset)) {
                        deleted = true;
                        break;
                    }
                }
                // search in change visibility requests
                if (!deleted) {
                    for (auto& [changeVisibilityRequestIdAndShard, changeVisibilityRequestInfo] : ChangeMessageVisibilityRequests_) {
                        if (changeVisibilityRequestIdAndShard.second == shard
                            && changeVisibilityRequestInfo.Candidates
                            && changeVisibilityRequestInfo.Candidates.Delete(offset)) {
                            deleted = true;
                            break;
                        }
                    }
                }
            }
        }
    } else {
        reqInfo.Answer->Statuses[index].Status = TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed;

        // return back to infly
        if (!IsFifoQueue_ && reqInfo.InflyMessages[index]) {
            Shards_[req->Get()->Shard].Infly->Add(std::move(reqInfo.InflyMessages[index]));
        }
    }

    if (reqInfo.AnswersGot == req->Get()->Messages.size()) {
        auto& statuses = reqInfo.Answer->Statuses;
        const ui64 deleted_number = std::count_if(
            statuses.cbegin(),
            statuses.cend(),
            [](auto& messageResult) { 
                return messageResult.Status == TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::OK;
            });
        ADD_COUNTER_COUPLE(Counters_, DeleteMessage_Count, deleted_count_per_second, deleted_number);

        Send(req->Sender, reqInfo.Answer.Release());
        DeleteMessageRequests_.erase(key);
        DecActiveMessageRequests(shard);
    }
}

void TQueueLeader::OnDeleteBatchExecuted(ui64 shard, ui64 batchId, const TSqsEvents::TEvExecuted::TRecord& reply) {
    auto& shardInfo = Shards_[shard];
    auto& batchingState = shardInfo.DeleteBatchingState;
    auto batchIt = batchingState.BatchesExecuting.find(batchId);
    Y_ABORT_UNLESS(batchIt != batchingState.BatchesExecuting.end());
    auto batch = batchIt->second;
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(reply.GetStatus());
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const TValue list(val["deleted"]);
        for (size_t i = 0; i < list.Size(); ++i) {
            auto messageResult = list[i];
            const ui64 offset = messageResult["Offset"];
            const auto [first, last] = batch->Offset2Entry.equal_range(offset);
            Y_ABORT_UNLESS(first != last);
            for (auto el = first; el != last; ++el) {
                const TDeleteBatchEntry& entry = batch->Entries[el->second];
                OnMessageDeleted(entry.RequestId, shard, entry.IndexInRequest, reply, &messageResult);
            }
            batch->Offset2Entry.erase(first, last);
            if (shardInfo.OldestMessageOffset == offset) {
                RequestOldestTimestampMetrics(shard);
            }
        }
        // others are already deleted messages:
        for (const auto& [offset, entryIndex] : batch->Offset2Entry) {
            const TDeleteBatchEntry& entry = batch->Entries[entryIndex];
            OnMessageDeleted(entry.RequestId, shard, entry.IndexInRequest, reply, nullptr);
        }

        SetMessagesCount(shard, val["newMessagesCount"]);
    } else {
        const TString* prevRequestId = nullptr;
        for (size_t i = 0; i < batch->Size(); ++i) {
            const TDeleteBatchEntry& entry = batch->Entries[i];
            if (!prevRequestId || *prevRequestId != entry.RequestId) {
                prevRequestId = &entry.RequestId;
                RLOG_SQS_REQ_ERROR(entry.RequestId, "Batch transaction failed: " << reply << ". BatchId: " << batch->BatchId);
            }
            OnMessageDeleted(entry.RequestId, shard, entry.IndexInRequest, reply, nullptr);
        }
    }
    batchingState.BatchesExecuting.erase(batchId);
    batchingState.TryExecute(this);
}

void TQueueLeader::HandleChangeMessageVisibilityBatchWhileIniting(TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr& ev) {
    auto key = std::make_pair(ev->Get()->RequestId, ev->Get()->Shard);
    Y_ABORT_UNLESS(ChangeMessageVisibilityRequests_.emplace(std::move(key), std::move(ev)).second);
}

void TQueueLeader::HandleChangeMessageVisibilityBatchWhileWorking(TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr& ev) {
    auto key = std::make_pair(ev->Get()->RequestId, ev->Get()->Shard);
    auto [reqIter, inserted] = ChangeMessageVisibilityRequests_.emplace(std::move(key), std::move(ev));
    Y_ABORT_UNLESS(inserted);
    ProcessChangeMessageVisibilityBatch(reqIter->second);
}

void TQueueLeader::ProcessChangeMessageVisibilityBatch(TChangeMessageVisibilityBatchRequestProcessing& reqInfo) {
    auto& req = *reqInfo.Event->Get();
    if (!IncActiveMessageRequests(req.Shard, req.RequestId)) {
        return;
    }
    TExecutorBuilder builder(SelfId(), req.RequestId);
    builder
        .User(UserName_)
        .Queue(QueueName_)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .Shard(req.Shard)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(CHANGE_VISIBILITY_ID)
        .Counters(Counters_)
        .RetryOnTimeout()
        .OnExecuted([this, requestId = req.RequestId, shard = req.Shard](const TSqsEvents::TEvExecuted::TRecord& ev) { OnVisibilityChanged(requestId, shard, ev); });

    builder.Params()
        .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
        .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
        .AddWithType("SHARD", req.Shard, TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
        .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(QueueVersion_, req.Shard))
        .Uint64("NOW", req.NowTimestamp.MilliSeconds())
        .Uint64("GROUPS_READ_ATTEMPT_IDS_PERIOD", Cfg().GetGroupsReadAttemptIdsPeriodMs());
    NClient::TWriteValue params = builder.ParamsValue();
    if (!IsFifoQueue_) {
        reqInfo.Candidates = TInflyMessages::TChangeVisibilityCandidates(Shards_[req.Shard].Infly);
    }
    for (const auto& messageReq : req.Messages) {
        if (!IsFifoQueue_) {
            if (!reqInfo.Candidates.Add(messageReq.Offset)) {
                RLOG_SQS_REQ_WARN(req.RequestId, "Message with offset " << messageReq.Offset << " was not found in infly");
            }
        }
        auto key = params["KEYS"].AddListItem();

        if (IsFifoQueue_) {
            key["GroupId"].Bytes(messageReq.MessageGroupId);
            key["ReceiveAttemptId"] = messageReq.ReceiveAttemptId;
        }
        key["LockTimestamp"] = ui64(messageReq.LockTimestamp.MilliSeconds());
        key["Offset"] = ui64(messageReq.Offset);
        key["NewVisibilityDeadline"] = ui64(messageReq.VisibilityDeadline.MilliSeconds());
    }

    builder.Start();
}

void TQueueLeader::OnVisibilityChanged(const TString& requestId, ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply) {
    auto key = std::make_pair(requestId, shard);
    auto reqIt = ChangeMessageVisibilityRequests_.find(key);
    Y_ABORT_UNLESS(reqIt != ChangeMessageVisibilityRequests_.end());
    auto& reqInfo = reqIt->second;
    auto& req = *reqInfo.Event->Get();
    auto status = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus(reply.GetStatus());
    RLOG_SQS_REQ_TRACE(req.RequestId, "Received reply from DB: " << status);
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const TValue list(val["result"]);
        for (size_t i = 0; i < list.Size(); ++i) {
            const bool exists = list[i]["Exists"];
            if (exists) {
                const bool changeCond = list[i]["ChangeCond"];
                if (changeCond) {
                    reqInfo.Answer->Statuses[i].Status = TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::OK;
                    if (!IsFifoQueue_) {
                        const auto& messageReq = req.Messages[i];
                        reqInfo.Candidates.SetVisibilityDeadline(messageReq.Offset, messageReq.VisibilityDeadline);
                    }
                } else {
                    reqInfo.Answer->Statuses[i].Status = TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::NotInFly;
                    // Correct visibility deadline
                    if (!IsFifoQueue_) {
                        const auto& messageReq = req.Messages[i];
                        const ui64 currentVisibilityDeadline = list[i]["CurrentVisibilityDeadline"];
                        reqInfo.Candidates.SetVisibilityDeadline(messageReq.Offset, TInstant::MilliSeconds(currentVisibilityDeadline));
                    }
                }
            } else {
                reqInfo.Answer->Statuses[i].Status = TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::NotFound;
                if (!IsFifoQueue_) {
                    reqInfo.Candidates.Delete(req.Messages[i].Offset);
                }
            }
        }
    } else {
        for (auto& status : reqInfo.Answer->Statuses) {
            status.Status = TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::Failed;
        }

        // If timeout, it's better to change infly so that if visibility deadline was changed.
        // It won't break consistency (because everything is done through database),
        // but the message may be processed as with new visibility timeout.
        if (!IsFifoQueue_ && status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout) {
            for (const auto& messageReq : req.Messages) {
                reqInfo.Candidates.SetVisibilityDeadline(messageReq.Offset, messageReq.VisibilityDeadline);
            }
        }
    }

    Send(reqInfo.Event->Sender, reqInfo.Answer.Release());
    ChangeMessageVisibilityRequests_.erase(key);

    DecActiveMessageRequests(shard);
}

void TQueueLeader::AnswerGetConfiguration(TSqsEvents::TEvGetConfiguration::TPtr& req) {
    auto resp = MakeHolder<TSqsEvents::TEvConfiguration>();

    resp->RootUrl = RootUrl_;
    resp->SqsCoreCounters = Counters_->RootCounters.SqsCounters;
    resp->QueueCounters = Counters_;
    resp->UserCounters = UserCounters_;
    resp->TablesFormat = TablesFormat_;
    resp->QueueVersion = QueueVersion_;
    resp->Shards = ShardsCount_;
    resp->UserExists = true;
    resp->QueueExists = true;
    resp->Fifo = IsFifoQueue_;
    resp->SchemeCache = SchemeCache_;
    resp->QueueLeader = SelfId();
    resp->QuoterResources = QuoterResources_;

    if (req->Get()->NeedQueueAttributes) {
        Y_ABORT_UNLESS(QueueAttributes_);
        resp->QueueAttributes = QueueAttributes_;
    }

    Send(req->Sender, std::move(resp));
}

void TQueueLeader::AnswerFailed(TSqsEvents::TEvGetConfiguration::TPtr& ev, bool queueRemoved) {
    auto answer = MakeHolder<TSqsEvents::TEvConfiguration>();
    answer->RootUrl = RootUrl_;
    answer->SqsCoreCounters = Counters_->RootCounters.SqsCounters;
    answer->QueueCounters = Counters_;
    answer->UserCounters = UserCounters_;
    
    answer->SchemeCache = SchemeCache_;
    answer->QuoterResources = QuoterResources_;
    if (queueRemoved) {
        answer->UserExists = true;
        answer->QueueExists = false;
    } else {
        answer->Fail = true;
    }
    Send(ev->Sender, answer.Release());
}

void TQueueLeader::RequestConfiguration() {
    TExecutorBuilder(SelfId(), "")
        .User(UserName_)
        .Queue(QueueName_)
        .TablesFormat(TablesFormat_)
        .RetryOnTimeout()
        .Text(Sprintf(GetQueueParamsQuery, Cfg().GetRoot().c_str()))
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnQueueConfiguration(ev); })
        .Counters(Counters_)
        .Params()
            .Utf8("NAME", QueueName_)
            .Utf8("USER_NAME", UserName_)
        .ParentBuilder().StartExecutorActor();
}

void TQueueLeader::OnQueueConfiguration(const TSqsEvents::TEvExecuted::TRecord& ev) {
    if (ev.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(ev.GetExecutionEngineEvaluatedResponse()));

        if (bool(val["exists"])) {
            const auto data(val["queue"]);
            ShardsCount_ = data["Shards"];
            PartitionsCount_ = data["Partitions"];
            QueueId_ = data["QueueId"];
            if (data["Version"].HaveValue()) {
                QueueVersion_ = ui64(data["Version"]);
            }
            if (data["TablesFormat"].HaveValue()) {
                TablesFormat_ = ui32(data["TablesFormat"]);
            }
            IsFifoQueue_ = bool(data["FifoQueue"]);
            Shards_.resize(ShardsCount_);
            const auto& cfg = Cfg();
            if (IsFifoQueue_) {
                for (size_t i = 0; i < ShardsCount_; ++i) {
                    auto& shard = Shards_[i];
                    shard.InflyLoadState = TShardInfo::EInflyLoadState::Fifo;
                    shard.SendBatchingState.Init(cfg.GetFifoQueueSendBatchingPolicy(), i, true);
                    shard.DeleteBatchingState.Init(cfg.GetFifoQueueDeleteBatchingPolicy(), i, true);
                }
            } else {
                for (size_t i = 0; i < ShardsCount_; ++i) {
                    auto& shard = Shards_[i];
                    shard.SendBatchingState.Init(cfg.GetStdQueueSendBatchingPolicy(), i, false);
                    shard.DeleteBatchingState.Init(cfg.GetStdQueueDeleteBatchingPolicy(), i, false);
                    shard.LoadBatchingState.Init(cfg.GetStdQueueLoadBatchingPolicy(), i, false);
                }
            }

            std::vector<TSqsEvents::TEvGetConfiguration::TPtr> needAttributesRequests;
            for (auto& req : GetConfigurationRequests_) {
                if (req->Get()->NeedQueueAttributes) {
                    needAttributesRequests.emplace_back(std::move(req));
                    continue;
                }

                AnswerGetConfiguration(req);
            }
            GetConfigurationRequests_.swap(needAttributesRequests);

            if (!GetConfigurationRequests_.empty()) {
                AskQueueAttributes();
            }

            if (!IsFifoQueue_) {
                StartLoadingInfly();
            }

            InitQuoterResources();

            BecomeWorking();
        } else {
            INC_COUNTER(Counters_, QueueMasterStartProblems);
            INC_COUNTER(Counters_, QueueLeaderStartProblems);

            for (auto& req : GetConfigurationRequests_) {
                RLOG_SQS_REQ_DEBUG(req->Get()->RequestId, "Queue [" << req->Get()->QueueName << "] was not found in Queues table for user [" << req->Get()->UserName << "]");
                auto answer = MakeHolder<TSqsEvents::TEvConfiguration>();
                answer->UserExists = true;
                answer->QueueExists = false;
                answer->RootUrl = RootUrl_;
                answer->SqsCoreCounters = Counters_->RootCounters.SqsCounters;
                answer->QueueCounters = Counters_;
                answer->UserCounters = UserCounters_;
                answer->Fail = false;
                answer->SchemeCache = SchemeCache_;
                answer->QuoterResources = QuoterResources_;
                Send(req->Sender, answer.Release());
            }
            GetConfigurationRequests_.clear();

            ScheduleGetConfigurationRetry();
        }
    } else {
        INC_COUNTER(Counters_, QueueMasterStartProblems);
        INC_COUNTER(Counters_, QueueLeaderStartProblems);
        FailRequestsDuringStartProblems();
        ScheduleGetConfigurationRetry();
    }
}

void TQueueLeader::FailRequestsDuringStartProblems() {
    for (auto& req : GetConfigurationRequests_) {
        AnswerFailed(req);
    }
    GetConfigurationRequests_.clear();
}

void TQueueLeader::ScheduleGetConfigurationRetry() {
    Schedule(TDuration::MilliSeconds(100 + RandomNumber<ui32>(300)), new TEvWakeup(REQUEST_CONFIGURATION_TAG));
}

void TQueueLeader::AskQueueAttributes() {
    if (AskQueueAttributesInProcess_) {
        return;
    }
    AskQueueAttributesInProcess_ = true;
    const TString reqId = CreateGuidAsString();
    LOG_SQS_DEBUG("Executing queue " << TLogQueueName(UserName_, QueueName_) << " attributes cache request. Req id: " << reqId);
    TExecutorBuilder(SelfId(), reqId)
        .User(UserName_)
        .Queue(QueueName_)
        .QueueLeader(SelfId())
        .QueryId(INTERNAL_GET_QUEUE_ATTRIBUTES_ID)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .TablesFormat(TablesFormat_)
        .RetryOnTimeout()
        .OnExecuted([this](const TSqsEvents::TEvExecuted::TRecord& ev) { OnQueueAttributes(ev); })
        .Counters(Counters_)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
        .ParentBuilder().Start();   
}

void TQueueLeader::OnQueueAttributes(const TSqsEvents::TEvExecuted::TRecord& ev) {
    AskQueueAttributesInProcess_ = false;
    const ui32 status = ev.GetStatus();
    bool queueExists = true;
    if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(ev.GetExecutionEngineEvaluatedResponse()));

        queueExists = val["queueExists"];
        if (queueExists) {
            const TValue& attrs(val["attrs"]);

            TSqsEvents::TQueueAttributes attributes;
            attributes.ContentBasedDeduplication = attrs["ContentBasedDeduplication"];
            attributes.DelaySeconds = TDuration::MilliSeconds(attrs["DelaySeconds"]);
            attributes.FifoQueue = attrs["FifoQueue"];
            attributes.MaximumMessageSize = attrs["MaximumMessageSize"];
            attributes.MessageRetentionPeriod = TDuration::MilliSeconds(attrs["MessageRetentionPeriod"]);
            attributes.ReceiveMessageWaitTime = TDuration::MilliSeconds(attrs["ReceiveMessageWaitTime"]);
            attributes.VisibilityTimeout = TDuration::MilliSeconds(attrs["VisibilityTimeout"]);

            const TValue showDetailedCountersDeadline = attrs["ShowDetailedCountersDeadline"];
            if (showDetailedCountersDeadline.HaveValue()) {
                const ui64 ms = showDetailedCountersDeadline;
                Counters_->ShowDetailedCounters(TInstant::MilliSeconds(ms));
            }

            // update dead letter queue info
            const auto& dlqNameVal(attrs["DlqName"]);
            const auto& maxReceiveCountVal(attrs["MaxReceiveCount"]);
            if (dlqNameVal.HaveValue() && maxReceiveCountVal.HaveValue()) {
                TTargetDlqInfo info;
                info.DlqName = TString(dlqNameVal);
                info.MaxReceiveCount = ui64(maxReceiveCountVal);
                if (info.DlqName && info.MaxReceiveCount) {
                    DlqInfo_ = info;
                    // now we have to discover queue id and version
                    Send(MakeSqsServiceID(SelfId().NodeId()), new TSqsEvents::TEvGetQueueId("DLQ", UserName_, info.DlqName, FolderId_));
                } else {
                    DlqInfo_.Clear();
                }
            }

            if (!QueueAttributes_ || QueueAttributes_->MessageRetentionPeriod > attributes.MessageRetentionPeriod) {
                RetentionWakeupPlannedAt_ = TInstant::Zero();
            }
            QueueAttributes_ = attributes;
            AttributesUpdateTime_ = TActivationContext::Now();
            for (auto& req : GetConfigurationRequests_) {
                AnswerGetConfiguration(req);
            }
            GetConfigurationRequests_.clear();
            return;
        }
    }

    for (auto& req : GetConfigurationRequests_) {
        AnswerFailed(req, !queueExists);
    }
    GetConfigurationRequests_.clear();
}

void TQueueLeader::HandleQueueId(TSqsEvents::TEvQueueId::TPtr& ev) {
    if (!DlqInfo_) {
        return;
    }

    if (ev->Get()->Failed) {
        LOG_SQS_DEBUG("Dlq discovering failed");
    } else {
        if (ev->Get()->Exists) {
            DlqInfo_->QueueId = ev->Get()->QueueId;
            DlqInfo_->QueueVersion = ev->Get()->Version;
            DlqInfo_->ShardsCount = ev->Get()->ShardsCount;
            DlqInfo_->TablesFormat = ev->Get()->TablesFormat;

            LOG_SQS_DEBUG("Discovered DLQ: name: " << DlqInfo_->DlqName
                << ", maxReceiveCount: " << DlqInfo_->MaxReceiveCount
                << ", queueId: " << DlqInfo_->QueueId
                << ", version: " << DlqInfo_->QueueVersion
                << ", shards count: " << DlqInfo_->ShardsCount
                << ", tables format: " << DlqInfo_->TablesFormat);
            return;
        }
    }

    DlqInfo_.Clear(); // something is off
}

void TQueueLeader::HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev) {
    ev->Get()->Call();
}

void TQueueLeader::HandlePurgeQueue(TSqsEvents::TEvPurgeQueue::TPtr& ev) {
    CreateBackgroundActors();
    Send(PurgeActor_, MakeHolder<TSqsEvents::TEvPurgeQueue>(*ev->Get()));
}

void TQueueLeader::CheckStillDLQ() {
    if (!IsFifoQueue_ && (TActivationContext::Now() - LatestDlqNotificationTs_ >= TDuration::MilliSeconds(Cfg().GetDlqNotificationGracePeriodMs()))) {
        if (IsDlqQueue_) {
            LOG_SQS_INFO("Stopped periodic message counting for queue " << TLogQueueName(UserName_, QueueName_)
                                                                        << ". Latest dlq notification was at " << LatestDlqNotificationTs_);
        }

        IsDlqQueue_ = false;
    }
}


void TQueueLeader::StartGatheringMetrics() {
    if (UseCPUOptimization) {
        return;
    }
    
    for (ui64 shard = 0; shard < ShardsCount_; ++shard) {
        if (IsFifoQueue_ || IsDlqQueue_) {
            RequestMessagesCountMetrics(shard);
        }
        RequestOldestTimestampMetrics(shard);
    }
}

void TQueueLeader::RequestMessagesCountMetrics(ui64 shard) {
    if (Shards_[shard].MessagesCountIsRequesting) {
        LOG_SQS_DEBUG("Messages count for " << TLogQueueName(UserName_, QueueName_, shard) << " is already requesting");
        return;
    }
    TExecutorBuilder(SelfId(), "")
        .User(UserName_)
        .Queue(QueueName_)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(GET_MESSAGE_COUNT_METRIC_ID)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .RetryOnTimeout()
        .OnExecuted([this, shard](const TSqsEvents::TEvExecuted::TRecord& ev) { ReceiveMessagesCountMetrics(shard, ev); })
        .Counters(Counters_)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
            .AddWithType("SHARD", shard, TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
        .ParentBuilder().Start();
    ++MetricsQueriesInfly_;

    Shards_[shard].MessagesCountIsRequesting = true;
}

void TQueueLeader::RequestOldestTimestampMetrics(ui64 shard) {
    if (Shards_[shard].OldestMessageAgeIsRequesting) {
        LOG_SQS_DEBUG("Oldest message timestamp " << TLogQueueName(UserName_, QueueName_, shard) << " is already requesting");
        return;
    }
    TExecutorBuilder(SelfId(), "")
        .User(UserName_)
        .Queue(QueueName_)
        .Shard(shard)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(GET_OLDEST_MESSAGE_TIMESTAMP_METRIC_ID)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .RetryOnTimeout()
        .OnExecuted([this, shard](const TSqsEvents::TEvExecuted::TRecord& ev) { ReceiveOldestTimestampMetrics(shard, ev); })
        .Counters(Counters_)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
            .AddWithType("SHARD", shard, TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
            .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(QueueVersion_, shard))
            .Uint64("TIME_FROM", Shards_[shard].LastSuccessfulOldestMessageTimestampValueMs) // optimization for accurate range selection // timestamp is always nondecreasing
        .ParentBuilder().Start();
    ++MetricsQueriesInfly_;

    Shards_[shard].OldestMessageAgeIsRequesting = true;
}

void TQueueLeader::ReceiveMessagesCountMetrics(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply) {
    LOG_SQS_DEBUG("Handle message count metrics for " << TLogQueueName(UserName_, QueueName_, shard));
    Y_ABORT_UNLESS(MetricsQueriesInfly_ > 0);
    --MetricsQueriesInfly_;
    if (MetricsQueriesInfly_ == 0 && !UseCPUOptimization) {
        ScheduleMetricsRequest();
    }
    Y_ABORT_UNLESS(shard < Shards_.size());
    Shards_[shard].MessagesCountIsRequesting = false;
    Shards_[shard].MessagesCountWasGot = true;
    if (reply.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        SetMessagesCount(shard, val["messagesCount"]);
        SetInflyMessagesCount(shard, val["inflyMessagesCount"]);
        const TValue createdTimestamp = val["createdTimestamp"];
        if (!createdTimestamp.IsNull()) {
            Shards_[shard].CreatedTimestamp = TInstant::MilliSeconds(ui64(createdTimestamp));
        }
        ProcessGetRuntimeQueueAttributes(shard);
    } else {
        LOG_SQS_ERROR("Failed to get message count metrics for " << TLogQueueName(UserName_, QueueName_, shard) << ": " << reply);
        // leave old metrics values
        FailGetRuntimeQueueAttributesForShard(shard);
    }
    ReportMessagesCountMetricsIfReady();
}

void TQueueLeader::PlanningRetentionWakeup() {
    TInstant now = TActivationContext::Now();
    if (!QueueAttributes_ || RetentionWakeupPlannedAt_ >= now || !UseCPUOptimization) {
        return;
    }
    TInstant firstExpiredAt = TInstant::Max();
    for (ui64 shard = 0; shard < ShardsCount_; ++shard) {
        auto oldestMessagesTimestampMs = Shards_[shard].OldestMessageTimestampMs;
        if (oldestMessagesTimestampMs != Max<ui64>()) {
            TInstant expiredAt = TInstant::MilliSeconds(oldestMessagesTimestampMs) + QueueAttributes_->MessageRetentionPeriod;
            firstExpiredAt = Min(firstExpiredAt, expiredAt);
        }
    }
    if (firstExpiredAt == TInstant::Max()) {
        return;
    }

    TInstant nextWakeupAt = Max(
        now,
        Max(RetentionWakeupPlannedAt_, firstExpiredAt) + RandomRetentionPeriod()
    );
    
    CreateBackgroundActors();
    LOG_SQS_DEBUG("Next retantion wakeup for " << TLogQueueName(UserName_, QueueName_) << " planned at " << nextWakeupAt
        << " retention period " << QueueAttributes_->MessageRetentionPeriod);
    RetentionWakeupPlannedAt_ = nextWakeupAt;
    TActivationContext::Schedule(RetentionWakeupPlannedAt_, new IEventHandle(RetentionActor_, SelfId(), new TEvWakeup()));
}

void TQueueLeader::ReceiveOldestTimestampMetrics(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply) {
    LOG_SQS_DEBUG("Handle oldest timestamp metrics for " << TLogQueueName(UserName_, QueueName_, shard));
    Y_ABORT_UNLESS(MetricsQueriesInfly_ > 0);
    --MetricsQueriesInfly_;
    if (MetricsQueriesInfly_ == 0 && !UseCPUOptimization) {
        ScheduleMetricsRequest();
    }
    Y_ABORT_UNLESS(shard < Shards_.size());
    Shards_[shard].OldestMessageAgeIsRequesting = false;
    if (reply.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const TValue list = val["messages"];
        if (list.Size()) {
            Shards_[shard].LastSuccessfulOldestMessageTimestampValueMs = Shards_[shard].OldestMessageTimestampMs = list[0]["SentTimestamp"];
            Shards_[shard].OldestMessageOffset = list[0]["Offset"];
        } else {
            Shards_[shard].OldestMessageTimestampMs = Max();
        }
    } else {
        LOG_SQS_ERROR("Failed to get oldest timestamp metrics for " << TLogQueueName(UserName_, QueueName_, shard) << ": " << reply);
        // leave old metrics values
    }
    ReportOldestTimestampMetricsIfReady();
}

ui64 GetStateValue(const NKikimr::NClient::TValue& value) {
    const i64 parsed = value;
    Y_ABORT_UNLESS(parsed >= 0);
    return static_cast<ui64>(parsed);
}

void TQueueLeader::SetInflyMessagesCount(ui64 shard, const NKikimr::NClient::TValue& value) {
    if (value.IsNull()) { // can be null in case of parallel queue deletion (SQS-292)
        return;
    }
    ui64 newValue = GetStateValue(value);
    Shards_[shard].InflyMessagesCount = newValue;
}

void TQueueLeader::SetMessagesCount(ui64 shard, const NKikimr::NClient::TValue& value) {
    if (value.IsNull()) { // can be null in case of parallel queue deletion (SQS-292)
        return;
    }
    ui64 newValue = GetStateValue(value);
    SetMessagesCount(shard, newValue);
}

void TQueueLeader::SetMessagesCount(ui64 shard, ui64 newMessagesCount) {
    TShardInfo& shardInfo = Shards_[shard];
    if (UseCPUOptimization) {
        if (shardInfo.MessagesCount == 0 && newMessagesCount > 0) {
            RequestOldestTimestampMetrics(shard);
        }
    }
    shardInfo.MessagesCount = newMessagesCount;
}

void TQueueLeader::ScheduleMetricsRequest() {
    const ui64 updateTime = Cfg().GetBackgroundMetricsUpdateTimeMs();
    const ui64 randomTimeToWait = RandomNumber<ui32>(updateTime / 4);
    Schedule(TDuration::MilliSeconds(updateTime + randomTimeToWait), new TEvWakeup(UPDATE_COUNTERS_TAG));
}

void TQueueLeader::ReportMessagesCountMetricsIfReady() {
    ui64 messagesCount = 0;
    ui64 inflyMessagesCount = 0;
    const TInstant now = TActivationContext::Now();
    for (const auto& shardInfo : Shards_) {
        if (IsFifoQueue_) {
            if (shardInfo.MessagesCountIsRequesting || !shardInfo.MessagesCountWasGot) {
                return;
            }
        } else {
            if (shardInfo.InflyLoadState != TShardInfo::EInflyLoadState::Loaded) {
                return;
            }
            inflyMessagesCount += shardInfo.Infly->GetInflyCount(now);
        }
        messagesCount += shardInfo.MessagesCount;
    }

    if (Counters_) {
        SET_COUNTER_COUPLE(Counters_, MessagesCount, stored_count, messagesCount);
        if (!IsFifoQueue_) { // for fifo queues infly is always 0
            SET_COUNTER_COUPLE(Counters_, InflyMessagesCount, inflight_count, inflyMessagesCount);
        }
    }
}

void TQueueLeader::ReportOldestTimestampMetricsIfReady() {
    ui64 oldestMessagesTimestamp = Max();
    for (const auto& shardInfo : Shards_) {
        if (shardInfo.OldestMessageAgeIsRequesting) {
            return;
        }
        ui64 ts = shardInfo.OldestMessageTimestampMs;
        if (UseCPUOptimization && shardInfo.MessagesCountWasGot && shardInfo.MessagesCount == 0) {
            ts = Max<ui64>();
        }
        oldestMessagesTimestamp = Min(oldestMessagesTimestamp, ts);
    }

    if (Counters_) {
        if (oldestMessagesTimestamp != Max<ui64>()) {
            auto age = (TActivationContext::Now() - TInstant::MilliSeconds(oldestMessagesTimestamp)).Seconds();
            SET_COUNTER_COUPLE(Counters_, OldestMessageAgeSeconds, oldest_age_milliseconds, age);
        } else {
            SET_COUNTER_COUPLE(Counters_, OldestMessageAgeSeconds, oldest_age_milliseconds, 0);
        }
    }
}

void TQueueLeader::CreateBackgroundActors() {
    if ((!IsFifoQueue_ || DeduplicationCleanupActor_) && (!IsFifoQueue_ || ReadsCleanupActor_) && RetentionActor_ && PurgeActor_) {
        return;
    }

    if (IsFifoQueue_) {
        auto createCleaner = [&](TCleanupActor::ECleanupType type) {
            auto actor = Register(new TCleanupActor(GetQueuePath(), TablesFormat_, SelfId(), type));
            LOG_SQS_DEBUG(
                "Created new " << type << " cleanup actor for queue " << TLogQueueName(UserName_, QueueName_)
                    << ". Actor id: " << actor
            );
            return actor;
        };
        if (!DeduplicationCleanupActor_) {
            DeduplicationCleanupActor_ = createCleaner(TCleanupActor::ECleanupType::Deduplication);
        }
        if (!ReadsCleanupActor_) {
            ReadsCleanupActor_ = createCleaner(TCleanupActor::ECleanupType::Reads);
        }
    }
    if (!RetentionActor_) {
        RetentionActor_ = Register(new TRetentionActor(GetQueuePath(), TablesFormat_, SelfId(), UseCPUOptimization));
        LOG_SQS_DEBUG("Created new retention actor for queue " << TLogQueueName(UserName_, QueueName_) << ". Actor id: " << RetentionActor_);
    }
    if (!PurgeActor_) {
        PurgeActor_ = Register(new TPurgeActor(GetQueuePath(), TablesFormat_, Counters_, SelfId(), IsFifoQueue_));
        LOG_SQS_DEBUG("Created new purge actor for queue " << TLogQueueName(UserName_, QueueName_) << ". Actor id: " << PurgeActor_);
    }
}

void TQueueLeader::MarkInflyReloading(ui64 shard, i64 invalidatedCount, const TString& invalidationReason) {
    LWPROBE(InflyInvalidation, UserName_, QueueName_, shard, invalidatedCount, invalidationReason);
    auto& shardInfo = Shards_[shard];
    if (!shardInfo.NeedInflyReload) {
        shardInfo.NeedInflyReload = true;
        LOG_SQS_WARN("Mark infly " << TLogQueueName(UserName_, QueueName_, shard) << " for reloading. Reason: " << invalidationReason);
    }
}

void TQueueLeader::StartLoadingInfly() {
    for (ui64 shard = 0; shard < Shards_.size(); ++shard) {
        StartLoadingInfly(shard);
    }
}

void TQueueLeader::StartLoadingInfly(ui64 shard, bool afterFailure) {
    auto& shardInfo = Shards_[shard];
    if (shardInfo.InflyLoadState == TShardInfo::EInflyLoadState::Fifo
        || shardInfo.InflyLoadState == TShardInfo::EInflyLoadState::WaitingForActiveRequests && shardInfo.ActiveMessageRequests > 0
        || shardInfo.InflyLoadState == TShardInfo::EInflyLoadState::WaitingForDbAnswer
        || shardInfo.InflyLoadState == TShardInfo::EInflyLoadState::Failed && !afterFailure) {
        LOG_SQS_TRACE("Start loading infly for queue " << TLogQueueName(UserName_, QueueName_, shard)
                    << ". Skipping. State: " << static_cast<int>(shardInfo.InflyLoadState)
                    << ". ActiveMessageRequests: " << shardInfo.ActiveMessageRequests
                    << ". After failure: " << afterFailure);
        return;
    }

    if (shardInfo.ActiveMessageRequests > 0) {
        LOG_SQS_DEBUG("Start loading infly for queue " << TLogQueueName(UserName_, QueueName_, shard) << ". Waiting for active message requests. Requests count: " << shardInfo.ActiveMessageRequests);
        shardInfo.InflyLoadState = TShardInfo::EInflyLoadState::WaitingForActiveRequests;
        return;
    }

    LOG_SQS_INFO("Start loading infly for queue " << TLogQueueName(UserName_, QueueName_, shard));
    shardInfo.InflyLoadState = TShardInfo::EInflyLoadState::WaitingForDbAnswer;
    Y_ABORT_UNLESS(!shardInfo.LoadInflyRequestInProcess);
    shardInfo.LoadInflyRequestInProcess = true;
    shardInfo.NeedInflyReload = false;
    shardInfo.Infly = nullptr;
    TExecutorBuilder(SelfId(), "")
        .User(UserName_)
        .Queue(QueueName_)
        .Shard(shard)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(LOAD_INFLY_ID)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .RetryOnTimeout()
        .OnExecuted([this, shard](const TSqsEvents::TEvExecuted::TRecord& ev) { OnInflyLoaded(shard, ev); })
        .Counters(Counters_)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
            .AddWithType("SHARD", shard, TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
            .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(QueueVersion_, shard))
        .ParentBuilder().Start();
}

void TQueueLeader::OnInflyLoaded(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply) {
    LOG_SQS_TRACE("Infly load reply for shard " << TLogQueueName(UserName_, QueueName_, shard) << ": " << reply);
    auto& shardInfo = Shards_[shard];
    Y_ABORT_UNLESS(shardInfo.LoadInflyRequestInProcess);
    shardInfo.LoadInflyRequestInProcess = false;
    const bool ok = reply.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete;
    if (ok) {
        shardInfo.Infly = MakeIntrusive<TInflyMessages>();
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const TValue list = val["infly"];
        const TInstant now = TActivationContext::Now();
        for (size_t i = 0, size = list.Size(); i < size; ++i) {
            const TValue& message = list[i];
            const TValue& visibilityDeadlineValue = message["VisibilityDeadline"];
            const ui64 visibilityDeadlineMs = visibilityDeadlineValue.HaveValue() ? ui64(visibilityDeadlineValue) : 0;
            const TValue& delayDeadlineValue = message["DelayDeadline"];
            const ui64 delayDeadlineMs = delayDeadlineValue.HaveValue() ? ui64(delayDeadlineValue) : 0;
            const TInstant delayDeadline = TInstant::MilliSeconds(delayDeadlineMs);
            if (delayDeadline && !shardInfo.DelayStatisticsInited && delayDeadline > now) {
                DelayStatistics_.AddDelayedMessage(delayDeadline, now);
            }
            const ui64 offset = message["Offset"];
            const ui32 receiveCount = message["ReceiveCount"];
            const TInstant maxVisibilityDeadline = TInstant::MilliSeconds(Max(visibilityDeadlineMs, delayDeadlineMs));
            LOG_SQS_TRACE("Adding message to infly struct for shard " << TLogQueueName(UserName_, QueueName_, shard) << ": { Offset: " << offset << ", VisibilityDeadline: " << maxVisibilityDeadline << ", ReceiveCount: " << receiveCount << " }");
            shardInfo.Infly->Add(MakeHolder<TInflyMessage>(offset, message["RandomId"], maxVisibilityDeadline, receiveCount));
        }
        LWPROBE(LoadInfly, UserName_, QueueName_, shard, list.Size());
        shardInfo.InflyVersion = val["inflyVersion"];
        LOG_SQS_DEBUG("Infly version for shard " << TLogQueueName(UserName_, QueueName_, shard) << ": " << shardInfo.InflyVersion);

        if (!val["messageCount"].HaveValue() ||
            !val["inflyCount"].HaveValue() ||
            !val["readOffset"].HaveValue() ||
            !val["createdTimestamp"].HaveValue()
        ) {
            return;  
        }
        SetMessagesCount(shard, val["messageCount"]);
        SetInflyMessagesCount(shard, val["inflyCount"]);
        shardInfo.ReadOffset = val["readOffset"];
        shardInfo.CreatedTimestamp = TInstant::MilliSeconds(ui64(val["createdTimestamp"]));
        

        shardInfo.DelayStatisticsInited = true;

        if (shardInfo.NeedAddingMessagesToInfly) {
            const ui64 limit = INFLY_LIMIT / ShardsCount_;
            if (shardInfo.MessagesCount == 0 || shardInfo.Infly->GetCapacity() >= limit) {
                ProcessReceivesAfterAddedMessagesToInfly(shard);
            } else {
                AddMessagesToInfly(shard);
            }
        }

        shardInfo.InflyLoadState = TShardInfo::EInflyLoadState::Loaded;
        StartMessageRequestsAfterInflyLoaded(shard);
        ProcessGetRuntimeQueueAttributes(shard);
    } else {
        LOG_SQS_ERROR("Failed to load infly for " << TLogQueueName(UserName_, QueueName_, shard) << ": " << reply);
        FailMessageRequestsAfterInflyLoadFailure(shard);
        FailGetRuntimeQueueAttributesForShard(shard);

        shardInfo.InflyLoadState = TShardInfo::EInflyLoadState::Failed;
        ScheduleInflyLoadAfterFailure(shard);
    }
}

bool TQueueLeader::AddMessagesToInfly(ui64 shard) {
    auto& shardInfo = Shards_[shard];
    LOG_SQS_INFO("Adding messages to infly for queue " << TLogQueueName(UserName_, QueueName_, shard));
    shardInfo.AddingMessagesToInfly = true;
    shardInfo.AddMessagesToInflyCheckAttempts = 0;
    const ui64 limit = INFLY_LIMIT / ShardsCount_;
    TExecutorBuilder(SelfId(), "")
        .User(UserName_)
        .Queue(QueueName_)
        .Shard(shard)
        .QueueLeader(SelfId())
        .TablesFormat(TablesFormat_)
        .QueryId(ADD_MESSAGES_TO_INFLY_ID)
        .QueueVersion(QueueVersion_)
        .Fifo(IsFifoQueue_)
        .OnExecuted([this, shard](const TSqsEvents::TEvExecuted::TRecord& ev) { OnAddedMessagesToInfly(shard, ev); })
        .Counters(Counters_)
        .Params()
            .Uint64("QUEUE_ID_NUMBER", QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(QueueVersion_))
            .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(QueueVersion_, shard))
            .AddWithType("SHARD", shard, TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
            .Uint64("INFLY_LIMIT", limit)
            .Uint64("FROM", shardInfo.ReadOffset)
            .Uint64("EXPECTED_MAX_COUNT", Min(limit - shardInfo.Infly->GetCapacity(), Cfg().GetAddMesagesToInflyBatchSize()))
        .ParentBuilder().Start();
    return true;
}

void TQueueLeader::OnAddedMessagesToInfly(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply) {
    auto& shardInfo = Shards_[shard];
    Y_ABORT_UNLESS(shardInfo.AddingMessagesToInfly);
    shardInfo.AddingMessagesToInfly = false;
    shardInfo.LastAddMessagesToInfly = TActivationContext::Now();

    bool markInflyReloading = false;
    i64 inflyVersionDiff = 0;
    if (reply.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete) {
        using NKikimr::NClient::TValue;
        const TValue val(TValue::Create(reply.GetExecutionEngineEvaluatedResponse()));
        const ui64 currentInflyVersion = val["currentInflyVersion"];
        if (shardInfo.InflyVersion != currentInflyVersion) {
            inflyVersionDiff = i64(currentInflyVersion) - shardInfo.InflyVersion;
            LOG_SQS_WARN("Concurrent infly version change detected for " << TLogQueueName(UserName_, QueueName_, shard) << ". Expected "
                       << shardInfo.InflyVersion << ", but got: " << currentInflyVersion << ". Mark infly for reloading");
            markInflyReloading = true;
        }
        if (shardInfo.InflyVersion > currentInflyVersion) {
            LOG_SQS_ERROR("Skip added messages to inflight because infly version is outdated for " << TLogQueueName(UserName_, QueueName_, shard)
                << ". Known " << shardInfo.InflyVersion << ", got " << currentInflyVersion);
        } else {
            shardInfo.InflyVersion = val["newInflyVersion"];

            const TValue list = val["messages"];
            for (size_t i = 0, size = list.Size(); i < size; ++i) {
                const TValue& message = list[i];
                const TValue& delayDeadlineValue = message["DelayDeadline"];
                const ui64 delayDeadlineMs = delayDeadlineValue.HaveValue() ? ui64(delayDeadlineValue) : 0;
                const TInstant delayDeadline = TInstant::MilliSeconds(delayDeadlineMs);
                const ui64 offset = message["Offset"];
                const ui32 receiveCount = 0; // as in transaction
                LOG_SQS_TRACE("Adding message to infly struct for shard " << TLogQueueName(UserName_, QueueName_, shard) << ": { Offset: " << offset << ", DelayDeadline: " << delayDeadline << ", ReceiveCount: " << receiveCount << " }");
                shardInfo.Infly->Add(MakeHolder<TInflyMessage>(offset, message["RandomId"], delayDeadline, receiveCount));
            }
            LWPROBE(AddMessagesToInfly, UserName_, QueueName_, shard, list.Size());
            shardInfo.ReadOffset = val["readOffset"];

            // Update messages count
            SetMessagesCount(shard, val["messagesCount"]);
        }
    } else {
        LOG_SQS_ERROR("Failed to add new messages to infly for " << TLogQueueName(UserName_, QueueName_, shard) << ": " << reply);
    }

    ProcessReceivesAfterAddedMessagesToInfly(shard);

    // First process requests and then reload infly
    if (markInflyReloading) {
        MarkInflyReloading(shard, inflyVersionDiff, INFLY_INVALIDATION_REASON_VERSION_CHANGED);
    }
}

void TQueueLeader::ProcessReceivesAfterAddedMessagesToInfly(ui64 shard) {
    std::vector<TReceiveMessageBatchRequestProcessing*> requestsToContinue;
    requestsToContinue.reserve(ReceiveMessageRequests_.size());
    for (auto&& [reqId, req] : ReceiveMessageRequests_) {
        if (req.GetCurrentShard() == shard && req.WaitingAddMessagesToInfly) {
            requestsToContinue.push_back(&req);
        }
    }
    for (auto* req : requestsToContinue) {
        req->WaitingAddMessagesToInfly = false;
        ProcessReceiveMessageBatch(*req);
    }
}

void TQueueLeader::FailMessageRequestsAfterInflyLoadFailure(ui64 shard) {
    std::vector<TString> requestsToDelete;
    requestsToDelete.reserve(Max(ReceiveMessageRequests_.size(), SendMessageRequests_.size()));
    for (auto&& [reqId, req] : ReceiveMessageRequests_) {
        if (req.GetCurrentShard() == shard) {
            const TString& requestId = reqId;
            requestsToDelete.emplace_back(requestId);
            RLOG_SQS_REQ_ERROR(requestId, "Failed to load infly for shard " << shard);
            req.Answer->Failed = true;
            Send(req.Event->Sender, std::move(req.Answer));
        }
    }
    for (const auto& reqId : requestsToDelete) {
        ReceiveMessageRequests_.erase(reqId);
    }
    Shards_[shard].LoadBatchingState.CancelRequestsAfterInflyLoadFailure();
    requestsToDelete.clear();

    for (auto&& [reqId, req] : SendMessageRequests_) {
        if (req.Shard == shard) {
            const TString& requestId = reqId;
            requestsToDelete.emplace_back(requestId);
            RLOG_SQS_REQ_ERROR(requestId, "Failed to load infly for shard " << shard);
            auto answer = MakeHolder<TSqsEvents::TEvSendMessageBatchResponse>();
            answer->Statuses.resize(req.Event->Get()->Messages.size());
            for (auto& s : answer->Statuses) {
                s.Status = TSqsEvents::TEvSendMessageBatchResponse::ESendMessageStatus::Failed;
            }
            Send(req.Event->Sender, answer.Release());
        }
    }
    for (const auto& reqId : requestsToDelete) {
        SendMessageRequests_.erase(reqId);
    }
    Shards_[shard].SendBatchingState.CancelRequestsAfterInflyLoadFailure();

    {
        std::vector<std::pair<TString, ui64>> failedDeleteRequests;
        failedDeleteRequests.reserve(DeleteMessageRequests_.size());
        for (auto&& [reqIdAndShard, reqInfo] : DeleteMessageRequests_) {
            if (reqInfo.Event->Get()->Shard == shard) {
                failedDeleteRequests.emplace_back(reqIdAndShard);
                const TString& requestId = reqIdAndShard.first;
                RLOG_SQS_REQ_ERROR(requestId, "Failed to load infly for shard " << shard);
                auto answer = MakeHolder<TSqsEvents::TEvDeleteMessageBatchResponse>();
                answer->Shard = shard;
                answer->Statuses.resize(reqInfo.Event->Get()->Messages.size());
                for (auto& status : answer->Statuses) {
                    status.Status = TSqsEvents::TEvDeleteMessageBatchResponse::EDeleteMessageStatus::Failed;
                }

                Send(reqInfo.Event->Sender, answer.Release());
            }
        }
        for (const auto& reqIdAndShard : failedDeleteRequests) {
            DeleteMessageRequests_.erase(reqIdAndShard);
        }
        Shards_[shard].DeleteBatchingState.CancelRequestsAfterInflyLoadFailure();
    }

    {
        std::vector<std::pair<TString, ui64>> failedChangeMessageVisibilityRequests;
        failedChangeMessageVisibilityRequests.reserve(ChangeMessageVisibilityRequests_.size());
        for (auto&& [reqIdAndShard, reqInfo] : ChangeMessageVisibilityRequests_) {
            if (reqInfo.Event->Get()->Shard == shard) {
                failedChangeMessageVisibilityRequests.emplace_back(reqIdAndShard);
                const TString& requestId = reqIdAndShard.first;
                RLOG_SQS_REQ_ERROR(requestId, "Failed to load infly for shard " << shard);
                for (auto& status : reqInfo.Answer->Statuses) {
                    status.Status = TSqsEvents::TEvChangeMessageVisibilityBatchResponse::EMessageStatus::Failed;
                }

                Send(reqInfo.Event->Sender, reqInfo.Answer.Release());
            }
        }
        for (const auto& reqIdAndShard : failedChangeMessageVisibilityRequests) {
            ChangeMessageVisibilityRequests_.erase(reqIdAndShard);
        }
    }
}

void TQueueLeader::StartMessageRequestsAfterInflyLoaded(ui64 shard) {
    {
        std::vector<TReceiveMessageBatchRequestProcessing*> receiveRequests;
        receiveRequests.reserve(ReceiveMessageRequests_.size());
        for (auto&& [reqId, req] : ReceiveMessageRequests_) {
            if (req.GetCurrentShard() == shard) {
                receiveRequests.push_back(&req);
            }
        }
        for (auto* req : receiveRequests) {
            ProcessReceiveMessageBatch(*req);
        }
    }

    {
        std::vector<TSendMessageBatchRequestProcessing*> sendRequests;
        sendRequests.reserve(SendMessageRequests_.size());
        for (auto&& [reqId, req] : SendMessageRequests_) {
            if (req.Shard == shard) {
                sendRequests.push_back(&req);
            }
        }
        for (auto* req : sendRequests) {
            ProcessSendMessageBatch(*req);
        }
    }

    {
        std::vector<TDeleteMessageBatchRequestProcessing*> deleteRequests;
        deleteRequests.reserve(DeleteMessageRequests_.size());
        for (auto&& [reqIdAndShard, reqInfo] : DeleteMessageRequests_) {
            if (reqInfo.Event->Get()->Shard == shard) {
                deleteRequests.push_back(&reqInfo);
            }
        }
        for (auto* reqInfo : deleteRequests) {
            ProcessDeleteMessageBatch(*reqInfo);
        }
    }

    {
        std::vector<TChangeMessageVisibilityBatchRequestProcessing*> changeMessageVisibilityRequests;
        changeMessageVisibilityRequests.reserve(ChangeMessageVisibilityRequests_.size());
        for (auto&& [reqIdAndShard, reqInfo] : ChangeMessageVisibilityRequests_) {
            if (reqInfo.Event->Get()->Shard == shard) {
                changeMessageVisibilityRequests.push_back(&reqInfo);
            }
        }
        for (auto* reqInfo : changeMessageVisibilityRequests) {
            ProcessChangeMessageVisibilityBatch(*reqInfo);
        }
    }
}

bool TQueueLeader::IncActiveMessageRequests(ui64 shard, const TString& requestId) {
    if (!IsFifoQueue_) {
        auto& shardInfo = Shards_[shard];
        if (shardInfo.InflyLoadState != TShardInfo::EInflyLoadState::Loaded) {
            RLOG_SQS_REQ_TRACE(requestId, "Waiting for loading infly for " << TLogQueueName(UserName_, QueueName_, shard));
            return false;
        }
        ++shardInfo.ActiveMessageRequests;
        LOG_SQS_TRACE("Increment active message requests for " << TLogQueueName(UserName_, QueueName_, shard) << ". ActiveMessageRequests: " << shardInfo.ActiveMessageRequests);
    }
    return true;
}

void TQueueLeader::DecActiveMessageRequests(ui64 shard) {
    if (!IsFifoQueue_) {
        auto& shardInfo = Shards_[shard];
        Y_ABORT_UNLESS(shardInfo.ActiveMessageRequests > 0);
        --shardInfo.ActiveMessageRequests;
        LOG_SQS_TRACE("Decrement active message requests for [" << TLogQueueName(UserName_, QueueName_, shard) << ". ActiveMessageRequests: " << shardInfo.ActiveMessageRequests);
        if (shardInfo.ActiveMessageRequests == 0 && shardInfo.InflyLoadState == TShardInfo::EInflyLoadState::WaitingForActiveRequests) {
            StartLoadingInfly(shard);
        }
    }
}

void TQueueLeader::ScheduleInflyLoadAfterFailure(ui64 shard) {
    const ui32 randomMs = 100 + RandomNumber<ui32>(300);
    LOG_SQS_INFO("Scheduling retry after infly " << TLogQueueName(UserName_, QueueName_, shard) << " load failure in " << randomMs << "ms");
    Schedule(TDuration::MilliSeconds(randomMs), new TEvWakeup(RELOAD_INFLY_TAG + shard));
}

void TQueueLeader::HandleInflyIsPurgingNotification(TSqsEvents::TEvInflyIsPurgingNotification::TPtr& ev) {
    LOG_SQS_TRACE("Handle infly purged notification for " << TLogQueueName(UserName_, QueueName_, ev->Get()->Shard) << ". Messages: " << ev->Get()->Offsets.size());
    if (!IsFifoQueue_) {
        auto& shardInfo = Shards_[ev->Get()->Shard];
        if (shardInfo.InflyLoadState != TShardInfo::EInflyLoadState::Loaded) {
            LOG_SQS_TRACE("Skipping infly " << TLogQueueName(UserName_, QueueName_, ev->Get()->Shard) << " purged notification. Infly load state: " << static_cast<int>(shardInfo.InflyLoadState));
            return;
        }
        for (ui64 offset : ev->Get()->Offsets) {
            if (!shardInfo.Infly->Delete(offset)) {
                // maybe there are several receive message requests that are about to get this message
                for (auto& [receiveRequestId, receiveRequestInfo] : ReceiveMessageRequests_) {
                    if (receiveRequestInfo.CurrentShardIndex < receiveRequestInfo.Shards.size()
                        && receiveRequestInfo.Shards[receiveRequestInfo.CurrentShardIndex] == ev->Get()->Shard
                        && receiveRequestInfo.ReceiveCandidates
                        && receiveRequestInfo.ReceiveCandidates.Delete(offset)) {
                        break;
                    }
                }
            }
        }
    }
}

void TQueueLeader::HandleQueuePurgedNotification(TSqsEvents::TEvQueuePurgedNotification::TPtr& ev) {
    ui64 shard = ev->Get()->Shard;
    auto& shardInfo = Shards_[shard];
    SetMessagesCount(shard, ev->Get()->NewMessagesCount);
    for (ui64 offset : ev->Get()->DeletedOffsets) {
        if (shardInfo.OldestMessageOffset <= offset) {
            RequestOldestTimestampMetrics(shard);
            break;
        }
    }
}

void TQueueLeader::HandleGetRuntimeQueueAttributesWhileIniting(TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr& ev) {
    auto&& [reqInfoIt, inserted] = GetRuntimeQueueAttributesRequests_.emplace(ev->Get()->RequestId, std::move(ev));
    Y_ABORT_UNLESS(inserted);
}

void TQueueLeader::HandleGetRuntimeQueueAttributesWhileWorking(TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr& ev) {
    auto&& [reqInfoIt, inserted] = GetRuntimeQueueAttributesRequests_.emplace(ev->Get()->RequestId, std::move(ev));
    Y_ABORT_UNLESS(inserted);
    ProcessGetRuntimeQueueAttributes(reqInfoIt->second);
}

void TQueueLeader::HandleDeadLetterQueueNotification(TSqsEvents::TEvDeadLetterQueueNotification::TPtr&) {
    LatestDlqNotificationTs_ = TActivationContext::Now();
    IsDlqQueue_ = true;
}

void TQueueLeader::ProcessGetRuntimeQueueAttributes(TGetRuntimeQueueAttributesRequestProcessing& reqInfo) {
    if (reqInfo.ShardProcessFlags.empty()) {
        Y_ABORT_UNLESS(ShardsCount_ > 0);
        reqInfo.ShardProcessFlags.resize(ShardsCount_);
    }

    for (ui64 shard = 0; shard < ShardsCount_; ++shard) {
        ProcessGetRuntimeQueueAttributes(shard, reqInfo);
    }
}

void TQueueLeader::ProcessGetRuntimeQueueAttributes(ui64 shard, TGetRuntimeQueueAttributesRequestProcessing& reqInfo) {
    Y_ABORT_UNLESS(shard < reqInfo.ShardProcessFlags.size());
    if (reqInfo.ShardProcessFlags[shard]) {
        return;
    }

    if (IsFifoQueue_) {
        if (Shards_[shard].MessagesCountWasGot) {
            reqInfo.Answer->MessagesCount += Shards_[shard].MessagesCount;
            reqInfo.Answer->CreatedTimestamp = Min(Shards_[shard].CreatedTimestamp, reqInfo.Answer->CreatedTimestamp);

            ++reqInfo.ShardsProcessed;
            reqInfo.ShardProcessFlags[shard] = true;
        } else {
            RequestMessagesCountMetrics(shard);
        }
    } else {
        if (Shards_[shard].InflyLoadState == TShardInfo::EInflyLoadState::Loaded) {
            const TInstant now = TActivationContext::Now();
            reqInfo.Answer->MessagesCount += Shards_[shard].MessagesCount;
            reqInfo.Answer->InflyMessagesCount += Shards_[shard].Infly->GetInflyCount(now);
            reqInfo.Answer->CreatedTimestamp = Min(Shards_[shard].CreatedTimestamp, reqInfo.Answer->CreatedTimestamp);

            ++reqInfo.ShardsProcessed;
            reqInfo.ShardProcessFlags[shard] = true;
        }
    }

    if (reqInfo.ShardsProcessed == reqInfo.ShardProcessFlags.size()) {
        reqInfo.Answer->MessagesDelayed = DelayStatistics_.UpdateAndGetMessagesDelayed(TActivationContext::Now());
        Send(reqInfo.Event->Sender, std::move(reqInfo.Answer));
        GetRuntimeQueueAttributesRequests_.erase(reqInfo.Event->Get()->RequestId);
    }
}

void TQueueLeader::FailGetRuntimeQueueAttributesForShard(ui64 shard) {
    std::vector<TString> reqIds;
    reqIds.reserve(GetRuntimeQueueAttributesRequests_.size());
    for (auto& [reqId, reqInfo] : GetRuntimeQueueAttributesRequests_) {
        Y_ABORT_UNLESS(shard < reqInfo.ShardProcessFlags.size());
        if (!reqInfo.ShardProcessFlags[shard]) { // don't fail requests that are already passed this shard
            const TString& requestId = reqId;
            RLOG_SQS_REQ_ERROR(requestId, "Failed to get runtime queue attributes for shard " << shard);
            reqInfo.Answer->Failed = true;
            Send(reqInfo.Event->Sender, std::move(reqInfo.Answer));
            reqIds.push_back(reqId);
        }
    }
    for (const TString& reqId : reqIds) {
        GetRuntimeQueueAttributesRequests_.erase(reqId);
    }
}

void TQueueLeader::ProcessGetRuntimeQueueAttributes(ui64 shard) {
    std::vector<TGetRuntimeQueueAttributesRequestProcessing*> requestsToProcess;
    requestsToProcess.reserve(GetRuntimeQueueAttributesRequests_.size());
    for (auto& [reqId, reqInfo] : GetRuntimeQueueAttributesRequests_) {
        requestsToProcess.push_back(&reqInfo);
    }
    for (auto* reqInfo : requestsToProcess) {
        ProcessGetRuntimeQueueAttributes(shard, *reqInfo);
    }
}

void TQueueLeader::InitQuoterResources() {
    const auto& cfg = Cfg().GetQuotingConfig();
    if (cfg.GetEnableQuoting()) {
        Y_ABORT_UNLESS(cfg.HasLocalRateLimiterConfig() != cfg.HasKesusQuoterConfig()); // exactly one must be set
        if (cfg.HasLocalRateLimiterConfig()) { // the only one that is fully supported
            Y_ABORT_UNLESS(QuoterResources_);
            const auto& rates = cfg.GetLocalRateLimiterConfig().GetRates();
            // allocate resources
            SendMessageQuoterResource_ = TLocalRateLimiterResource(IsFifoQueue_ ? rates.GetFifoSendMessageRate() : rates.GetStdSendMessageRate());
            ReceiveMessageQuoterResource_ = TLocalRateLimiterResource(IsFifoQueue_ ? rates.GetFifoReceiveMessageRate() : rates.GetStdReceiveMessageRate());
            DeleteMessageQuoterResource_ = TLocalRateLimiterResource(IsFifoQueue_ ? rates.GetFifoDeleteMessageRate() : rates.GetStdDeleteMessageRate());
            ChangeMessageVisibilityQuoterResource_ = TLocalRateLimiterResource(IsFifoQueue_ ? rates.GetFifoChangeMessageVisibilityRate() : rates.GetStdChangeMessageVisibilityRate());
            // fill map
            {
                TSqsEvents::TQuoterResourcesForActions::TResourceDescription res{TEvQuota::TResourceLeaf::QuoterSystem, SendMessageQuoterResource_};
                QuoterResources_->ActionsResources.emplace(EAction::SendMessage, res);
                QuoterResources_->ActionsResources.emplace(EAction::SendMessageBatch, res);
            }
            {
                TSqsEvents::TQuoterResourcesForActions::TResourceDescription res{TEvQuota::TResourceLeaf::QuoterSystem, ReceiveMessageQuoterResource_};
                QuoterResources_->ActionsResources.emplace(EAction::ReceiveMessage, res);
            }
            {
                TSqsEvents::TQuoterResourcesForActions::TResourceDescription res{TEvQuota::TResourceLeaf::QuoterSystem, DeleteMessageQuoterResource_};
                QuoterResources_->ActionsResources.emplace(EAction::DeleteMessage, res);
                QuoterResources_->ActionsResources.emplace(EAction::DeleteMessageBatch, res);
            }
            {
                TSqsEvents::TQuoterResourcesForActions::TResourceDescription res{TEvQuota::TResourceLeaf::QuoterSystem, ChangeMessageVisibilityQuoterResource_};
                QuoterResources_->ActionsResources.emplace(EAction::ChangeMessageVisibility, res);
                QuoterResources_->ActionsResources.emplace(EAction::ChangeMessageVisibilityBatch, res);
            }
        }
    }
}

TQueueLeader::TShardInfo::~TShardInfo() = default;

TQueueLeader::TSendMessageBatchRequestProcessing::TSendMessageBatchRequestProcessing(TSqsEvents::TEvSendMessageBatch::TPtr&& ev)
    : Event(std::move(ev))
{
    Statuses.resize(Event->Get()->Messages.size());
}

void TQueueLeader::TSendMessageBatchRequestProcessing::Init(ui64 shardsCount) {
    if (!Inited) {
        Shard = RandomNumber<ui64>() % shardsCount;
        Inited = true;
    }
}

TQueueLeader::TReceiveMessageBatchRequestProcessing::TReceiveMessageBatchRequestProcessing(TSqsEvents::TEvReceiveMessageBatch::TPtr&& ev)
    : Event(std::move(ev))
    , Answer(MakeHolder<TSqsEvents::TEvReceiveMessageBatchResponse>())
{
    Answer->Messages.reserve(Event->Get()->MaxMessagesCount);
}

void TQueueLeader::TReceiveMessageBatchRequestProcessing::Init(ui64 shardsCount) {
    if (!Inited) {
        Shards.resize(shardsCount);
        for (ui64 i = 0; i < shardsCount; ++i) {
            Shards[i] = i;
        }

        Shuffle(Shards.begin(), Shards.end());

        Inited = true;
    }
}

TQueueLeader::TDeleteMessageBatchRequestProcessing::TDeleteMessageBatchRequestProcessing(TSqsEvents::TEvDeleteMessageBatch::TPtr&& ev)
    : Event(std::move(ev))
    , Answer(MakeHolder<TSqsEvents::TEvDeleteMessageBatchResponse>())
{
    Answer->Shard = Event->Get()->Shard;
    Answer->Statuses.resize(Event->Get()->Messages.size());
    InflyMessages.reserve(Event->Get()->Messages.size());
}

TQueueLeader::TChangeMessageVisibilityBatchRequestProcessing::TChangeMessageVisibilityBatchRequestProcessing(TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr&& ev)
    : Event(std::move(ev))
    , Answer(MakeHolder<TSqsEvents::TEvChangeMessageVisibilityBatchResponse>())
{
    Answer->Statuses.resize(Event->Get()->Messages.size());
    Answer->Shard = Event->Get()->Shard;
}

TQueueLeader::TGetRuntimeQueueAttributesRequestProcessing::TGetRuntimeQueueAttributesRequestProcessing(TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr&& ev)
    : Event(std::move(ev))
    , Answer(MakeHolder<TSqsEvents::TEvGetRuntimeQueueAttributesResponse>())
{
    Answer->CreatedTimestamp = TInstant::Max(); // for proper min operation
}

template <class TBatch>
TQueueLeader::TBatchingState<TBatch>::~TBatchingState() = default;

template <class TBatch>
void TQueueLeader::TBatchingState<TBatch>::Init(const NKikimrConfig::TSqsConfig::TBatchingPolicy& policy, ui64 shard, bool isFifo) {
    Policy = policy;
    Shard = shard;
    IsFifoQueue = isFifo;
}

template <class TBatch>
void TQueueLeader::TBatchingState<TBatch>::TryExecute(TQueueLeader* leader) {
    while (BatchesExecuting.size() < Policy.GetTransactionsMaxInflyPerShard() && !BatchesIniting.empty()) {
        auto& batchPtr = BatchesIniting.front();
        if (!BatchesExecuting.empty() && !CanExecute(*batchPtr)) {
            break;
        }

        BatchesExecuting[batchPtr->BatchId] = batchPtr;
        batchPtr->Execute(leader);
        BatchesIniting.pop_front();
    }
}

template <class TBatch>
TBatch& TQueueLeader::TBatchingState<TBatch>::NewBatch() {
    auto newBatch = MakeIntrusive<TBatch>(Shard, Policy.GetBatchSize(), IsFifoQueue);
    newBatch->BatchId = NextBatchId++;
    BatchesIniting.push_back(newBatch);
    return *newBatch;
}

template <class TBatch>
void TQueueLeader::TBatchingState<TBatch>::CancelRequestsAfterInflyLoadFailure() {
    Y_ABORT_UNLESS(BatchesExecuting.empty());
    BatchesIniting.clear();
}

template <class TBatch>
template <class TRequestProcessing>
void TQueueLeader::TBatchingStateWithGroupsRestrictions<TBatch>::AddRequest(TRequestProcessing& reqInfo) {
    const auto& msgs = reqInfo.Event->Get()->Messages;
    if (this->IsFifoQueue) {
        for (size_t i = 0; i < msgs.size(); ++i) {
            const TString& groupId = msgs[i].MessageGroupId;
            bool added = false;
            for (const auto& batch : this->BatchesIniting) {
                if (!batch->IsFull() && !batch->HasGroup(groupId)) {
                    batch->AddEntry(reqInfo, i);
                    added = true;
                    break;
                }
            }
            if (!added) {
                this->NewBatch().AddEntry(reqInfo, i);
            }
        }
    } else {
        size_t i = 0;
        while (i < msgs.size()) {
            if (this->BatchesIniting.empty() || this->BatchesIniting.back()->IsFull()) {
                this->NewBatch();
            }
            auto& batch = *this->BatchesIniting.back();
            do {
                batch.AddEntry(reqInfo, i);
                ++i;
            } while (i < msgs.size() && !batch.IsFull());
        }
    }
}

template <class TBatch>
bool TQueueLeader::TBatchingStateWithGroupsRestrictions<TBatch>::CanExecute(const TBatch& batch) const {
    using TBatchingState = TBatchingState<TBatch>;
    if (this->IsFifoQueue) {
        // find whether groups from batch are already executing
        for (const auto& [id, executingBatch] : this->BatchesExecuting) {
            auto executingIt = executingBatch->Groups.begin();
            auto it = batch.Groups.begin();
            while (executingIt != executingBatch->Groups.end() && it != batch.Groups.end()) {
                if (*executingIt == *it) {
                    return false; // there is already executing request with such group
                } else if (*executingIt < *it) {
                    ++executingIt;
                } else {
                    ++it;
                }
            }
        }
        return true;
    } else {
        return TBatchingState::CanExecute(batch);
    }
}

void TQueueLeader::TSendBatch::AddEntry(TSendMessageBatchRequestProcessing& reqInfo, size_t i) {
    RLOG_SQS_REQ_DEBUG(reqInfo.Event->Get()->RequestId, "Add message[" << i << "] to send batch. BatchId: " << BatchId);
    Entries.emplace_back(reqInfo.Event->Get()->RequestId, reqInfo.Event->Get()->SenderId, reqInfo.Event->Get()->Messages[i], i);
    if (IsFifoQueue) {
        AddGroup(reqInfo.Event->Get()->Messages[i].MessageGroupId);
    }
}

void TQueueLeader::TSendBatch::Execute(TQueueLeader* leader) {
    RLOG_SQS_DEBUG(TLogQueueName(leader->UserName_, leader->QueueName_, Shard) << " Executing send batch. BatchId: " << BatchId << ". Size: " << Size());
    TransactionStartedTime = TActivationContext::Now();
    TExecutorBuilder builder(SelfId(), RequestId_);
    builder
        .User(leader->UserName_)
        .Queue(leader->QueueName_)
        .Shard(Shard)
        .QueueVersion(leader->QueueVersion_)
        .QueueLeader(SelfId())
        .TablesFormat(leader->TablesFormat_)
        .QueryId(WRITE_MESSAGE_ID)
        .Fifo(IsFifoQueue)
        .Counters(leader->Counters_)
        .RetryOnTimeout(IsFifoQueue) // Fifo queues have deduplication, so we can retry even on unknown transaction state
        .OnExecuted([leader, shard = Shard, batchId = BatchId](const TSqsEvents::TEvExecuted::TRecord& ev) { leader->OnSendBatchExecuted(shard, batchId, ev); })
        .Params()
            .Uint64("QUEUE_ID_NUMBER", leader->QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(leader->QueueVersion_))
            .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(leader->QueueVersion_, Shard))
            .Uint64("RANDOM_ID",  RandomNumber<ui64>())
            .Uint64("TIMESTAMP",  TransactionStartedTime.MilliSeconds())
            .AddWithType("SHARD", Shard, leader->TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
            .Uint64("DEDUPLICATION_PERIOD", Cfg().GetDeduplicationPeriodMs());

    NClient::TWriteValue params = builder.ParamsValue();
    const TString* prevRequestId = nullptr;
    for (ui64 i = 0; i < Entries.size(); ++i) {
        const TSendBatchEntry& entry = Entries[i];
        const TSqsEvents::TEvSendMessageBatch::TMessageEntry& msgEntry = entry.Message;
        auto message = params["MESSAGES"].AddListItem();
        message["Attributes"].Bytes(msgEntry.Attributes);
        message["Data"].Bytes(msgEntry.Body);
        message["MessageId"].Bytes(msgEntry.MessageId);
        message["SenderId"].Bytes(entry.SenderId);
        message["Delay"] = ui64(msgEntry.Delay.MilliSeconds());
        message["Index"] = i;
        if (IsFifoQueue) {
            message["GroupId"].Bytes(msgEntry.MessageGroupId);
            message["DeduplicationId"].Bytes(msgEntry.DeduplicationId);
        }
        if (!prevRequestId || *prevRequestId != entry.RequestId) {
            prevRequestId = &entry.RequestId;
            RLOG_SQS_REQ_DEBUG(entry.RequestId, "Send batch transaction to database. BatchId: " << BatchId);
        }
    }

    builder.Start();
}

void TQueueLeader::TDeleteBatch::AddEntry(TDeleteMessageBatchRequestProcessing& reqInfo, size_t i) {
    RLOG_SQS_REQ_DEBUG(reqInfo.Event->Get()->RequestId, "Add message[" << i << "] to delete batch. BatchId: " << BatchId);
    Entries.emplace_back(reqInfo.Event->Get()->RequestId, reqInfo.Event->Get()->Messages[i], i);
    if (IsFifoQueue) {
        AddGroup(reqInfo.Event->Get()->Messages[i].MessageGroupId);
    }
}

void TQueueLeader::TDeleteBatch::Execute(TQueueLeader* leader) {
    RLOG_SQS_DEBUG(TLogQueueName(leader->UserName_, leader->QueueName_, Shard) << " Executing delete batch. BatchId: " << BatchId << ". Size: " << Size());
    TExecutorBuilder builder(SelfId(), RequestId_);
    builder
        .User(leader->UserName_)
        .Queue(leader->QueueName_)
        .Shard(Shard)
        .QueueVersion(leader->QueueVersion_)
        .QueueLeader(SelfId())
        .TablesFormat(leader->TablesFormat_)
        .Fifo(IsFifoQueue)
        .QueryId(DELETE_MESSAGE_ID)
        .Counters(leader->Counters_)
        .RetryOnTimeout()
        .OnExecuted([leader, shard = Shard, batchId = BatchId](const TSqsEvents::TEvExecuted::TRecord& ev) { leader->OnDeleteBatchExecuted(shard, batchId, ev); })
        .Params()
            .Uint64("QUEUE_ID_NUMBER", leader->QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(leader->QueueVersion_))
            .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(leader->QueueVersion_, Shard))
            .Uint64("NOW", TActivationContext::Now().MilliSeconds())
            .AddWithType("SHARD", Shard, leader->TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
            .Uint64("GROUPS_READ_ATTEMPT_IDS_PERIOD", Cfg().GetGroupsReadAttemptIdsPeriodMs());

    NClient::TWriteValue params = builder.ParamsValue();
    const TString* prevRequestId = nullptr;

    Offset2Entry.reserve(Entries.size());
    for (ui64 i = 0; i < Entries.size(); ++i) {
        const TDeleteBatchEntry& entry = Entries[i];
        const TSqsEvents::TEvDeleteMessageBatch::TMessageEntry& msgEntry = entry.Message;
        const bool hasOffset = Offset2Entry.find(msgEntry.Offset) != Offset2Entry.end();
        Offset2Entry.emplace(msgEntry.Offset, i);
        if (!hasOffset) {
            auto key = params["KEYS"].AddListItem();
            if (IsFifoQueue) {
                key["GroupId"].Bytes(msgEntry.MessageGroupId);
                key["ReceiveAttemptId"] = msgEntry.ReceiveAttemptId;
            }
            key["Offset"] = ui64(msgEntry.Offset);
            key["LockTimestamp"] = ui64(msgEntry.LockTimestamp.MilliSeconds());
        }
        if (!prevRequestId || *prevRequestId != entry.RequestId) {
            prevRequestId = &entry.RequestId;
            RLOG_SQS_REQ_DEBUG(entry.RequestId, "Send batch transaction to database. BatchId: " << BatchId);
        }
    }

    builder.Start();
}

void TQueueLeader::TLoadBatchingState::AddRequest(TReceiveMessageBatchRequestProcessing& reqInfo) {
    auto msg = reqInfo.ReceiveCandidates.Begin();
    const auto end = reqInfo.ReceiveCandidates.End();
    while (msg != end) {
        if (BatchesIniting.empty() || BatchesIniting.back()->IsFull()) {
            NewBatch();
        }
        auto& batch = *BatchesIniting.back();
        RLOG_SQS_REQ_DEBUG(reqInfo.Event->Get()->RequestId, "Add load batch request. BatchId: " << batch.BatchId);
        do {
            batch.Entries.emplace_back(reqInfo.Event->Get()->RequestId, &msg->Message(), reqInfo.Event->Get()->VisibilityTimeout);
            ++msg;
        } while (msg != end && !batch.IsFull());
    }
}

void TQueueLeader::TLoadBatch::Execute(TQueueLeader* leader) {
    RLOG_SQS_DEBUG(TLogQueueName(leader->UserName_, leader->QueueName_, Shard) << " Executing load batch. BatchId: " << BatchId << ". Size: " << Size());

    TExecutorBuilder builder(SelfId(), RequestId_);
    const auto now = TActivationContext::Now();
    builder
        .User(leader->UserName_)
        .Queue(leader->QueueName_)
        .Shard(Shard)
        .QueueVersion(leader->QueueVersion_)
        .Fifo(IsFifoQueue)
        .QueueLeader(SelfId())
        .TablesFormat(leader->TablesFormat_)
        .Counters(leader->Counters_)
        .RetryOnTimeout()
        .Params()
            .Uint64("QUEUE_ID_NUMBER", leader->QueueVersion_)
            .Uint64("QUEUE_ID_NUMBER_HASH", GetKeysHash(leader->QueueVersion_))
            .Uint64("QUEUE_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(leader->QueueVersion_, Shard))
            .Uint64("NOW", now.MilliSeconds())
            .Uint64("READ_ID", RandomNumber<ui64>())
            .AddWithType("SHARD", Shard, leader->TablesFormat_ == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64);

    ui32 maxReceiveCount = 0; // not set
    if (Cfg().GetEnableDeadLetterQueues() && leader->DlqInfo_) {
        const auto& dlqInfo(*leader->DlqInfo_);
        if (dlqInfo.DlqName && dlqInfo.QueueId) {
            // dlq is set and resolved
            maxReceiveCount = dlqInfo.MaxReceiveCount;
        }
    }

    NClient::TWriteValue params = builder.ParamsValue();
    const TString* prevRequestId = nullptr;
    size_t deadLettersCounter = 0;
    THashSet<ui64> offsets; // check for duplicates
    for (const TLoadBatchEntry& entry : Entries) {
        Y_ABORT_UNLESS(offsets.insert(entry.Offset).second);

        auto item = params["KEYS"].AddListItem();
        item["RandomId"] = entry.RandomId;
        item["Offset"] = entry.Offset;
        item["CurrentVisibilityDeadline"] = ui64(entry.CurrentVisibilityDeadline.MilliSeconds());
        item["VisibilityDeadline"] = ui64((now + entry.VisibilityTimeout).MilliSeconds());
        if (maxReceiveCount && entry.ReceiveCount >= maxReceiveCount) {
            item["DlqIndex"] = ui64(deadLettersCounter);
            ++deadLettersCounter;
            item["IsDeadLetter"] = true;
        } else {
            item["DlqIndex"] = ui64(0);
            item["IsDeadLetter"] = false;
        }

        if (!prevRequestId || *prevRequestId != entry.RequestId) {
            prevRequestId = &entry.RequestId;
            RLOG_SQS_REQ_DEBUG(entry.RequestId, "Send batch transaction to database. BatchId: " << BatchId);
        }
    }

    if (deadLettersCounter) {
        // perform heavy read and move transaction (DLQ)
        Y_ABORT_UNLESS(leader->DlqInfo_);
        const auto& dlqInfo(*leader->DlqInfo_);
        const auto dlqShard = Shard % dlqInfo.ShardsCount;
        builder
            .DlqName(dlqInfo.QueueId)
            .DlqShard(dlqShard)
            .DlqVersion(dlqInfo.QueueVersion)
            .DlqTablesFormat(dlqInfo.TablesFormat)
            .CreateExecutorActor(true)
            .QueryId(LOAD_OR_REDRIVE_MESSAGE_ID);

        builder.Params()
            .Uint64("DLQ_ID_NUMBER", dlqInfo.QueueVersion)
            .Uint64("DLQ_ID_NUMBER_HASH", GetKeysHash(dlqInfo.QueueVersion))
            .AddWithType("DLQ_SHARD", dlqShard, dlqInfo.TablesFormat == 1 ? NScheme::NTypeIds::Uint32 : NScheme::NTypeIds::Uint64)
            .Uint64("DLQ_ID_NUMBER_AND_SHARD_HASH", GetKeysHash(dlqInfo.QueueVersion, dlqShard))
            .Uint64("DEAD_LETTERS_COUNT", deadLettersCounter);
    } else {
        // perform simple read transaction
        builder.QueryId(LOAD_MESSAGES_ID);
    }

    const bool usedDLQ = deadLettersCounter;
    builder.OnExecuted([leader, shard = Shard, batchId = BatchId, usedDLQ] (const TSqsEvents::TEvExecuted::TRecord& ev) {
        leader->OnLoadStdMessagesBatchExecuted(shard, batchId, usedDLQ, ev);
    });

    builder.Start();
}

bool TQueueLeader::TShardInfo::HasMessagesToAddToInfly() const {
    return Infly ? Infly->GetCapacity() < MessagesCount : MessagesCount > 0;
}

bool TQueueLeader::TShardInfo::NeedAddMessagesToInflyCheckInDatabase() const {
    const NKikimrConfig::TSqsConfig& cfg = Cfg();
    if (AddMessagesToInflyCheckAttempts < cfg.GetAddMessagesToInflyMinCheckAttempts()) {
        return false;
    }
    const TInstant now = TActivationContext::Now();
    return now - LastAddMessagesToInfly > TDuration::MilliSeconds(cfg.GetAddMessagesToInflyCheckPeriodMs());
}

} // namespace NKikimr::NSQS

template<>
void Out<NKikimr::NSQS::TSqsEvents::TQueueAttributes>(IOutputStream& o,
        typename TTypeTraits<NKikimr::NSQS::TSqsEvents::TQueueAttributes>::TFuncParam x) {
    o << "{ ContentBasedDeduplication: " << x.ContentBasedDeduplication;
    o << " DelaySeconds: " << x.DelaySeconds;
    o << " FifoQueue: " << x.FifoQueue;
    o << " MaximumMessageSize: " << x.MaximumMessageSize;
    o << " MessageRetentionPeriod: " << x.MessageRetentionPeriod;
    o << " ReceiveMessageWaitTime: " << x.ReceiveMessageWaitTime;
    o << " VisibilityTimeout: " << x.VisibilityTimeout;
    o << " }";
}

template<>
void Out<NKikimr::NSQS::TQueuePath>(IOutputStream& o,
        typename TTypeTraits<NKikimr::NSQS::TQueuePath>::TFuncParam path) {
    o << path.GetQueuePath();
}
