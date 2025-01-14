#pragma once
#include "defs.h"
#include "cfg.h"
#include "events.h"
#include "local_rate_limiter_allocator.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/core/ymq/actor/infly.h>
#include <ydb/core/ymq/actor/message_delay_stats.h>
#include <ydb/core/ymq/base/counters.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash_multi_map.h>
#include <util/generic/guid.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>

#include <deque>

namespace NKikimr::NSQS {

class TQueueLeader : public TActorBootstrapped<TQueueLeader> {
    struct TSendMessageBatchRequestProcessing;
    struct TReceiveMessageBatchRequestProcessing;
    struct TDeleteMessageBatchRequestProcessing;
    struct TChangeMessageVisibilityBatchRequestProcessing;
    struct TGetRuntimeQueueAttributesRequestProcessing;
    struct TShardInfo;
    struct TLoadBatch;

public:
    TQueueLeader(
        TString userName,
        TString queueName,
        TString folderId,
        TString rootUrl,
        TIntrusivePtr<TQueueCounters> counters,
        TIntrusivePtr<TUserCounters> userCounters,
        const TActorId& schemeCache,
        const TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions>& quoterResourcesForUser,
        bool useCPUOptimization
    );

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_QUEUE_LEADER_ACTOR;
    }

private:
    STATEFN(StateInit);
    STATEFN(StateWorking);

    void PassAway() override;
    void HandleWakeup(TEvWakeup::TPtr& ev);
    void HandleState(const TSqsEvents::TEvExecuted::TRecord& ev);
    void HandleGetConfigurationWhileIniting(TSqsEvents::TEvGetConfiguration::TPtr& ev);
    void HandleGetConfigurationWhileWorking(TSqsEvents::TEvGetConfiguration::TPtr& ev);
    void HandleActionCounterChanged(TSqsEvents::TEvActionCounterChanged::TPtr& ev);
    void HandleLocalCounterChanged(TSqsEvents::TEvLocalCounterChanged::TPtr& ev);
    void HandleExecuteWhileIniting(TSqsEvents::TEvExecute::TPtr& ev);
    void HandleExecuteWhileWorking(TSqsEvents::TEvExecute::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandleQueueId(TSqsEvents::TEvQueueId::TPtr& ev);
    void HandleClearQueueAttributesCache(TSqsEvents::TEvClearQueueAttributesCache::TPtr& ev);
    void HandlePurgeQueue(TSqsEvents::TEvPurgeQueue::TPtr& ev);
    void HandleSendMessageBatchWhileIniting(TSqsEvents::TEvSendMessageBatch::TPtr& ev);
    void HandleSendMessageBatchWhileWorking(TSqsEvents::TEvSendMessageBatch::TPtr& ev);
    void HandleReceiveMessageBatchWhileIniting(TSqsEvents::TEvReceiveMessageBatch::TPtr& ev);
    void HandleReceiveMessageBatchWhileWorking(TSqsEvents::TEvReceiveMessageBatch::TPtr& ev);
    void HandleDeleteMessageBatchWhileIniting(TSqsEvents::TEvDeleteMessageBatch::TPtr& ev);
    void HandleDeleteMessageBatchWhileWorking(TSqsEvents::TEvDeleteMessageBatch::TPtr& ev);
    void HandleChangeMessageVisibilityBatchWhileIniting(TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr& ev);
    void HandleChangeMessageVisibilityBatchWhileWorking(TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr& ev);
    void HandleInflyIsPurgingNotification(TSqsEvents::TEvInflyIsPurgingNotification::TPtr& ev);
    void HandleQueuePurgedNotification(TSqsEvents::TEvQueuePurgedNotification::TPtr& ev);
    void HandleGetRuntimeQueueAttributesWhileIniting(TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr& ev);
    void HandleGetRuntimeQueueAttributesWhileWorking(TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr& ev);
    void HandleDeadLetterQueueNotification(TSqsEvents::TEvDeadLetterQueueNotification::TPtr& ev);
    void HandleForceReloadState(TSqsEvents::TEvForceReloadState::TPtr& ev);
    void HandleReloadStateRequest(TSqsEvents::TEvReloadStateRequest::TPtr& ev);

    void CheckStillDLQ();
    void ForceReloadState();
    void BecomeWorking();
    void RequestConfiguration();
    void UpdateStateRequest();
    void StartGatheringMetrics();
    void RequestMessagesCountMetrics(ui64 shard);
    void RequestOldestTimestampMetrics(ui64 shard);
    void ReceiveMessagesCountMetrics(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply);
    void PlanningRetentionWakeup();
    void ReceiveOldestTimestampMetrics(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply);
    void ReportMessagesCountMetricsIfReady();
    void ReportOldestTimestampMetricsIfReady();
    void OnQueueConfiguration(const TSqsEvents::TEvExecuted::TRecord& ev);
    void ScheduleGetConfigurationRetry();
    void Prepare(TSqsEvents::TEvExecute::TPtr& ev);
    void OnQueryPrepared(TSqsEvents::TEvExecute::TPtr& ev, const TSqsEvents::TEvExecuted::TRecord& record);
    void ExecuteRequest(TSqsEvents::TEvExecute::TPtr& ev, const TString& compiled);
    void OnQueryExecuted(TSqsEvents::TEvExecute::TPtr& ev, const TSqsEvents::TEvExecuted::TRecord& record);
    void RemoveCachedRequest(size_t shard, size_t idx);
    void CreateBackgroundActors();
    void AnswerGetConfiguration(TSqsEvents::TEvGetConfiguration::TPtr& req);
    void AnswerFailed(TSqsEvents::TEvGetConfiguration::TPtr& ev, bool queueRemoved = false);
    void AskQueueAttributes();
    void OnQueueAttributes(const TSqsEvents::TEvExecuted::TRecord& ev);
    void MarkInflyReloading(ui64 shard, i64 invalidatedCount, const TString& invalidationReason);
    void StartLoadingInfly();
    void StartLoadingInfly(ui64 shard, bool afterFailure = false);
    void OnInflyLoaded(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply);
    void OnStateLoaded(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply);
    void ScheduleInflyLoadAfterFailure(ui64 shard);
    bool AddMessagesToInfly(ui64 shard);
    void OnAddedMessagesToInfly(ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply);
    void ProcessReceivesAfterAddedMessagesToInfly(ui64 shard);
    void StartMessageRequestsAfterInflyLoaded(ui64 shard);
    void FailMessageRequestsAfterInflyLoadFailure(ui64 shard);
    void ProcessGetRuntimeQueueAttributes(TGetRuntimeQueueAttributesRequestProcessing& reqInfo);
    void ProcessGetRuntimeQueueAttributes(ui64 shard, TGetRuntimeQueueAttributesRequestProcessing& reqInfo);
    void ProcessGetRuntimeQueueAttributes(ui64 shard);
    void FailGetRuntimeQueueAttributesForShard(ui64 shard);
    void FailRequestsDuringStartProblems();
    void SendReloadStateRequestToDLQ();

    // send
    void ProcessSendMessageBatch(TSendMessageBatchRequestProcessing& reqInfo);
    void OnMessageSent(const TString& requestId, size_t index, const TSqsEvents::TEvExecuted::TRecord& reply, const NKikimr::NClient::TValue* messageRecord);
    // batching
    void OnSendBatchExecuted(ui64 shard, ui64 batchId, const TSqsEvents::TEvExecuted::TRecord& reply);

    // receive
    void ProcessReceiveMessageBatch(TReceiveMessageBatchRequestProcessing& reqInfo);
    void LockFifoGroup(TReceiveMessageBatchRequestProcessing& reqInfo);
    void OnFifoGroupLocked(const TString& requestId, const TSqsEvents::TEvExecuted::TRecord& ev);
    void ReadFifoMessages(TReceiveMessageBatchRequestProcessing& reqInfo);
    void OnFifoMessagesReadSuccess(const NKikimr::NClient::TValue& value, TReceiveMessageBatchRequestProcessing& reqInfo);
    void OnFifoMessagesRead(const TString& requestId, const TSqsEvents::TEvExecuted::TRecord& ev, bool usedDLQ);

    void GetMessagesFromInfly(TReceiveMessageBatchRequestProcessing& reqInfo);
    void LoadStdMessages(TReceiveMessageBatchRequestProcessing& reqInfo);
    void OnLoadStdMessageResult(const TString& requestId, ui64 offset, bool success, const NKikimr::NClient::TValue* messageRecord, bool ignoreMessageLoadingErrors);
    void TryReceiveAnotherShard(TReceiveMessageBatchRequestProcessing& reqInfo);
    void WaitAddMessagesToInflyOrTryAnotherShard(TReceiveMessageBatchRequestProcessing& reqInfo);
    void Reply(TReceiveMessageBatchRequestProcessing& reqInfo);
    // batching
    void OnLoadStdMessagesBatchSuccess(const NKikimr::NClient::TValue& value, ui64 shard, TShardInfo& shardInfo, TIntrusivePtr<TLoadBatch> batch);
    void OnLoadStdMessagesBatchExecuted(ui64 shard, ui64 batchId, const bool usedDLQ, const TSqsEvents::TEvExecuted::TRecord& reply);

    // delete
    void ProcessDeleteMessageBatch(TDeleteMessageBatchRequestProcessing& reqInfo);
    void OnMessageDeleted(const TString& requestId, ui64 shard, size_t index, const TSqsEvents::TEvExecuted::TRecord& reply, const NKikimr::NClient::TValue* messageRecord);
    // batching
    void OnDeleteBatchExecuted(ui64 shard, ui64 batchId, const TSqsEvents::TEvExecuted::TRecord& reply);

    // change message visibility
    void ProcessChangeMessageVisibilityBatch(TChangeMessageVisibilityBatchRequestProcessing& reqInfo);
    void OnVisibilityChanged(const TString& requestId, ui64 shard, const TSqsEvents::TEvExecuted::TRecord& reply);

    TQueuePath GetQueuePath() {
        return TQueuePath(Cfg().GetRoot(), UserName_, QueueName_, QueueVersion_);
    }
    void SetInflyMessagesCount(ui64 shard, const NKikimr::NClient::TValue& value);
    void SetMessagesCount(ui64 shard, const NKikimr::NClient::TValue& value);
    void SetMessagesCount(ui64 shard, ui64 value);

    void ScheduleMetricsRequest();

    bool IncActiveMessageRequests(ui64 shard, const TString& requestId);
    void DecActiveMessageRequests(ui64 shard);

    void InitQuoterResources();

private:
    // const info
    TString UserName_;
    TString QueueName_;
    TString FolderId_;
    TString RootUrl_;
    ui64 ShardsCount_ = 0;
    ui64 PartitionsCount_ = 0;
    bool IsFifoQueue_ = false;
    TString QueueId_;
    ui64 QueueVersion_ = 0;
    ui32 TablesFormat_ = 0;
    TActorId SchemeCache_;
    TIntrusivePtr<TSqsEvents::TQuoterResourcesForActions> QuoterResources_;
    TLocalRateLimiterResource SendMessageQuoterResource_;
    TLocalRateLimiterResource ReceiveMessageQuoterResource_;
    TLocalRateLimiterResource DeleteMessageQuoterResource_;
    TLocalRateLimiterResource ChangeMessageVisibilityQuoterResource_;

    // attributes cache
    TDuration QueueAttributesCacheTime_ = TDuration::Zero();
    TInstant AttributesUpdateTime_ = TInstant::Zero();
    TMaybe<TSqsEvents::TQueueAttributes> QueueAttributes_;

    // tags cache
    TMaybe<NJson::TJsonMap> QueueTags_;

    // counters
    TIntrusivePtr<TQueueCounters> Counters_;
    TIntrusivePtr<TUserCounters> UserCounters_;
    size_t MetricsQueriesInfly_ = 0;

    // dead letter queue params
    struct TTargetDlqInfo {
        // from attributes
        TString DlqName;
        ui64 MaxReceiveCount = 0;
        // discovered via service
        TString QueueId; // unique name or resource id in cloud
        ui64 QueueVersion = 0;
        ui64 ShardsCount = 0;
        ui32 TablesFormat = 0;
    };
    TMaybe<TTargetDlqInfo> DlqInfo_;
    bool IsDlqQueue_ = false;
    TInstant LatestDlqNotificationTs_ = TInstant::Zero();

    // shards
    enum class EQueryState {
        Empty,
        Preparing,
        Cached,
    };

    struct TQuery {
        /// Compiled program
        TString     Compiled;
        /// A vector of queries that are awaiting compilation completion
        TVector<TSqsEvents::TEvExecute::TPtr> Deferred;
        /// Program state
        EQueryState State = EQueryState::Empty;
    };

    // send/receive/delete batching
    template <class TEntry>
    struct TBatchBase : TSimpleRefCount<TBatchBase<TEntry>> {
        explicit TBatchBase(ui64 shard, ui64 sizeLimit, bool isFifo)
            : Shard(shard)
            , SizeLimit(sizeLimit)
            , IsFifoQueue(isFifo)
        {
            Entries.reserve(SizeLimit);
        }

        virtual ~TBatchBase() = default;

        void Execute(TQueueLeader*) {
        }

        size_t Size() const {
            return Entries.size();
        }

        bool IsFull() const {
            return Size() >= SizeLimit;
        }

        TActorIdentity SelfId() const {
            return TActorIdentity(TActivationContext::AsActorContext().SelfID);
        }

        ui64 BatchId = 0;
        std::vector<TEntry> Entries;
        ui64 Shard = 0;
        ui64 SizeLimit = 0;
        bool IsFifoQueue = false;
        const TString RequestId_ = CreateGuidAsString(); // Debug request id to take part in sample logging.
    };

    template <class TBatch>
    struct TBatchingState {
        virtual ~TBatchingState();
        void Init(const NKikimrConfig::TSqsConfig::TBatchingPolicy& policy, ui64 shard, bool isFifo);
        void TryExecute(TQueueLeader* leader);
        virtual bool CanExecute(const TBatch& batch) const { // Called for next batches when we have some batches in flight.
            return batch.IsFull();
        }
        TBatch& NewBatch();
        void CancelRequestsAfterInflyLoadFailure();

        NKikimrConfig::TSqsConfig::TBatchingPolicy Policy;
        std::deque<TIntrusivePtr<TBatch>> BatchesIniting;
        THashMap<ui64, TIntrusivePtr<TBatch>> BatchesExecuting; // Id -> batch
        ui64 Shard = 0;
        bool IsFifoQueue = false;
        ui64 NextBatchId = 1;
    };

    struct TSendBatchEntry {
        TSendBatchEntry(const TString& requestId, const TString& senderId, const TSqsEvents::TEvSendMessageBatch::TMessageEntry& message, size_t index)
            : RequestId(requestId)
            , SenderId(senderId)
            , Message(message)
            , IndexInRequest(index)
        {
        }

        TString RequestId;
        TString SenderId;
        TSqsEvents::TEvSendMessageBatch::TMessageEntry Message;
        size_t IndexInRequest = 0;
    };

    struct TDeleteBatchEntry {
        TDeleteBatchEntry(const TString& requestId, const TSqsEvents::TEvDeleteMessageBatch::TMessageEntry& message, size_t index)
            : RequestId(requestId)
            , Message(message)
            , IndexInRequest(index)
        {
        }
        TString RequestId;
        TSqsEvents::TEvDeleteMessageBatch::TMessageEntry Message;
        size_t IndexInRequest = 0;
    };

    struct TLoadBatchEntry {
        TLoadBatchEntry(const TString& requestId, TInflyMessage* msg, TDuration visibilityTimeout)
            : RequestId(requestId)
            , RandomId(msg->GetRandomId())
            , Offset(msg->GetOffset())
            , ReceiveCount(msg->GetReceiveCount())
            , CurrentVisibilityDeadline(msg->GetVisibilityDeadline())
            , VisibilityTimeout(visibilityTimeout)
        {
        }

        TString RequestId;
        ui64 RandomId = 0;
        ui64 Offset = 0;
        ui32 ReceiveCount = 0;
        TInstant CurrentVisibilityDeadline;
        TDuration VisibilityTimeout;
    };

    template <class TEntry>
    struct TBatchWithGroupInfo : public TBatchBase<TEntry> {
        using TBatchBase<TEntry>::TBatchBase;

        bool HasGroup(const TString& group) {
            return Groups.find(group) != Groups.end();
        }

        void AddGroup(const TString& group) {
            Groups.insert(group);
        }

        std::set<TString> Groups;
    };

    struct TSendBatch : public TBatchWithGroupInfo<TSendBatchEntry> {
        using TBatchWithGroupInfo<TSendBatchEntry>::TBatchWithGroupInfo;

        void AddEntry(TSendMessageBatchRequestProcessing& reqInfo, size_t i);
        void Execute(TQueueLeader* leader);

        TInstant TransactionStartedTime;
    };

    struct TDeleteBatch : public TBatchWithGroupInfo<TDeleteBatchEntry> {
        using TBatchWithGroupInfo<TDeleteBatchEntry>::TBatchWithGroupInfo;

        void AddEntry(TDeleteMessageBatchRequestProcessing& reqInfo, size_t i);
        void Execute(TQueueLeader* leader);

        THashMultiMap<ui64, size_t> Offset2Entry;
    };

    struct TLoadBatch : public TBatchBase<TLoadBatchEntry> {
        using TBatchBase<TLoadBatchEntry>::TBatchBase;

        void Execute(TQueueLeader* leader);
    };

    template <class TBatch>
    struct TBatchingStateWithGroupsRestrictions : public TBatchingState<TBatch> {
        template <class TRequestProcessing>
        void AddRequest(TRequestProcessing& reqInfo);
        bool CanExecute(const TBatch& batch) const override;
    };

    struct TSendBatchingState : public TBatchingStateWithGroupsRestrictions<TSendBatch> {
    };

    struct TDeleteBatchingState : public TBatchingStateWithGroupsRestrictions<TDeleteBatch> {
    };

    struct TLoadBatchingState : public TBatchingState<TLoadBatch> {
        void AddRequest(TReceiveMessageBatchRequestProcessing& reqInfo);
    };

    struct TShardInfo {
        ~TShardInfo();

        /// Counters
        ui64 MessagesCount = 0;
        ui64 InflyMessagesCount = 0;
        ui64 OldestMessageTimestampMs = Max();
        ui64 OldestMessageOffset = 0;
        ui64 LastSuccessfulOldestMessageTimestampValueMs = 0; // for query optimization - more accurate range

        bool MessagesCountIsRequesting = false;
        bool MessagesCountWasGot = false;
        bool OldestMessageAgeIsRequesting = false;

        TInstant CreatedTimestamp;

        /// Compiled queries
        TQuery Queries[QUERY_VECTOR_SIZE];

        TIntrusivePtr<TInflyMessages> Infly; // Infly for standard queues
        enum class EInflyLoadState {
            New, // initial state for std queues
            Fifo, // doesn't need to load
            Loaded, // we can use infly
            WaitingForActiveRequests, // waiting for active requests to finish
            WaitingForDbAnswer, // db query was sent
            Failed, // waiting for scheduled retry
        };
        EInflyLoadState InflyLoadState = EInflyLoadState::New;
        bool LoadInflyRequestInProcess = false;
        size_t ActiveMessageRequests = 0;
        ui64 ReadOffset = 0;
        bool AddingMessagesToInfly = false;
        bool NeedInflyReload = false;
        bool NeedAddingMessagesToInfly = false;
        ui64 InflyVersion = 0;
        bool DelayStatisticsInited = false;
        TInstant LastAddMessagesToInfly; // Time when AddMessagesToInfly procedure worked
        ui64 AddMessagesToInflyCheckAttempts = 0;

        TSendBatchingState SendBatchingState;
        TDeleteBatchingState DeleteBatchingState;
        TLoadBatchingState LoadBatchingState;

        bool HasMessagesToAddToInfly() const;
        bool NeedAddMessagesToInflyCheckInDatabase() const;
    };
    std::vector<TShardInfo> Shards_;
    TMessageDelayStatistics DelayStatistics_;
    TInstant RetentionWakeupPlannedAt_;
    bool AskQueueAttributesInProcess_ = false;
    TInstant UpdateStateRequestStartedAt;
    THashSet<ui32> ReloadStateRequestedFromNodes;

    bool UseCPUOptimization = false;

    // background actors
    TActorId DeduplicationCleanupActor_;
    TActorId ReadsCleanupActor_;
    TActorId RetentionActor_;
    TActorId PurgeActor_;

    struct TSendMessageBatchRequestProcessing {
        TSendMessageBatchRequestProcessing(TSqsEvents::TEvSendMessageBatch::TPtr&& ev);
        void Init(ui64 shardsCount);

        TSqsEvents::TEvSendMessageBatch::TPtr Event;
        size_t AnswersGot = 0;
        std::vector<TSqsEvents::TEvSendMessageBatchResponse::TMessageResult> Statuses;
        ui64 Shard = 0;
        bool Inited = false;
        TInstant TransactionStartedTime;
    };

    struct TReceiveMessageBatchRequestProcessing {
        TReceiveMessageBatchRequestProcessing(TSqsEvents::TEvReceiveMessageBatch::TPtr&& ev);
        void Init(ui64 shardsCount);

        ui64 GetCurrentShard() const {
            return Shards[CurrentShardIndex];
        }

        TSqsEvents::TEvReceiveMessageBatch::TPtr Event;
        THolder<TSqsEvents::TEvReceiveMessageBatchResponse> Answer;
        std::vector<ui64> Shards;
        size_t CurrentShardIndex = 0;
        TInstant LockSendTs = TInstant::Zero();
        size_t LockCount = 0;
        TInflyMessages::TReceiveCandidates ReceiveCandidates;
        size_t LoadAnswersLeft = 0;
        bool LoadError = false;
        bool Inited = false;
        bool TriedAddMessagesToInfly = false;
        bool WaitingAddMessagesToInfly = false;
        TString FromGroup; // Start group position in LOCK_GROUPS query
        struct TLockedFifoMessage {
            ui64 RandomId = 0;
            ui64 Offset = 0;
            TString GroupId;
        };
        std::vector<TLockedFifoMessage> LockedFifoMessages;
    };

    struct TDeleteMessageBatchRequestProcessing {
        TDeleteMessageBatchRequestProcessing(TSqsEvents::TEvDeleteMessageBatch::TPtr&& ev);

        TSqsEvents::TEvDeleteMessageBatch::TPtr Event;
        THolder<TSqsEvents::TEvDeleteMessageBatchResponse> Answer;
        std::vector<THolder<TInflyMessage>> InflyMessages;
        size_t AnswersGot = 0;
    };

    struct TChangeMessageVisibilityBatchRequestProcessing {
        TChangeMessageVisibilityBatchRequestProcessing(TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr&& ev);

        TSqsEvents::TEvChangeMessageVisibilityBatch::TPtr Event;
        THolder<TSqsEvents::TEvChangeMessageVisibilityBatchResponse> Answer;
        TInflyMessages::TChangeVisibilityCandidates Candidates;
    };

    struct TGetRuntimeQueueAttributesRequestProcessing {
        TGetRuntimeQueueAttributesRequestProcessing(TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr&& ev);

        TSqsEvents::TEvGetRuntimeQueueAttributes::TPtr Event;
        THolder<TSqsEvents::TEvGetRuntimeQueueAttributesResponse> Answer;
        std::vector<bool> ShardProcessFlags;
        ui64 ShardsProcessed = 0;
    };

    // requests
    std::vector<TSqsEvents::TEvGetConfiguration::TPtr> GetConfigurationRequests_;
    std::vector<TSqsEvents::TEvExecute::TPtr> ExecuteRequests_; // execute requests that wait for queue leader init
    THashMap<TString, TSendMessageBatchRequestProcessing> SendMessageRequests_; // request id -> request
    THashMap<TString, TReceiveMessageBatchRequestProcessing> ReceiveMessageRequests_; // request id -> request
    THashMap<std::pair<TString, ui64>, TDeleteMessageBatchRequestProcessing> DeleteMessageRequests_; // (request id, shard) -> request
    THashMap<std::pair<TString, ui64>, TChangeMessageVisibilityBatchRequestProcessing> ChangeMessageVisibilityRequests_; // (request id, shard) -> request
    THashMap<TString, TGetRuntimeQueueAttributesRequestProcessing> GetRuntimeQueueAttributesRequests_; // request id -> request
};

} // namespace NKikimr::NSQS
