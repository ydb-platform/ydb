#pragma once
#include "defs.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/msgbus.pb.h>

#include <ydb/library/http_proxy/error/error.h>

#include <ydb/core/ymq/base/action.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/core/ymq/base/processed_request_attributes.h>
#include <ydb/core/ymq/base/query_id.h>
#include <ydb/core/ymq/base/queue_path.h>
#include <ydb/core/ymq/proto/events.pb.h>
#include <ydb/core/ymq/proto/records.pb.h>
#include <ydb/core/protos/http_config.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/event_local.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

#include <map>
#include <set>

namespace NKikimr::NSQS {

enum class EQueueState {
    Creating = 0,
    Active   = 1,
    Deleting = 2,
};

struct TSqsEvents {
    enum EEv {
        /// Request for queue configuration
        EvGetConfiguration = EventSpaceBegin(TKikimrEvents::ES_SQS),
        EvConfiguration,
        /// Request for transaction execution
        EvExecute,
        /// Notification about completed transaction
        EvExecuted,
        /// Request to select the requested number of visible messages
        EvLockRequest, // not used
        EvLockResponse, // not used
        /// Notification about queue creation
        EvQueueCreated,
        /// Notification about queue deletion
        EvQueueDeleted,
        /// Notification about catalogs creation for a new user
        EvUserCreated,
        EvUserDeleted,
        /// Clear the queue
        EvPurgeQueue,
        /// Go to the next request from a packet
        EvNextRequest, // not used
        /// Request from a proxy to sqs service
        EvSqsRequest,
        /// Response from sqs service to the proxy
        EvSqsResponse,
        /// Request for proxying request to another node
        EvProxySqsRequest,
        /// Response to proxy from the leader
        EvProxySqsResponse,
        /// Update queue attributes cache
        EvClearQueueAttributesCache,
        /// Incrementing of atomic counter
        EvAtomicCounterIncrementResult,

        /// Request for finding leader node for the given queue
        EvGetLeaderNodeForQueueRequest,
        EvGetLeaderNodeForQueueResponse,

        EvQueueLeaderDecRef,

        EvGetQueueId,
        EvQueueId,

        // Cloud specific
        EvGetQueueFolderIdAndCustomName,
        EvQueueFolderIdAndCustomName,
        EvCountQueues,
        EvCountQueuesResponse,

        // Send/Receive/Delete requests. Action actor sends these requests to queue leader
        EvSendMessageBatch,
        EvSendMessageBatchResponse,

        EvReceiveMessageBatch,
        EvReceiveMessageBatchResponse,

        EvDeleteMessageBatch,
        EvDeleteMessageBatchResponse,

        EvChangeMessageVisibilityBatch,
        EvChangeMessageVisibilityBatchResponse,

        EvGetRuntimeQueueAttributes,
        EvGetRuntimeQueueAttributesResponse,

        EvInflyIsPurgingNotification,
        EvQueuePurgedNotification,

        EvMigrationDone,

        EvReportProcessedRequestAttributes,

        EvInsertQueueCounters,

        EvUserSettingsChanged,

        EvReadQueuesList,
        EvQueuesList,

        EvDeadLetterQueueNotification,

        EvSchemeTraversalResult,

        EvGarbageCleaningResult,

        EvGarbageSearchResult,

        EvCleanupQueryComplete,

        EvNodeTrackerSubscribeRequest,
        EvNodeTrackerUnsubscribeRequest,
        EvNodeTrackerSubscriptionStatus,

        EvForceReloadState,
        EvReloadStateRequest,
        EvReloadStateResponse,
        EvLeaderStarted,

        EvActionCounterChanged,
        EvLocalCounterChanged,

        EvEnd,
    };

    using TExecutedCallback = std::function<void (const NKikimrTxUserProxy::TEvProposeTransactionStatus&)>;

    struct TQueueAttributes {
        bool ContentBasedDeduplication = false;
        TDuration DelaySeconds = TDuration::Zero();
        bool FifoQueue = false;
        size_t MaximumMessageSize = 0;
        TDuration MessageRetentionPeriod = TDuration::Zero();
        TDuration ReceiveMessageWaitTime = TDuration::Zero();
        TDuration VisibilityTimeout = TDuration::Zero();

        // has operator<<
    };

    struct TEvGetConfiguration : public NActors::TEventLocal<TEvGetConfiguration, EvGetConfiguration> {
        TString  RequestId;
        TString  UserName;
        TString  QueueName;
        TString  FolderId;
        bool EnableThrottling = true;
        ui64 Flags = 0;

        enum EFlags {
            NeedQueueLeader = 1,
            NeedQueueAttributes = NeedQueueLeader | 2, // attributes are stored in leader actor, so, when you need attributes, you need leader
        };

        TEvGetConfiguration() = default;
        TEvGetConfiguration(const TEvGetConfiguration& other) = default;
        TEvGetConfiguration(TString requestId, const TString& user, const TString& name, ui64 flags = 0)
            : RequestId(std::move(requestId))
            , UserName(user)
            , QueueName(name)
            , Flags(flags)
        { }
        TEvGetConfiguration(
                TString requestId,
                const TString& user,
                const TString& name,
                const TString& folderId,
                bool enableThrottling,
                ui64 flags = 0
        )   : RequestId(std::move(requestId))
            , UserName(user)
            , QueueName(name)
            , FolderId(folderId)
            , EnableThrottling(enableThrottling)
            , Flags(flags)
        { }
    };

    struct TQuoterResourcesForActions : public TAtomicRefCount<TQuoterResourcesForActions> {
        virtual ~TQuoterResourcesForActions() = default;

        struct TResourceDescription {
            ui64 QuoterId;
            ui64 ResourceId;
        };

        THashMap<EAction, TResourceDescription> ActionsResources;
        TResourceDescription OtherActions;
        TResourceDescription CreateQueueAction; // Separate action for create queue. Quota is requested only when actor knows that there is no such queue. SQS-620
    };

    struct TEvConfiguration : public NActors::TEventLocal<TEvConfiguration, EvConfiguration> {
        // Success status
        bool Fail = false;

        // Existence
        bool UserExists = false;
        bool QueueExists = false;

        // Event processing was throttled
        bool Throttled = false;

        // Queue info
        ui32 TablesFormat = 0;
        ui64 QueueVersion = 0;
        ui64 Shards = 1;
        bool Fifo = false;
        TMaybe<TQueueAttributes> QueueAttributes;
        TIntrusivePtr<TQuoterResourcesForActions> QuoterResources;

        // Counters
        TIntrusivePtr<::NMonitoring::TDynamicCounters> SqsCoreCounters; // Raw counters interface. Is is not prefered to use them
        TIntrusivePtr<TUserCounters> UserCounters;
        TIntrusivePtr<TQueueCounters> QueueCounters;

        // Common info
        TString RootUrl;
        TActorId SchemeCache;
        TActorId QueueLeader;
    };

    struct TEvClearQueueAttributesCache : public NActors::TEventLocal<TEvClearQueueAttributesCache, EvClearQueueAttributesCache> {
    };

    struct TEvExecute : public NActors::TEventLocal<TEvExecute, EvExecute> {
        /// Query sender
        TActorId Sender;
        // User request id this query belongs to
        TString RequestId;
        /// Queue path in the catalog
        TQueuePath QueuePath;
        /// Shard id we address by this query
        ui64 Shard;
        /// Query index to execute
        EQueryId QueryIdx;
        /// Query params
        NKikimrMiniKQL::TParams Params;
        /// Callback that is called on response receiving
        TExecutedCallback Cb;
        /// This option specifies if it safe to retry transaction in case of undertermined transaction status,
        /// for example timeout. If transaction is idempotent, this options makes the whole system less sensitive to
        /// tablets restarts.
        bool RetryOnTimeout = false;

        TEvExecute() = default;

        TEvExecute(const TEvExecute& other) = default;

        TEvExecute(const TActorId& sender, TString requestId, const TQueuePath& path, const EQueryId idx, const ui64 shard = 0)
            : Sender(sender)
            , RequestId(std::move(requestId))
            , QueuePath(path)
            , Shard(shard)
            , QueryIdx(idx)
        {
            Y_ABORT_UNLESS(QueryIdx < EQueryId::QUERY_VECTOR_SIZE);
        }
    };

    struct TEvExecuted : public NActors::TEventPB<TEvExecuted, NKikimrTxUserProxy::TEvProposeTransactionStatus, EvExecuted> {
        using TEventBase = NActors::TEventPB<TEvExecuted, NKikimrTxUserProxy::TEvProposeTransactionStatus, EvExecuted>;
        using TRecord = ProtoRecordType;

        TExecutedCallback Cb;
        ui64 Shard;

        TEvExecuted()
            : Shard(0)
        { }

        explicit TEvExecuted(TExecutedCallback cb, ui64 shard)
            : Cb(cb)
            , Shard(shard)
        { }

        TEvExecuted(const TRecord& rec, TExecutedCallback cb, ui64 shard)
            : TEventBase(rec)
            , Cb(cb)
            , Shard(shard)
        { }

        void Call() {
            if (Cb) {
                Cb(Record);
            }
        }

        static bool IsOk(const NKikimrTxUserProxy::TEvProposeTransactionStatus& record) {
            const ui32 status = record.GetStatus();
            return status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete;
        }

        bool IsOk() const {
            return IsOk(Record);
        }

        // Error that occurs when user has deleted & created queue and then requested other node to do some prepared query.
        // In these cases we should clear prepared query with old tables ids and recompile them.
        static bool IsResolvingError(const NKikimrTxUserProxy::TEvProposeTransactionStatus& record) {
            if (record.GetStatus() == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError) {
                return true;
            }
            if (record.GetStatusCode() == NKikimrIssues::TStatusIds::PATH_NOT_EXIST || record.GetStatusCode() == NKikimrIssues::TStatusIds::SCHEME_ERROR) {
                return true;
            }
            for (const Ydb::Issue::IssueMessage& issue : record.GetIssues()) {
                const ui32 issueCode = issue.issue_code();
                if (issueCode == NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR
                    || issueCode == NKikimrIssues::TIssuesIds::PATH_NOT_EXIST) {
                    return true;
                }
            }
            return false;
        }

        bool IsResolvingError() const {
            return IsResolvingError(Record);
        }
    };

    struct TEvUserCreated : public NActors::TEventLocal<TEvUserCreated, EvUserCreated> {
        bool    Success;

        TEvUserCreated(bool success)
            : Success(success)
        {
        }
    };

    struct TEvUserDeleted : public NActors::TEventLocal<TEvUserDeleted, EvUserDeleted> {
        bool    Success;
        TString Error;

        TEvUserDeleted(bool success, const TString& error = TString())
            : Success(success)
            , Error(error)
        {
        }
    };

    struct TEvQueueCreated : public NActors::TEventLocal<TEvQueueCreated, EvQueueCreated> {
        TString      QueueId;
        TString      ExistingQueueResourceId;
        TString      Error;
        EQueueState  State;
        bool         AlreadyExists;
        bool         Success;
        const TErrorClass* ErrorClass = nullptr;
    };

    struct TEvQueueDeleted : public NActors::TEventLocal<TEvQueueDeleted, EvQueueDeleted> {
        TQueuePath QueuePath;
        TString    Message;
        bool       Success;

        TEvQueueDeleted(const TQueuePath& path, bool success, const TString& message = TString())
            : QueuePath(path)
            , Message(message)
            , Success(success)
        {
        }

        TEvQueueDeleted(const TEvQueueDeleted& other)
            : QueuePath(other.QueuePath)
            , Message(other.Message)
            , Success(other.Success)
        {
        }
    };

    struct TEvAtomicCounterIncrementResult : public NActors::TEventLocal<TEvAtomicCounterIncrementResult, EvAtomicCounterIncrementResult> {
        bool Success;
        TString Error;
        ui64 NewValue;

        TEvAtomicCounterIncrementResult(bool success, const TString& error = TString(), ui64 newValue = 0)
            : Success(success)
            , Error(error)
            , NewValue(newValue)
        {
        }

        TEvAtomicCounterIncrementResult(const TEvAtomicCounterIncrementResult& other) = default;
    };

    struct TEvPurgeQueue : public NActors::TEventLocal<TEvPurgeQueue, EvPurgeQueue> {
        /// Queue path in the catalog
        TQueuePath QueuePath;
        /// All messages must be deleted until this timestamp
        TInstant Boundary;
        ui64 Shard;

        TEvPurgeQueue() = default;

        TEvPurgeQueue(const TEvPurgeQueue& other)
            : QueuePath(other.QueuePath)
            , Boundary(other.Boundary)
            , Shard(other.Shard)
        {
        }
    };

    struct TEvActionCounterChanged: public NActors::TEventPB<TEvActionCounterChanged, NKikimrClient::TSqsActionCounterChanged, EvActionCounterChanged> {
        using TEventBase = NActors::TEventPB<TEvActionCounterChanged, NKikimrClient::TSqsActionCounterChanged, EvActionCounterChanged>;
        using TEventBase::TEventBase;  
    };

    struct TEvLocalCounterChanged: public NActors::TEventLocal<TEvLocalCounterChanged, EvLocalCounterChanged> {
        enum class ECounterType{
            ReceiveMessageImmediateDuration,
            ReceiveMessageEmptyCount,
            MessagesPurged,
            ClientMessageProcessingDuration,
        };

        ECounterType CounterType;
        ui32 Value;
        TEvLocalCounterChanged(ECounterType counterType, ui32 value)
            : CounterType(counterType)
            , Value(value)
        {
        }
    };

    // Request that is sent from proxy to sqs service actor on other (leader) node
    struct TEvSqsRequest : public NActors::TEventPB<TEvSqsRequest, NKikimrClient::TSqsRequest, EvSqsRequest> {
        using TEventBase = NActors::TEventPB<TEvSqsRequest, NKikimrClient::TSqsRequest, EvSqsRequest>;
        using TEventBase::TEventBase;  
    };

    // Response to TEvSqsRequest
    struct TEvSqsResponse : public NActors::TEventPB<TEvSqsResponse, NKikimrClient::TSqsResponse, EvSqsResponse> {
        using TEventBase = NActors::TEventPB<TEvSqsResponse, NKikimrClient::TSqsResponse, EvSqsResponse>;
        using TEventBase::TEventBase;  
    };

    // Request for proxying request to sqs service actor on other (leader) node
    struct TEvProxySqsRequest : public NActors::TEventLocal<TEvProxySqsRequest, EvProxySqsRequest> {
        NKikimrClient::TSqsRequest Record;

        // Information to identify leader node
        TString UserName;
        TString QueueName;

        TString RequestId;

        TEvProxySqsRequest() = default;

        TEvProxySqsRequest(const NKikimrClient::TSqsRequest& record, TString userName, TString queueName)
            : Record(record)
            , UserName(std::move(userName))
            , QueueName(std::move(queueName))
            , RequestId(Record.GetRequestId())
        {
        }

        TEvProxySqsRequest(NKikimrClient::TSqsRequest&& record, TString userName, TString queueName)
            : UserName(std::move(userName))
            , QueueName(std::move(queueName))
            , RequestId(record.GetRequestId())
        {
            Record.Swap(&record);
        }
    };

    // Response to TEvProxySqsRequest
    struct TEvProxySqsResponse : public NActors::TEventLocal<TEvProxySqsResponse, EvProxySqsResponse> {
        enum class EProxyStatus { // can be written to text stream
            OK,
            LeaderResolvingError,
            SessionError,
            QueueDoesNotExist,
            UserDoesNotExist,
            Throttled,
        };

        NKikimrClient::TSqsResponse Record;
        EProxyStatus ProxyStatus = EProxyStatus::OK;

        TEvProxySqsResponse() = default;

        TEvProxySqsResponse(const NKikimrClient::TSqsResponse& record, EProxyStatus status = EProxyStatus::OK)
            : Record(record)
            , ProxyStatus(status)
        {
        }

        TEvProxySqsResponse(NKikimrClient::TSqsResponse&& record, EProxyStatus status = EProxyStatus::OK)
            : ProxyStatus(status)
        {
            Record.Swap(&record);
        }
    };

    struct TEvGetLeaderNodeForQueueRequest : public NActors::TEventLocal<TEvGetLeaderNodeForQueueRequest, EvGetLeaderNodeForQueueRequest> {
        TString RequestId;
        TString UserName;
        TString QueueName;

        TEvGetLeaderNodeForQueueRequest(TString requestId, TString user, TString queue)
            : RequestId(std::move(requestId))
            , UserName(std::move(user))
            , QueueName(std::move(queue))
        {
        }
    };

    struct TEvGetLeaderNodeForQueueResponse : public NActors::TEventLocal<TEvGetLeaderNodeForQueueResponse, EvGetLeaderNodeForQueueResponse> {
        enum class EStatus {
            OK,
            NoUser,
            NoQueue,
            FailedToConnectToLeader,
            Error,
            Throttled,
        };

        TString RequestId;
        TString UserName;
        TString QueueName;
        ui64 NodeId = 0;
        EStatus Status = EStatus::OK;

        TEvGetLeaderNodeForQueueResponse(TString requestId, TString user, TString queue, ui64 nodeId)
            : RequestId(std::move(requestId))
            , UserName(std::move(user))
            , QueueName(std::move(queue))
            , NodeId(nodeId)
        {
        }

        TEvGetLeaderNodeForQueueResponse(TString requestId, TString user, TString queue, EStatus error)
            : RequestId(std::move(requestId))
            , UserName(std::move(user))
            , QueueName(std::move(queue))
            , Status(error)
        {
        }
    };

    struct TEvQueueLeaderDecRef : public NActors::TEventLocal<TEvQueueLeaderDecRef, EvQueueLeaderDecRef> {
    };

    struct TEvGetQueueId : public NActors::TEventLocal<TEvGetQueueId, EvGetQueueId> {
        TString RequestId;
        TString UserName;
        TString CustomQueueName; // custom name in case of Yandex.Cloud mode and queue name in case of Yandex
        TString FolderId; // empty in case of Yandex mode

        TEvGetQueueId(TString requestId, TString userName, TString customQueueName, TString folderId)
            : RequestId(std::move(requestId))
            , UserName(std::move(userName))
            , CustomQueueName(std::move(customQueueName))
            , FolderId(std::move(folderId))
        {
        }
    };

    struct TEvQueueId : public NActors::TEventLocal<TEvQueueId, EvQueueId> {
        bool Exists = false;
        bool Failed = false;
        // Event processing was throttled
        bool Throttled = false;
        TString QueueId; // resource id in case of Yandex.Cloud mode and queue name in case of Yandex
        ui64 Version = 0; // last queue version registered in service actor
        ui64 ShardsCount = 0; // number of queue shards
        ui32 TablesFormat = 0;

        TEvQueueId(const bool failed = false, const bool throttled = false)
            : Failed(failed)
            , Throttled(throttled)
        {
        }

        explicit TEvQueueId(const TString& queueId, const ui64 version, const ui64 shardsCount, const ui32 tablesFormat)
            : Exists(true)
            , QueueId(queueId)
            , Version(version)
            , ShardsCount(shardsCount)
            , TablesFormat(tablesFormat)
        {
        }
    };

    struct TEvGetQueueFolderIdAndCustomName : public NActors::TEventLocal<TEvGetQueueFolderIdAndCustomName, EvGetQueueFolderIdAndCustomName> {
        TString RequestId;
        TString UserName;
        TString QueueName;

        TEvGetQueueFolderIdAndCustomName(TString requestId, TString userName, TString queueName)
            : RequestId(std::move(requestId))
            , UserName(std::move(userName))
            , QueueName(std::move(queueName))
        {
        }
    };

    struct TEvQueueFolderIdAndCustomName : public NActors::TEventLocal<TEvQueueFolderIdAndCustomName, EvQueueFolderIdAndCustomName> {
        bool Exists = false;
        bool Failed = false;
        // Event processing was throttled
        bool Throttled = false;

        TString QueueFolderId;
        TString QueueCustomName;

        TEvQueueFolderIdAndCustomName(bool failed = false, bool throttled = false)
            : Failed(failed)
            , Throttled(throttled)
        {
        }

        explicit TEvQueueFolderIdAndCustomName(TString queueFolderId, TString queueCustomName)
            : Exists(true)
            , QueueFolderId(std::move(queueFolderId))
            , QueueCustomName(std::move(queueCustomName))
        {
        }
    };

    struct TEvCountQueues : public NActors::TEventLocal<TEvCountQueues, EvCountQueues> {
        TString RequestId;
        TString UserName;
        TString FolderId;

        TEvCountQueues(TString requestId, TString userName, TString folderId)
            : RequestId(std::move(requestId))
            , UserName(std::move(userName))
            , FolderId(std::move(folderId))
        {
        }
    };

    struct TEvCountQueuesResponse : public NActors::TEventLocal<TEvCountQueuesResponse, EvCountQueuesResponse> {
        bool Failed = false;
        bool Exists = false;
        ui64 Count = 0;

        explicit TEvCountQueuesResponse(bool failed, bool exists = false, ui64 count = 0)
            : Failed(failed)
            , Exists(exists)
            , Count(count)
        {
        }
    };

    struct TEvSendMessageBatch : public NActors::TEventLocal<TEvSendMessageBatch, EvSendMessageBatch> {
        struct TMessageEntry {
            TString MessageId;

            TString Body;
            TString Attributes; // serialized attributes
            TDuration Delay;

            // for fifo
            TString DeduplicationId;
            TString MessageGroupId;
        };

        TString RequestId;
        TString SenderId;
        std::vector<TMessageEntry> Messages;
    };

    struct TEvSendMessageBatchResponse : public NActors::TEventLocal<TEvSendMessageBatchResponse, EvSendMessageBatchResponse> {
        enum class ESendMessageStatus {
            OK,
            AlreadySent, // deduplicated
            Failed,
        };
        struct TMessageResult {
            ESendMessageStatus Status;
            TString MessageId; // In case of deduplication reasons message id can be different from one that was provided by action actor
            ui64 SequenceNumber = 0;
        };
        std::vector<TMessageResult> Statuses;
    };

    // Request to try to receive message batch.
    // While processing this request leader doesn't perform long polling.
    struct TEvReceiveMessageBatch : public NActors::TEventLocal<TEvReceiveMessageBatch, EvReceiveMessageBatch> {
        TString RequestId;
        size_t MaxMessagesCount = 0;
        TString ReceiveAttemptId;
        TDuration VisibilityTimeout = TDuration::Zero();
        TInstant WaitDeadline = TInstant::Zero();
    };

    struct TEvReceiveMessageBatchResponse : public NActors::TEventLocal<TEvReceiveMessageBatchResponse, EvReceiveMessageBatchResponse> {
        bool Failed = false;
        bool OverLimit = false;
        bool Retried = false;

        struct TMessageResult {
            TInstant FirstReceiveTimestamp = TInstant::Zero();
            ui32 ReceiveCount = 0;
            TString MessageId;
            TString MessageDeduplicationId;
            TString MessageGroupId;
            TString MessageAttributes;
            TString Data;
            TReceipt ReceiptHandle;
            TInstant SentTimestamp = TInstant::Zero();
            ui64 SequenceNumber = 0;
            TString SenderId;
        };
        std::vector<TMessageResult> Messages;
    };

    struct TEvDeleteMessageBatch : public NActors::TEventLocal<TEvDeleteMessageBatch, EvDeleteMessageBatch> {
        struct TMessageEntry {
            ui64 Offset = 0;
            TInstant LockTimestamp = TInstant::Zero();

            // for fifo
            TString MessageGroupId;
            TString ReceiveAttemptId;
        };

        TString RequestId;
        ui64 Shard = 0;
        std::vector<TMessageEntry> Messages;
    };

    struct TEvDeleteMessageBatchResponse : public NActors::TEventLocal<TEvDeleteMessageBatchResponse, EvDeleteMessageBatchResponse> {
        enum class EDeleteMessageStatus {
            OK,
            NotFound,
            Failed,
        };
        struct TMessageResult {
            EDeleteMessageStatus Status = EDeleteMessageStatus::NotFound;
        };
        ui64 Shard = 0;
        std::vector<TMessageResult> Statuses;
    };

    struct TEvChangeMessageVisibilityBatch : public NActors::TEventLocal<TEvChangeMessageVisibilityBatch, EvChangeMessageVisibilityBatch> {
        struct TMessageEntry {
            ui64 Offset = 0;
            TInstant LockTimestamp;

            // for fifo
            TString MessageGroupId;
            TString ReceiveAttemptId;

            TInstant VisibilityDeadline;
        };

        TString RequestId;
        ui64 Shard = 0;
        TInstant NowTimestamp;
        std::vector<TMessageEntry> Messages;
    };

    struct TEvChangeMessageVisibilityBatchResponse : public NActors::TEventLocal<TEvChangeMessageVisibilityBatchResponse, EvChangeMessageVisibilityBatchResponse> {
        enum class EMessageStatus {
            OK,
            NotFound,
            NotInFly,
            Failed,
        };
        struct TMessageResult {
            EMessageStatus Status = EMessageStatus::NotFound;
        };
        ui64 Shard = 0;
        std::vector<TMessageResult> Statuses;
    };

    struct TEvInflyIsPurgingNotification : public NActors::TEventLocal<TEvInflyIsPurgingNotification, EvInflyIsPurgingNotification> {
        ui64 Shard = 0;
        std::vector<ui64> Offsets;
    };

    struct TEvQueuePurgedNotification : public NActors::TEventLocal<TEvQueuePurgedNotification, EvQueuePurgedNotification> {
        ui64 Shard = 0;
        ui64 NewMessagesCount = 0;
        TVector<ui64> DeletedOffsets;
    };

    struct TEvGetRuntimeQueueAttributes : public NActors::TEventLocal<TEvGetRuntimeQueueAttributes, EvGetRuntimeQueueAttributes> {
        TString RequestId;

        TEvGetRuntimeQueueAttributes(const TString& requestId)
            :RequestId(requestId)
        {
        }
    };

    struct TEvGetRuntimeQueueAttributesResponse : public NActors::TEventLocal<TEvGetRuntimeQueueAttributesResponse, EvGetRuntimeQueueAttributesResponse> {
        bool Failed = false;

        size_t MessagesCount = 0;
        size_t InflyMessagesCount = 0;
        size_t MessagesDelayed = 0;
        TInstant CreatedTimestamp;
    };

    struct TEvMigrationDone : public NActors::TEventLocal<TEvMigrationDone, EvMigrationDone> {
        bool Success = true;

        TEvMigrationDone(bool ok = true)
            : Success(ok)
        {
        }
    };

    struct TEvReportProcessedRequestAttributes : public NActors::TEventLocal<TEvReportProcessedRequestAttributes, EvReportProcessedRequestAttributes> {
        TProcessedRequestAttributes Data;
    };

    struct TEvInsertQueueCounters : public NActors::TEventLocal<TEvInsertQueueCounters, EvInsertQueueCounters> {
        TEvInsertQueueCounters(const TString& user, const TString& queue, ui64 version)
            : User(user)
            , Queue(queue)
            , Version(version)
        {
        }

        TString User;
        TString Queue;
        ui64 Version;
    };

    struct TEvUserSettingsChanged : public NActors::TEventLocal<TEvUserSettingsChanged, EvUserSettingsChanged> {
        TEvUserSettingsChanged(const TString& userName, std::shared_ptr<const std::map<TString, TString>> settings,
                std::shared_ptr<const std::set<TString>> diff)
            : UserName(userName)
            , Settings(std::move(settings))
            , Diff(std::move(diff))
        {
        }

        TString UserName;
        std::shared_ptr<const std::map<TString, TString>> Settings;
        std::shared_ptr<const std::set<TString>> Diff;
    };

    struct TEvReadQueuesList : public NActors::TEventLocal<TEvReadQueuesList, EvReadQueuesList> {
    };

    struct TEvQueuesList : public NActors::TEventLocal<TEvQueuesList, EvQueuesList> {
        struct TQueueRecord {
            TString UserName;
            TString QueueName;
            ui64 LeaderTabletId = 0;
            TString CustomName;
            TString FolderId;
            ui32 TablesFormat = 0;
            TString DlqName;
            ui64 Version = 0;
            ui64 ShardsCount = 0;
            TInstant CreatedTimestamp;
            bool IsFifo = false;

            bool operator<(const TQueueRecord& r) const {
                return std::tie(UserName, QueueName) < std::tie(r.UserName, r.QueueName);
            }
        };

        bool Success = true;
        std::vector<TQueueRecord> SortedQueues;

        explicit TEvQueuesList(bool success = true)
            : Success(success)
        {
        }
    };

    // Used by service to notify dead letter queue leader
    struct TEvDeadLetterQueueNotification : public NActors::TEventLocal<TEvDeadLetterQueueNotification, EvDeadLetterQueueNotification> {
    };

    using TSchemePath = TVector<TString>;
    using TSchemeCacheNavigate = NSchemeCache::TSchemeCacheNavigate;

    struct TSchemeNode {
        TSchemePath Path;
        TSchemeCacheNavigate::EKind Kind = TSchemeCacheNavigate::EKind::KindUnknown;
        TInstant CreationTs = TInstant::Zero();

        THashMap<TString, TSchemeNode> Children;
    };

    struct TEvSchemeTraversalResult : public NActors::TEventLocal<TEvSchemeTraversalResult, EvSchemeTraversalResult> {
        explicit TEvSchemeTraversalResult(bool success = true)
            : Success(success)
        {
        }

        bool Success = true;
        THolder<TSchemeNode> RootHolder;
    };

    struct TEvGarbageCleaningResult : public NActors::TEventLocal<TEvGarbageCleaningResult, EvGarbageCleaningResult> {
        explicit TEvGarbageCleaningResult(bool success = true) {
            Record.Success = success;
        }

        struct TCleaningResult {
            bool Success = true;

            TString Account;
            TString HintPath;
            TVector<TString> RemovedNodes;

            TInstant StartedAt;
            TDuration Duration;
        };

        TCleaningResult Record;
    };

    struct TEvGarbageSearchResult : public NActors::TEventLocal<TEvGarbageSearchResult, EvGarbageSearchResult> {
        explicit TEvGarbageSearchResult(bool success = true)
            : Success(success)
        {
        }

        struct TGarbageHint {
            enum class EReason {
                Unknown,
                UnusedVersion,
                UnregisteredQueue
            };

            TString Account;
            TSchemeNode SchemeNode;
            EReason Reason = EReason::Unknown;
        };

        bool Success = true;
        THashMap<TString, TGarbageHint> GarbageHints;
    };
    struct TEvCleanupQueryComplete : public NActors::TEventLocal<TEvCleanupQueryComplete, EvCleanupQueryComplete> {
        explicit TEvCleanupQueryComplete(const TString& name, ui64 type)
            : Name(name)
            , Type(type)
        {
        }
        TString Name;
        ui64 Type;
    };

    struct TEvNodeTrackerSubscribeRequest 
        : public NActors::TEventLocal<TEvNodeTrackerSubscribeRequest, EvNodeTrackerSubscribeRequest>
    {
        explicit TEvNodeTrackerSubscribeRequest(
            ui64 subscriptionId,
            ui64 queueIdNumber,
            bool isFifo,
            std::optional<ui64> tabletId = {}
        )
            : SubscriptionId(subscriptionId)
            , QueueIdNumber(queueIdNumber)
            , IsFifo(isFifo)
            , TabletId(tabletId)
        {}
        ui64 SubscriptionId;
        ui64 QueueIdNumber;
        bool IsFifo;
        std::optional<ui64> TabletId;
    };
    
    struct TEvNodeTrackerUnsubscribeRequest 
        : public NActors::TEventLocal<TEvNodeTrackerUnsubscribeRequest, EvNodeTrackerUnsubscribeRequest>
    {
        TEvNodeTrackerUnsubscribeRequest(ui64 subscriptionId)
            : SubscriptionId(subscriptionId)
        {}
        ui64 SubscriptionId;
    };

    struct TEvNodeTrackerSubscriptionStatus : public NActors::TEventLocal<TEvNodeTrackerSubscriptionStatus, EvNodeTrackerSubscriptionStatus> {
        TEvNodeTrackerSubscriptionStatus(ui64 subscriptionId, ui32 nodeId, bool disconnected=false)
            : SubscriptionId(subscriptionId)
            , NodeId(nodeId)
            , Disconnected(disconnected)
        {}
        ui64 SubscriptionId;
        ui32 NodeId;
        bool Disconnected;
    };
    
    struct TEvForceReloadState : public NActors::TEventLocal<TEvForceReloadState, EvForceReloadState> {
        explicit TEvForceReloadState(TDuration nextTryAfter = TDuration::Zero())
            : NextTryAfter(nextTryAfter)
        {}
        TDuration NextTryAfter;
    };
    
    struct TEvReloadStateRequest : public NActors::TEventPB<TEvReloadStateRequest, TReloadStateRequest, EvReloadStateRequest> {
        TEvReloadStateRequest() = default;
        
        TEvReloadStateRequest(const TString& user, const TString& queue) {
            Record.MutableTarget()->SetUserName(user);
            Record.MutableTarget()->SetQueueName(queue);
        }
    };

    struct TEvReloadStateResponse : public NActors::TEventPB<TEvReloadStateResponse, TReloadStateResponse, EvReloadStateResponse> {
        TEvReloadStateResponse() = default;
        TEvReloadStateResponse(const TString& user, const TString& queue, TInstant reloadedAt) {
            Record.MutableWho()->SetUserName(user);
            Record.MutableWho()->SetQueueName(queue);
            Record.SetReloadedAtMs(reloadedAt.MilliSeconds());
        }
    };

    struct TEvLeaderStarted : public NActors::TEventLocal<TEvLeaderStarted, EvLeaderStarted> {
    };
};

} // namespace NKikimr::NSQS
