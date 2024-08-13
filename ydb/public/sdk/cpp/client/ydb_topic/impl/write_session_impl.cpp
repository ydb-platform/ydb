#include "write_session_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/trace_lazy.h>

#include <library/cpp/string_utils/url/url.h>

#include <google/protobuf/util/time_util.h>

#include <util/generic/store_policy.h>
#include <util/generic/utility.h>
#include <util/stream/buffer.h>
#include <util/generic/guid.h>


namespace NYdb::NTopic {

const TDuration UPDATE_TOKEN_PERIOD = TDuration::Hours(1);
// Error code from file ydb/public/api/protos/persqueue_error_codes_v1.proto
const ui64 WRITE_ERROR_PARTITION_INACTIVE = 500029;

namespace {

using TTxId = std::pair<std::string_view, std::string_view>;
using TTxIdOpt = std::optional<TTxId>;

TTxIdOpt GetTransactionId(const Ydb::Topic::StreamWriteMessage_WriteRequest& request)
{
    Y_ABORT_UNLESS(request.messages_size());

    if (!request.has_tx()) {
        return std::nullopt;
    }

    const Ydb::Topic::TransactionIdentity& tx = request.tx();
    return TTxId(tx.session(), tx.id());
}

TTxIdOpt GetTransactionId(const NTable::TTransaction* tx)
{
    if (!tx) {
        return std::nullopt;
    }

    return TTxId(tx->GetSession().GetId(), tx->GetId());
}

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSessionImpl

TWriteSessionImpl::TWriteSessionImpl(
        const TWriteSessionSettings& settings,
         std::shared_ptr<TTopicClient::TImpl> client,
         std::shared_ptr<TGRpcConnectionsImpl> connections,
         TDbDriverStatePtr dbDriverState)
    : Settings(settings)
    , Client(std::move(client))
    , Connections(std::move(connections))
    , DbDriverState(std::move(dbDriverState))
    , PrevToken(DbDriverState->CredentialsProvider ? DbDriverState->CredentialsProvider->GetAuthInfo() : "")
    , InitSeqNoPromise(NThreading::NewPromise<ui64>())
    , WakeupInterval(
            Settings.BatchFlushInterval_.GetOrElse(TDuration::Zero()) ?
                std::min(Settings.BatchFlushInterval_.GetOrElse(TDuration::Seconds(1)) / 5, TDuration::MilliSeconds(100))
                :
                TDuration::MilliSeconds(100)
    )
{
    if (!Settings.RetryPolicy_) {
        Settings.RetryPolicy_ = IRetryPolicy::GetDefaultPolicy();
    }
    if (Settings.Counters_.Defined()) {
        Counters = *Settings.Counters_;
    } else {
        Counters = MakeIntrusive<TWriterCounters>(new ::NMonitoring::TDynamicCounters());
    }
}

void TWriteSessionImpl::Start(const TDuration& delay) {
    Y_ABORT_UNLESS(SelfContext);

    if (!EventsQueue) {
#define WRAP_HANDLER(type, handler, ...)                                                                    \
        if (auto h = Settings.EventHandlers_.handler##_) {                                                  \
            Settings.EventHandlers_.handler([ctx = SelfContext, h = std::move(h)](__VA_ARGS__ type& ev){    \
                if (auto self = ctx->LockShared()) {                                                        \
                    h(ev);                                                                                  \
                }                                                                                           \
            });                                                                                             \
        }
        WRAP_HANDLER(TWriteSessionEvent::TAcksEvent, AcksHandler);
        WRAP_HANDLER(TWriteSessionEvent::TReadyToAcceptEvent, ReadyToAcceptHandler);
        WRAP_HANDLER(TSessionClosedEvent, SessionClosedHandler, const);
        WRAP_HANDLER(TWriteSessionEvent::TEvent, CommonHandler);
#undef WRAP_HANDLER

        EventsQueue = std::make_shared<TWriteSessionEventsQueue>(Settings);
    }

    if (!Started) {
        with_lock(Lock) {
            HandleWakeUpImpl();
        }
        InitWriter();
    }
    with_lock (Lock) {
        ++ConnectionAttemptsDone;
        Started = true;
        if (Settings.DirectWriteToPartition_ && (Settings.PartitionId_.Defined() || DirectWriteToPartitionId.Defined())) {
            PreferredPartitionLocation = {};
            ConnectToPreferredPartitionLocation(delay);
            return;
        }
    }
    Connect(delay);
}

// Returns true if we need to switch to another DirectWriteToPartitionId.
bool NeedToSwitchPartition(const TPlainStatus& status) {
    switch (status.Status) {
    // Server statuses:
    case EStatus::OVERLOADED:
        // In general OVERLOADED is temporary, but it's also returned on partition split/merge,
        // in which case we need to switch to another partition.
        for (auto const& issue : status.Issues) {
            if (issue.IssueCode == WRITE_ERROR_PARTITION_INACTIVE) {
                return true;
            }
        }
    case EStatus::UNAUTHORIZED:
    case EStatus::SUCCESS:
    case EStatus::UNAVAILABLE:
    case EStatus::SESSION_EXPIRED:
    case EStatus::CANCELLED:
    case EStatus::UNDETERMINED:
    case EStatus::SESSION_BUSY:
    case EStatus::TIMEOUT:

    // Client statuses:
    case EStatus::TRANSPORT_UNAVAILABLE:
    case EStatus::CLIENT_RESOURCE_EXHAUSTED:
    case EStatus::CLIENT_DEADLINE_EXCEEDED:
    case EStatus::CLIENT_INTERNAL_ERROR:
    case EStatus::CLIENT_OUT_OF_RANGE:
    case EStatus::CLIENT_LIMITS_REACHED:
    case EStatus::CLIENT_DISCOVERY_FAILED:
        return false;
    default:
        return true;
    }
}

TWriteSessionImpl::THandleResult TWriteSessionImpl::RestartImpl(const TPlainStatus& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    TRACE_LAZY(DbDriverState->Log, "Error",
        TRACE_KV("status", status.Status));

    THandleResult result;
    if (AtomicGet(Aborting)) {
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session is aborting and will not restart");
        return result;
    }
    SessionEstablished = false;

    // Keep DirectWriteToPartitionId value on temporary errors.
    if (DirectWriteToPartitionId.Defined() && NeedToSwitchPartition(status)) {
        TRACE_LAZY(DbDriverState->Log, "ClearDirectWriteToPartitionId");
        DirectWriteToPartitionId.Clear();
        // We need to clear PreferredPartitionLocation here, because in Start,
        // with both Settings.PartitionId_ and DirectWriteToPartitionId undefined,
        // Connect is called, and PreferredPartitionLocation is used there to fill in the reqSettings.
        PreferredPartitionLocation = {};
    }

    TMaybe<TDuration> nextDelay = TDuration::Zero();
    if (!RetryState) {
        RetryState = Settings.RetryPolicy_->CreateRetryState();
    }
    nextDelay = RetryState->GetNextRetryDelay(status.Status);

    if (nextDelay) {
        result.StartDelay = *nextDelay;
        result.DoRestart = true;
        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Got error. " << status.ToDebugString());
        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session will restart in " << result.StartDelay);
        ResetForRetryImpl();
    } else {
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Got error. " << status.ToDebugString());
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Write session will not restart after a fatal error");
        result.DoStop = true;
        CheckHandleResultImpl(result);
    }
    return result;
}

TString FullTopicPath(TStringBuf dbPath, TStringBuf topic) {
    if (topic.starts_with(dbPath)) {
        return TString(topic);
    }
    TString full;
    full.reserve(dbPath.size() + 1 + topic.size());
    full.append(dbPath);
    if (!full.EndsWith('/')) {
        full.push_back('/');
    }
    topic.SkipPrefix("/");
    full.append(topic);
    return full;
}

void TWriteSessionImpl::ConnectToPreferredPartitionLocation(const TDuration& delay)
{
    Y_ABORT_UNLESS(Lock.IsLocked());
    Y_ABORT_UNLESS(Settings.DirectWriteToPartition_ && (Settings.PartitionId_.Defined() || DirectWriteToPartitionId.Defined()));

    if (AtomicGet(Aborting)) {
        return;
    }

    auto partition_id = Settings.PartitionId_.Defined() ? *Settings.PartitionId_ : *DirectWriteToPartitionId;

    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Get partition location async, partition " << partition_id << ", delay " << delay );

    NYdbGrpc::IQueueClientContextPtr prevDescribePartitionContext;
    NYdbGrpc::IQueueClientContextPtr describePartitionContext = Client->CreateContext();

    if (!describePartitionContext) {
        AbortImpl();
        return;
    }

    ++ConnectionGeneration;

    prevDescribePartitionContext = std::exchange(DescribePartitionContext, describePartitionContext);
    Y_ASSERT(DescribePartitionContext);
    Cancel(prevDescribePartitionContext);

    Ydb::Topic::DescribePartitionRequest request;
    // Currently, the whole topic path needs to be sent in the DescribePartitionRequest.
    request.set_path(FullTopicPath(DbDriverState->Database, Settings.Path_));
    request.set_partition_id(partition_id);
    request.set_include_location(true);

    TRACE_LAZY(DbDriverState->Log, "DescribePartitionRequest",
        TRACE_KV("path", request.path()),
        TRACE_KV("partition_id", request.partition_id()));

    auto extractor = [cbContext = SelfContext, context = describePartitionContext](Ydb::Topic::DescribePartitionResponse* response, TPlainStatus status) mutable {
        Ydb::Topic::DescribePartitionResult result;
        if (response)
            response->operation().result().UnpackTo(&result);

        TStatus st = status.Status == EStatus::SUCCESS ? MakeErrorFromProto(response->operation()) : std::move(status);

        if (auto self = cbContext->LockShared()) {
            self->OnDescribePartition(st, result, context);
        }
    };

    auto callback = [req = std::move(request), extr = std::move(extractor),
                     connections = std::shared_ptr<TGRpcConnectionsImpl>(Connections), dbState = DbDriverState,
                     context = describePartitionContext, prefix = TString(LogPrefix()),
                     partId = partition_id]() mutable {
        LOG_LAZY(dbState->Log, TLOG_DEBUG, prefix + " Getting partition location, partition " + ToString(partId));
        connections->Run<Ydb::Topic::V1::TopicService, Ydb::Topic::DescribePartitionRequest, Ydb::Topic::DescribePartitionResponse>(
            std::move(req),
            std::move(extr),
            &Ydb::Topic::V1::TopicService::Stub::AsyncDescribePartition,
            dbState,
            {},
            context);
    };

    Connections->ScheduleOneTimeTask(std::move(callback), delay);
}

void TWriteSessionImpl::OnDescribePartition(const TStatus& status, const Ydb::Topic::DescribePartitionResult& proto, const NYdbGrpc::IQueueClientContextPtr& describePartitionContext)
{
    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Got PartitionLocation response. Status " << status.GetStatus() << ", proto:\n" << proto.DebugString());
    TString endpoint, name;
    THandleResult handleResult;

    with_lock (Lock) {
        if (DescribePartitionContext == describePartitionContext) {
            DescribePartitionContext = nullptr;
        } else {
            return;
        }
    }

    if (!status.IsSuccess()) {
        with_lock (Lock) {
            if (status.GetStatus() == EStatus::CLIENT_CALL_UNIMPLEMENTED) {
                Settings.DirectWriteToPartition_ = false;
                handleResult = OnErrorImpl({
                    EStatus::UNAVAILABLE,
                    MakeIssueWithSubIssues("The server does not support direct write, fallback to in-direct write", status.GetIssues())
                });
            } else {
                handleResult = OnErrorImpl({status.GetStatus(), MakeIssueWithSubIssues("Failed to get partition location", status.GetIssues())});
            }
        }
        ProcessHandleResult(handleResult);
        return;
    }

    const Ydb::Topic::DescribeTopicResult_PartitionInfo& partition = proto.partition();

    TRACE_LAZY(DbDriverState->Log, "DescribePartitionResponse",
        TRACE_KV("partition_id", partition.partition_id()),
        TRACE_KV("active", partition.active()),
        TRACE_KV("pl_node_id", partition.partition_location().node_id()),
        TRACE_KV("pl_generation", partition.partition_location().generation()));

    if (partition.partition_id() != Settings.PartitionId_ && Settings.PartitionId_.Defined() ||
        !partition.has_partition_location() || partition.partition_location().node_id() == 0 || partition.partition_location().generation() == 0) {
        with_lock (Lock) {
            handleResult = OnErrorImpl({EStatus::INTERNAL_ERROR, "Wrong partition location"});
        }
        ProcessHandleResult(handleResult);
        return;
    }

    TMaybe<TEndpointKey> preferredEndpoint;
    with_lock (Lock) {
        preferredEndpoint = GetPreferredEndpointImpl(partition.partition_id(), partition.partition_location().node_id());
    }

    if (!preferredEndpoint.Defined()) {
        with_lock (Lock) {
            handleResult = OnErrorImpl({EStatus::UNAVAILABLE, "Partition preferred endpoint is not found"});
        }
        ProcessHandleResult(handleResult);
        return;
    }

    with_lock (Lock) {
        PreferredPartitionLocation = {*preferredEndpoint, partition.partition_location().generation()};
    }
    TRACE_LAZY(DbDriverState->Log, "PreferredPartitionLocation",
        TRACE_KV("Endpoint", PreferredPartitionLocation.Endpoint.Endpoint),
        TRACE_KV("NodeId", PreferredPartitionLocation.Endpoint.NodeId),
        TRACE_KV("Generation", PreferredPartitionLocation.Generation));

    Connect(TDuration::Zero());
}

TMaybe<TEndpointKey> TWriteSessionImpl::GetPreferredEndpointImpl(ui32 partitionId, ui64 partitionNodeId) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    TEndpointKey preferredEndpoint{"", partitionNodeId};

    bool nodeIsKnown = (bool)DbDriverState->EndpointPool.GetEndpoint(preferredEndpoint, true);
    if (nodeIsKnown)
    {
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "GetPreferredEndpoint: partitionId " << partitionId << ", partitionNodeId " << partitionNodeId << " exists in the endpoint pool.");
        return preferredEndpoint;
    }
    else
    {
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "GetPreferredEndpoint: partitionId " << partitionId << ", nodeId " << partitionNodeId << " does not exist in the endpoint pool.");
        DbDriverState->EndpointPool.UpdateAsync();
        return {};
    }
}

TString GenerateProducerId() {
    return CreateGuidAsString();
}

void TWriteSessionImpl::InitWriter() { // No Lock, very initial start - no race yet as well.
    if (!Settings.DeduplicationEnabled_.Defined()) {
        // Deduplication settings not provided - will enable deduplication if ProducerId or MessageGroupId is provided.
        Settings.DeduplicationEnabled_ = !Settings.ProducerId_.empty() || !Settings.MessageGroupId_.empty();
    } else if (Settings.DeduplicationEnabled_.GetRef()) {
        // Deduplication explicitly enabled.

        // If both are provided, will validate they are equal in the check below.
        if (Settings.ProducerId_.empty()) {
            if (Settings.MessageGroupId_.empty()) {
                // Both ProducerId and MessageGroupId are empty, will generate random string and use it
                Settings.MessageGroupId(GenerateProducerId());
            }
            // MessageGroupId is non-empty (either provided by user of generated above) and ProducerId is empty, copy value there.
            Settings.ProducerId(Settings.MessageGroupId_);
        } else if (Settings.MessageGroupId_.empty()) {
            // MessageGroupId is empty, copy ProducerId value.
            Settings.MessageGroupId(Settings.ProducerId_);
        }
    } else {
        // Deduplication explicitly disabled, ProducerId & MessageGroupId must be empty.
        if (!Settings.ProducerId_.empty() || !Settings.MessageGroupId_.empty()) {
            LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix()
                     << "ProducerId or MessageGroupId is not empty when deduplication is switched off");
            ThrowFatalError("Explicitly disabled deduplication conflicts with non-empty ProducerId or MessageGroupId");
        }
    }
    if (!Settings.ProducerId_.empty() && !Settings.MessageGroupId_.empty() && Settings.ProducerId_ != Settings.MessageGroupId_) {
            LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix()
                     << "ProducerId and MessageGroupId mismatch");
            ThrowFatalError("ProducerId != MessageGroupId scenario is currently not supported");
    }
    CompressionExecutor = Settings.CompressionExecutor_;
    IExecutor::TPtr executor;
    executor = CreateSyncExecutor();
    executor->Start();
    Executor = std::move(executor);

    Settings.CompressionExecutor_->Start();
    Settings.EventHandlers_.HandlersExecutor_->Start();

}
// Client method
NThreading::TFuture<ui64> TWriteSessionImpl::GetInitSeqNo() {
    if (!Settings.DeduplicationEnabled_.GetOrElse(true)) {
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "GetInitSeqNo called with deduplication disabled");
        ThrowFatalError("Cannot call GetInitSeqNo when deduplication is disabled");
    }
    if (Settings.ValidateSeqNo_) {
        if (AutoSeqNoMode.Defined() && *AutoSeqNoMode) {
            LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Cannot call GetInitSeqNo in Auto SeqNo mode");
            ThrowFatalError("Cannot call GetInitSeqNo in Auto SeqNo mode");
        }
        else
            AutoSeqNoMode = false;
    }
    return InitSeqNoPromise.GetFuture();
}

TString DebugString(const TWriteSessionEvent::TEvent& event) {
    return std::visit([](const auto& ev) { return ev.DebugString(); }, event);
}

// Client method
TMaybe<TWriteSessionEvent::TEvent> TWriteSessionImpl::GetEvent(bool block) {
    return EventsQueue->GetEvent(block);
}

// Client method
TVector<TWriteSessionEvent::TEvent> TWriteSessionImpl::GetEvents(bool block, TMaybe<size_t> maxEventsCount) {
    return EventsQueue->GetEvents(block, maxEventsCount);
}

ui64 TWriteSessionImpl::GetIdImpl(ui64 seqNo) {
    Y_ABORT_UNLESS(AutoSeqNoMode.Defined());
    Y_ABORT_UNLESS(!*AutoSeqNoMode || InitSeqNo.Defined() && seqNo > *InitSeqNo);
    return *AutoSeqNoMode ? seqNo - *InitSeqNo : seqNo;
}

ui64 TWriteSessionImpl::GetSeqNoImpl(ui64 id) {
    Y_ABORT_UNLESS(AutoSeqNoMode.Defined());
    Y_ABORT_UNLESS(InitSeqNo.Defined());
    return *AutoSeqNoMode ? id + *InitSeqNo : id;

}

ui64 TWriteSessionImpl::GetNextIdImpl(const TMaybe<ui64>& seqNo) {

    Y_ABORT_UNLESS(Lock.IsLocked());

    ui64 id = ++NextId;
    if (!AutoSeqNoMode.Defined()) {
        AutoSeqNoMode = !seqNo.Defined();
    }
    if (seqNo.Defined()) {
        if (!Settings.DeduplicationEnabled_.GetOrElse(true)) {
            LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "SeqNo is provided on write when deduplication is disabled");
            ThrowFatalError("Cannot provide SeqNo on Write() when deduplication is disabled");
        }
        if (*AutoSeqNoMode) {
            LOG_LAZY(DbDriverState->Log,
                TLOG_ERR,
                LogPrefix() << "Cannot call write() with defined SeqNo on WriteSession running in auto-seqNo mode"
            );
            ThrowFatalError(
                "Cannot call write() with defined SeqNo on WriteSession running in auto-seqNo mode"
            );
        } else {
            id = *seqNo;
        }
    } else if (!(*AutoSeqNoMode)) {
        LOG_LAZY(DbDriverState->Log,
            TLOG_ERR,
            LogPrefix() << "Cannot call write() without defined SeqNo on WriteSession running in manual-seqNo mode"
        );
        ThrowFatalError(
            "Cannot call write() without defined SeqNo on WriteSession running in manual-seqNo mode"
        );
    }
    return id;
}

inline void TWriteSessionImpl::CheckHandleResultImpl(THandleResult& result) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    result.DoSetSeqNo = result.DoStop && !InitSeqNoSetDone && (InitSeqNoSetDone = true);
}

void TWriteSessionImpl::ProcessHandleResult(THandleResult& result) {
    if (result.DoRestart) {
        Start(result.StartDelay);
    } else if (result.DoSetSeqNo) {
        InitSeqNoPromise.SetException("session closed");
    }
}

NThreading::TFuture<void> TWriteSessionImpl::WaitEvent() {
    return EventsQueue->WaitEvent();
}

void TWriteSessionImpl::WriteInternal(TContinuationToken&&, TWriteMessage&& message) {
    TInstant createdAtValue = message.CreateTimestamp_.Defined() ? *message.CreateTimestamp_ : TInstant::Now();
    bool readyToAccept = false;
    size_t bufferSize = message.Data.size();
    with_lock(Lock) {
        CurrentBatch.Add(
                GetNextIdImpl(message.SeqNo_), createdAtValue, message.Data, message.Codec, message.OriginalSize,
                message.MessageMeta_,
                message.GetTxPtr()
        );

        FlushWriteIfRequiredImpl();
        readyToAccept = OnMemoryUsageChangedImpl(bufferSize).NowOk;
    }
    if (readyToAccept) {
        EventsQueue->PushEvent(TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
    }
}

// Client method.
void TWriteSessionImpl::Write(TContinuationToken&& token, TWriteMessage&& message) {
    WriteInternal(std::move(token), std::move(message));
}

// Client method.
void TWriteSessionImpl::WriteEncoded(TContinuationToken&& token, TWriteMessage&& message)
{
    WriteInternal(std::move(token), std::move(message));
}

TWriteSessionImpl::THandleResult TWriteSessionImpl::OnErrorImpl(NYdb::TPlainStatus&& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    (*Counters->Errors)++;
    auto result = RestartImpl(status);
    if (result.DoStop) {
        CloseImpl(status.Status, std::move(status.Issues));
    }
    return result;
}

// No lock
void TWriteSessionImpl::Connect(const TDuration& delay) {
    NYdbGrpc::IQueueClientContextPtr prevConnectContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectDelayContext;
    NYdbGrpc::IQueueClientContextPtr connectContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectDelayContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectTimeoutContext = nullptr;
    TRpcRequestSettings reqSettings;
    std::shared_ptr<IWriteSessionConnectionProcessorFactory> connectionFactory;

    // Callbacks
    std::function<void(TPlainStatus&&, typename IProcessor::TPtr&&)> connectCallback;
    std::function<void(bool)> connectTimeoutCallback;

    with_lock(Lock) {
        if (Aborting) {
            return;
        }

        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Start write session. Will connect to nodeId: " << PreferredPartitionLocation.Endpoint.NodeId);

        ++ConnectionGeneration;

        if (!ClientContext) {
            ClientContext = Client->CreateContext();
            if (!ClientContext) {
                AbortImpl();
                // Grpc and WriteSession is closing right now.
                return;
            }
        }

        ServerMessage = std::make_shared<TServerMessage>();

        connectionFactory = Client->CreateWriteSessionConnectionProcessorFactory();
        ConnectionFactory = connectionFactory;

        connectContext = ClientContext->CreateContext();
        if (delay)
            connectDelayContext = ClientContext->CreateContext();
        connectTimeoutContext = ClientContext->CreateContext();

        // Previous operations contexts.

        // Set new context
        prevConnectContext = std::exchange(ConnectContext, connectContext);
        prevConnectTimeoutContext = std::exchange(ConnectTimeoutContext, connectTimeoutContext);
        prevConnectDelayContext = std::exchange(ConnectDelayContext, connectDelayContext);
        Y_ASSERT(ConnectContext);
        Y_ASSERT(ConnectTimeoutContext);

        // Cancel previous operations.
        Cancel(prevConnectContext);
        if (prevConnectDelayContext) {
            Cancel(prevConnectDelayContext);
        }
        Cancel(prevConnectTimeoutContext);

        if (Processor) {
            Processor->Cancel();
        }

        reqSettings = TRpcRequestSettings::Make(Settings, PreferredPartitionLocation.Endpoint);

        connectCallback = [cbContext = SelfContext,
                           connectContext = connectContext](TPlainStatus&& st, typename IProcessor::TPtr&& processor) {
            if (auto self = cbContext->LockShared()) {
                self->OnConnect(std::move(st), std::move(processor), connectContext);
            }
        };

        connectTimeoutCallback = [cbContext = SelfContext, connectTimeoutContext = connectTimeoutContext](bool ok) {
            if (ok) {
                if (auto self = cbContext->LockShared()) {
                    self->OnConnectTimeout(connectTimeoutContext);
                }
            }
        };
    }

    connectionFactory->CreateProcessor(
            std::move(connectCallback),
            reqSettings,
            std::move(connectContext),
            TDuration::Seconds(30) /* connect timeout */, // TODO: make connect timeout setting.
            std::move(connectTimeoutContext),
            std::move(connectTimeoutCallback),
            delay,
            std::move(connectDelayContext)
    );
}

// RPC callback.
void TWriteSessionImpl::OnConnectTimeout(const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext) {
    LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Write session: connect timeout");
    THandleResult handleResult;
    with_lock (Lock) {
        if (ConnectTimeoutContext == connectTimeoutContext) {
            Cancel(ConnectContext);
            ConnectContext = nullptr;
            ConnectTimeoutContext = nullptr;
            ConnectDelayContext = nullptr;
        } else {
            return;
        }
        TStringBuilder description;
        description << "Failed to establish connection to server. Attempts done: " << ConnectionAttemptsDone;
        handleResult = RestartImpl(TPlainStatus(EStatus::TIMEOUT, description));
        if (handleResult.DoStop) {
            CloseImpl(
                    EStatus::TIMEOUT,
                    description
            );
        }
    }
    ProcessHandleResult(handleResult);
}

// RPC callback.
void TWriteSessionImpl::OnConnect(
        TPlainStatus&& st, typename IProcessor::TPtr&& processor, const NYdbGrpc::IQueueClientContextPtr& connectContext
) {
    THandleResult handleResult;
    with_lock (Lock) {
        if (ConnectContext == connectContext) {
            Cancel(ConnectTimeoutContext);
            ConnectContext = nullptr;
            ConnectTimeoutContext = nullptr;
            ConnectDelayContext = nullptr;

            if (st.Ok()) {
                Processor = std::move(processor);
                InitImpl();
                // Still should call ReadFromProcessor();
            }
        } else {
            return;
        }
        if (!st.Ok()) {
            handleResult = RestartImpl(st);
            if (handleResult.DoStop) {
                CloseImpl(
                        st.Status,
                        MakeIssueWithSubIssues(
                                TStringBuilder() << "Failed to establish connection to server \"" << st.Endpoint
                                                 << "\". Attempts done: " << ConnectionAttemptsDone,
                                st.Issues
                        )
                );
            }
        }
    }
    if (st.Ok())
        ReadFromProcessor(); // Out of Init
    ProcessHandleResult(handleResult);
}

// Produce init request for session.
void TWriteSessionImpl::InitImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    TClientMessage req;
    auto* init = req.mutable_init_request();
    init->set_path(Settings.Path_);
    init->set_producer_id(Settings.ProducerId_);

    if (Settings.DirectWriteToPartition_ && (Settings.PartitionId_.Defined() || DirectWriteToPartitionId.Defined())) {
        auto partition_id = Settings.PartitionId_.Defined() ? *Settings.PartitionId_ : *DirectWriteToPartitionId;
        auto* p = init->mutable_partition_with_generation();
        p->set_partition_id(partition_id);
        p->set_generation(PreferredPartitionLocation.Generation);
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: direct write to partition: " << partition_id << ", generation " << PreferredPartitionLocation.Generation);
    } else if (Settings.PartitionId_.Defined()) {
        init->set_partition_id(*Settings.PartitionId_);
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: write to partition: " << *Settings.PartitionId_);
    } else {
        init->set_message_group_id(Settings.MessageGroupId_);
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: write to message_group: " << Settings.MessageGroupId_);
    }

    for (const auto& attr : Settings.Meta_.Fields) {
        (*init->mutable_write_session_meta())[attr.first] = attr.second;
    }
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: send init request: "<< req.ShortDebugString());

    TRACE_LAZY(DbDriverState->Log, "InitRequest",
        TRACE_KV_IF(init->has_partition_id(), "partition_id", init->partition_id()),
        TRACE_IF(init->has_partition_with_generation(),
            TRACE_KV("pwg_partition_id", init->partition_with_generation().partition_id()),
            TRACE_KV("pwg_generation", init->partition_with_generation().generation())
        ));

    WriteToProcessorImpl(std::move(req));
}

// Called under lock. Invokes Processor->Write, which is assumed to be deadlock-safe
void TWriteSessionImpl::WriteToProcessorImpl(TWriteSessionImpl::TClientMessage&& req) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Y_ASSERT(Processor);
    if (Aborting) {
        return;
    }
    auto callback = [cbContext = SelfContext,
                     connectionGeneration = ConnectionGeneration](NYdbGrpc::TGrpcStatus&& grpcStatus) {
        if (auto self = cbContext->LockShared()) {
            self->OnWriteDone(std::move(grpcStatus), connectionGeneration);
        }
    };

    Processor->Write(std::move(req), callback);
}

void TWriteSessionImpl::ReadFromProcessor() {
    Y_ASSERT(Processor);
    IProcessor::TPtr prc;
    ui64 generation;
    std::function<void(NYdbGrpc::TGrpcStatus&&)> callback;
    with_lock(Lock) {
        if (Aborting) {
            return;
        }
        prc = Processor;
        generation = ConnectionGeneration;
        callback = [cbContext = SelfContext,
                    connectionGeneration = generation,
                    processor = prc,
                    serverMessage = ServerMessage]
                    (NYdbGrpc::TGrpcStatus&& grpcStatus) {
            if (auto self = cbContext->LockShared()) {
                self->OnReadDone(std::move(grpcStatus), connectionGeneration);
            }
        };
    }
    prc->Read(ServerMessage.get(), std::move(callback));
}

void TWriteSessionImpl::OnWriteDone(NYdbGrpc::TGrpcStatus&& status, size_t connectionGeneration) {
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: OnWriteDone " << status.ToDebugString());

    THandleResult handleResult;
    with_lock (Lock) {
        if (connectionGeneration != ConnectionGeneration) {
            return; // Message from previous connection. Ignore.
        }
        if (Aborting) {
            return;
        }
        if(!status.Ok()) {
            handleResult = OnErrorImpl(status);
        }
    }
    ProcessHandleResult(handleResult);
}

void TWriteSessionImpl::OnReadDone(NYdbGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration) {
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: OnReadDone " << grpcStatus.ToDebugString());

    TPlainStatus errorStatus;
    TProcessSrvMessageResult processResult;
    bool needSetValue = false;
    if (!grpcStatus.Ok()) {
        errorStatus = TPlainStatus(std::move(grpcStatus));
    }
    bool doRead = false;
    with_lock (Lock) {
        UpdateTimedCountersImpl();
        if (connectionGeneration != ConnectionGeneration) {
            return; // Message from previous connection. Ignore.
        }
        if (errorStatus.Ok()) {
            if (IsErrorMessage(*ServerMessage)) {
                errorStatus = MakeErrorFromProto(*ServerMessage);
            } else {
                processResult = ProcessServerMessageImpl();
                needSetValue = !InitSeqNoSetDone && processResult.InitSeqNo.Defined() && (InitSeqNoSetDone = true);
                if (errorStatus.Ok() && processResult.Ok) {
                    doRead = true;
                }
            }
        }
    }

    for (auto& event : processResult.Events) {
        EventsQueue->PushEvent(std::move(event));
    }

    if (doRead)
        ReadFromProcessor();

    with_lock(Lock) {
        if (!errorStatus.Ok()) {
            if (processResult.Ok) { // Otherwise, OnError was already called
                processResult.HandleResult = RestartImpl(errorStatus);
            }
        }
        if (processResult.HandleResult.DoStop) {
            CloseImpl(std::move(errorStatus));
        }
    }
    if (needSetValue) {
        InitSeqNoPromise.SetValue(*processResult.InitSeqNo);
        processResult.HandleResult.DoSetSeqNo = false; // Redundant. Just in case.
    }
    ProcessHandleResult(processResult.HandleResult);
}

TStringBuilder TWriteSessionImpl::LogPrefix() const {
    TStringBuilder ret;
    ret << " SessionId [" << SessionId << "] ";

    if (Settings.PartitionId_.Defined() || DirectWriteToPartitionId.Defined()) {
        auto partition_id = Settings.PartitionId_.Defined() ? *Settings.PartitionId_ : *DirectWriteToPartitionId;
        ret << " PartitionId [" << partition_id << "] ";
        if (Settings.DirectWriteToPartition_) {
            ret << " Generation [" << PreferredPartitionLocation.Generation << "] ";
        }
    } else {
        ret << " MessageGroupId [" << Settings.MessageGroupId_ << "] ";
    }

    return ret;
}

template<>
void TPrintable<TWriteSessionEvent::TAcksEvent>::DebugString(TStringBuilder& res, bool) const {
    const auto* self = static_cast<const TWriteSessionEvent::TAcksEvent*>(this);
    res << "AcksEvent:";
    for (auto& ack : self->Acks) {
        res << " { seqNo : " << ack.SeqNo << ", State : " << ack.State;
        if (ack.Details) {
            res << ", offset : " << ack.Details->Offset << ", partitionId : " << ack.Details->PartitionId;
        }
        res << " }";
    }
    if (!self->Acks.empty() && self->Acks.back().Stat) {
        auto& stat = self->Acks.back().Stat;
        res << " write stat: Write time " << stat->WriteTime
            << " minimal time in partition queue " << stat->MinTimeInPartitionQueue
            << " maximal time in partition queue " << stat->MaxTimeInPartitionQueue
            << " partition quoted time " << stat->PartitionQuotedTime
            << " topic quoted time " << stat->TopicQuotedTime;
    }
}

template<>
void TPrintable<TWriteSessionEvent::TReadyToAcceptEvent>::DebugString(TStringBuilder& res, bool) const {
    res << "ReadyToAcceptEvent";
}

TWriteSessionImpl::TProcessSrvMessageResult TWriteSessionImpl::ProcessServerMessageImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    TProcessSrvMessageResult result;
    switch (ServerMessage->GetServerMessageCase()) {
        case TServerMessage::SERVER_MESSAGE_NOT_SET: {
            SessionEstablished = false;
            result.HandleResult = OnErrorImpl({
                            static_cast<NYdb::EStatus>(ServerMessage->status()),
                            {NYql::TIssue{ServerMessage->DebugString()}}
                    });
            result.Ok = false;
            break;
        }
        case TServerMessage::kInitResponse: {
            const auto& initResponse = ServerMessage->init_response();
            LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session established. Init response: " << initResponse.ShortDebugString());
            TRACE_LAZY(DbDriverState->Log, "InitResponse",
                TRACE_KV("partition_id", initResponse.partition_id()),
                TRACE_KV("session_id", initResponse.session_id()));
            SessionId = initResponse.session_id();

            auto prevDirectWriteToPartitionId = DirectWriteToPartitionId;
            if (Settings.DirectWriteToPartition_ && !Settings.PartitionId_.Defined()) {
                LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: set DirectWriteToPartitionId " << initResponse.partition_id());
                DirectWriteToPartitionId = initResponse.partition_id();
            }
            PartitionId = initResponse.partition_id();

            ui64 newLastSeqNo = initResponse.last_seq_no();
            if (!Settings.DeduplicationEnabled_.GetOrElse(true)) {
                newLastSeqNo = 0;
            }
            result.InitSeqNo = newLastSeqNo;
            if (!InitSeqNo.Defined()) {
                InitSeqNo = newLastSeqNo;
            }

            OnErrorResolved();

            if (Settings.DirectWriteToPartition_ && DirectWriteToPartitionId.Defined() && !prevDirectWriteToPartitionId.Defined()) {
                result.HandleResult.DoRestart = true;
                result.HandleResult.StartDelay = TDuration::Zero();
                break;
            }

            SessionEstablished = true;
            LastCountersUpdateTs = TInstant::Now();
            SessionStartedTs = TInstant::Now();

            if (!FirstTokenSent) {
                result.Events.emplace_back(TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
                FirstTokenSent = true;
            }
            // Kickstart send after session reestablishment
            SendImpl();
            break;
        }
        case TServerMessage::kWriteResponse: {
            TWriteSessionEvent::TAcksEvent acksEvent;
            const auto& batchWriteResponse = ServerMessage->write_response();
            LOG_LAZY(DbDriverState->Log,
                TLOG_DEBUG,
                LogPrefix() << "Write session got write response: " << batchWriteResponse.ShortDebugString()
            );
            TWriteStat::TPtr writeStat = new TWriteStat{};
            const auto& stat = batchWriteResponse.write_statistics();

            auto durationConv = [](const ::google::protobuf::Duration& dur) {
                return TDuration::MilliSeconds(::google::protobuf::util::TimeUtil::DurationToMilliseconds(dur));
            };

            writeStat->WriteTime = durationConv(stat.persisting_time());
            writeStat->MinTimeInPartitionQueue = durationConv(stat.min_queue_wait_time());
            writeStat->MaxTimeInPartitionQueue = durationConv(stat.max_queue_wait_time());
            writeStat->PartitionQuotedTime = durationConv(stat.partition_quota_wait_time());
            writeStat->TopicQuotedTime = durationConv(stat.topic_quota_wait_time());

            for (size_t messageIndex = 0, endIndex = batchWriteResponse.acks_size(); messageIndex != endIndex; ++messageIndex) {
                // TODO: Fill writer statistics
                auto ack = batchWriteResponse.acks(messageIndex);
                ui64 sequenceNumber = ack.seq_no();

                Y_ABORT_UNLESS(ack.has_written() || ack.has_skipped());
                auto msgWriteStatus = ack.has_written()
                                ? TWriteSessionEvent::TWriteAck::EES_WRITTEN
                                : (ack.skipped().reason() == Ydb::Topic::StreamWriteMessage_WriteResponse_WriteAck_Skipped_Reason::StreamWriteMessage_WriteResponse_WriteAck_Skipped_Reason_REASON_ALREADY_WRITTEN
                                    ? TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN
                                    : TWriteSessionEvent::TWriteAck::EES_DISCARDED);

                ui64 offset = ack.has_written() ? ack.written().offset() : 0;

                acksEvent.Acks.push_back(TWriteSessionEvent::TWriteAck{
                    GetIdImpl(sequenceNumber),
                    msgWriteStatus,
                    TWriteSessionEvent::TWriteAck::TWrittenMessageDetails {
                        offset,
                        PartitionId,
                    },
                    writeStat,
                });

                if (CleanupOnAcknowledged(GetIdImpl(sequenceNumber))) {
                    result.Events.emplace_back(TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
                }
            }
            //EventsQueue->PushEvent(std::move(acksEvent));
            result.Events.emplace_back(std::move(acksEvent));
            break;
        }
        case TServerMessage::kUpdateTokenResponse: {
            UpdateTokenInProgress = false;
            LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: token updated successfully");
            UpdateTokenIfNeededImpl();
            break;
        }
    }
    return result;
}

bool TWriteSessionImpl::CleanupOnAcknowledged(ui64 id) {
    bool result = false;
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: acknoledged message " << id);
    UpdateTimedCountersImpl();
    const auto& sentFront = SentOriginalMessages.front();
    ui64 size = 0;
    ui64 compressedSize = 0;
    if(!SentPackedMessage.empty() && SentPackedMessage.front().Offset == id) {
        auto memoryUsage = OnMemoryUsageChangedImpl(-SentPackedMessage.front().Data.size());
        result = memoryUsage.NowOk && !memoryUsage.WasOk;
        const auto& front = SentPackedMessage.front();
        if (front.Compressed) {
            compressedSize = front.Data.size();
        } else {
            size = front.Data.size();
        }

        (*Counters->MessagesWritten) += front.MessageCount;
        (*Counters->MessagesInflight) -= front.MessageCount;
        (*Counters->BytesWritten) += front.OriginalSize;

        SentPackedMessage.pop();
    } else {
        size = sentFront.Size;
        (*Counters->BytesWritten) += sentFront.Size;
        (*Counters->MessagesWritten)++;
        (*Counters->MessagesInflight)--;
    }

    (*Counters->BytesInflightCompressed) -= compressedSize;
    (*Counters->BytesWrittenCompressed) += compressedSize;
    (*Counters->BytesInflightUncompressed) -= size;

    Y_ABORT_UNLESS(Counters->BytesInflightCompressed->Val() >= 0);
    Y_ABORT_UNLESS(Counters->BytesInflightUncompressed->Val() >= 0);

    Y_ABORT_UNLESS(sentFront.Id == id);

    (*Counters->BytesInflightTotal) = MemoryUsage;
    SentOriginalMessages.pop();
    return result;
}

TMemoryUsageChange TWriteSessionImpl::OnMemoryUsageChangedImpl(i64 diff) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    bool wasOk = MemoryUsage <= Settings.MaxMemoryUsage_;
    //if (diff < 0) {
    //    Y_ABORT_UNLESS(MemoryUsage >= static_cast<size_t>(std::abs(diff)));
    //}
    MemoryUsage += diff;
    bool nowOk = MemoryUsage <= Settings.MaxMemoryUsage_;
    if (wasOk != nowOk) {
        if (wasOk) {
            LOG_LAZY(DbDriverState->Log,
                TLOG_DEBUG,
                LogPrefix() << "Estimated memory usage " << MemoryUsage
                    << "[B] reached maximum (" << Settings.MaxMemoryUsage_ << "[B])"
            );
        }
        else {
            LOG_LAZY(DbDriverState->Log,
                TLOG_DEBUG,
                LogPrefix() << "Estimated memory usage got back to normal " << MemoryUsage << "[B]"
            );
        }
    }
    return {wasOk, nowOk};
}

TBuffer CompressBuffer(std::shared_ptr<TTopicClient::TImpl> client, TVector<TStringBuf>& data, ECodec codec, i32 level) {
    TBuffer result;
    Y_UNUSED(client);
    THolder<IOutputStream> coder = TCodecMap::GetTheCodecMap().GetOrThrow((ui32)codec)->CreateCoder(result, level);
    for (auto& buffer : data) {
        coder->Write(buffer.data(), buffer.size());
    }
    coder->Finish();
    return result;
}

// May call OnCompressed with sync executor. No external lock.
void TWriteSessionImpl::CompressImpl(TBlock&& block_) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (Aborting) {
        return;
    }
    Y_ABORT_UNLESS(block_.Valid);

    std::shared_ptr<TBlock> blockPtr(std::make_shared<TBlock>());
    blockPtr->Move(block_);
    auto lambda = [cbContext = SelfContext,
                   codec = Settings.Codec_,
                   level = Settings.CompressionLevel_,
                   isSyncCompression = !CompressionExecutor->IsAsync(),
                   blockPtr,
                   client = Client]() mutable {
        Y_ABORT_UNLESS(!blockPtr->Compressed);

        auto compressedData = CompressBuffer(
            std::move(client), blockPtr->OriginalDataRefs, codec, level
        );
        Y_ABORT_UNLESS(!compressedData.Empty());
        blockPtr->Data = std::move(compressedData);
        blockPtr->Compressed = true;
        blockPtr->CodecID = static_cast<ui32>(codec);
        if (auto self = cbContext->LockShared()) {
            self->OnCompressed(std::move(*blockPtr), isSyncCompression);
        }
    };

    CompressionExecutor->Post(lambda);
}

void TWriteSessionImpl::OnCompressed(TBlock&& block, bool isSyncCompression) {
    TMemoryUsageChange memoryUsage;
    if (!isSyncCompression) {
        with_lock(Lock) {
            memoryUsage = OnCompressedImpl(std::move(block));
        }
    } else {
        memoryUsage = OnCompressedImpl(std::move(block));
    }
    if (memoryUsage.NowOk && !memoryUsage.WasOk) {
        EventsQueue->PushEvent(TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
    }
}

TMemoryUsageChange TWriteSessionImpl::OnCompressedImpl(TBlock&& block) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    UpdateTimedCountersImpl();
    Y_ABORT_UNLESS(block.Valid);
    auto memoryUsage = OnMemoryUsageChangedImpl(static_cast<i64>(block.Data.size()) - block.OriginalMemoryUsage);
    (*Counters->BytesInflightUncompressed) -= block.OriginalSize;
    (*Counters->BytesInflightCompressed) += block.Data.size();

    PackedMessagesToSend.emplace(std::move(block));
    SendImpl();
    return memoryUsage;
}

// Set SessionEstablished = false and bring back "sent" messages to proper queues
void TWriteSessionImpl::ResetForRetryImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    SessionEstablished = false;
    const size_t totalPackedMessages = PackedMessagesToSend.size() + SentPackedMessage.size();
    const size_t totalOriginalMessages = OriginalMessagesToSend.size() + SentOriginalMessages.size();
    while (!SentPackedMessage.empty()) {
        PackedMessagesToSend.emplace(std::move(SentPackedMessage.front()));
        SentPackedMessage.pop();
    }
    ui64 minId = PackedMessagesToSend.empty() ? NextId + 1 : PackedMessagesToSend.top().Offset;
    std::queue<TOriginalMessage> freshOriginalMessagesToSend;
    OriginalMessagesToSend.swap(freshOriginalMessagesToSend);
    while (!SentOriginalMessages.empty()) {
        OriginalMessagesToSend.emplace(std::move(SentOriginalMessages.front()));
        SentOriginalMessages.pop();
    }
    while (!freshOriginalMessagesToSend.empty()) {
        OriginalMessagesToSend.emplace(std::move(freshOriginalMessagesToSend.front()));
        freshOriginalMessagesToSend.pop();
    }
    if (!OriginalMessagesToSend.empty() && OriginalMessagesToSend.front().Id < minId)
        minId = OriginalMessagesToSend.front().Id;
    MinUnsentId = minId;
    Y_ABORT_UNLESS(PackedMessagesToSend.size() == totalPackedMessages);
    Y_ABORT_UNLESS(OriginalMessagesToSend.size() == totalOriginalMessages);
}

// Called from client Write() methods
void TWriteSessionImpl::FlushWriteIfRequiredImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (!CurrentBatch.Empty() && !CurrentBatch.FlushRequested) {
        MessagesAcquired += static_cast<ui64>(CurrentBatch.Acquire());
        if (TInstant::Now() - CurrentBatch.StartedAt >= Settings.BatchFlushInterval_.GetOrElse(TDuration::Zero())
            || CurrentBatch.CurrentSize >= Settings.BatchFlushSizeBytes_.GetOrElse(0)
            || CurrentBatch.CurrentSize >= MaxBlockSize
            || CurrentBatch.Messages.size() >= MaxBlockMessageCount
            || CurrentBatch.HasCodec()
        ) {
            WriteBatchImpl();
            return;
        }
    }
}

size_t TWriteSessionImpl::WriteBatchImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log,
        TLOG_DEBUG,
        LogPrefix() << "Write " << CurrentBatch.Messages.size() << " messages with Id from "
            << CurrentBatch.Messages.begin()->Id << " to " << CurrentBatch.Messages.back().Id
    );

    Y_ABORT_UNLESS(CurrentBatch.Messages.size() <= MaxBlockMessageCount);

    const bool skipCompression = Settings.Codec_ == ECodec::RAW || CurrentBatch.HasCodec();
    if (!skipCompression && Settings.CompressionExecutor_->IsAsync()) {
        MessagesAcquired += static_cast<ui64>(CurrentBatch.Acquire());
    }

    size_t size = 0;
    for (size_t i = 0; i != CurrentBatch.Messages.size();) {
        TBlock block{};
        for (; block.OriginalSize < MaxBlockSize && i != CurrentBatch.Messages.size(); ++i) {
            auto& currMessage = CurrentBatch.Messages[i];

            // If MaxBlockSize or MaxBlockMessageCount values are ever changed from infinity and 1 correspondingly,
            // create a new block, if the existing one is non-empty AND (adding another message will overflow it OR
            //                                                           its codec is different from the codec of the next message).

            auto id = currMessage.Id;
            auto createTs = currMessage.CreatedAt;

            if (!block.MessageCount) {
                block.Offset = id;
            }

            block.MessageCount += 1;
            const auto& datum = currMessage.DataRef;
            block.OriginalSize += datum.size();
            block.OriginalMemoryUsage = CurrentBatch.Data.size();
            block.OriginalDataRefs.emplace_back(datum);
            if (CurrentBatch.Messages[i].Codec.Defined()) {
                Y_ABORT_UNLESS(CurrentBatch.Messages.size() == 1);
                block.CodecID = static_cast<ui32>(*currMessage.Codec);
                block.OriginalSize = currMessage.OriginalSize;
                block.Compressed = false;
            }
            size += datum.size();
            UpdateTimedCountersImpl();
            (*Counters->BytesInflightUncompressed) += datum.size();
            (*Counters->MessagesInflight)++;
            if (!currMessage.MessageMeta.empty()) {
                OriginalMessagesToSend.emplace(id, createTs, datum.size(),
                                               std::move(currMessage.MessageMeta),
                                               currMessage.Tx);
            } else {
                OriginalMessagesToSend.emplace(id, createTs, datum.size(),
                                               currMessage.Tx);
            }
        }
        block.Data = std::move(CurrentBatch.Data);
        if (skipCompression) {
            PackedMessagesToSend.emplace(std::move(block));
        } else {
            CompressImpl(std::move(block));
        }
    }
    CurrentBatch.Reset();
    if (skipCompression) {
        SendImpl();
    }
    return size;
}

size_t GetMaxGrpcMessageSize() {
    return 120_MB;
}

bool TWriteSessionImpl::IsReadyToSendNextImpl() const {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (!SessionEstablished) {
        return false;
    }
    if (Aborting)
        return false;
    if (PackedMessagesToSend.empty()) {
        return false;
    }
    Y_ABORT_UNLESS(!OriginalMessagesToSend.empty(), "There are packed messages but no original messages");
    Y_ABORT_UNLESS(OriginalMessagesToSend.front().Id <= PackedMessagesToSend.top().Offset, "Lost original message(s)");

    return PackedMessagesToSend.top().Offset == OriginalMessagesToSend.front().Id;
}


void TWriteSessionImpl::UpdateTokenIfNeededImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: try to update token");

    if (!DbDriverState->CredentialsProvider || UpdateTokenInProgress || !SessionEstablished) {
        return;
    }

    auto token = DbDriverState->CredentialsProvider->GetAuthInfo();
    if (token == PrevToken) {
        return;
    }

    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: updating token");

    UpdateTokenInProgress = true;
    PrevToken = token;

    TClientMessage clientMessage;
    clientMessage.mutable_update_token_request()->set_token(token);
    Processor->Write(std::move(clientMessage));
}

bool TWriteSessionImpl::TxIsChanged(const Ydb::Topic::StreamWriteMessage_WriteRequest* writeRequest) const
{
    Y_ABORT_UNLESS(writeRequest);

    if (!writeRequest->messages_size()) {
        return false;
    }

    Y_ABORT_UNLESS(!OriginalMessagesToSend.empty());

    return GetTransactionId(*writeRequest) != GetTransactionId(OriginalMessagesToSend.front().Tx);
}

void TWriteSessionImpl::SendImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    // External cycle splits ready blocks into multiple gRPC messages. Current gRPC message size hard limit is 64MiB.
    while (IsReadyToSendNextImpl()) {
        TClientMessage clientMessage;
        auto* writeRequest = clientMessage.mutable_write_request();
        ui32 prevCodec = 0;

        ui64 currentSize = 0;

        // Send blocks while we can without messages reordering.
        while (IsReadyToSendNextImpl() && currentSize < GetMaxGrpcMessageSize()) {
            const auto& block = PackedMessagesToSend.top();
            Y_ABORT_UNLESS(block.Valid);
            if (writeRequest->messages_size() > 0 && prevCodec != block.CodecID) {
                break;
            }
            if (TxIsChanged(writeRequest)) {
                break;
            }
            prevCodec = block.CodecID;
            writeRequest->set_codec(static_cast<i32>(block.CodecID));
            Y_ABORT_UNLESS(block.MessageCount == 1);
            for (size_t i = 0; i != block.MessageCount; ++i) {
                Y_ABORT_UNLESS(!OriginalMessagesToSend.empty());

                auto& message = OriginalMessagesToSend.front();
                auto* msgData = writeRequest->add_messages();

                if (message.Tx) {
                    writeRequest->mutable_tx()->set_id(message.Tx->GetId());
                    writeRequest->mutable_tx()->set_session(message.Tx->GetSession().GetId());
                }

                msgData->set_seq_no(GetSeqNoImpl(message.Id));
                *msgData->mutable_created_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(message.CreatedAt.MilliSeconds());

                for (auto& [k, v] : message.MessageMeta) {
                    auto* pair = msgData->add_metadata_items();
                    pair->set_key(k);
                    pair->set_value(v);
                }
                SentOriginalMessages.emplace(std::move(message));
                OriginalMessagesToSend.pop();

                msgData->set_uncompressed_size(block.OriginalSize);
                if (block.Compressed) {
                    msgData->set_data(block.Data.data(), block.Data.size());
                } else {
                    for (auto& buffer: block.OriginalDataRefs) {
                        msgData->set_data(buffer.data(), buffer.size());
                    }
                }
            }

            TBlock moveBlock;
            moveBlock.Move(block);
            SentPackedMessage.emplace(std::move(moveBlock));
            PackedMessagesToSend.pop();

            currentSize += writeRequest->ByteSizeLong();
        }
        UpdateTokenIfNeededImpl();
        LOG_LAZY(DbDriverState->Log,
            TLOG_DEBUG,
            LogPrefix() << "Send " << writeRequest->messages_size() << " message(s) ("
                << OriginalMessagesToSend.size() << " left), first sequence number is "
                << writeRequest->messages(0).seq_no()
        );
        Processor->Write(std::move(clientMessage));
    }
}

// Client method, no Lock
bool TWriteSessionImpl::Close(TDuration closeTimeout) {
    if (AtomicGet(Aborting)) {
        return false;
    }
    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session: close. Timeout " << closeTimeout);
    auto startTime = TInstant::Now();
    auto remaining = closeTimeout;
    bool ready = false;
    bool needSetSeqNoValue = false;
    while (remaining > TDuration::Zero()) {
        with_lock(Lock) {
            if (OriginalMessagesToSend.empty() && SentOriginalMessages.empty()) {
                ready = true;
            }
            if (AtomicGet(Aborting))
                break;
        }
        if (ready) {
            break;
        }
        remaining = closeTimeout - (TInstant::Now() - startTime);
        Sleep(Min(TDuration::MilliSeconds(100), remaining));
    }
    with_lock(Lock) {
        ready = (OriginalMessagesToSend.empty() && SentOriginalMessages.empty()) && !AtomicGet(Aborting);
    }
    with_lock(Lock) {
        CloseImpl(EStatus::SUCCESS, NYql::TIssues{});
        needSetSeqNoValue = !InitSeqNoSetDone && (InitSeqNoSetDone = true);
    }
    if (needSetSeqNoValue) {
        InitSeqNoPromise.SetException("session closed");
    }
    if (ready) {
        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session: gracefully shut down, all writes complete");
    } else {
        LOG_LAZY(DbDriverState->Log, TLOG_WARNING, LogPrefix() << "Write session: could not confirm all writes in time or session aborted, perform hard shutdown");
    }
    return ready;
}

void TWriteSessionImpl::HandleWakeUpImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    FlushWriteIfRequiredImpl();
    if (AtomicGet(Aborting)) {
        return;
    }
    auto callback = [cbContext = SelfContext] (bool ok)
    {
        if (!ok) {
            return;
        }
        if (auto self = cbContext->LockShared()) {
            with_lock(self->Lock) {
                self->HandleWakeUpImpl();
            }
        }
    };
    if (TInstant::Now() - LastTokenUpdate > UPDATE_TOKEN_PERIOD) {
        LastTokenUpdate = TInstant::Now();
        UpdateTokenIfNeededImpl();
    }

    const auto flushAfter = CurrentBatch.StartedAt == TInstant::Zero()
        ? WakeupInterval
        : WakeupInterval - Min(Now() - CurrentBatch.StartedAt, WakeupInterval);
    Connections->ScheduleCallback(flushAfter, std::move(callback));
}

void TWriteSessionImpl::UpdateTimedCountersImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    auto now = TInstant::Now();
    auto delta = (now - LastCountersUpdateTs).MilliSeconds();
    double percent = 100.0 / Settings.MaxMemoryUsage_;

    Counters->TotalBytesInflightUsageByTime->Collect(*Counters->BytesInflightTotal * percent, delta);
    Counters->UncompressedBytesInflightUsageByTime->Collect(*Counters->BytesInflightUncompressed * percent, delta);
    Counters->CompressedBytesInflightUsageByTime->Collect(*Counters->BytesInflightCompressed * percent, delta);

    *Counters->CurrentSessionLifetimeMs = (TInstant::Now() - SessionStartedTs).MilliSeconds();
    LastCountersUpdateTs = now;
    if (LastCountersLogTs == TInstant::Zero() || TInstant::Now() - LastCountersLogTs > TDuration::Seconds(60)) {
        LastCountersLogTs = TInstant::Now();

#define LOG_COUNTER(counter)                                            \
    << " " Y_STRINGIZE(counter) ": "                                    \
    << Counters->counter->Val()                                        \
        /**/

        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix()
            << "Counters: {"
            LOG_COUNTER(Errors)
            LOG_COUNTER(CurrentSessionLifetimeMs)
            LOG_COUNTER(BytesWritten)
            LOG_COUNTER(MessagesWritten)
            LOG_COUNTER(BytesWrittenCompressed)
            LOG_COUNTER(BytesInflightUncompressed)
            LOG_COUNTER(BytesInflightCompressed)
            LOG_COUNTER(BytesInflightTotal)
            LOG_COUNTER(MessagesInflight)
            << " }"
        );

#undef LOG_COUNTER
    }
}

void TWriteSessionImpl::AbortImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (!AtomicGet(Aborting)) {
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: aborting");
        AtomicSet(Aborting, 1);
        Cancel(DescribePartitionContext);
        Cancel(ConnectContext);
        Cancel(ConnectTimeoutContext);
        Cancel(ConnectDelayContext);
        if (Processor)
            Processor->Cancel();

        Cancel(ClientContext);
        ClientContext.reset(); // removes context from contexts set from underlying gRPC-client.
    }
}

void TWriteSessionImpl::CloseImpl(EStatus statusCode, NYql::TIssues&& issues) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session will now close");
    EventsQueue->Close(TSessionClosedEvent(statusCode, std::move(issues)));
    AbortImpl();
}

void TWriteSessionImpl::CloseImpl(EStatus statusCode, const TString& message) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    NYql::TIssues issues;
    issues.AddIssue(message);
    CloseImpl(statusCode, std::move(issues));
}

void TWriteSessionImpl::CloseImpl(TPlainStatus&& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session will now close");
    EventsQueue->Close(TSessionClosedEvent(std::move(status)));
    AbortImpl();
}

TWriteSessionImpl::~TWriteSessionImpl() {
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: destroy");
    bool needClose = false;
    with_lock(Lock) {
        if (!AtomicGet(Aborting)) {
            CloseImpl(EStatus::SUCCESS, NYql::TIssues{});

            needClose = !InitSeqNoSetDone && (InitSeqNoSetDone = true);
        }
    }
    if (needClose) {
        InitSeqNoPromise.SetException("session closed");
    }
}

}  // namespace NYdb::NTopic
