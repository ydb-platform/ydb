#include "write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/log_lazy.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <library/cpp/string_utils/url/url.h>

#include <google/protobuf/util/time_util.h>

#include <util/generic/store_policy.h>
#include <util/generic/utility.h>
#include <util/stream/buffer.h>
#include <util/generic/guid.h>


namespace NYdb::NTopic {
using ::NMonitoring::TDynamicCounterPtr;
using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;


const TDuration UPDATE_TOKEN_PERIOD = TDuration::Hours(1);

namespace NCompressionDetails {
    THolder<IOutputStream> CreateCoder(ECodec codec, TBuffer& result, int quality);
}

#define HISTOGRAM_SETUP ::NMonitoring::ExplicitHistogram({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})
TWriterCounters::TWriterCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    Errors = counters->GetCounter("errors", true);
    CurrentSessionLifetimeMs = counters->GetCounter("currentSessionLifetimeMs", false);
    BytesWritten = counters->GetCounter("bytesWritten", true);
    MessagesWritten = counters->GetCounter("messagesWritten", true);
    BytesWrittenCompressed = counters->GetCounter("bytesWrittenCompressed", true);
    BytesInflightUncompressed = counters->GetCounter("bytesInflightUncompressed", false);
    BytesInflightCompressed = counters->GetCounter("bytesInflightCompressed", false);
    BytesInflightTotal = counters->GetCounter("bytesInflightTotal", false);
    MessagesInflight = counters->GetCounter("messagesInflight", false);

    TotalBytesInflightUsageByTime = counters->GetHistogram("totalBytesInflightUsageByTime", HISTOGRAM_SETUP);
    UncompressedBytesInflightUsageByTime = counters->GetHistogram("uncompressedBytesInflightUsageByTime", HISTOGRAM_SETUP);
    CompressedBytesInflightUsageByTime = counters->GetHistogram("compressedBytesInflightUsageByTime", HISTOGRAM_SETUP);
}
#undef HISTOGRAM_SETUP

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSessionImpl

TWriteSessionImpl::TWriteSessionImpl(
        const TWriteSessionSettings& settings,
         std::shared_ptr<TTopicClient::TImpl> client,
         std::shared_ptr<TGRpcConnectionsImpl> connections,
         TDbDriverStatePtr dbDriverState,
         std::shared_ptr<NPersQueue::TImplTracker> tracker)
    : Settings(settings)
    , Client(std::move(client))
    , Connections(std::move(connections))
    , DbDriverState(std::move(dbDriverState))
    , PrevToken(DbDriverState->CredentialsProvider ? DbDriverState->CredentialsProvider->GetAuthInfo() : "")
    , Tracker(tracker)
    , EventsQueue(std::make_shared<TWriteSessionEventsQueue>(Settings, Tracker))
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
    ++ConnectionAttemptsDone;
    if (!Started) {
        with_lock(Lock) {
            HandleWakeUpImpl();
        }
        InitWriter();
    }
    Started = true;

    if (Settings.PartitionId_.Defined() && Settings.DirectWriteToPartition_)
    {
        with_lock (Lock) {
            PreferredPartitionLocation = {};

            return ConnectToPreferredPartitionLocation(delay);
        }
    }
    else
    {
        return Connect(delay);
    }
}

TWriteSessionImpl::THandleResult TWriteSessionImpl::RestartImpl(const TPlainStatus& status) {
    Y_VERIFY(Lock.IsLocked());

    THandleResult result;
    if (AtomicGet(Aborting)) {
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session is aborting and will not restart");
        return result;
    }
    LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Got error. " << status.ToDebugString());
    SessionEstablished = false;
    TMaybe<TDuration> nextDelay = TDuration::Zero();
    if (!RetryState) {
        RetryState = Settings.RetryPolicy_->CreateRetryState();
    }
    nextDelay = RetryState->GetNextRetryDelay(status.Status);

    if (nextDelay) {
        result.StartDelay = *nextDelay;
        result.DoRestart = true;
        LOG_LAZY(DbDriverState->Log, TLOG_WARNING, LogPrefix() << "Write session will restart in " << result.StartDelay);
        ResetForRetryImpl();

    } else {
        LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Write session will not restart after a fatal error");
        result.DoStop = true;
        CheckHandleResultImpl(result);
    }
    return result;
}

void TWriteSessionImpl::ConnectToPreferredPartitionLocation(const TDuration& delay)
{
    Y_VERIFY(Lock.IsLocked());
    Y_VERIFY(Settings.PartitionId_.Defined() && Settings.DirectWriteToPartition_);

    if (AtomicGet(Aborting)) {
        return;
    }

    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Get partition location async, partition " << *Settings.PartitionId_ << ", delay " << delay );

    NGrpc::IQueueClientContextPtr prevDescribePartitionContext;
    NGrpc::IQueueClientContextPtr describePartitionContext = Client->CreateContext();

    if (!describePartitionContext) {
        AbortImpl();
        return;
    }

    prevDescribePartitionContext = std::exchange(DescribePartitionContext, describePartitionContext);
    Y_ASSERT(DescribePartitionContext);
    NPersQueue::Cancel(prevDescribePartitionContext);

    Ydb::Topic::DescribePartitionRequest request;
    request.set_path(Settings.Path_);
    request.set_partition_id(*Settings.PartitionId_);
    request.set_include_location(true);

    auto extractor = [sharedThis = shared_from_this(), wire = Tracker->MakeTrackedWire(), context = describePartitionContext](Ydb::Topic::DescribePartitionResponse* response, TPlainStatus status) mutable {
        Ydb::Topic::DescribePartitionResult result;
        if (response)
            response->operation().result().UnpackTo(&result);

        TStatus st(std::move(status));
        sharedThis->OnDescribePartition(st, result, context);
    };

    auto callback = [sharedThis = this->shared_from_this(), wire = Tracker->MakeTrackedWire(), req = std::move(request), extr = std::move(extractor), connections = std::shared_ptr<TGRpcConnectionsImpl>(Connections), dbState = DbDriverState, context = describePartitionContext]() mutable {
        LOG_LAZY(dbState->Log, TLOG_DEBUG, sharedThis->LogPrefix() << " Getting partition location, partition " << sharedThis->Settings.PartitionId_);
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

void TWriteSessionImpl::OnDescribePartition(const TStatus& status, const Ydb::Topic::DescribePartitionResult& proto, const NGrpc::IQueueClientContextPtr& describePartitionContext)
{
    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Got PartitionLocation response. Status " << status.GetStatus() << ", proto:\n" << proto.DebugString());
    TString endpoint, name;
    THandleResult handleResult;

    with_lock (Lock) {
        if (DescribePartitionContext == describePartitionContext)
            DescribePartitionContext = nullptr;
        else 
            return;
    }

    if (!status.IsSuccess()) {
        with_lock (Lock) {
            handleResult = OnErrorImpl({status.GetStatus(), NPersQueue::MakeIssueWithSubIssues("Failed to get partition location", status.GetIssues())});
        }
        ProcessHandleResult(handleResult);
        return;
    }

    const Ydb::Topic::DescribeTopicResult_PartitionInfo& partition = proto.partition();
    if (partition.partition_id() != Settings.PartitionId_ || !partition.has_partition_location() || partition.partition_location().node_id() == 0 || partition.partition_location().generation() == 0) {
        with_lock (Lock) {
            handleResult = OnErrorImpl({EStatus::INTERNAL_ERROR, "Wrong partition location"});
        }
        ProcessHandleResult(handleResult);
        return;
    }

    TMaybe<TEndpointKey> preferredEndpoint;
    with_lock (Lock) {
        preferredEndpoint = GetPreferredEndpointImpl(*Settings.PartitionId_, partition.partition_location().node_id());
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

    Connect(TDuration::Zero());
}

TMaybe<TEndpointKey> TWriteSessionImpl::GetPreferredEndpointImpl(ui32 partitionId, ui64 partitionNodeId) {
    Y_VERIFY(Lock.IsLocked());

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
        Settings.DeduplicationEnabled_ = !(Settings.ProducerId_.empty());
    }
    else if (Settings.DeduplicationEnabled_.GetRef()) {
        if (Settings.ProducerId_.empty()) {
            Settings.ProducerId(GenerateProducerId());
        }
    } else {
        if (!Settings.ProducerId_.empty()) {
            LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "ProducerId is not empty when deduplication is switched off");
            ThrowFatalError("Cannot disable deduplication when non-empty ProducerId is provided");
        }
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

ui64 TWriteSessionImpl::GetNextSeqNoImpl(const TMaybe<ui64>& seqNo) {
    Y_VERIFY(Lock.IsLocked());

    ui64 seqNoValue = LastSeqNo + 1;
    if (!AutoSeqNoMode.Defined()) {
        AutoSeqNoMode = !seqNo.Defined();
        //! Disable SeqNo shift for manual SeqNo mode;
        if (seqNo.Defined()) {
            OnSeqNoShift = false;
            SeqNoShift = 0;
        }
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
            seqNoValue = *seqNo;
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
    LastSeqNo = seqNoValue;
    return seqNoValue;
}
inline void TWriteSessionImpl::CheckHandleResultImpl(THandleResult& result) {
    Y_VERIFY(Lock.IsLocked());

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

// Client method.
void TWriteSessionImpl::WriteInternal(TContinuationToken&&, TWriteMessage&& message) {
    TInstant createdAtValue = message.CreateTimestamp_.Defined() ? *message.CreateTimestamp_ : TInstant::Now();
    bool readyToAccept = false;
    size_t bufferSize = message.Data.size();
    with_lock(Lock) {
        CurrentBatch.Add(
                GetNextSeqNoImpl(message.SeqNo_), createdAtValue, message.Data, message.Codec, message.OriginalSize,
                message.MessageMeta_
        );

        FlushWriteIfRequiredImpl();
        readyToAccept = OnMemoryUsageChangedImpl(bufferSize).NowOk;
    }
    if (readyToAccept) {
        EventsQueue->PushEvent(TWriteSessionEvent::TReadyToAcceptEvent{{}, TContinuationToken{}});
    }
}

// Client method.
void TWriteSessionImpl::Write(TContinuationToken&& token, TWriteMessage&& message) {
    WriteInternal(std::move(token), std::move(message));
}


TWriteSessionImpl::THandleResult TWriteSessionImpl::OnErrorImpl(NYdb::TPlainStatus&& status) {
    Y_VERIFY(Lock.IsLocked());

    (*Counters->Errors)++;
    auto result = RestartImpl(status);
    if (result.DoStop) {
        CloseImpl(status.Status, std::move(status.Issues));
    }
    return result;
}

// No lock
void TWriteSessionImpl::Connect(const TDuration& delay) {
    NGrpc::IQueueClientContextPtr prevConnectContext;
    NGrpc::IQueueClientContextPtr prevConnectTimeoutContext;
    NGrpc::IQueueClientContextPtr prevConnectDelayContext;
    NGrpc::IQueueClientContextPtr connectContext = nullptr;
    NGrpc::IQueueClientContextPtr connectDelayContext = nullptr;
    NGrpc::IQueueClientContextPtr connectTimeoutContext = nullptr;
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
        auto subclient = Client;
        connectionFactory = subclient->CreateWriteSessionConnectionProcessorFactory();
        auto clientContext = subclient->CreateContext();
        ConnectionFactory = connectionFactory;

        ClientContext = std::move(clientContext);
        ServerMessage = std::make_shared<TServerMessage>();

        if (!ClientContext) {
            AbortImpl();
            // Grpc and WriteSession is closing right now.
            return;
        }

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
        NPersQueue::Cancel(prevConnectContext);
        if (prevConnectDelayContext)
            NPersQueue::Cancel(prevConnectDelayContext);
        NPersQueue::Cancel(prevConnectTimeoutContext);

        reqSettings = TRpcRequestSettings::Make(Settings, PreferredPartitionLocation.Endpoint);

        connectCallback = [sharedThis = shared_from_this(),
                                wire = Tracker->MakeTrackedWire(),
                                connectContext = connectContext]
                (TPlainStatus&& st, typename IProcessor::TPtr&& processor) {
            sharedThis->OnConnect(std::move(st), std::move(processor), connectContext);
        };

        connectTimeoutCallback = [sharedThis = shared_from_this(),
                                    wire = Tracker->MakeTrackedWire(),
                                    connectTimeoutContext = connectTimeoutContext]
                                    (bool ok) {
            if (ok) {
                sharedThis->OnConnectTimeout(connectTimeoutContext);
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
void TWriteSessionImpl::OnConnectTimeout(const NGrpc::IQueueClientContextPtr& connectTimeoutContext) {
    LOG_LAZY(DbDriverState->Log, TLOG_ERR, LogPrefix() << "Write session: connect timeout");
    THandleResult handleResult;
    with_lock (Lock) {
        if (ConnectTimeoutContext == connectTimeoutContext) {
            NPersQueue::Cancel(ConnectContext);
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
        TPlainStatus&& st, typename IProcessor::TPtr&& processor, const NGrpc::IQueueClientContextPtr& connectContext
) {
    THandleResult handleResult;
    with_lock (Lock) {
        if (ConnectContext == connectContext) {
            NPersQueue::Cancel(ConnectTimeoutContext);
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
                        NPersQueue::MakeIssueWithSubIssues(
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
    Y_VERIFY(Lock.IsLocked());

    TClientMessage req;
    auto* init = req.mutable_init_request();
    init->set_path(Settings.Path_);
    init->set_producer_id(Settings.ProducerId_);
    
    if (Settings.PartitionId_.Defined()) {
        if (Settings.DirectWriteToPartition_) {
            auto* partitionWithGeneration = init->mutable_partition_with_generation();
            partitionWithGeneration->set_partition_id(*Settings.PartitionId_);
            partitionWithGeneration->set_generation(PreferredPartitionLocation.Generation);
            LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: direct write to partition: " << *Settings.PartitionId_ << ", generation " << PreferredPartitionLocation.Generation);
        }
        else {
            init->set_partition_id(*Settings.PartitionId_);
            LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: write to partition: " << *Settings.PartitionId_);
        }
    }
    else
    {
        init->set_message_group_id(Settings.MessageGroupId_);
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: write to message_group: " << Settings.MessageGroupId_);
    }

    for (const auto& attr : Settings.Meta_.Fields) {
        (*init->mutable_write_session_meta())[attr.first] = attr.second;
    }
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: send init request: "<< req.ShortDebugString());
    WriteToProcessorImpl(std::move(req));
}

// Called under lock. Invokes Processor->Write, which is assumed to be deadlock-safe
void TWriteSessionImpl::WriteToProcessorImpl(TWriteSessionImpl::TClientMessage&& req) {
    Y_VERIFY(Lock.IsLocked());

    Y_ASSERT(Processor);
    if (Aborting) {
        return;
    }
    auto callback = [sharedThis = shared_from_this(),
                     wire = Tracker->MakeTrackedWire(),
                     connectionGeneration = ConnectionGeneration](NGrpc::TGrpcStatus&& grpcStatus) {
        sharedThis->OnWriteDone(std::move(grpcStatus), connectionGeneration);
    };

    Processor->Write(std::move(req), callback);
}

void TWriteSessionImpl::ReadFromProcessor() {
    Y_ASSERT(Processor);
    IProcessor::TPtr prc;
    ui64 generation;
    std::function<void(NGrpc::TGrpcStatus&&)> callback;
    with_lock(Lock) {
        if (Aborting) {
            return;
        }
        prc = Processor;
        generation = ConnectionGeneration;
        callback = [sharedThis = shared_from_this(),
                        wire = Tracker->MakeTrackedWire(),
                        connectionGeneration = generation,
                        processor = prc,
                        serverMessage = ServerMessage]
                        (NGrpc::TGrpcStatus&& grpcStatus) {
            sharedThis->OnReadDone(std::move(grpcStatus), connectionGeneration);
        };
    }
    prc->Read(ServerMessage.get(), std::move(callback));
}

void TWriteSessionImpl::OnWriteDone(NGrpc::TGrpcStatus&& status, size_t connectionGeneration) {
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

void TWriteSessionImpl::OnReadDone(NGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration) {
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
            if (NPersQueue::IsErrorMessage(*ServerMessage)) {
                errorStatus = NPersQueue::MakeErrorFromProto(*ServerMessage);
            } else {
                processResult = ProcessServerMessageImpl();
                needSetValue = !InitSeqNoSetDone && processResult.InitSeqNo.Defined() && (InitSeqNoSetDone = true);
                if (errorStatus.Ok() && processResult.Ok) {
                    doRead = true;
                }
            }
        }
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
    for (auto& event : processResult.Events) {
        EventsQueue->PushEvent(std::move(event));
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

    if (Settings.PartitionId_.Defined()) {
        ret << " PartitionId [" << *Settings.PartitionId_ << "] ";
        if (Settings.DirectWriteToPartition_)
            ret << " Generation [" << PreferredPartitionLocation.Generation << "] ";
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
    Y_VERIFY(Lock.IsLocked());

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
            SessionId = initResponse.session_id();
            PartitionId = initResponse.partition_id();
            ui64 newLastSeqNo = initResponse.last_seq_no();
            // SeqNo increased, so there's a risk of loss, apply SeqNo shift.
            // MinUnsentSeqNo must be > 0 if anything was ever sent yet
            if (Settings.DeduplicationEnabled_.GetOrElse(true)) {
                 if(MinUnsentSeqNo && OnSeqNoShift && newLastSeqNo > MinUnsentSeqNo) {
                     SeqNoShift = newLastSeqNo - MinUnsentSeqNo;
                 }
            } else {
                newLastSeqNo = 1;
	    }
            result.InitSeqNo = newLastSeqNo;
            LastSeqNo = newLastSeqNo;

            SessionEstablished = true;
            LastCountersUpdateTs = TInstant::Now();
            SessionStartedTs = TInstant::Now();
            OnErrorResolved();

            if (!FirstTokenSent) {
                result.Events.emplace_back(TWriteSessionEvent::TReadyToAcceptEvent{{}, TContinuationToken{}});
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

                Y_VERIFY(ack.has_written() || ack.has_skipped());
                auto msgWriteStatus = ack.has_written()
                                ? TWriteSessionEvent::TWriteAck::EES_WRITTEN
                                : (ack.skipped().reason() == Ydb::Topic::StreamWriteMessage_WriteResponse_WriteAck_Skipped_Reason::StreamWriteMessage_WriteResponse_WriteAck_Skipped_Reason_REASON_ALREADY_WRITTEN
                                    ? TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN
                                    : TWriteSessionEvent::TWriteAck::EES_DISCARDED);

                ui64 offset = ack.has_written() ? ack.written().offset() : 0;

                acksEvent.Acks.push_back(TWriteSessionEvent::TWriteAck{
                    sequenceNumber - SeqNoShift,
                    msgWriteStatus,
                    TWriteSessionEvent::TWriteAck::TWrittenMessageDetails {
                        offset,
                        PartitionId,
                    },
                    writeStat,
                });

                if (CleanupOnAcknowledged(sequenceNumber - SeqNoShift)) {
                    result.Events.emplace_back(TWriteSessionEvent::TReadyToAcceptEvent{{}, TContinuationToken{}});
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

bool TWriteSessionImpl::CleanupOnAcknowledged(ui64 sequenceNumber) {
    bool result = false;
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: acknoledged message " << sequenceNumber);
    UpdateTimedCountersImpl();
    const auto& sentFront = SentOriginalMessages.front();
    ui64 size = 0;
    ui64 compressedSize = 0;
    if(!SentPackedMessage.empty() && SentPackedMessage.front().Offset == sequenceNumber) {
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

    Y_VERIFY(Counters->BytesInflightCompressed->Val() >= 0);
    Y_VERIFY(Counters->BytesInflightUncompressed->Val() >= 0);

    Y_VERIFY(sentFront.SeqNo == sequenceNumber);

    (*Counters->BytesInflightTotal) = MemoryUsage;
    SentOriginalMessages.pop();
    return result;
}

TMemoryUsageChange TWriteSessionImpl::OnMemoryUsageChangedImpl(i64 diff) {
    Y_VERIFY(Lock.IsLocked());

    bool wasOk = MemoryUsage <= Settings.MaxMemoryUsage_;
    //if (diff < 0) {
    //    Y_VERIFY(MemoryUsage >= static_cast<size_t>(std::abs(diff)));
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

TBuffer CompressBuffer(TVector<TStringBuf>& data, ECodec codec, i32 level) {
    TBuffer result;
    THolder<IOutputStream> coder = NCompressionDetails::CreateCoder(codec, result, level);
    for (auto& buffer : data) {
        coder->Write(buffer.data(), buffer.size());
    }
    coder->Finish();
    return result;
}

// May call OnCompressed with sync executor. No external lock.
void TWriteSessionImpl::CompressImpl(TBlock&& block_) {
    Y_VERIFY(Lock.IsLocked());

    if (Aborting) {
        return;
    }
    Y_VERIFY(block_.Valid);

    std::shared_ptr<TBlock> blockPtr(std::make_shared<TBlock>());
    blockPtr->Move(block_);
    auto lambda = [sharedThis = shared_from_this(),
                   wire = Tracker->MakeTrackedWire(),
                   codec = Settings.Codec_,
                   level = Settings.CompressionLevel_,
                   isSyncCompression = !CompressionExecutor->IsAsync(),
                   blockPtr]() mutable {
        Y_VERIFY(!blockPtr->Compressed);

        auto compressedData = CompressBuffer(
                blockPtr->OriginalDataRefs, codec, level
        );
        Y_VERIFY(!compressedData.Empty());
        blockPtr->Data = std::move(compressedData);
        blockPtr->Compressed = true;
        blockPtr->CodecID = static_cast<ui32>(sharedThis->Settings.Codec_);
        sharedThis->OnCompressed(std::move(*blockPtr), isSyncCompression);
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
        EventsQueue->PushEvent(TWriteSessionEvent::TReadyToAcceptEvent{{}, TContinuationToken{}});
    }
}

TMemoryUsageChange TWriteSessionImpl::OnCompressedImpl(TBlock&& block) {
    Y_VERIFY(Lock.IsLocked());

    UpdateTimedCountersImpl();
    Y_VERIFY(block.Valid);
    auto memoryUsage = OnMemoryUsageChangedImpl(static_cast<i64>(block.Data.size()) - block.OriginalMemoryUsage);
    (*Counters->BytesInflightUncompressed) -= block.OriginalSize;
    (*Counters->BytesInflightCompressed) += block.Data.size();

    PackedMessagesToSend.emplace(std::move(block));
    SendImpl();
    return memoryUsage;
}

void TWriteSessionImpl::ResetForRetryImpl() {
    Y_VERIFY(Lock.IsLocked());

    SessionEstablished = false;
    const size_t totalPackedMessages = PackedMessagesToSend.size() + SentPackedMessage.size();
    const size_t totalOriginalMessages = OriginalMessagesToSend.size() + SentOriginalMessages.size();
    while (!SentPackedMessage.empty()) {
        PackedMessagesToSend.emplace(std::move(SentPackedMessage.front()));
        SentPackedMessage.pop();
    }
    ui64 minSeqNo = PackedMessagesToSend.empty() ? LastSeqNo + 1 : PackedMessagesToSend.top().Offset;
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
    if (!OriginalMessagesToSend.empty() && OriginalMessagesToSend.front().SeqNo < minSeqNo)
        minSeqNo = OriginalMessagesToSend.front().SeqNo;
    MinUnsentSeqNo = minSeqNo;
    Y_VERIFY(PackedMessagesToSend.size() == totalPackedMessages);
    Y_VERIFY(OriginalMessagesToSend.size() == totalOriginalMessages);
}

// Called from client Write() methods
void TWriteSessionImpl::FlushWriteIfRequiredImpl() {
    Y_VERIFY(Lock.IsLocked());

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
    Y_VERIFY(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log,
        TLOG_DEBUG,
        LogPrefix() << "Write " << CurrentBatch.Messages.size() << " messages with seqNo from "
            << CurrentBatch.Messages.begin()->SeqNo << " to " << CurrentBatch.Messages.back().SeqNo
    );

    Y_VERIFY(CurrentBatch.Messages.size() <= MaxBlockMessageCount);

    const bool skipCompression = Settings.Codec_ == ECodec::RAW || CurrentBatch.HasCodec();
    if (!skipCompression && Settings.CompressionExecutor_->IsAsync()) {
        MessagesAcquired += static_cast<ui64>(CurrentBatch.Acquire());
    }

    size_t size = 0;
    for (size_t i = 0; i != CurrentBatch.Messages.size();) {
        TBlock block{};
        for (; block.OriginalSize < MaxBlockSize && i != CurrentBatch.Messages.size(); ++i) {
            auto& currMessage = CurrentBatch.Messages[i];
            auto sequenceNumber = currMessage.SeqNo;
            auto createTs = currMessage.CreatedAt;

            if (!block.MessageCount) {
                block.Offset = sequenceNumber;
            }

            block.MessageCount += 1;
            const auto& datum = currMessage.DataRef;
            block.OriginalSize += datum.size();
            block.OriginalMemoryUsage = CurrentBatch.Data.size();
            block.OriginalDataRefs.emplace_back(datum);
            if (CurrentBatch.Messages[i].Codec.Defined()) {
                Y_VERIFY(CurrentBatch.Messages.size() == 1);
                block.CodecID = static_cast<ui32>(*currMessage.Codec);
                block.OriginalSize = currMessage.OriginalSize;
                block.Compressed = false;
            }
            size += datum.size();
            UpdateTimedCountersImpl();
            (*Counters->BytesInflightUncompressed) += datum.size();
            (*Counters->MessagesInflight)++;
            if (!currMessage.MessageMeta.empty()) {
                OriginalMessagesToSend.emplace(sequenceNumber, createTs, datum.size(),
                                               std::move(currMessage.MessageMeta));
            } else {
                OriginalMessagesToSend.emplace(sequenceNumber, createTs, datum.size());
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
    Y_VERIFY(Lock.IsLocked());

    if (!SessionEstablished) {
        return false;
    }
    if (Aborting)
        return false;
    if (PackedMessagesToSend.empty()) {
        return false;
    }
    Y_VERIFY(!OriginalMessagesToSend.empty(), "There are packed messages but no original messages");
    Y_VERIFY(OriginalMessagesToSend.front().SeqNo <= PackedMessagesToSend.top().Offset, "Lost original message(s)");

    return PackedMessagesToSend.top().Offset == OriginalMessagesToSend.front().SeqNo;
}


void TWriteSessionImpl::UpdateTokenIfNeededImpl() {
    Y_VERIFY(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: try to update token");

    if (!DbDriverState->CredentialsProvider || UpdateTokenInProgress || !SessionEstablished)
        return;
    TClientMessage clientMessage;
    auto* updateRequest = clientMessage.mutable_update_token_request();
    auto token = DbDriverState->CredentialsProvider->GetAuthInfo();
    if (token == PrevToken)
        return;
    UpdateTokenInProgress = true;
    updateRequest->set_token(token);
    PrevToken = token;

    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: updating token");

    Processor->Write(std::move(clientMessage));
}

void TWriteSessionImpl::SendImpl() {
    Y_VERIFY(Lock.IsLocked());

    // External cycle splits ready blocks into multiple gRPC messages. Current gRPC message size hard limit is 64MiB
    while(IsReadyToSendNextImpl()) {
        TClientMessage clientMessage;
        auto* writeRequest = clientMessage.mutable_write_request();

        // Sent blocks while we can without messages reordering
        while (IsReadyToSendNextImpl() && clientMessage.ByteSizeLong() < GetMaxGrpcMessageSize()) {
            const auto& block = PackedMessagesToSend.top();
            Y_VERIFY(block.Valid);
            writeRequest->set_codec(static_cast<i32>(block.CodecID));
            Y_VERIFY(block.MessageCount == 1);
            for (size_t i = 0; i != block.MessageCount; ++i) {
                Y_VERIFY(!OriginalMessagesToSend.empty());

                auto& message = OriginalMessagesToSend.front();

                auto* msgData = writeRequest->add_messages();


                msgData->set_seq_no(message.SeqNo + SeqNoShift);
                *msgData->mutable_created_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(message.CreatedAt.MilliSeconds());

                if (!message.MessageMeta.empty()) {
                    for (auto& [k, v] : message.MessageMeta) {
                        auto* pair = msgData->add_metadata_items();
                        pair->set_key(k);
                        pair->set_value(v);
                    }
                }
                SentOriginalMessages.emplace(std::move(message));
                OriginalMessagesToSend.pop();

                msgData->set_uncompressed_size(block.OriginalSize);
                if (block.Compressed)
                    msgData->set_data(block.Data.data(), block.Data.size());
                else {
                    for (auto& buffer: block.OriginalDataRefs) {
                        msgData->set_data(buffer.data(), buffer.size());
                    }
                }
            }


            TBlock moveBlock;
            moveBlock.Move(block);
            SentPackedMessage.emplace(std::move(moveBlock));
            PackedMessagesToSend.pop();
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
    if (AtomicGet(Aborting))
        return false;
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
    Y_VERIFY(Lock.IsLocked());

    FlushWriteIfRequiredImpl();
    if (AtomicGet(Aborting)) {
        return;
    }
    auto callback = [sharedThis = this->shared_from_this(), wire = Tracker->MakeTrackedWire()] (bool ok)
    {
        if (!ok) {
            return;
        }
        with_lock(sharedThis->Lock) {
            sharedThis->HandleWakeUpImpl();
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
    Y_VERIFY(Lock.IsLocked());

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
    Y_VERIFY(Lock.IsLocked());

    if (!AtomicGet(Aborting)) {
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: aborting");
        AtomicSet(Aborting, 1);
        NPersQueue::Cancel(DescribePartitionContext);
        NPersQueue::Cancel(ConnectContext);
        NPersQueue::Cancel(ConnectTimeoutContext);
        NPersQueue::Cancel(ConnectDelayContext);
        if (Processor)
            Processor->Cancel();

        NPersQueue::Cancel(ClientContext);
        ClientContext.reset(); // removes context from contexts set from underlying gRPC-client.
    }
}

void TWriteSessionImpl::CloseImpl(EStatus statusCode, NYql::TIssues&& issues) {
    Y_VERIFY(Lock.IsLocked());

    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session will now close");
    EventsQueue->Close(TSessionClosedEvent(statusCode, std::move(issues)));
    AbortImpl();
}

void TWriteSessionImpl::CloseImpl(EStatus statusCode, const TString& message) {
    Y_VERIFY(Lock.IsLocked());

    NYql::TIssues issues;
    issues.AddIssue(message);
    CloseImpl(statusCode, std::move(issues));
}

void TWriteSessionImpl::CloseImpl(TPlainStatus&& status) {
    Y_VERIFY(Lock.IsLocked());

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

}; // namespace NYdb::NTopic
