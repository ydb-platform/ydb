#include "write_session_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>

#include <library/cpp/string_utils/url/url.h>

#include <util/generic/store_policy.h>
#include <util/generic/utility.h>
#include <util/stream/buffer.h>


namespace NYdb::NPersQueue {

const TDuration UPDATE_TOKEN_PERIOD = TDuration::Hours(1);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteSessionImpl

TWriteSessionImpl::TWriteSessionImpl(
        const TWriteSessionSettings& settings,
         std::shared_ptr<TPersQueueClient::TImpl> client,
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
    if (Settings.PreferredCluster_ && !Settings.AllowFallbackToOtherClusters_) {
        TargetCluster = *Settings.PreferredCluster_;
        TargetCluster.to_lower();
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

    ++ConnectionAttemptsDone;
    if (!Started) {
        with_lock(Lock) {
            HandleWakeUpImpl();
        }
        InitWriter();
    }
    Started = true;

    DoCdsRequest(delay);
}

TWriteSessionImpl::THandleResult TWriteSessionImpl::RestartImpl(const TPlainStatus& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    THandleResult result;
    if (AtomicGet(Aborting)) {
        LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session is aborting and will not restart");
        return result;
    }

    SessionEstablished = false;
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

bool IsFederation(const TString& endpoint) {
    TStringBuf host = GetHost(endpoint);
    return host == "logbroker.yandex.net" || host == "logbroker-prestable.yandex.net";
}

void TWriteSessionImpl::DoCdsRequest(TDuration delay) {
    bool cdsRequestIsUnnecessary;
    with_lock (Lock) {
        if (AtomicGet(Aborting)) {
            return;
        }
        LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Write session: Do CDS request");

        cdsRequestIsUnnecessary = (Settings.ClusterDiscoveryMode_ == EClusterDiscoveryMode::Off ||
            (Settings.ClusterDiscoveryMode_ == EClusterDiscoveryMode::Auto && !IsFederation(DbDriverState->DiscoveryEndpoint)));

        if (!cdsRequestIsUnnecessary) {
            auto extractor = [cbContext = SelfContext]
                    (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TStatus st(std::move(status));
                if (auto self = cbContext->LockShared()) {
                    self->OnCdsResponse(st, result);
                }
            };

            Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest req;
            auto* params = req.add_write_sessions();
            params->set_topic(Settings.Path_);
            params->set_source_id(Settings.MessageGroupId_);
            if (Settings.PartitionGroupId_.Defined())
                params->set_partition_group(*Settings.PartitionGroupId_);
            if (Settings.PreferredCluster_.Defined())
                params->set_preferred_cluster_name(*Settings.PreferredCluster_);

            LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Do schedule cds request after " << delay.MilliSeconds() << " ms\n");
            auto cdsRequestCall = [req_=std::move(req), extr=std::move(extractor), connections = std::shared_ptr<TGRpcConnectionsImpl>(Connections), dbState=DbDriverState, settings=Settings]() mutable {
                LOG_LAZY(dbState->Log, TLOG_INFO, TStringBuilder() << "MessageGroupId [" << settings.MessageGroupId_ << "] Running cds request ms\n");
                connections->RunDeferred<Ydb::PersQueue::V1::ClusterDiscoveryService,
                                        Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest,
                                        Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse>(
                    std::move(req_),
                    std::move(extr),
                    &Ydb::PersQueue::V1::ClusterDiscoveryService::Stub::AsyncDiscoverClusters,
                    dbState,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings)); // TODO: make client timeout setting
            };
            Connections->ScheduleOneTimeTask(std::move(cdsRequestCall), delay);
            return;
        }
    }

    if (cdsRequestIsUnnecessary) {
        DoConnect(delay, DbDriverState->DiscoveryEndpoint);
        return;
    }
}

void TWriteSessionImpl::OnCdsResponse(
        TStatus& status, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult& result
) {
    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Got CDS response: \n" << result.ShortDebugString());
    TString endpoint, name;
    THandleResult handleResult;
    if (!status.IsSuccess()) {
        with_lock (Lock) {
            handleResult = OnErrorImpl({
                    status.GetStatus(),
                    MakeIssueWithSubIssues("Failed to discover clusters", status.GetIssues())
            });
        }
        ProcessHandleResult(handleResult);
        return;
    }

    NYql::TIssues issues;
    EStatus errorStatus = EStatus::INTERNAL_ERROR;
    with_lock (Lock) {
        const Ydb::PersQueue::ClusterDiscovery::WriteSessionClusters& wsClusters = result.write_sessions_clusters(0);
        bool isFirst = true;

        for (const auto& clusterInfo : wsClusters.clusters()) {
            TString normalizedName = clusterInfo.name();
            normalizedName.to_lower();

            if(isFirst) {
                isFirst = false;
                PreferredClusterByCDS = clusterInfo.name();
            }

            if (!clusterInfo.available()) {
                if (TargetCluster && TargetCluster == normalizedName) {
                    errorStatus = EStatus::UNAVAILABLE;
                    issues.AddIssue(TStringBuilder() << "Selected destination cluster: " << normalizedName
                                                     << " is currently disabled");
                    break;
                }
                continue;
            }
            if (clusterInfo.endpoint().empty()) {
                issues.AddIssue(TStringBuilder() << "Unexpected reply from cluster discovery. Empty endpoint for cluster "
                                                 << normalizedName);
            } else {
                name = clusterInfo.name();
                endpoint = ApplyClusterEndpoint(DbDriverState->DiscoveryEndpoint, clusterInfo.endpoint());
                break;
            }
        }
        if (endpoint.empty()) {
            errorStatus = EStatus::GENERIC_ERROR;
            issues.AddIssue(TStringBuilder() << "Could not get valid endpoint from cluster discovery");
        }
    }
    if (issues) {
        with_lock(Lock) {
            handleResult = OnErrorImpl({errorStatus, std::move(issues)});
        }
        ProcessHandleResult(handleResult);
            return;
    }
    with_lock(Lock) {
        if (!InitialCluster) {
            InitialCluster = name;
        }
        CurrentCluster = name;
    }
    DoConnect(TDuration::Zero(), endpoint);

}

void TWriteSessionImpl::InitWriter() { // No Lock, very initial start - no race yet as well.
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
    Y_ABORT_UNLESS(!*AutoSeqNoMode || InitSeqNo.contains(CurrentCluster) && seqNo > InitSeqNo[CurrentCluster]);
    return *AutoSeqNoMode ? seqNo - InitSeqNo[CurrentCluster] : seqNo;
}

ui64 TWriteSessionImpl::GetSeqNoImpl(ui64 id) {
    Y_ABORT_UNLESS(AutoSeqNoMode.Defined());
    return *AutoSeqNoMode ? id + InitSeqNo[CurrentCluster] : id;

}

ui64 TWriteSessionImpl::GetNextIdImpl(const TMaybe<ui64>& seqNo) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    ui64 id = ++NextId;

    if (!AutoSeqNoMode.Defined()) {
        AutoSeqNoMode = !seqNo.Defined();
    }
    if (seqNo.Defined()) {
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

// Client method.
void TWriteSessionImpl::WriteInternal(
            TContinuationToken&&, TStringBuf data, TMaybe<ECodec> codec, ui32 originalSize, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp
        ) {
    TInstant createdAtValue = createTimestamp.Defined() ? *createTimestamp : TInstant::Now();
    bool readyToAccept = false;
    size_t bufferSize = data.size();
    with_lock(Lock) {
        CurrentBatch.Add(GetNextIdImpl(seqNo), createdAtValue, data, codec, originalSize);

        FlushWriteIfRequiredImpl();
        readyToAccept = OnMemoryUsageChangedImpl(bufferSize).NowOk;
    }
    if (readyToAccept) {
        EventsQueue->PushEvent(TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
    }
}

// Client method.
void TWriteSessionImpl::WriteEncoded(
            TContinuationToken&& token, TStringBuf data, ECodec codec, ui32 originalSize, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp
        ) {
    WriteInternal(std::move(token), data, codec, originalSize, seqNo, createTimestamp);
}

void TWriteSessionImpl::Write(
            TContinuationToken&& token, TStringBuf data, TMaybe<ui64> seqNo, TMaybe<TInstant> createTimestamp
        ) {
    WriteInternal(std::move(token), data, {}, 0, seqNo, createTimestamp);
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
void TWriteSessionImpl::DoConnect(const TDuration& delay, const TString& endpoint) {
    LOG_LAZY(DbDriverState->Log, TLOG_INFO, LogPrefix() << "Start write session. Will connect to endpoint: " << endpoint);

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
        ++ConnectionGeneration;
        auto subclient = Client->GetClientForEndpoint(endpoint);

        auto clientContext = subclient->CreateContext();
        if (!clientContext) {
            AbortImpl();
            // Grpc and WriteSession is closing right now.
            return;
        }
        auto prevClientContext = std::exchange(ClientContext, clientContext);

        ServerMessage = std::make_shared<TServerMessage>();

        connectionFactory = subclient->CreateWriteSessionConnectionProcessorFactory();
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
        if (prevConnectDelayContext)
            Cancel(prevConnectDelayContext);
        Cancel(prevConnectTimeoutContext);
        Cancel(prevClientContext);
        Y_ASSERT(connectContext);
        Y_ASSERT(connectTimeoutContext);

        if (Processor) {
            Processor->Cancel();
        }

        reqSettings = TRpcRequestSettings::Make(Settings);

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

    Ydb::PersQueue::V1::StreamingWriteClientMessage req;
    auto* init = req.mutable_init_request();
    init->set_topic(Settings.Path_);
    init->set_message_group_id(Settings.MessageGroupId_);
    if (Settings.PartitionGroupId_) {
        init->set_partition_group_id(*Settings.PartitionGroupId_);
    }
    init->set_max_supported_format_version(0);
    init->set_preferred_cluster(PreferredClusterByCDS);

    for (const auto& attr : Settings.Meta_.Fields) {
        (*init->mutable_session_meta())[attr.first] = attr.second;
    }
    LOG_LAZY(DbDriverState->Log, TLOG_DEBUG, LogPrefix() << "Write session: send init request: "<< req.ShortDebugString());
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

    Processor->Write(std::move(req), std::move(callback));
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
    return TStringBuilder() << "MessageGroupId [" << Settings.MessageGroupId_ << "] SessionId [" << SessionId << "] ";
}

TString TWriteSessionEvent::TAcksEvent::DebugString() const {
    TStringBuilder res;
    res << "AcksEvent:";
    for (auto& ack : Acks) {
        res << " { seqNo : " << ack.SeqNo << ", State : " << ack.State;
        if (ack.Details) {
            res << ", offset : " << ack.Details->Offset << ", partitionId : " << ack.Details->PartitionId;
        }
        res << " }";
    }
    if (!Acks.empty() && Acks.back().Stat) {
        auto& stat = Acks.back().Stat;
        res << " write stat: Write time " << stat->WriteTime << " total time in partition queue " << stat->TotalTimeInPartitionQueue
            << " partition quoted time " << stat->PartitionQuotedTime << " topic quoted time " << stat->TopicQuotedTime;
    }
    return res;
}

TString TWriteSessionEvent::TReadyToAcceptEvent::DebugString() const {
    return "ReadyToAcceptEvent";
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
            SessionId = initResponse.session_id();
            PartitionId = initResponse.partition_id();
            ui64 newLastSeqNo = initResponse.last_sequence_number();
            result.InitSeqNo = newLastSeqNo;
            if (!InitSeqNo.contains(CurrentCluster)) {
                InitSeqNo[CurrentCluster] = newLastSeqNo >= MinUnsentId ? newLastSeqNo - MinUnsentId + 1 : 0;
            }

            SessionEstablished = true;
            LastCountersUpdateTs = TInstant::Now();
            SessionStartedTs = TInstant::Now();
            OnErrorResolved();

            if (!FirstTokenSent) {
                result.Events.emplace_back(TWriteSessionEvent::TReadyToAcceptEvent{IssueContinuationToken()});
                FirstTokenSent = true;
            }
            // Kickstart send after session reestablishment
            SendImpl();
            break;
        }
        case TServerMessage::kBatchWriteResponse: {
            TWriteSessionEvent::TAcksEvent acksEvent;
            const auto& batchWriteResponse = ServerMessage->batch_write_response();
            LOG_LAZY(DbDriverState->Log,
                TLOG_DEBUG,
                LogPrefix() << "Write session got write response: " << batchWriteResponse.ShortDebugString()
            );
            TWriteStat::TPtr writeStat = new TWriteStat{};
            const auto& stat = batchWriteResponse.write_statistics();
            writeStat->WriteTime = TDuration::MilliSeconds(stat.persist_duration_ms());
            writeStat->TotalTimeInPartitionQueue = TDuration::MilliSeconds(stat.queued_in_partition_duration_ms());
            writeStat->PartitionQuotedTime = TDuration::MilliSeconds(stat.throttled_on_partition_duration_ms());
            writeStat->TopicQuotedTime = TDuration::MilliSeconds(stat.throttled_on_topic_duration_ms());

            for (size_t messageIndex = 0, endIndex = batchWriteResponse.sequence_numbers_size(); messageIndex != endIndex; ++messageIndex) {
                // TODO: Fill writer statistics
                ui64 sequenceNumber = batchWriteResponse.sequence_numbers(messageIndex);

                acksEvent.Acks.push_back(TWriteSessionEvent::TWriteAck{
                    GetIdImpl(sequenceNumber),
                    batchWriteResponse.already_written(messageIndex) ? TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN:
                                                                       TWriteSessionEvent::TWriteAck::EES_WRITTEN,
                    TWriteSessionEvent::TWriteAck::TWrittenMessageDetails {
                        static_cast<ui64>(batchWriteResponse.offsets(messageIndex)),
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
    if (SentOriginalMessages.empty() || SentOriginalMessages.front().Id != id){
        Cerr << "State before restart was:\n" << StateStr << "\n\n";
        DumpState();
        Cerr << "State on ack with id " << id << " is:\n";
        Cerr << StateStr << "\n\n";
        Y_ABORT("got unknown ack");
    }

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

TBuffer CompressBuffer(std::shared_ptr<TPersQueueClient::TImpl> client, TVector<TStringBuf>& data, ECodec codec, i32 level) {
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
        blockPtr->CodecID = GetCodecId(codec);
        if (auto self = cbContext->LockShared()) {
            self->OnCompressed(std::move(*blockPtr), isSyncCompression);
        }
    };

    CompressionExecutor->Post(std::move(lambda));
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

void TWriteSessionImpl::ResetForRetryImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

    DumpState();

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
            auto id = CurrentBatch.Messages[i].Id;
            auto createTs = CurrentBatch.Messages[i].CreatedAt;

            if (!block.MessageCount) {
                block.Offset = id;
            }

            block.MessageCount += 1;
            const auto& datum = CurrentBatch.Messages[i].DataRef;
            block.OriginalSize += datum.size();
            block.OriginalMemoryUsage = CurrentBatch.Data.size();
            block.OriginalDataRefs.emplace_back(datum);
            if (CurrentBatch.Messages[i].Codec.Defined()) {
                Y_ABORT_UNLESS(CurrentBatch.Messages.size() == 1);
                block.CodecID = GetCodecId(*CurrentBatch.Messages[i].Codec);
                block.OriginalSize = CurrentBatch.Messages[i].OriginalSize;
                block.Compressed = false;
            }
            size += datum.size();
            UpdateTimedCountersImpl();
            (*Counters->BytesInflightUncompressed) += datum.size();
            (*Counters->MessagesInflight)++;
            OriginalMessagesToSend.emplace(id, createTs, datum.size());
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

bool TWriteSessionImpl::IsReadyToSendNextImpl() {
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
    if (OriginalMessagesToSend.front().Id > PackedMessagesToSend.top().Offset) {

        Cerr << " State before restart was:\n" << StateStr << "\n\n";
        DumpState();
        Cerr << " State after restart is:\n" << StateStr << "\n\n";
        Y_ABORT("Lost original message(s)");
    }

    return PackedMessagesToSend.top().Offset == OriginalMessagesToSend.front().Id;
}

void TWriteSessionImpl::DumpState() {
    TStringBuilder s;
    s << "STATE:\n";

    auto omts = OriginalMessagesToSend;
    s << "OriginalMessagesToSend(" << omts.size() << "):";
    ui32 i = 20;
    while(!omts.empty() && i-- > 0) {
        s << " " << omts.front().Id;
        omts.pop();
    }
    if (!omts.empty()) s << " ...";
    s << "\n";

    s << "SentOriginalMessages(" << SentOriginalMessages.size() << "):";
    omts = SentOriginalMessages;
    i = 20;
    while(!omts.empty() && i-- > 0) {
        s << " " << omts.front().Id;
        omts.pop();
    }
    if (omts.size() > 20) {
        s << " ...";
        for (ui32 skip = omts.size() <= 20 ? 0 : (omts.size() - 20); skip > 0; --skip) {
            omts.pop();
        }
    }
    while(!omts.empty()) {
        s << " " << omts.front().Id;
        omts.pop();
    }
    s << "\n";
    s << "PackedMessagesToSend(" << PackedMessagesToSend.size() << "):";
    i = 20;
    std::vector<TBlock> tmpPackedMessagesToSend;
    while (!PackedMessagesToSend.empty() && i-- > 0) {
        s << " (" << PackedMessagesToSend.top().Offset << ", " << PackedMessagesToSend.top().MessageCount << ")";
        TBlock block;
        block.Move(PackedMessagesToSend.top());
        PackedMessagesToSend.pop();
        tmpPackedMessagesToSend.emplace_back(std::move(block));
    }
    if (PackedMessagesToSend.size() > 0) s << " ...";
    s << "\n";
    for (auto it = tmpPackedMessagesToSend.begin(); it != tmpPackedMessagesToSend.end(); ++it) {
        PackedMessagesToSend.push(std::move(*it));
    }
    tmpPackedMessagesToSend.clear();

    auto spm = std::move(SentPackedMessage);
    s << "SentPackedMessages(" << spm.size() << "):";
    while(!spm.empty()) {
        s << " (" << spm.front().Offset << ", " << spm.front().MessageCount << ")";
        SentPackedMessage.push(std::move(spm.front()));
        spm.pop();
    }
    s << "\n";

    StateStr = s;
}


void TWriteSessionImpl::UpdateTokenIfNeededImpl() {
    Y_ABORT_UNLESS(Lock.IsLocked());

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
    Y_ABORT_UNLESS(Lock.IsLocked());

    // External cycle splits ready blocks into multiple gRPC messages. Current gRPC message size hard limit is 64MiB
    while(IsReadyToSendNextImpl()) {
        TClientMessage clientMessage;
        auto* writeRequest = clientMessage.mutable_write_request();
        auto sentAtMs = TInstant::Now().MilliSeconds();

        // Sent blocks while we can without messages reordering
        while (IsReadyToSendNextImpl() && clientMessage.ByteSizeLong() < GetMaxGrpcMessageSize()) {
            const auto& block = PackedMessagesToSend.top();
            Y_ABORT_UNLESS(block.Valid);
            for (size_t i = 0; i != block.MessageCount; ++i) {
                Y_ABORT_UNLESS(!OriginalMessagesToSend.empty());

                auto& message = OriginalMessagesToSend.front();

                writeRequest->add_sent_at_ms(sentAtMs);
                writeRequest->add_sequence_numbers(GetSeqNoImpl(message.Id));
                writeRequest->add_message_sizes(message.Size);
                writeRequest->add_created_at_ms(message.CreatedAt.MilliSeconds());

                SentOriginalMessages.emplace(std::move(message));
                OriginalMessagesToSend.pop();
            }

            writeRequest->add_blocks_offsets(block.Offset);
            writeRequest->add_blocks_message_counts(block.MessageCount);
            writeRequest->add_blocks_part_numbers(block.PartNumber);
            writeRequest->add_blocks_uncompressed_sizes(block.OriginalSize);
            writeRequest->add_blocks_headers(block.CodecID);
            if (block.Compressed)
                writeRequest->add_blocks_data(block.Data.data(), block.Data.size());
            else {
                for (auto& buffer: block.OriginalDataRefs) {
                    writeRequest->add_blocks_data(buffer.data(), buffer.size());
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
            LogPrefix() << "Send " << writeRequest->sequence_numbers_size() << " message(s) ("
                << OriginalMessagesToSend.size() << " left), first sequence number is "
                << writeRequest->sequence_numbers(0)
        );
        Processor->Write(std::move(clientMessage));
    }
}

// Client method, no Lock
bool TWriteSessionImpl::Close(TDuration closeTimeout) {
    if (AtomicGet(Aborting))
        return false;
    LOG_LAZY(DbDriverState->Log,
        TLOG_INFO,
        LogPrefix() << "Write session: close. Timeout = " << closeTimeout.MilliSeconds() << " ms"
    );
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
        LOG_LAZY(DbDriverState->Log,
            TLOG_WARNING,
            LogPrefix() << "Write session: could not confirm all writes in time"
                << " or session aborted, perform hard shutdown"
        );
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

}  // namespace NYdb::NPersQueue
