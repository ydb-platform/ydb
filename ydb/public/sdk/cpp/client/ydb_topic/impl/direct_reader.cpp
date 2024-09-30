#include "direct_reader.h"
#include "read_session_impl.ipp"

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>


namespace NYdb::NTopic {

TDirectReadClientMessage TDirectReadPartitionSession::MakeStartRequest() const {
    TDirectReadClientMessage req;
    auto& start = *req.mutable_start_direct_read_partition_session_request();
    start.set_partition_session_id(PartitionSessionId);
    start.set_last_direct_read_id(NextDirectReadId - 1);
    start.set_generation(Location.GetGeneration());
    return req;
}

[[nodiscard]] bool TDirectReadPartitionSession::TransitionTo(EState next) {
    /*
            On lost connection
    +---------------------<----------+
    |                     |          |
    |       On start      |          |
    |  +----------------+ |          |
    v  |                v |          |
    IDLE<---DELAYED--->STARTING--->WORKING
               ^          |          |
         Retry |          |          |
               +----------<----------+
               |     on StopDRPS
               |
               | Retry policy denied another retry
               v
         Destroy read session

    DELAYED->IDLE if callback is called when there's no connection established.
    */
    if (State == next) {
        return true;
    }

    switch (next) {
    case EState::IDLE: {
        switch (State) {
        case EState::DELAYED:
        case EState::STARTING:
        case EState::WORKING:
            State = EState::IDLE;
            break;
        default:
            return false;
        }

        break;
    }
    case EState::DELAYED: {
        switch (State) {
        case EState::STARTING:
        case EState::WORKING:
            State = EState::DELAYED;
            break;
        default:
            return false;
        }

        break;
    }
    case EState::STARTING: {
        switch (State) {
        case EState::IDLE:
        case EState::DELAYED:
            State = EState::STARTING;
            break;
        default:
            return false;
        }

        break;
    }
    case EState::WORKING: {
        if (State != EState::STARTING)  {
            return false;
        }

        State = EState::WORKING;
        RetryState = nullptr;

        break;
    }
    }

    Y_ABORT_UNLESS(State == next);
    return true;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadSessionControlCallbacks

TDirectReadSessionControlCallbacks::TDirectReadSessionControlCallbacks(TSingleClusterReadSessionContextPtr contextPtr)
    : SingleClusterReadSessionContextPtr(contextPtr)
    {}

void TDirectReadSessionControlCallbacks::OnDirectReadDone(
    Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response,
    TDeferredActions<false>& deferred
) {
    if (auto s = SingleClusterReadSessionContextPtr->LockShared()) {
        s->OnDirectReadDone(std::move(response), deferred);
    }
}

void TDirectReadSessionControlCallbacks::AbortSession(TSessionClosedEvent&& closeEvent) {
    if (auto s = SingleClusterReadSessionContextPtr->LockShared()) {
        s->AbortSession(std::move(closeEvent));
    }
}

void TDirectReadSessionControlCallbacks::ScheduleCallback(TDuration delay, std::function<void()> callback) {
    if (auto s = SingleClusterReadSessionContextPtr->LockShared()) {
        s->ScheduleCallback(
            delay,
            [callback = std::move(callback)](bool ok) {
                if (ok) {
                    callback();
                }
            }
        );
    }
}

void TDirectReadSessionControlCallbacks::ScheduleCallback(TDuration delay, std::function<void()> callback, TDeferredActions<false>& deferred) {
    deferred.DeferScheduleCallback(
        delay,
        [callback = std::move(callback)](bool ok) {
            if (ok) {
                callback();
            }
        },
        SingleClusterReadSessionContextPtr
    );
}

void TDirectReadSessionControlCallbacks::StopPartitionSession(TPartitionSessionId partitionSessionId) {
    if (auto s = SingleClusterReadSessionContextPtr->LockShared()) {
        s->StopPartitionSession(partitionSessionId);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadSessionManager

TDirectReadSessionManager::TDirectReadSessionManager(
    TServerSessionId serverSessionId,
    const NYdb::NTopic::TReadSessionSettings settings,
    IDirectReadSessionControlCallbacks::TPtr controlCallbacks,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadProcessorFactoryPtr processorFactory,
    TLog log
)
    : ReadSessionSettings(settings)
    , ServerSessionId(serverSessionId)
    , ClientContext(clientContext)
    , ProcessorFactory(processorFactory)
    , ControlCallbacks(controlCallbacks)
    , Log(log)
    {}

TDirectReadSessionManager::~TDirectReadSessionManager() {
    Close();
}

TStringBuilder TDirectReadSessionManager::GetLogPrefix() const {
    return TStringBuilder() << "TDirectReadSessionManager ServerSessionId=" << ServerSessionId << " ";
}

TDirectReadSessionContextPtr TDirectReadSessionManager::CreateDirectReadSession(TNodeId nodeId) {
    return MakeWithCallbackContext<TDirectReadSession>(
        nodeId,
        ServerSessionId,
        ReadSessionSettings,
        ControlCallbacks,
        ClientContext->CreateContext(),
        ProcessorFactory,
        Log);
}

void TDirectReadSessionManager::Close() {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Close");

    // TODO(qyryq) Cancel contexts, anything else?

    for (auto& [_, nodeSession] : NodeSessions) {
        if (auto s = nodeSession->LockShared()) {
            s->Close();
        }
        nodeSession->Cancel();
    }
}

void TDirectReadSessionManager::StartPartitionSession(TDirectReadPartitionSession&& partitionSession) {
    auto nodeId = partitionSession.Location.GetNodeId();
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "StartPartitionSession " << partitionSession.PartitionSessionId);
    TDirectReadSessionContextPtr& session = NodeSessions[nodeId];
    if (!session) {
        session = CreateDirectReadSession(nodeId);
    }
    if (auto s = session->LockShared()) {
        s->Start();
        s->AddPartitionSession(std::move(partitionSession));
    }
    Locations.emplace(partitionSession.PartitionSessionId, partitionSession.Location);
}

// Delete a partition session from a node (TDirectReadSession), and if there are no more
// partition sessions on the node, drop connection to it.
void TDirectReadSessionManager::DeletePartitionSession(TPartitionSessionId partitionSessionId, TNodeSessionsMap::iterator it) {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeletePartitionSession " << partitionSessionId);

    bool cancelCallbackContext = false;
    if (auto session = it->second->LockShared()) {
        session->DeletePartitionSession(partitionSessionId);
        if (session->Closed()) {
            cancelCallbackContext = true;
            NodeSessions.erase(it);
            Locations.erase(partitionSessionId);
        }
    }
    if (cancelCallbackContext) {
        it->second->Cancel();
    }
}

void TDirectReadSessionManager::UpdatePartitionSession(TPartitionSessionId partitionSessionId, TPartitionLocation newLocation) {
    auto locIt = Locations.find(partitionSessionId);
    Y_ABORT_UNLESS(locIt != Locations.end());
    auto oldNodeId = locIt->second.GetNodeId();

    auto sessionIt = NodeSessions.find(oldNodeId);
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    // If oldLoc == newLoc and sessionIt->Empty() after deleting the partition session,
    // we have to reconnect back to the same node as before. Maybe it's worth to add a special case here.
    DeletePartitionSession(partitionSessionId, sessionIt);

    // TODO(qyryq) std::move an old RetryState?
    StartPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = newLocation });
}

void TDirectReadSessionManager::ErasePartitionSession(TPartitionSessionId partitionSessionId) {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "ErasePartitionSession " << partitionSessionId);

    auto locIt = Locations.find(partitionSessionId);
    Y_ABORT_UNLESS(locIt != Locations.end());
    auto nodeId = locIt->second.GetNodeId();

    auto sessionIt = NodeSessions.find(nodeId);
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    // Still need to Cancel the TCallbackContext<TDirectReadSession>.
    NodeSessions.erase(sessionIt);
    Locations.erase(partitionSessionId);
}

void TDirectReadSessionManager::StopPartitionSession(TPartitionSessionId partitionSessionId) {
    auto locIt = Locations.find(partitionSessionId);
    Y_ABORT_UNLESS(locIt != Locations.end());
    auto nodeId = locIt->second.GetNodeId();

    auto sessionIt = NodeSessions.find(nodeId);
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    DeletePartitionSession(partitionSessionId, sessionIt);
}

void TDirectReadSessionManager::StopPartitionSessionGracefully(TPartitionSessionId partitionSessionId, TDirectReadId lastDirectReadId) {
    auto locIt = Locations.find(partitionSessionId);
    Y_ABORT_UNLESS(locIt != Locations.end());

    auto sessionIt = NodeSessions.find(locIt->second.GetNodeId());
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    if (auto session = sessionIt->second->LockShared()) {
        session->SetLastDirectReadId(partitionSessionId, lastDirectReadId);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadSession

TDirectReadSession::TDirectReadSession(
    TNodeId nodeId,
    TString serverSessionId,
    const NYdb::NTopic::TReadSessionSettings settings,
    IDirectReadSessionControlCallbacks::TPtr controlCallbacks,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadProcessorFactoryPtr processorFactory,
    TLog log
)
    : ClientContext(clientContext)
    , ReadSessionSettings(settings)
    , ServerSessionId(serverSessionId)
    , ProcessorFactory(processorFactory)
    , NodeId(nodeId)
    , ControlCallbacks(controlCallbacks)
    , State(EState::CREATED)
    , Log(log)
    {
    }


void TDirectReadSession::Start()  {
    with_lock (Lock) {
        if (State != EState::CREATED) {
            return;
        }
    }
    Reconnect(TPlainStatus());
}

void TDirectReadSession::Close() {
    with_lock (Lock) {
        CloseImpl();
    }
}

void TDirectReadSession::CloseImpl() {
    if (State >= EState::CLOSING) {
        return;
    }
    State = EState::CLOSED;

    ::NYdb::NTopic::Cancel(ConnectContext);
    ::NYdb::NTopic::Cancel(ConnectTimeoutContext);
    ::NYdb::NTopic::Cancel(ConnectDelayContext);
    if (Processor) {
        Processor->Cancel();
    }

    // TODO(qyryq) Do we need to wait for something here?
    // TODO(qyryq) Do we need a separate CLOSING state?
}

bool TDirectReadSession::Empty() const {
    with_lock (Lock) {
        return PartitionSessions.empty();
    }
}

bool TDirectReadSession::Closed() const {
    with_lock (Lock) {
        return State >= EState::CLOSED;
    }
}

void TDirectReadSession::AddPartitionSession(TDirectReadPartitionSession&& session) {
    TDeferredActions<false> deferred;
    with_lock (Lock) {
        Y_ABORT_UNLESS(State < EState::CLOSING);

        auto [it, inserted] = PartitionSessions.emplace(session.PartitionSessionId, std::move(session));
        // TODO(qyryq) Abort? Ignore new? Replace old? Anything else?
        Y_ABORT_UNLESS(inserted);

        SendStartRequestImpl(it->second);
    }
}

void TDirectReadSession::SetLastDirectReadId(TPartitionSessionId partitionSessionId, TDirectReadId lastDirectReadId) {
    with_lock (Lock) {
        auto it = PartitionSessions.find(partitionSessionId);
        Y_ABORT_UNLESS(it != PartitionSessions.end());

        if (it->second.LastDirectReadId < lastDirectReadId) {
            it->second.LastDirectReadId = lastDirectReadId;
        } else {
            DeletePartitionSessionImpl(partitionSessionId);
        }
    }
}

void TDirectReadSession::DeletePartitionSession(TPartitionSessionId partitionSessionId) {
    with_lock (Lock) {
        auto it = PartitionSessions.find(partitionSessionId);
        if (it == PartitionSessions.end()) {
            // TODO(qyryq) Log it.
            return;
        }

        PartitionSessions.erase(it);

        if (PartitionSessions.empty()) {
            CloseImpl();
        }
    }
}

void TDirectReadSession::DeletePartitionSessionImpl(TPartitionSessionId partitionSessionId) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    auto it = PartitionSessions.find(partitionSessionId);
    if (it == PartitionSessions.end()) {
        // TODO(qyryq) Log it.
        return;
    }

    PartitionSessions.erase(it);

    if (PartitionSessions.empty()) {
        CloseImpl();
        ControlCallbacks->StopPartitionSession(partitionSessionId);
    }
}

void TDirectReadSession::AbortImpl(TPlainStatus&& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Abort");
    if (State < EState::CLOSING) {
        State = EState::CLOSED;
        ControlCallbacks->AbortSession(std::move(status));
    }
}

void TDirectReadSession::OnReadDone(NYdbGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration) {
    TPlainStatus errorStatus;
    if (!grpcStatus.Ok()) {
        errorStatus = TPlainStatus(std::move(grpcStatus));
    }

    TDeferredActions<false> deferred;
    with_lock (Lock) {
        if (State >= EState::CLOSING) {
            return;
        }

        if (connectionGeneration != ConnectionGeneration) {
            // TODO(qyryq) Test it.
            // Message from previous connection. Ignore.
            return;
        }

        if (errorStatus.Ok()) {
            if (IsErrorMessage(*ServerMessage)) {
                errorStatus = MakeErrorFromProto(*ServerMessage);
            } else {
                switch (ServerMessage->server_message_case()) {
                case TDirectReadServerMessage::kInitResponse:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_init_response()), deferred);
                    break;
                case TDirectReadServerMessage::kStartDirectReadPartitionSessionResponse:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_start_direct_read_partition_session_response()), deferred);
                    break;
                case TDirectReadServerMessage::kStopDirectReadPartitionSession:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_stop_direct_read_partition_session()), deferred);
                    break;
                case TDirectReadServerMessage::kDirectReadResponse:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_direct_read_response()), deferred);
                    break;
                case TDirectReadServerMessage::kUpdateTokenResponse:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_update_token_response()), deferred);
                    break;
                case TDirectReadServerMessage::SERVER_MESSAGE_NOT_SET:
                    errorStatus = TPlainStatus::Internal("Server message is not set");
                    break;
                default:
                    errorStatus = TPlainStatus::Internal("Unexpected response from server");
                    break;
                }
            }

            if (errorStatus.Ok()) {
                ReadFromProcessorImpl(deferred); // Read next.
            }
        }
    }

    if (!errorStatus.Ok()) {
        ReadSessionSettings.Counters_->Errors->Inc();

        if (!Reconnect(errorStatus)) {
            with_lock (Lock) {
                AbortImpl(std::move(errorStatus));
            }
        }
    }
}

void TDirectReadSession::SendStartRequestImpl(TPartitionSessionId id, bool delayedCall) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    auto it = PartitionSessions.find(id);

    if (it == PartitionSessions.end()) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartRequestImpl partition session not found, id=" << id);
        return;
    }

    SendStartRequestImpl(it->second, delayedCall);
}

void TDirectReadSession::SendStartRequestImpl(TDirectReadPartitionSession& partitionSession, bool delayedCall) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartRequestImpl");

    bool isImmediateCall = partitionSession.State == TDirectReadPartitionSession::EState::IDLE && !delayedCall;
    bool isDelayedCall = partitionSession.State == TDirectReadPartitionSession::EState::DELAYED && delayedCall;
    Y_ABORT_UNLESS(isImmediateCall || isDelayedCall);

    if (State < EState::WORKING) {
        if (isDelayedCall) {
            // It's time to send a delayed Start-request, but there is no working connection at the moment.
            // Reset the partition session state, so the request is sent as soon as the connection is reestablished.
            bool transitioned = partitionSession.TransitionTo(TDirectReadPartitionSession::EState::IDLE);
            Y_ABORT_UNLESS(transitioned);
        } // Otherwise, the session is already IDLE.
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartRequestImpl bail out 1");
        return;
    }

    if (State > EState::WORKING) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartRequestImpl: the session is not usable anymore");
        return;
    }

    Y_ABORT_UNLESS(State == EState::WORKING);

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartRequestImpl send request");
    bool transitioned = partitionSession.TransitionTo(TDirectReadPartitionSession::EState::STARTING);
    Y_ABORT_UNLESS(transitioned);
    WriteToProcessorImpl(partitionSession.MakeStartRequest());
}

void TDirectReadSession::DelayStartRequestImpl(TDirectReadPartitionSession& partitionSession, TPlainStatus&& status, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Y_ABORT_UNLESS(partitionSession.State == TDirectReadPartitionSession::EState::STARTING ||
                   partitionSession.State == TDirectReadPartitionSession::EState::WORKING);

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DelayStartRequestImpl");

    if (!partitionSession.RetryState) {
        partitionSession.RetryState = ReadSessionSettings.RetryPolicy_->CreateRetryState();
    }

    TMaybe<TDuration> delay = partitionSession.RetryState->GetNextRetryDelay(status.Status);
    if (!delay.Defined()) {
        AbortImpl(std::move(status));
        return;
    }

    bool transitioned = partitionSession.TransitionTo(TDirectReadPartitionSession::EState::DELAYED);
    Y_ABORT_UNLESS(transitioned);
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Send StartDirectReadPartitionSession request in " << delay);

    ControlCallbacks->ScheduleCallback(
        *delay,
        [context = this->SelfContext, id = partitionSession.PartitionSessionId]() {
            if (auto s = context->LockShared()) {
                with_lock (s->Lock) {
                    s->SendStartRequestImpl(id, /* delayedCall = */ true);
                }
            }
        },
        deferred
    );
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::InitResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Y_ABORT_UNLESS(State == EState::INITIALIZING);
    State = EState::WORKING;

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got InitResponse " << response.ShortDebugString());

    RetryState = nullptr;

    // Successful init. Send StartDirectReadPartitionSession requests.
    for (auto& [id, partitionSession] : PartitionSessions) {
        SendStartRequestImpl(partitionSession);
    }
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got StartDirectReadPartitionSessionResponse " << response.ShortDebugString());

    auto partitionSessionId = response.partition_session_id();
    // TODO(qyryq) Check response.generation().
    auto it = PartitionSessions.find(partitionSessionId);

    // TODO(qyryq) If the control session stops the partition session,
    //             and then we get a late Start-response, the iterator will point to the end.
    //             In that case, we simply need to return here.
    Y_ABORT_UNLESS(it != PartitionSessions.end());

    auto& partitionSession = it->second;

    auto transitioned = partitionSession.TransitionTo(TDirectReadPartitionSession::EState::WORKING);
    Y_ABORT_UNLESS(transitioned);
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got StopDirectReadPartitionSession " << response.ShortDebugString());

    auto partitionSessionId = response.partition_session_id();
    auto it = PartitionSessions.find(partitionSessionId);
    // TODO(qyryq) Simply ignore the Stop-request, if we do not have such a partition session?
    Y_ABORT_UNLESS(it != PartitionSessions.end());
    auto& partitionSession = it->second;

    DelayStartRequestImpl(partitionSession, MakeErrorFromProto(response), deferred);

    // TODO(qyryq) Send status/issues to the control session?
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got DirectReadResponse " << response.ShortDebugString());

    auto partitionSessionId = response.partition_session_id();
    Y_ABORT_UNLESS(partitionSessionId == response.partition_data().partition_session_id());

    auto it = PartitionSessions.find(partitionSessionId);
    Y_ABORT_UNLESS(it != PartitionSessions.end());
    auto& partitionSession = it->second;

    auto directReadId = response.direct_read_id();

    if (directReadId < partitionSession.NextDirectReadId) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got a DirectReadResponse with direct_read_id=" << directReadId
                                                 << ", but we are waiting for direct_read_id=" << partitionSession.NextDirectReadId);
        return;
    }

    Y_ABORT_UNLESS(directReadId == partitionSession.NextDirectReadId);

    partitionSession.NextDirectReadId = directReadId + 1;

    ControlCallbacks->OnDirectReadDone(std::move(response), deferred);

    // If here we get a DirectReadResponse(direct_read_id) and after that the control session receives
    // a StopPartitionSession command with the same direct_read_id, we need to stop it from the control session.

    if (partitionSession.LastDirectReadId.Defined() && partitionSession.NextDirectReadId == partitionSession.LastDirectReadId) {
        // We've already got a graceful StopPartitionSessionRequest through the control session,
        // that told us the LastDirectReadId, we should read up to.
        // We have read all available data, time to stop the partition session.
        DeletePartitionSessionImpl(partitionSessionId);
    }
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got UpdateTokenResponse " << response.ShortDebugString());
}

void TDirectReadSession::WriteToProcessorImpl(TDirectReadClientMessage&& req) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (Processor) {
        Processor->Write(std::move(req));
    }
}

void TDirectReadSession::ReadFromProcessorImpl(TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (State >= EState::CLOSING) {
        return;
    }

    if (Processor) {
        ServerMessage->Clear();

        Y_ABORT_UNLESS(this->SelfContext);

        auto callback = [cbContext = this->SelfContext,
                         // Capture message & processor not to read in freed memory.
                         serverMessage = ServerMessage,
                         connectionGeneration = ConnectionGeneration,
                         processor = Processor](NYdbGrpc::TGrpcStatus&& grpcStatus) {
            bool cancelContext = false;
            if (auto s = cbContext->LockShared()) {
                s->OnReadDone(std::move(grpcStatus), connectionGeneration);
                if (s->State == EState::CLOSED) {
                    cancelContext = true;
                }
            }
            if (cancelContext) {
                cbContext->Cancel();
            }
        };

        deferred.DeferReadFromProcessor(Processor, ServerMessage.get(), std::move(callback));
    }
}

TStringBuilder TDirectReadSession::GetLogPrefix() const {
    return TStringBuilder() << "TDirectReadSession ServerSessionId=" << ServerSessionId << " NodeId=" << NodeId << " ";
}

void TDirectReadSession::InitImpl(TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Y_ABORT_UNLESS(State == EState::CONNECTED);
    State = EState::INITIALIZING;

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Successfully connected. Initializing session");

    TDirectReadClientMessage req;
    auto& init = *req.mutable_init_request();
    init.set_session_id(ServerSessionId);
    init.set_consumer(ReadSessionSettings.ConsumerName_);

    for (const TTopicReadSettings& topic : ReadSessionSettings.Topics_) {
        auto* topicSettings = init.add_topics_read_settings();
        topicSettings->set_path(topic.Path_);
    }

    WriteToProcessorImpl(std::move(req));
    ReadFromProcessorImpl(deferred);
}

void TDirectReadSession::OnConnectTimeout(
    const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext
) {
    Y_UNUSED(connectTimeoutContext);
}

void TDirectReadSession::OnConnect(
    TPlainStatus&& status,
    IDirectReadProcessor::TPtr&& connection,
    const NYdbGrpc::IQueueClientContextPtr& connectContext
) {
    TDeferredActions<false> deferred;
    with_lock (Lock) {
        if (ConnectContext != connectContext) {
            return;
        }

        ::NYdb::NTopic::Cancel(ConnectTimeoutContext);
        ConnectContext = nullptr;
        ConnectTimeoutContext = nullptr;
        ConnectDelayContext = nullptr;

        if (State >= EState::CLOSING) {
            return;
        }

        if (status.Ok()) {
            State = EState::CONNECTED;
            Processor = std::move(connection);
            ConnectionAttemptsDone = 0;
            InitImpl(deferred);
            return;
        }
    }

    if (!status.Ok()) {
        ReadSessionSettings.Counters_->Errors->Inc();
        if (!Reconnect(status)) {
            with_lock (Lock) {
                AbortImpl(TPlainStatus(
                    status.Status,
                    MakeIssueWithSubIssues(
                        TStringBuilder() << "Failed to establish connection to server \"" << status.Endpoint << "\". Attempts done: " << ConnectionAttemptsDone,
                        status.Issues)));
            }
        }
    }
}

bool TDirectReadSession::Reconnect(const TPlainStatus& status) {
    // TODO(qyryq) Are concurrent calls possible here?

    TDuration delay = TDuration::Zero();

    // Previous operations contexts.
    NYdbGrpc::IQueueClientContextPtr prevConnectContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectDelayContext;

    // Callbacks
    std::function<void(TPlainStatus&&, IDirectReadProcessor::TPtr&&)> connectCallback;
    std::function<void(bool)> connectTimeoutCallback;

    if (!status.Ok()) {
        LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Got error. Status: " << status.Status
                                               << ". Description: " << IssuesSingleLineString(status.Issues));
    }

    NYdbGrpc::IQueueClientContextPtr connectContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectTimeoutContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectDelayContext = nullptr;

    with_lock (Lock) {
        if (State >= EState::CLOSING) {
            return false;
        }

        connectContext = ClientContext->CreateContext();
        connectTimeoutContext = ClientContext->CreateContext();
        if (!connectContext || !connectTimeoutContext) {
            return false;
        }

        State = EState::CONNECTING;
        for (auto& [_, partitionSession] : PartitionSessions) {
            if (partitionSession.State != TDirectReadPartitionSession::EState::DELAYED) {
                bool transitioned = partitionSession.TransitionTo(TDirectReadPartitionSession::EState::IDLE);
                Y_ABORT_UNLESS(transitioned);
            }
        }

        if (Processor) {
            Processor->Cancel();
        }

        Processor = nullptr;
        // TODO(qyryq) WaitingReadResponse = false;
        ServerMessage = std::make_shared<TDirectReadServerMessage>();
        ++ConnectionGeneration;

        if (!status.Ok()) {
            if (!RetryState) {
                RetryState = ReadSessionSettings.RetryPolicy_->CreateRetryState();
            }
            TMaybe<TDuration> nextDelay = RetryState->GetNextRetryDelay(status.Status);
            if (!nextDelay) {
                return false;
            }
            delay = *nextDelay;
            connectDelayContext = ClientContext->CreateContext();
            if (!connectDelayContext) {
                return false;
            }
        }

        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Reconnecting direct read session to node " << NodeId << " in " << delay);

        ++ConnectionAttemptsDone;

        // Set new context
        prevConnectContext = std::exchange(ConnectContext, connectContext);
        prevConnectTimeoutContext = std::exchange(ConnectTimeoutContext, connectTimeoutContext);
        prevConnectDelayContext = std::exchange(ConnectDelayContext, connectDelayContext);

        Y_ASSERT(ConnectContext);
        Y_ASSERT(ConnectTimeoutContext);
        Y_ASSERT((delay == TDuration::Zero()) == !ConnectDelayContext);
        Y_ABORT_UNLESS(this->SelfContext);

        connectCallback =
            [cbContext = this->SelfContext, connectContext]
            (TPlainStatus&& st, IDirectReadProcessor::TPtr&& connection) {
                if (auto self = cbContext->LockShared()) {
                    self->OnConnect(std::move(st), std::move(connection), connectContext);
                }
            };

        connectTimeoutCallback =
            [cbContext = this->SelfContext, connectTimeoutContext](bool ok) {
                if (ok) {
                    if (auto self = cbContext->LockShared()) {
                        self->OnConnectTimeout(connectTimeoutContext);
                    }
                }
            };
    }

    // Cancel previous operations.
    ::NYdb::NTopic::Cancel(prevConnectContext);
    ::NYdb::NTopic::Cancel(prevConnectTimeoutContext);
    ::NYdb::NTopic::Cancel(prevConnectDelayContext);

    Y_ASSERT(connectContext);
    Y_ASSERT(connectTimeoutContext);
    Y_ASSERT((delay == TDuration::Zero()) == !connectDelayContext);
    ProcessorFactory->CreateProcessor(
        std::move(connectCallback),
        TRpcRequestSettings::Make(ReadSessionSettings, TEndpointKey(NodeId)),
        std::move(connectContext),
        TDuration::Seconds(30) /* connect timeout */, // TODO: make connect timeout setting.
        std::move(connectTimeoutContext),
        std::move(connectTimeoutCallback),
        delay,
        std::move(connectDelayContext));
    return true;
}

}
