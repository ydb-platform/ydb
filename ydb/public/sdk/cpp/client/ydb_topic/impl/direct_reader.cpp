#include "direct_reader.h"
#include "read_session_impl.ipp"

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

template<>
void Out<NYdb::NTopic::TDirectReadSession::EState>(IOutputStream& o, NYdb::NTopic::TDirectReadSession::EState state) {
    #define DIRECT_READ_SESSION_STATE_NAME_OUT(state) case NYdb::NTopic::TDirectReadSession::EState::state: o << #state; break;

    switch (state) {
    DIRECT_READ_SESSION_STATE_NAME_OUT(CREATED)
    DIRECT_READ_SESSION_STATE_NAME_OUT(CONNECTING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(CONNECTED)
    DIRECT_READ_SESSION_STATE_NAME_OUT(INITIALIZING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(WORKING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(CLOSING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(CLOSED)
    }

    #undef DIRECT_READ_SESSION_STATE_NAME_OUT
}

template<>
void Out<NYdb::NTopic::TDirectReadPartitionSession::EState>(IOutputStream& o, NYdb::NTopic::TDirectReadPartitionSession::EState state) {
    #define DIRECT_READ_SESSION_STATE_NAME_OUT(state) case NYdb::NTopic::TDirectReadPartitionSession::EState::state: o << #state; break;

    switch (state) {
    DIRECT_READ_SESSION_STATE_NAME_OUT(CREATED)
    DIRECT_READ_SESSION_STATE_NAME_OUT(STARTING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(STARTED)
    }

    #undef DIRECT_READ_SESSION_STATE_NAME_OUT
}


template<>
void Out<NYdb::NTopic::TDirectReadPartitionSession>(IOutputStream& o, NYdb::NTopic::TDirectReadPartitionSession const& session) {
    o << "{ PartitionSessionId: \"" << session.PartitionSessionId << "\"";
    o << ", Location: " << session.Location << "";
    o << ", State: " << session.State << "";
    o << ", LastDirectReadId: " << session.LastDirectReadId << "";
    o << " }";
}

namespace NYdb::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadConnectionManager

TDirectReadSessionManager::TDirectReadSessionManager(
    TServerSessionId serverSessionId,
    const NYdb::NTopic::TReadSessionSettings settings,
    // TSingleClusterReadSessionPtr<false> singleClusterReadSession,
    TDirectReadSessionCallbacks callbacks,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadConnectionFactoryPtr connectionFactory,
    TLog log
)
    : ReadSessionSettings(settings)
    , ServerSessionId(serverSessionId)
    , Callbacks(callbacks)
    , ClientContext(clientContext)
    , ConnectionFactory(connectionFactory)
    , Log(log)
    {}

TStringBuilder TDirectReadSessionManager::GetLogPrefix() const {
    return TStringBuilder() << "TDirectReadSessionManager ServerSessionId=" << ServerSessionId << " ";
}

TDirectReadSessionCbContextPtr TDirectReadSessionManager::CreateDirectReadSession(TNodeId nodeId) {
    return MakeWithCallbackContext<TDirectReadSession>(
        nodeId,
        ServerSessionId,
        ReadSessionSettings,
        Callbacks,
        ClientContext->CreateContext(),
        ConnectionFactory,
        Log);
}

void TDirectReadSessionManager::Close() {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Close");
    for (auto& [_, nodeSession] : NodeSessions) {
        nodeSession->Cancel();
    }
}

void TDirectReadSessionManager::StartPartitionSession(TDirectReadPartitionSession&& partitionSession) {
    auto nodeId = partitionSession.Location.GetNodeId();
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "StartPartitionSession " << partitionSession);
    TDirectReadSessionCbContextPtr& session = NodeSessions[nodeId];
    if (!session) {
        session = CreateDirectReadSession(nodeId);
    }
    if (auto s = session->LockShared()) {
        s->Start();
        s->AddPartitionSession(std::move(partitionSession));
    }
}

void TDirectReadSessionManager::DeleteNodeSessionIfEmpty(TNodeId id) {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeleteNodeSessionIfEmpty " << id);

    auto it = NodeSessions.find(id);
    Y_ABORT_UNLESS(it != NodeSessions.end());

    bool erase = false;

    if (auto session = it->second->LockShared()) {
        if (session->Empty()) {
            erase = true;
            session->Close();
        }
    } else {
        erase = true;
    }

    if (erase) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeleteNodeSessionIfEmpty deleted node " << id);
        NodeSessions.erase(it);
    } else {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeleteNodeSessionIfEmpty did not delete node " << id << " as it is not empty");
    }
}

void TDirectReadSessionManager::DeletePartitionSession(TPartitionSessionId id, TNodeSessionsMap::iterator it) {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeletePartitionSession " << id);

    if (auto session = it->second->LockShared()) {
        session->DeletePartitionSession(id);
        if (session->Empty()) {
            session->Close();
            NodeSessions.erase(it);
        }
    }
}

void TDirectReadSessionManager::UpdatePartitionSession(TPartitionSessionId id, TPartitionLocation newLocation) {
    auto locIt = Locations.find(id);
    Y_ABORT_UNLESS(locIt != Locations.end());
    auto oldNodeId = locIt->second.GetNodeId();

    auto sessionIt = NodeSessions.find(oldNodeId);
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    // TODO(qyryq) Delete the "same node" special case.
    if (oldNodeId == newLocation.GetNodeId()) {
        if (auto s = sessionIt->second->LockShared()) {
            s->UpdatePartitionSessionGeneration(id, newLocation);
        }
    } else {
        DeletePartitionSession(id, sessionIt);

        // TODO(qyryq) std::move an old RetryState?
        StartPartitionSession({ .PartitionSessionId = id, .Location = newLocation });
    }
}

void TDirectReadSessionManager::StopPartitionSession(TPartitionSessionId id) {
    auto locIt = Locations.find(id);
    Y_ABORT_UNLESS(locIt != Locations.end());

    auto sessionIt = NodeSessions.find(locIt->second.GetNodeId());
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    DeletePartitionSession(id, sessionIt);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadConnection

TDirectReadSession::TDirectReadSession(
    TNodeId nodeId,
    TString serverSessionId,
    const NYdb::NTopic::TReadSessionSettings settings,
    TDirectReadSessionCallbacks callbacks,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadConnectionFactoryPtr connectionFactory,
    TLog log
)
    : ClientContext(clientContext)
    , ReadSessionSettings(settings)
    , Callbacks(callbacks)
    , ServerSessionId(serverSessionId)
    , ConnectionFactory(connectionFactory)
    , State(EState::CREATED)
    , NodeId(nodeId)
    , Log(log)
    {
    }

void TDirectReadSession::Start()  {
    with_lock (Lock) {
        if (State != EState::CREATED) {
            return;
        }
        State = EState::CONNECTING;
    }
    Reconnect(TPlainStatus());
}

void TDirectReadSession::Close() {
    with_lock (Lock) {
        if (State < EState::CLOSING) {
            State = EState::CLOSED;
        }
    }
}

bool TDirectReadSession::Empty() const {
    with_lock (Lock) {
        return PartitionSessions.empty();
    }
}

void TDirectReadSession::AddPartitionSession(TDirectReadPartitionSession&& session) {
    with_lock (Lock) {
        Y_ABORT_UNLESS(State < EState::CLOSING);

        auto [it, inserted] = PartitionSessions.emplace(session.PartitionSessionId, std::move(session));
        Y_ABORT_UNLESS(inserted);

        SendStartDirectReadPartitionSessionImpl(it->second, TPlainStatus());
    }
}

void TDirectReadSession::UpdatePartitionSessionGeneration(TPartitionSessionId id, TPartitionLocation location) {
    with_lock (Lock) {
        auto it = PartitionSessions.find(id);
        Y_ABORT_UNLESS(it != PartitionSessions.end());

        // TODO(qyryq) Add TPartitionLocation::SetGeneration method?
        it->second = {
            .PartitionSessionId = id,
            .Location = location,
            // TODO(qyryq) Reset RetryState?
            .RetryState = std::move(it->second.RetryState),
        };

        SendStartDirectReadPartitionSessionImpl(it->second, TPlainStatus());
    }
}

void TDirectReadSession::DeletePartitionSession(TPartitionSessionId id) {
    with_lock (Lock) {
        PartitionSessions.erase(id);
    }
}

void TDirectReadSession::AbortImpl(TSessionClosedEvent&& closeEvent) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Abort");
    bool doCallback = false;
    if (State < EState::CLOSING) {
        State = EState::CLOSED;
        doCallback = true;
    }
    if (doCallback) {
        Callbacks.OnAbortSession(std::move(closeEvent));
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
            return; // Message from previous connection. Ignore.
        }

        if (errorStatus.Ok()) {
            if (IsErrorMessage(*ServerMessage)) {
                errorStatus = MakeErrorFromProto(*ServerMessage);
            } else {
                switch (ServerMessage->server_message_case()) {
                case TDirectReadServerMessage::kInitDirectReadResponse:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_init_direct_read_response()), deferred);
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

void TDirectReadSession::SendStartDirectReadPartitionSessionImpl(TPartitionSessionId id, TPlainStatus&& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto it = PartitionSessions.find(id);

    if (it == PartitionSessions.end()) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl no partition session found, id=" << id);
        return;
    }

    SendStartDirectReadPartitionSessionImpl(it->second, std::move(status));
}

void TDirectReadSession::SendStartDirectReadPartitionSessionImpl(TDirectReadPartitionSession& partitionSession, TPlainStatus&& status) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    // Send the StartDirectReadPartitionSession request only if we're already connected.
    // In other cases adding the session to PartitionSessions is enough,
    // the request will be sent from OnReadDoneImpl(InitDirectReadResponse).
    if (State != EState::WORKING || partitionSession.State != TDirectReadPartitionSession::EState::CREATED) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl bail out, State=" << State
                                                << " partitionSession.State=" << partitionSession.State);
        return;
    }

    if (status.Ok()) {
        partitionSession.State = TDirectReadPartitionSession::EState::STARTING;

        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl send request");

        TDirectReadClientMessage req;
        auto& start = *req.mutable_start_direct_read_partition_session_request();
        start.set_partition_session_id(partitionSession.PartitionSessionId);
        start.set_last_direct_read_id(partitionSession.LastDirectReadId);
        start.set_generation(partitionSession.Location.GetGeneration());
        WriteToProcessorImpl(std::move(req));

        return;
    }

    if (!partitionSession.RetryState) {
        partitionSession.RetryState = ReadSessionSettings.RetryPolicy_->CreateRetryState();
    }
    TMaybe<TDuration> delay = partitionSession.RetryState->GetNextRetryDelay(status.Status);
    if (!delay.Defined()) {
        AbortImpl(std::move(status));
        return;
    }

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl retry in " << delay);

    Callbacks.OnSchedule(
        *delay,
        [id = partitionSession.PartitionSessionId, cbContext = this->SelfContext]() {
            if (auto s = cbContext->LockShared()) {
                s->SendStartDirectReadPartitionSessionImpl(id, TPlainStatus());
            }
        }
    );
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::InitDirectReadResponse&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Y_ABORT_UNLESS(State == EState::INITIALIZING);

    State = EState::WORKING;

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got InitDirectReadResponse " << response.ShortDebugString());

    RetryState = nullptr;

    // Successful init. Send StartDirectReadPartitionSession requests.
    for (auto& [id, session] : PartitionSessions) {
        SendStartDirectReadPartitionSessionImpl(session, TPlainStatus());
    }

    ReadFromProcessorImpl(deferred);
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got StartDirectReadPartitionSessionResponse " << response.ShortDebugString());

    auto partitionSessionId = response.partition_session_id();
    auto it = PartitionSessions.find(partitionSessionId);
    Y_ABORT_UNLESS(it != PartitionSessions.end());
    auto& partitionSession = it->second;

    partitionSession.RetryState = nullptr;
    partitionSession.State = TDirectReadPartitionSession::EState::STARTED;
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got StopDirectReadPartitionSession " << response.ShortDebugString());

    auto partitionSessionId = response.partition_session_id();
    auto it = PartitionSessions.find(partitionSessionId);
    Y_ABORT_UNLESS(it != PartitionSessions.end());

    TPlainStatus errorStatus = MakeErrorFromProto(response);

    auto& partitionSession = it->second;
    partitionSession.State = TDirectReadPartitionSession::EState::CREATED;

    SendStartDirectReadPartitionSessionImpl(partitionSession, std::move(errorStatus));

    // TODO(qyryq) Send status/issues to the control session?
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got DirectReadResponse " << response.ShortDebugString());

    auto it = PartitionSessions.find(response.partition_session_id());
    Y_ABORT_UNLESS(it != PartitionSessions.end());
    it->second.LastDirectReadId = response.direct_read_id();

    Callbacks.OnDirectReadDone(std::move(response), deferred);
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got UpdateTokenResponse " << response.ShortDebugString());
}

void TDirectReadSession::WriteToProcessorImpl(TDirectReadClientMessage&& req) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (Connection) {
        Connection->Write(std::move(req));
    }
}

void TDirectReadSession::ReadFromProcessorImpl(TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (State >= EState::CLOSING) {
        return;
    }

    if (Connection) {
        ServerMessage->Clear();

        Y_ABORT_UNLESS(this->SelfContext);

        auto callback = [cbContext = this->SelfContext,
                         // Capture message & processor not to read in freed memory.
                         serverMessage = ServerMessage,
                         connectionGeneration = ConnectionGeneration,
                         connection = Connection](NYdbGrpc::TGrpcStatus&& grpcStatus) {
            if (auto borrowedSelf = cbContext->LockShared()) {
                borrowedSelf->OnReadDone(std::move(grpcStatus), connectionGeneration);
            }
        };

        deferred.DeferReadFromProcessor(Connection, ServerMessage.get(), std::move(callback));
    }
}

TStringBuilder TDirectReadSession::GetLogPrefix() const {
    return TStringBuilder() << "TDirectReadSession ServerSessionId=" << ServerSessionId << " NodeId=" << NodeId << " ";
}

void TDirectReadSession::InitImpl(TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Y_ABORT_UNLESS(State == EState::CONNECTED);

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Successfully connected. Initializing session");

    State = EState::INITIALIZING;

    TDirectReadClientMessage req;
    auto& init = *req.mutable_init_direct_read_request();
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
    TPlainStatus&& st,
    IDirectReadConnection::TPtr&& connection,
    const NYdbGrpc::IQueueClientContextPtr& connectContext
) {
    State = EState::CONNECTED;
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

        if (st.Ok()) {
            Connection = std::move(connection);
            ConnectionAttemptsDone = 0;
            InitImpl(deferred);
            return;
        }
    }

    if (!st.Ok()) {
        ReadSessionSettings.Counters_->Errors->Inc();
        if (!Reconnect(st)) {
            with_lock (Lock) {
                AbortImpl(TSessionClosedEvent(
                    st.Status,
                    MakeIssueWithSubIssues(
                        TStringBuilder() << "Failed to establish connection to server \"" << st.Endpoint << "\". Attempts done: " << ConnectionAttemptsDone,
                        st.Issues)));
            }
        }
    }
}

bool TDirectReadSession::Reconnect(const TPlainStatus& status) {
    State = EState::CONNECTING;
    TDuration delay = TDuration::Zero();

    // Previous operations contexts.
    NYdbGrpc::IQueueClientContextPtr prevConnectContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectDelayContext;

    // Callbacks
    std::function<void(TPlainStatus&&, IDirectReadConnection::TPtr&&)> connectCallback;
    std::function<void(bool)> connectTimeoutCallback;

    if (!status.Ok()) {
        LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Got error. Status: " << status.Status
                                               << ". Description: " << IssuesSingleLineString(status.Issues));
    }

    NYdbGrpc::IQueueClientContextPtr delayContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectTimeoutContext = nullptr;

    with_lock (Lock) {
        connectContext = ClientContext->CreateContext();
        connectTimeoutContext = ClientContext->CreateContext();
        if (!connectContext || !connectTimeoutContext) {
            return false;
        }

        if (State == EState::CLOSING) {
            ::NYdb::NTopic::Cancel(connectContext);
            ::NYdb::NTopic::Cancel(connectTimeoutContext);
            return false;
        }
        Connection = nullptr;
        // WaitingReadResponse = false;
        ServerMessage = std::make_shared<TDirectReadServerMessage>();
        ++ConnectionGeneration;

        if (!RetryState) {
            RetryState = ReadSessionSettings.RetryPolicy_->CreateRetryState();
        }

        if (!status.Ok()) {
            TMaybe<TDuration> nextDelay = RetryState->GetNextRetryDelay(status.Status);
            if (!nextDelay) {
                return false;
            }
            delay = *nextDelay;
            delayContext = ClientContext->CreateContext();
            if (!delayContext) {
                return false;
            }
        }

        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Reconnecting direct read session to node " << NodeId << " in " << delay);

        ++ConnectionAttemptsDone;

        // Set new context
        prevConnectContext = std::exchange(ConnectContext, connectContext);
        prevConnectTimeoutContext = std::exchange(ConnectTimeoutContext, connectTimeoutContext);
        prevConnectDelayContext = std::exchange(ConnectDelayContext, delayContext);

        Y_ASSERT(ConnectContext);
        Y_ASSERT(ConnectTimeoutContext);
        Y_ASSERT((delay == TDuration::Zero()) == !ConnectDelayContext);
        Y_ABORT_UNLESS(this->SelfContext);

        connectCallback =
            [cbContext = this->SelfContext, connectContext]
            (TPlainStatus&& st, IDirectReadConnection::TPtr&& connection) {
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
    Y_ASSERT((delay == TDuration::Zero()) == !delayContext);
    ConnectionFactory->CreateProcessor(
        std::move(connectCallback),
        TRpcRequestSettings::Make(ReadSessionSettings, TEndpointKey(NodeId)),
        std::move(connectContext),
        TDuration::Seconds(30) /* connect timeout */, // TODO: make connect timeout setting.
        std::move(connectTimeoutContext),
        std::move(connectTimeoutCallback),
        delay,
        std::move(delayContext));
    return true;
}

}
