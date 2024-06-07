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
    DIRECT_READ_SESSION_STATE_NAME_OUT(IDLE)
    DIRECT_READ_SESSION_STATE_NAME_OUT(STARTING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(WORKING)
    DIRECT_READ_SESSION_STATE_NAME_OUT(DELAYED)
    }

#undef DIRECT_READ_SESSION_STATE_NAME_OUT
}


template<>
void Out<NYdb::NTopic::TDirectReadPartitionSession>(IOutputStream& o, NYdb::NTopic::TDirectReadPartitionSession const& session) {
    o << "{ PartitionSessionId: \"" << session.PartitionSessionId << "\"";
    o << ", Location: " << session.Location << "";
    o << ", State: " << session.State << "";
    o << ", PrevDirectReadId: " << session.PrevDirectReadId << "";
    o << ", LastDirectReadId: " << session.LastDirectReadId << "";
    o << " }";
}

namespace NYdb::NTopic {

TDirectReadClientMessage TDirectReadPartitionSession::MakeStartRequest() const {
    TDirectReadClientMessage req;
    auto& start = *req.mutable_start_direct_read_partition_session_request();
    start.set_partition_session_id(PartitionSessionId);
    start.set_last_direct_read_id(PrevDirectReadId);
    start.set_generation(Location.GetGeneration());
    return req;
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
    for (auto& [_, nodeSession] : NodeSessions) {
        nodeSession->Cancel();
    }
}

void TDirectReadSessionManager::StartPartitionSession(TDirectReadPartitionSession&& partitionSession) {
    with_lock (Lock) {
        StartPartitionSessionImpl(std::move(partitionSession));
    }
}

void TDirectReadSessionManager::StartPartitionSessionImpl(TDirectReadPartitionSession&& partitionSession) {
    auto nodeId = partitionSession.Location.GetNodeId();
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "StartPartitionSession " << partitionSession);
    TDirectReadSessionContextPtr& session = NodeSessions[nodeId];
    if (!session) {
        session = CreateDirectReadSession(nodeId);
    }
    if (auto s = session->LockShared()) {
        s->Start();
        s->AddPartitionSession(std::move(partitionSession));
    }
}

void TDirectReadSessionManager::DeleteNodeSessionIfEmptyImpl(TNodeId nodeId) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeleteNodeSessionIfEmpty " << nodeId);

    auto it = NodeSessions.find(nodeId);
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
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeleteNodeSessionIfEmpty deleted node " << nodeId);
        NodeSessions.erase(it);
    } else {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeleteNodeSessionIfEmpty did not delete node " << nodeId << " as it is not empty");
    }
}

void TDirectReadSessionManager::DeletePartitionSessionImpl(TPartitionSessionId id, TNodeSessionsMap::iterator it) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "DeletePartitionSession " << id);

    if (auto session = it->second->LockShared()) {
        session->DeletePartitionSession(id);
        if (session->Empty()) {
            session->Close();
            NodeSessions.erase(it);
        }
    }
}

void TDirectReadSessionManager::UpdatePartitionSession(TPartitionSessionId partitionSessionId, TPartitionLocation newLocation) {
    with_lock (Lock) {
        auto locIt = Locations.find(partitionSessionId);
        Y_ABORT_UNLESS(locIt != Locations.end());
        auto oldNodeId = locIt->second.GetNodeId();

        auto sessionIt = NodeSessions.find(oldNodeId);
        Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

        DeletePartitionSessionImpl(partitionSessionId, sessionIt);

        // TODO(qyryq) std::move an old RetryState?
        StartPartitionSessionImpl({ .PartitionSessionId = partitionSessionId, .Location = newLocation });
    }
}

void TDirectReadSessionManager::StopPartitionSession(TPartitionSessionId partitionSessionId) {
    with_lock (Lock) {
        auto locIt = Locations.find(partitionSessionId);
        Y_ABORT_UNLESS(locIt != Locations.end());

        auto sessionIt = NodeSessions.find(locIt->second.GetNodeId());
        Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

        DeletePartitionSessionImpl(partitionSessionId, sessionIt);
    }
}

void TDirectReadSessionManager::StopPartitionSessionGracefully(TPartitionSessionId partitionSessionId, i64 committedOffset, TDirectReadId lastDirectReadId) {
    with_lock (Lock) {
        auto locIt = Locations.find(partitionSessionId);
        Y_ABORT_UNLESS(locIt != Locations.end());

        auto sessionIt = NodeSessions.find(locIt->second.GetNodeId());
        Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

        if (auto session = sessionIt->second->LockShared()) {
            session->SetLastDirectReadId(partitionSessionId, committedOffset, lastDirectReadId);
        }
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
        State = EState::CONNECTING;
    }
    Reconnect(TPlainStatus());
}

void TDirectReadSession::Close() {
    with_lock (Lock) {
        if (State >= EState::CLOSING) {
            return;
        }
        State = EState::CLOSED;
    }
}

bool TDirectReadSession::Empty() const {
    with_lock (Lock) {
        return PartitionSessions.empty();
    }
}

/* XXXXX
    TDirectReadSessionCallbacks {
        .OnDirectReadDone = [context = this->SelfContext](Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred) {
            if (auto s = context->LockShared()) {
                s->OnDirectReadDone(std::move(response), deferred);
            }
        },
        .OnAbortSession = [context = this->SelfContext](TSessionClosedEvent&& closeEvent) {
            if (auto s = context->LockShared()) {
                s->AbortSession(std::move(closeEvent));
            }
        },
        .OnSchedule = ScheduleCallback,
    },
*/

void TDirectReadSession::AddPartitionSession(TDirectReadPartitionSession&& session) {
    TDeferredActions<false> deferred;
    with_lock (Lock) {
        Y_ABORT_UNLESS(State < EState::CLOSING);

        auto [it, inserted] = PartitionSessions.emplace(session.PartitionSessionId, std::move(session));
        // TODO(qyryq) Abort? Ignore new? Replace old? Anything else?
        Y_ABORT_UNLESS(inserted);

        SendStartDirectReadPartitionSessionImpl(it->second, TPlainStatus(), deferred);
    }
}

void TDirectReadSession::SetLastDirectReadId(TPartitionSessionId partitionSessionId, i64 committedOffset, TDirectReadId lastDirectReadId) {
    with_lock (Lock) {
        auto it = PartitionSessions.find(partitionSessionId);
        Y_ABORT_UNLESS(it != PartitionSessions.end());
        it->second.LastDirectReadId = lastDirectReadId;
        it->second.CommittedOffset = committedOffset;
    }
}

void TDirectReadSession::DeletePartitionSession(TPartitionSessionId partitionSessionId) {
    with_lock (Lock) {
        DeletePartitionSessionImpl(partitionSessionId);
    }
}

void TDirectReadSession::DeletePartitionSessionImpl(TPartitionSessionId partitionSessionId) {
    PartitionSessions.erase(partitionSessionId);
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

void TDirectReadSession::SendStartDirectReadPartitionSessionImpl(TPartitionSessionId id, TPlainStatus&& status, TDeferredActions<false>& deferred, bool delayedCall) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    auto it = PartitionSessions.find(id);

    if (it == PartitionSessions.end()) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl no partition session found, id=" << id);
        return;
    }

    SendStartDirectReadPartitionSessionImpl(it->second, std::move(status), deferred, delayedCall);
}

void TDirectReadSession::SendStartDirectReadPartitionSessionImpl(
    TDirectReadPartitionSession& partitionSession, TPlainStatus&& status, TDeferredActions<false>& deferred, bool delayedCall
) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (State < EState::WORKING && partitionSession.State == TDirectReadPartitionSession::EState::DELAYED && delayedCall) {
        // It's time to send a delayed Start-request, but there is no working connection at the moment.
        // Reset the partition session state, so the request is sent as soon as the connection is reestablished.
        partitionSession.State = TDirectReadPartitionSession::EState::IDLE;
        partitionSession.RetryState = nullptr;
        return;
    }

    // Send the StartDirectReadPartitionSession request only if we're already connected.
    // In other cases adding the session to PartitionSessions is enough,
    // the request will be sent from OnReadDoneImpl(InitDirectReadResponse).
    bool sendOrSchedule = State == EState::WORKING && (
            partitionSession.State == TDirectReadPartitionSession::EState::IDLE && !delayedCall ||
            partitionSession.State == TDirectReadPartitionSession::EState::DELAYED && delayedCall);

    if (!sendOrSchedule) {
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl bail out, State=" << State
                                                 << " partitionSession.State=" << partitionSession.State
                                                 << " delayedCall=" << delayedCall);
        return;
    }

    if (status.Ok()) {
        partitionSession.State = TDirectReadPartitionSession::EState::STARTING;
        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl send request");
        WriteToProcessorImpl(partitionSession.MakeStartRequest());
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

    // TODO(qyryq) What if the delay is zero? Schedule or call SendStartDirectReadPartitionSessionImpl right away?

    partitionSession.State = TDirectReadPartitionSession::EState::DELAYED;
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "SendStartDirectReadPartitionSessionImpl retry in " << delay);

    ControlCallbacks->ScheduleCallback(
        *delay,
        [context = this->SelfContext, id = partitionSession.PartitionSessionId]() {
            if (auto s = context->LockShared()) {
                TDeferredActions<false> deferred;
                with_lock (s->Lock) {
                    s->SendStartDirectReadPartitionSessionImpl(id, TPlainStatus(), deferred, /* delayedCall = */ true);
                }
            }
        },
        deferred
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
        SendStartDirectReadPartitionSessionImpl(session, TPlainStatus(), deferred);
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

    partitionSession.State = TDirectReadPartitionSession::EState::WORKING;
    partitionSession.RetryState = nullptr;
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got StopDirectReadPartitionSession " << response.ShortDebugString());

    auto partitionSessionId = response.partition_session_id();
    auto it = PartitionSessions.find(partitionSessionId);
    Y_ABORT_UNLESS(it != PartitionSessions.end());

    TPlainStatus errorStatus = MakeErrorFromProto(response);

    auto& partitionSession = it->second;
    partitionSession.State = TDirectReadPartitionSession::EState::IDLE;

    SendStartDirectReadPartitionSessionImpl(partitionSession, std::move(errorStatus), deferred);

    // TODO(qyryq) Send status/issues to the control session?
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Got DirectReadResponse " << response.ShortDebugString());

    auto it = PartitionSessions.find(response.partition_session_id());
    Y_ABORT_UNLESS(it != PartitionSessions.end());
    auto& partitionSession = it->second;
    partitionSession.PrevDirectReadId = response.direct_read_id();

    ControlCallbacks->OnDirectReadDone(std::move(response), deferred);

    // TODO(qyryq) Ситуация: получаем DirectReadResponse, после этого приходит в контрольной сессии
    // StopPartitionSession с тем же идентификатором direct_read_id.
    // Следующий код в этом случае уже не выполнится. Кто должен отправить клиенту TStopPartitionSessionEvent?

    if (partitionSession.LastDirectReadId.Defined() && partitionSession.PrevDirectReadId + 1 == partitionSession.LastDirectReadId) {
        // We've already got a graceful StopPartitionSessionRequest through the control session,
        // that told us the LastDirectReadId, we should read up to.
        // We have read all available data, time to stop the partition session.
        DeletePartitionSessionImpl(partitionSession.PartitionSessionId);

        // ControlCallbacks.StopPartitionSession(partitionSession.PartitionSessionId);

        if (Empty()) {
            // ControlCallbacks.DeleteNodeSessionIfEmpty(NodeId);
        }
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
            if (auto s = cbContext->LockShared()) {
                s->OnReadDone(std::move(grpcStatus), connectionGeneration);
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
    IDirectReadProcessor::TPtr&& connection,
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
            Processor = std::move(connection);
            ConnectionAttemptsDone = 0;
            InitImpl(deferred);
            return;
        }
    }

    if (!st.Ok()) {
        ReadSessionSettings.Counters_->Errors->Inc();
        if (!Reconnect(st)) {
            with_lock (Lock) {
                AbortImpl(TPlainStatus(
                    st.Status,
                    MakeIssueWithSubIssues(
                        TStringBuilder() << "Failed to establish connection to server \"" << st.Endpoint << "\". Attempts done: " << ConnectionAttemptsDone,
                        st.Issues)));
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

        State = EState::CONNECTING;
        for (auto& [_, partitionSession] : PartitionSessions) {
            if (partitionSession.State >= TDirectReadPartitionSession::EState::STARTING) {
                partitionSession.State = TDirectReadPartitionSession::EState::IDLE;
                partitionSession.RetryState = nullptr;
            }
        }

        if (Processor) {
            Processor->Cancel();
        }

        Processor = nullptr;
        // TODO(qyryq) WaitingReadResponse = false;
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
    Y_ASSERT((delay == TDuration::Zero()) == !delayContext);
    ProcessorFactory->CreateProcessor(
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
