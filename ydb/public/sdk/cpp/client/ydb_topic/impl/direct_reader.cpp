#include "direct_reader.h"
#include "read_session_impl.ipp"

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

namespace NYdb::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadConnectionManager

TDirectReadSessionManager::TDirectReadSessionManager(
    TServerSessionId serverSessionId,
    const NYdb::NTopic::TReadSessionSettings settings,
    TSingleClusterReadSessionPtr<false> singleClusterReadSession,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadConnectionFactoryPtr connectionFactory,
    TLog log
)
    : ReadSessionSettings(settings)
    , ServerSessionId(serverSessionId)
    , SingleClusterReadSession(singleClusterReadSession)
    , ClientContext(clientContext)
    , ConnectionFactory(connectionFactory)
    , Log(log)
    {}

TStringBuilder TDirectReadSessionManager::GetLogPrefix() const {
    return TStringBuilder() << "TDirectReadSessionManager ServerSessionId=" << ServerSessionId << " ";
}

TDirectReadSessionPtr TDirectReadSessionManager::CreateDirectReadSession(TNodeId nodeId) {
    return MakeWithCallbackContext<TDirectReadSession>(
        nodeId, ServerSessionId, ReadSessionSettings, SingleClusterReadSession, ClientContext->CreateContext(), ConnectionFactory, Log);
}

void TDirectReadSessionManager::Close() {
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Close");
}

void TDirectReadSessionManager::StartPartitionSession(TDirectReadPartitionSession&& partitionSession) {
    TDirectReadSessionPtr session;
    auto nodeId = partitionSession.Location.GetNodeId();

    session = NodeSessions[nodeId];
    if (!session) {
        session = CreateDirectReadSession(nodeId);
    }
    if (auto s = session->LockShared()) {
        s->Start();
        s->AddPartitionSession(std::move(partitionSession));
    }
}

void TDirectReadSessionManager::DeletePartitionSession(TPartitionSessionId id, TNodeSessionsMap::iterator it) {
    if (auto session = it->second->LockShared()) {
        session->DeletePartitionSession(id);
        if (session->Empty()) {
            session->Close();
            NodeSessions.erase(it);
        }
    }
}

void TDirectReadSessionManager::UpdatePartitionSession(TPartitionSessionId id, TPartitionLocation location) {
    auto locIt = Locations.find(id);
    Y_ABORT_UNLESS(locIt != Locations.end());

    auto sessionIt = NodeSessions.find(locIt->second.GetNodeId());
    Y_ABORT_UNLESS(sessionIt != NodeSessions.end());

    if (locIt->second.GetNodeId() == location.GetNodeId()) {
        if (auto session = sessionIt->second->LockShared()) {
            session->UpdatePartitionSessionGeneration(id, location);
        }
    } else {
        DeletePartitionSession(id, sessionIt);

        // TODO(qyryq) std::move an old RetryState?
        StartPartitionSession({ .Id = id, .Location = location });
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
    TSingleClusterReadSessionPtr<false> singleClusterReadSession,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadConnectionFactoryPtr connectionFactory,
    TLog log
)
    : ClientContext(clientContext)
    , ReadSessionSettings(settings)
    , SingleClusterReadSession(singleClusterReadSession)
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
        State = EState::CLOSING;
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

        auto [it, inserted] = PartitionSessions.emplace(session.Id, std::move(session));
        Y_ABORT_UNLESS(inserted);

        SendStartDirectReadPartitionSessionImpl(it->second);
    }
}

void TDirectReadSession::UpdatePartitionSessionGeneration(TPartitionSessionId id, TPartitionLocation location) {
    with_lock (Lock) {
        auto it = PartitionSessions.find(id);
        Y_ABORT_UNLESS(it != PartitionSessions.end());

        // TODO(qyryq) Add TPartitionLocation::SetGeneration method?
        it->second = {
            .Id = id,
            .Location = location,
            .RetryState = std::move(it->second.RetryState),
        };

        SendStartDirectReadPartitionSessionImpl(it->second);
    }
}

void TDirectReadSession::DeletePartitionSession(TPartitionSessionId id) {
    with_lock (Lock) {
        PartitionSessions.erase(id);
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

    // if (!errorStatus.Ok()) {
    //     ++*Settings.Counters_->Errors;

    //     if (!Reconnect(errorStatus)) {
    //         AbortSession(std::move(errorStatus));
    //     }
    // }
}

void TDirectReadSession::SendStartDirectReadPartitionSessionImpl(const TDirectReadPartitionSession& session) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    // Send the StartDirectReadPartitionSession request only if we're already connected.
    // In other cases adding the session to PartitionSessions is enough,
    // the request will be sent from OnReadDoneImpl(InitDirectReadResponse).
    if (State != EState::CONNECTED) {
        return;
    }

    TDirectReadClientMessage req;
    auto& start = *req.mutable_start_direct_read_partition_session_request();
    start.set_partition_session_id(session.Id);
    // TODO(qyryq) start.set_last_direct_read_id();
    start.set_generation(session.Location.GetGeneration());
    WriteToProcessorImpl(std::move(req));
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::InitDirectReadResponse&&, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "InitDirectReadResponse");

    // RetryState = nullptr;

    // Successful init. Send StartDirectReadPartitionSession requests.
    for (auto& [id, session] : PartitionSessions) {
        SendStartDirectReadPartitionSessionImpl(session);
    }

    ReadFromProcessorImpl(deferred);
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    Cerr << response.DebugString() << Endl;

    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Start partition_session_id=" << response.partition_session_id());
    // RetryState = nullptr;
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    Cerr << response.DebugString() << Endl;

    // TODO(qyryq) Call manager's DeletePartitionSession through deferred?
    // deferred.DeferDeleteDirectPartitionSession(response.partition_session_id());

    // TODO(qyryq) Send status/issues to the control session?
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());
    Cerr << response.DebugString() << Endl;

    if (auto session = SingleClusterReadSession->LockShared()) {
        session->OnDirectReadDone(std::move(response), deferred);
    }
}

void TDirectReadSession::OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&& response, TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    Cerr << response.DebugString() << Endl;
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

    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Successfully connected. Initializing session");

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
    [[maybe_unused]] const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext
) {

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

        // if (Closing || Aborting) {
        //     CallCloseCallbackImpl();
        //     return;
        // }

        if (st.Ok()) {
            Connection = std::move(connection);
            // ConnectionAttemptsDone = 0;
            InitImpl(deferred);
            return;
        }
    }

    // if (!st.Ok()) {
    //     ++*Settings.Counters_->Errors;
    //     if (!Reconnect(st)) {
    //         AbortSession(
    //             st.Status, MakeIssueWithSubIssues(TStringBuilder() << "Failed to establish connection to server \""
    //                                                                << st.Endpoint << "\" ( cluster " << ClusterName
    //                                                                << "). Attempts done: " << ConnectionAttemptsDone,
    //                                               st.Issues));
    //     }
    // }
}

bool TDirectReadSession::Reconnect([[maybe_unused]] const TPlainStatus& status) {
    State = EState::CONNECTING;
    TDuration delay = TDuration::Zero();

    // // Previous operations contexts.
    NYdbGrpc::IQueueClientContextPtr prevConnectContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr prevConnectDelayContext;

    // // Callbacks
    std::function<void(TPlainStatus&&, IDirectReadConnection::TPtr&&)> connectCallback;
    std::function<void(bool)> connectTimeoutCallback;

    // if (!status.Ok()) {
    //     LOG_LAZY(Log, TLOG_ERR, GetLogPrefix() << "Got error. Status: " << status.Status
    //                                         << ". Description: " << IssuesSingleLineString(status.Issues));
    // }

    NYdbGrpc::IQueueClientContextPtr delayContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectContext = nullptr;
    NYdbGrpc::IQueueClientContextPtr connectTimeoutContext = nullptr;

    TDeferredActions<true> deferred;
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
    //     WaitingReadResponse = false;
        ServerMessage = std::make_shared<Ydb::Topic::StreamDirectReadMessage::FromServer>();
        ++ConnectionGeneration;

    //     LOG_LAZY(Log, TLOG_DEBUG,
    //             GetLogPrefix() << "In Reconnect, ReadSizeBudget = " << ReadSizeBudget
    //                             << ", ReadSizeServerDelta = " << ReadSizeServerDelta);

    //     ReadSizeBudget += ReadSizeServerDelta;
    //     ReadSizeServerDelta = 0;

    //     LOG_LAZY(Log, TLOG_DEBUG,
    //             GetLogPrefix() << "New values: ReadSizeBudget = " << ReadSizeBudget
    //                             << ", ReadSizeServerDelta = " << ReadSizeServerDelta);

    //     if (!RetryState) {
    //         RetryState = Settings.RetryPolicy_->CreateRetryState();
    //     }
    //     if (!status.Ok()) {
    //         TMaybe<TDuration> nextDelay = RetryState->GetNextRetryDelay(status.Status);
    //         if (!nextDelay) {
    //             return false;
    //         }
    //         delay = *nextDelay;
    //         delayContext = ClientContext->CreateContext();
    //         if (!delayContext) {
    //             return false;
    //         }
    //     }

    //     LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Reconnecting session to cluster " << ClusterName << " in " << delay);

    //     ++ConnectionAttemptsDone;

        // Set new context
        prevConnectContext = std::exchange(ConnectContext, connectContext);
        prevConnectTimeoutContext = std::exchange(ConnectTimeoutContext, connectTimeoutContext);
        prevConnectDelayContext = std::exchange(ConnectDelayContext, delayContext);

        Y_ASSERT(ConnectContext);
        Y_ASSERT(ConnectTimeoutContext);
    //     Y_ASSERT((delay == TDuration::Zero()) == !ConnectDelayContext);

    //     // Destroy all partition streams before connecting.
    //     DestroyAllPartitionStreamsImpl(deferred);

    //     Y_ABORT_UNLESS(this->SelfContext);

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

    // // Cancel previous operations.
    ::NYdb::NTopic::Cancel(prevConnectContext);
    ::NYdb::NTopic::Cancel(prevConnectTimeoutContext);
    ::NYdb::NTopic::Cancel(prevConnectDelayContext);

    Y_ASSERT(connectContext);
    Y_ASSERT(connectTimeoutContext);
    // Y_ASSERT((delay == TDuration::Zero()) == !delayContext);
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
