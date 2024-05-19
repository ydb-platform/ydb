#include "direct_reader.h"
#include "read_session_impl.ipp"

namespace NYdb::NTopic {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadConnectionManager

std::shared_ptr<TCallbackContext<TDirectReadConnection>> TDirectReadConnectionManager::CreateConnection(TNodeId nodeId) {
    return MakeWithCallbackContext<TDirectReadConnection>(
        ServerSessionId, ReadSessionSettings, SingleClusterReadSession, ClientContext->CreateContext(), ConnectionFactory, nodeId);
}

void TDirectReadConnectionManager::StartPartitionSession(TNodeId nodeId, [[maybe_unused]] TGeneration generation, TPartitionSessionId partitionSessionId) {
    std::shared_ptr<TCallbackContext<TDirectReadConnection>> connection;
    with_lock (Lock) {
        connection = Connections[nodeId];
        if (!connection) {
            connection = CreateConnection(nodeId);
            if (auto c = connection->LockShared()) {
                c->Start();
                c->AddPartitionSession(partitionSessionId, generation);
            }
        }
        NodePartitionSessions[nodeId].insert(partitionSessionId);
    }
}

void TDirectReadConnectionManager::StopPartitionSession(TNodeId nodeId, TPartitionSessionId partitionSessionId) {
    with_lock (Lock) {
        auto& sessions = NodePartitionSessions[nodeId];
        sessions.erase(partitionSessionId);
        if (sessions.empty()) {
            // Release connection
            auto it = Connections.find(nodeId);
            it->second->Cancel();
            Connections.erase(it);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDirectReadConnection

TDirectReadConnection::TDirectReadConnection(
    TString serverSessionId,
    const NYdb::NTopic::TReadSessionSettings settings,
    TCallbackContextPtr<false> singleClusterReadSession,
    NYdbGrpc::IQueueClientContextPtr clientContext,
    IDirectReadConnectionFactoryPtr connectionFactory,
    TNodeId nodeId
)
    : ClientContext(clientContext)
    , ReadSessionSettings(settings)
    , SingleClusterReadSession(singleClusterReadSession)
    , ServerSessionId(serverSessionId)
    , ConnectionFactory(connectionFactory)
    , State(EState::CREATED)
    , NodeId(nodeId)
    {
    }

void TDirectReadConnection::Start()  {
    if (State == EState::CREATED) {
        Reconnect(TPlainStatus());
    }
}

void TDirectReadConnection::AddPartitionSession(TPartitionSessionId id, TGeneration generation) {
    PartitionSessionGenerations[id] = generation;
}

void TDirectReadConnection::DeletePartitionSession(TPartitionSessionId id) {
    PartitionSessionGenerations.erase(id);
}

void TDirectReadConnection::OnReadDone(NYdbGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration) {
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

void TDirectReadConnection::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::InitDirectReadResponse&&, TDeferredActions<false>& deferred) {
    // Y_ABORT_UNLESS(Lock.IsLocked());
    // LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Server session id: " << msg.session_id());

    // RetryState = nullptr;

    // Successful init. Send StartDirectReadPartitionSession requests.
    for (auto [id, generation] : PartitionSessionGenerations) {
        TDirectReadClientMessage req;
        auto& start = *req.mutable_start_direct_read_partition_session_request();
        start.set_partition_session_id(id);
        // TODO(qyryq) start.set_last_direct_read_id();
        start.set_generation(generation);
        WriteToProcessorImpl(std::move(req));
    }

    ReadFromProcessorImpl(deferred);
}

void TDirectReadConnection::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionResponse&& response, TDeferredActions<false>&) {
    // Y_ABORT_UNLESS(Lock.IsLocked());
    // LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Server session id: " << msg.session_id());

    // RetryState = nullptr;

    Cerr << response.DebugString() << Endl;
}

void TDirectReadConnection::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&& response, TDeferredActions<false>&) {
    Cerr << response.DebugString() << Endl;
}

template <>
inline void TSingleClusterReadSessionImpl<false>::OnDirectReadDone(Ydb::Topic::StreamReadMessage::ReadResponse&& message, TDeferredActions<false>& deferred);

void TDirectReadConnection::OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred) {
    Cerr << response.DebugString() << Endl;
    Ydb::Topic::StreamReadMessage::ReadResponse r;
    r.set_bytes_size(response.ByteSizeLong());
    auto* data = r.add_partition_data();
    data->CopyFrom(response.partition_data());
    if (auto session = SingleClusterReadSession->LockShared()) {
        session->OnDirectReadDone(std::move(r), deferred);
    }
}

void TDirectReadConnection::OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&& response, TDeferredActions<false>&) {
    Cerr << response.DebugString() << Endl;
}

void TDirectReadConnection::WriteToProcessorImpl(
    TDirectReadClientMessage&& req
) {
    // Y_ABORT_UNLESS(Lock.IsLocked());

    if (Connection) {
        Connection->Write(std::move(req));
    }
}

void TDirectReadConnection::ReadFromProcessorImpl(
    TDeferredActions<false>& deferred
) {
    // Y_ABORT_UNLESS(Lock.IsLocked());
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

void TDirectReadConnection::InitImpl([[maybe_unused]] TDeferredActions<false>& deferred) {
    // Y_ABORT_UNLESS(Lock.IsLocked());
    // LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Successfully connected. Initializing session");
    TDirectReadClientMessage req;
    auto& init = *req.mutable_init_direct_read_request();
    init.set_session_id(ServerSessionId);
    init.set_consumer(ReadSessionSettings.ConsumerName_);

    for ([[maybe_unused]] const TTopicReadSettings& topic : ReadSessionSettings.Topics_) {
        auto* topicSettings = init.add_topics_read_settings();
        topicSettings->set_path(topic.Path_);
        // topicSettings->set_path("");
    }

    WriteToProcessorImpl(std::move(req));
    ReadFromProcessorImpl(deferred);
}


void TDirectReadConnection::OnConnectTimeout(
    [[maybe_unused]] const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext
) {

}

void TDirectReadConnection::OnConnect(
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

bool TDirectReadConnection::Reconnect([[maybe_unused]] const TPlainStatus& status, [[maybe_unused]] TNodeId nodeId) {
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
        TRpcRequestSettings::Make(ReadSessionSettings, TEndpointKey(nodeId)),
        std::move(connectContext),
        TDuration::Seconds(30) /* connect timeout */, // TODO: make connect timeout setting.
        std::move(connectTimeoutContext),
        std::move(connectTimeoutCallback),
        delay,
        std::move(delayContext));
    return true;
}

}
