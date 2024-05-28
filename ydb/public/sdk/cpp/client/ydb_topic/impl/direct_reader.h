#pragma once

#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/include/control_plane.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>


namespace Ydb::Topic {
    class UpdateTokenResponse;
    class StreamDirectReadMessage;
    class StreamDirectReadMessage_FromServer;
    class StreamDirectReadMessage_FromClient;
    class StreamDirectReadMessage_InitDirectReadResponse;
    class StreamDirectReadMessage_StartDirectReadPartitionSessionResponse;
    class StreamDirectReadMessage_DirectReadResponse;
    class StreamDirectReadMessage_StopDirectReadPartitionSession;
}

namespace NYdb::NTopic {

template <bool UseMigrationProtocol>
class TDeferredActions;

template <bool UseMigrationProtocol>
class TSingleClusterReadSessionImpl;

template <bool UseMigrationProtocol>
using TSingleClusterReadSessionPtr = std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<UseMigrationProtocol>>>;

using TNodeId = i32;
using TGeneration = i64;
using TPartitionId = i64;
using TPartitionSessionId = ui64;
using TServerSessionId = TString;
using TDirectReadId = i64;

using TDirectReadServerMessage = Ydb::Topic::StreamDirectReadMessage_FromServer;
using TDirectReadClientMessage = Ydb::Topic::StreamDirectReadMessage_FromClient;
using IDirectReadConnectionFactory = ISessionConnectionProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>;
using IDirectReadConnectionFactoryPtr = std::shared_ptr<IDirectReadConnectionFactory>;
using IDirectReadConnection = IDirectReadConnectionFactory::IProcessor;
class TDirectReadSession;
using TDirectReadSessionPtr = std::shared_ptr<TCallbackContext<TDirectReadSession>>;

struct TDirectReadPartitionSession {
    enum class EState {
        CREATED,
        STARTING,
        STARTED
    };

    TPartitionSessionId PartitionSessionId;
    TPartitionLocation Location;
    EState State = EState::CREATED;
    IRetryPolicy::IRetryState::TPtr RetryState = {};
    TDirectReadId LastDirectReadId = 0;

    // TODO(qyryq) min read id, partition id, done read id?
};

// One TDirectReadSession instance comprises multiple TDirectReadPartitionSessions.
// It wraps a gRPC connection to a particular node, where the partition sessions live.
class TDirectReadSession : public TEnableSelfContext<TDirectReadSession> {
public:
    using TSelf = TDirectReadSession;
    using TPtr = std::shared_ptr<TSelf>;
    using TOnDirectReadDoneCallback = std::function<void(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred)>;
    using TOnAbortSessionCallback = std::function<void(TSessionClosedEvent&& closeEvent)>;

    TDirectReadSession(
        TNodeId node,
        TString serverSessionId,
        const NYdb::NTopic::TReadSessionSettings settings,
        // TSingleClusterReadSessionPtr<false> singleClusterReadSession,
        TOnDirectReadDoneCallback onDirectReadDoneCallback,
        TOnAbortSessionCallback onAbortSessionCallback,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory,
        TLog log
    );

    void Start();
    void Close();
    void AddPartitionSession(TDirectReadPartitionSession&&);
    void UpdatePartitionSessionGeneration(TPartitionSessionId, TPartitionLocation);
    void DeletePartitionSession(TPartitionSessionId);
    bool Empty() const;

private:

    bool Reconnect(
        const TPlainStatus& status
        // TGeneration generation
    );

    void InitImpl(TDeferredActions<false>&);

    void WriteToProcessorImpl(TDirectReadClientMessage&& req);
    void ReadFromProcessorImpl(TDeferredActions<false>&);
    void OnReadDone(NYdbGrpc::TGrpcStatus&&, size_t connectionGeneration);

    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage_InitDirectReadResponse&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage_StartDirectReadPartitionSessionResponse&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage_DirectReadResponse&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage_StopDirectReadPartitionSession&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&&, TDeferredActions<false>&);

    void OnConnect(
        TPlainStatus&& st,
        IDirectReadConnection::TPtr&& processor,
        const NYdbGrpc::IQueueClientContextPtr& connectContext
    );

    void OnConnectTimeout(
        const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext
    );

    void SendStartDirectReadPartitionSessionImpl(TDirectReadPartitionSession&);

    void AbortControlSession(TSessionClosedEvent&& closeEvent);

    TStringBuilder GetLogPrefix() const;

private:

    enum class EState {
        CREATED,
        CONNECTING,
        CONNECTED,
        INITIALIZING,
        WORKING,
        CLOSING,
        CLOSED
    };

    friend void Out<NYdb::NTopic::TDirectReadSession::EState>(IOutputStream& o, NYdb::NTopic::TDirectReadSession::EState state);

private:
    TAdaptiveLock Lock;

    NYdbGrpc::IQueueClientContextPtr ClientContext;
    NYdbGrpc::IQueueClientContextPtr ConnectContext;
    NYdbGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr ConnectDelayContext;
    size_t ConnectionGeneration = 0;

    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    // TSingleClusterReadSessionPtr<false> SingleClusterReadSession;
    TOnDirectReadDoneCallback OnDirectReadDoneCallback;
    TOnAbortSessionCallback OnAbortSessionCallback;
    TServerSessionId ServerSessionId;
    IDirectReadConnection::TPtr Connection;
    IDirectReadConnectionFactoryPtr ConnectionFactory;
    std::shared_ptr<TDirectReadServerMessage> ServerMessage;

    THashMap<TPartitionSessionId, TDirectReadPartitionSession> PartitionSessions;
    IRetryPolicy::IRetryState::TPtr RetryState = {};
    size_t ConnectionAttemptsDone = 0;

    EState State;
    TNodeId NodeId;

    TLog Log;
};


class TDirectReadSessionManager {
public:
    TDirectReadSessionManager(
        TServerSessionId serverSessionId,
        const NYdb::NTopic::TReadSessionSettings,
        // TSingleClusterReadSessionPtr<false> singleClusterReadSession,
        TDirectReadSession::TOnDirectReadDoneCallback onDirectReadDoneCallback,
        TDirectReadSession::TOnAbortSessionCallback onAbortSessionCallback,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory,
        TLog log
    );

    void StartPartitionSession(TDirectReadPartitionSession&&);
    void UpdatePartitionSession(TPartitionSessionId, TPartitionLocation);
    void StopPartitionSession(TPartitionSessionId);
    void Close();

private:

    using TNodeSessionsMap = TMap<TNodeId, TDirectReadSessionPtr>;

    TDirectReadSessionPtr CreateDirectReadSession(TNodeId);
    void DeletePartitionSession(TPartitionSessionId id, TNodeSessionsMap::iterator it);

    TStringBuilder GetLogPrefix() const;

private:
    TMutex Lock;
    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    TServerSessionId ServerSessionId;
    // TSingleClusterReadSessionPtr<false> SingleClusterReadSession;
    TDirectReadSession::TOnDirectReadDoneCallback OnDirectReadDoneCallback;
    TDirectReadSession::TOnAbortSessionCallback OnAbortSessionCallback;
    NYdbGrpc::IQueueClientContextPtr ClientContext;
    IDirectReadConnectionFactoryPtr ConnectionFactory;
    TNodeSessionsMap NodeSessions;
    TMap<TPartitionSessionId, TPartitionLocation> Locations;
    TLog Log;
};

}
