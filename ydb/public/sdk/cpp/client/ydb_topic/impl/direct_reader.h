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

using TSingleClusterReadSessionContextPtr = std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<false>>>;

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

using TDirectReadSessionContextPtr = std::shared_ptr<TCallbackContext<TDirectReadSession>>;

class IDirectReadSessionManager;

using TDirectReadSessionManagerContextPtr = std::shared_ptr<TCallbackContext<IDirectReadSessionManager>>;

struct TDirectReadSessionCallbacks {
    using TOnDirectReadDone = std::function<void(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>& deferred)>;
    using TOnAbortSession = std::function<void(TSessionClosedEvent&& closeEvent)>;
    using TOnSchedule = std::function<void(TDuration delay, std::function<void()> callback)>;

    TOnDirectReadDone OnDirectReadDone;
    TOnAbortSession OnAbortSession;
    TOnSchedule OnSchedule;
};

struct TDirectReadPartitionSession {
    enum class EState {
        IDLE,
        DELAYED,  // Got an error, SendStartDirectReadPartitionSessionImpl will be called later
        STARTING, // Sent StartDirectReadPartitionSessionRequest, waiting for response
        WORKING   // Got StartDirectReadPartitionSessionResponse

        /*
        +---------------------+
        |                     |
        |  +-------+          |
        v  v       |          |
        IDLE--->DELAYED--->STARTING--->WORKING
        ^ |                   ^           |
        | |                   |           |
        | +-------------------+           |
        |                                 |
        +---------------------------------+
                   on errors
        */
    };

    TPartitionSessionId PartitionSessionId;
    TPartitionLocation Location;
    EState State = EState::IDLE;
    IRetryPolicy::IRetryState::TPtr RetryState = {};
    TDirectReadId PrevDirectReadId = 0;

    // If the control session sends StopPartitionSessionRequest(graceful=true, last_direct_read_id),
    // we need to remember the Id, read up to it, and then kill the partition session (and probably the direct session altogether).
    TMaybe<TDirectReadId> LastDirectReadId = Nothing();
    TMaybe<i64> CommittedOffset = Nothing();

    // TODO(qyryq) min read id, partition id, done read id?

    TDirectReadClientMessage MakeStartRequest() const;
};

// One TDirectReadSession instance comprises multiple TDirectReadPartitionSessions.
// It wraps a gRPC connection to a particular node, where the partition sessions live.
class TDirectReadSession : public TEnableSelfContext<TDirectReadSession> {
public:
    using TSelf = TDirectReadSession;
    using TPtr = std::shared_ptr<TSelf>;

    TDirectReadSession(
        TNodeId node,
        TString serverSessionId,
        const NYdb::NTopic::TReadSessionSettings settings,
        TDirectReadSessionManagerContextPtr managerContextPtr,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory,
        TLog log
    );

    void Start();
    void Close();
    void AddPartitionSession(TDirectReadPartitionSession&&);
    void UpdatePartitionSessionGeneration(TPartitionSessionId, TPartitionLocation);
    void SetLastDirectReadId(TPartitionSessionId, i64 committedOffset, TDirectReadId);
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

    // delayedCall may be true only if the method is called from a scheduled callback.
    void SendStartDirectReadPartitionSessionImpl(TDirectReadPartitionSession&, TPlainStatus&&, TDeferredActions<false>&, bool delayedCall = false);
    void SendStartDirectReadPartitionSessionImpl(TPartitionSessionId, TPlainStatus&&, TDeferredActions<false>&, bool delayedCall = false);

    void DeletePartitionSessionImpl(TPartitionSessionId);

    void AbortImpl(TPlainStatus&&);

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

private:
    TAdaptiveLock Lock;

    NYdbGrpc::IQueueClientContextPtr ClientContext;
    NYdbGrpc::IQueueClientContextPtr ConnectContext;
    NYdbGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr ConnectDelayContext;
    size_t ConnectionGeneration = 0;

    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    TDirectReadSessionManagerContextPtr ManagerContextPtr;
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

class IDirectReadSessionManager {
public:

    virtual void StartPartitionSession(TDirectReadPartitionSession&&) = 0;
    virtual void UpdatePartitionSession(TPartitionSessionId, TPartitionLocation) = 0;
    virtual void StopPartitionSession(TPartitionSessionId) = 0;
    virtual void StopPartitionSessionGracefully(TPartitionSessionId, i64 committedOffset, TDirectReadId lastDirectReadId) = 0;
    virtual void Close() = 0;
};

class TDirectReadSessionManager : public IDirectReadSessionManager,
                                  public TEnableSelfContext<IDirectReadSessionManager> {
    friend TDirectReadSession;
public:
    using TSelf = TDirectReadSessionManager;
    using TPtr = std::shared_ptr<TSelf>;

    TDirectReadSessionManager(
        TServerSessionId,
        const NYdb::NTopic::TReadSessionSettings,
        TSingleClusterReadSessionContextPtr,
        NYdbGrpc::IQueueClientContextPtr,
        IDirectReadConnectionFactoryPtr,
        TLog
    );

    void StartPartitionSession(TDirectReadPartitionSession&&) override;
    void UpdatePartitionSession(TPartitionSessionId, TPartitionLocation) override;
    void StopPartitionSession(TPartitionSessionId) override;
    void StopPartitionSessionGracefully(TPartitionSessionId, i64 committedOffset, TDirectReadId lastDirectReadId) override;
    void Close() override;

private:

    using TNodeSessionsMap = TMap<TNodeId, TDirectReadSessionContextPtr>;

    TDirectReadSessionContextPtr CreateDirectReadSession(TNodeId);
    void StartPartitionSessionImpl(TDirectReadPartitionSession&&);
    void DeletePartitionSessionImpl(TPartitionSessionId id, TNodeSessionsMap::iterator it);
    void DeleteNodeSessionIfEmptyImpl(TNodeId);

    TStringBuilder GetLogPrefix() const;

private:
    TAdaptiveLock Lock;

    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    const TServerSessionId ServerSessionId;
    TSingleClusterReadSessionContextPtr SingleClusterReadSessionContextPtr;
    const NYdbGrpc::IQueueClientContextPtr ClientContext;
    const IDirectReadConnectionFactoryPtr ConnectionFactory;
    TNodeSessionsMap NodeSessions;
    TMap<TPartitionSessionId, TPartitionLocation> Locations;
    TLog Log;
};

}
