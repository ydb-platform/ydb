#pragma once

#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/include/control_plane.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>


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

using TDirectReadServerMessage = Ydb::Topic::StreamDirectReadMessage::FromServer;
using TDirectReadClientMessage = Ydb::Topic::StreamDirectReadMessage::FromClient;
using IDirectReadProcessorFactory = ISessionConnectionProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>;
using IDirectReadProcessorFactoryPtr = std::shared_ptr<IDirectReadProcessorFactory>;
using IDirectReadProcessor = IDirectReadProcessorFactory::IProcessor;

class TDirectReadSession;
using TDirectReadSessionContextPtr = std::shared_ptr<TCallbackContext<TDirectReadSession>>;

struct IDirectReadSessionControlCallbacks {
    using TPtr = std::shared_ptr<IDirectReadSessionControlCallbacks>;

    virtual ~IDirectReadSessionControlCallbacks() {}
    virtual void OnDirectReadDone(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&&, TDeferredActions<false>&) {}
    virtual void AbortSession(TSessionClosedEvent&&) {}
    virtual void ScheduleCallback(TDuration, std::function<void()>) {}
    virtual void ScheduleCallback(TDuration, std::function<void()>, TDeferredActions<false>&) {}

    virtual void StopPartitionSession(TPartitionSessionId) {}
};

class TDirectReadSessionControlCallbacks : public IDirectReadSessionControlCallbacks {
public:

    TDirectReadSessionControlCallbacks(TSingleClusterReadSessionContextPtr contextPtr);
    void OnDirectReadDone(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& response, TDeferredActions<false>&) override;
    void AbortSession(TSessionClosedEvent&& closeEvent) override;
    void ScheduleCallback(TDuration delay, std::function<void()> callback) override;
    void ScheduleCallback(TDuration delay, std::function<void()> callback, TDeferredActions<false>&) override;

    void StopPartitionSession(TPartitionSessionId) override;

private:

    TSingleClusterReadSessionContextPtr SingleClusterReadSessionContextPtr;
};

class TDirectReadPartitionSession {
public:
    enum class EState {
        IDLE,     // The partition session has just been created. RetryState is empty.
        DELAYED,  // Got an error, SendStartRequestImpl will be called later
        STARTING, // Sent StartDirectReadPartitionSessionRequest, waiting for response
        WORKING   // Got StartDirectReadPartitionSessionResponse

        // See all possible transitions in TDirectReadPartitionSession::TransitionTo.
    };

    TPartitionSessionId PartitionSessionId;
    TPartitionLocation Location;
    EState State = EState::IDLE;
    IRetryPolicy::IRetryState::TPtr RetryState = {};

    TDirectReadId NextDirectReadId = 1;

    // If the control session sends StopPartitionSessionRequest(graceful=true, last_direct_read_id),
    // we need to remember the Id, read up to it, and then kill the partition session (and its direct session if it becomes empty).
    TMaybe<TDirectReadId> LastDirectReadId = Nothing();

    TDirectReadClientMessage MakeStartRequest() const;
    bool TransitionTo(EState);
};

// One TDirectReadSession instance comprises multiple TDirectReadPartitionSessions.
// It wraps a gRPC connection to a particular node, where the partition sessions live.
class TDirectReadSession : public TEnableSelfContext<TDirectReadSession> {
public:
    using TSelf = TDirectReadSession;
    using TPtr = std::shared_ptr<TSelf>;

    TDirectReadSession(
        TNodeId,
        TServerSessionId,
        const NYdb::NTopic::TReadSessionSettings,
        IDirectReadSessionControlCallbacks::TPtr,
        NYdbGrpc::IQueueClientContextPtr,
        const IDirectReadProcessorFactoryPtr,
        TLog
    );

    void Start();
    void Close();
    bool Empty() const;
    bool Closed() const;

    void AddPartitionSession(TDirectReadPartitionSession&&);
    void UpdatePartitionSessionGeneration(TPartitionSessionId, TPartitionLocation);
    void SetLastDirectReadId(TPartitionSessionId, TDirectReadId);
    void DeletePartitionSession(TPartitionSessionId);

private:

    bool Reconnect(
        const TPlainStatus& status
        // TGeneration generation
    );

    void InitImpl(TDeferredActions<false>&);
    void CloseImpl();

    void WriteToProcessorImpl(TDirectReadClientMessage&& req);
    void ReadFromProcessorImpl(TDeferredActions<false>&);
    void OnReadDone(NYdbGrpc::TGrpcStatus&&, size_t connectionGeneration);

    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::InitResponse&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionResponse&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&&, TDeferredActions<false>&);
    void OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&&, TDeferredActions<false>&);

    void OnConnect(
        TPlainStatus&& st,
        IDirectReadProcessor::TPtr&& processor,
        const NYdbGrpc::IQueueClientContextPtr& connectContext
    );

    void OnConnectTimeout(
        const NYdbGrpc::IQueueClientContextPtr& connectTimeoutContext
    );

    // delayedCall may be true only if the method is called from a scheduled callback.
    void SendStartRequestImpl(TPartitionSessionId, bool delayedCall = false);
    void SendStartRequestImpl(TDirectReadPartitionSession&, bool delayedCall = false);
    void DelayStartRequestImpl(TDirectReadPartitionSession&, TPlainStatus&&, TDeferredActions<false>&);

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
    const TServerSessionId ServerSessionId;
    const IDirectReadProcessorFactoryPtr ProcessorFactory;
    const TNodeId NodeId;

    IDirectReadSessionControlCallbacks::TPtr ControlCallbacks;
    IDirectReadProcessor::TPtr Processor;
    std::shared_ptr<TDirectReadServerMessage> ServerMessage;
    THashMap<TPartitionSessionId, TDirectReadPartitionSession> PartitionSessions;
    IRetryPolicy::IRetryState::TPtr RetryState = {};
    size_t ConnectionAttemptsDone = 0;
    EState State;

    TLog Log;
};


// TDirectReadSessionManager is NOT thread-safe. Its methods must be used under a lock.
class TDirectReadSessionManager {
public:
    using TSelf = TDirectReadSessionManager;
    using TPtr = std::shared_ptr<TSelf>;

    TDirectReadSessionManager(
        TServerSessionId,
        const NYdb::NTopic::TReadSessionSettings,
        IDirectReadSessionControlCallbacks::TPtr,
        NYdbGrpc::IQueueClientContextPtr,
        IDirectReadProcessorFactoryPtr,
        TLog
    );

    ~TDirectReadSessionManager();

    void StartPartitionSession(TDirectReadPartitionSession&&);
    void UpdatePartitionSession(TPartitionSessionId, TPartitionLocation);
    void ErasePartitionSession(TPartitionSessionId);
    void StopPartitionSession(TPartitionSessionId);
    void StopPartitionSessionGracefully(TPartitionSessionId, TDirectReadId lastDirectReadId);

    void Close();

private:

    using TNodeSessionsMap = TMap<TNodeId, TDirectReadSessionContextPtr>;

    TDirectReadSessionContextPtr CreateDirectReadSession(TNodeId);
    void DeletePartitionSession(TPartitionSessionId id, TNodeSessionsMap::iterator it);

    TStringBuilder GetLogPrefix() const;

private:
    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    const TServerSessionId ServerSessionId;
    const NYdbGrpc::IQueueClientContextPtr ClientContext;
    const IDirectReadProcessorFactoryPtr ProcessorFactory;

    IDirectReadSessionControlCallbacks::TPtr ControlCallbacks;
    TNodeSessionsMap NodeSessions;
    TMap<TPartitionSessionId, TPartitionLocation> Locations;
    TLog Log;
};

}
