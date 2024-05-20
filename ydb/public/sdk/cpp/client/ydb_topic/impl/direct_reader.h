#pragma once

#include "common.h"

#include "ydb/public/sdk/cpp/client/ydb_topic/include/control_plane.h"
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
using TPartitionSessionId = ui64;
using TServerSessionId = TString;

using TDirectReadServerMessage = Ydb::Topic::StreamDirectReadMessage_FromServer;
using TDirectReadClientMessage = Ydb::Topic::StreamDirectReadMessage_FromClient;
using IDirectReadConnectionFactory = ISessionConnectionProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>;
using IDirectReadConnectionFactoryPtr = std::shared_ptr<IDirectReadConnectionFactory>;
using IDirectReadConnection = IDirectReadConnectionFactory::IProcessor;
class TDirectReadSession;
using TDirectReadSessionPtr = std::shared_ptr<TCallbackContext<TDirectReadSession>>;

struct TDirectReadPartitionSession {
    TPartitionSessionId Id;
    TPartitionLocation Location;
    IRetryPolicy::IRetryState::TPtr RetryState = {};

    // min read id, partition id, done read id?
};

// One TDirectReadSession instance comprises multiple TDirectReadPartitionSessions.
// It wraps a gRPC connection to a particular node, where the partition sessions live.
class TDirectReadSession : public TEnableSelfContext<TDirectReadSession> {
public:
    using TSelf = TDirectReadSession;
    using TPtr = std::shared_ptr<TSelf>;

    TDirectReadSession(
        TString serverSessionId,
        const NYdb::NTopic::TReadSessionSettings settings,
        TSingleClusterReadSessionPtr<false> singleClusterReadSession,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory,
        TNodeId node
    );

    void Start();
    void Cancel();
    void AddPartitionSession(TDirectReadPartitionSession&&);
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

private:

    enum class EState {
        CREATED,
        CONNECTING,
        CONNECTED,
        CLOSING,
        CLOSED
    };

private:
    TMutex Lock;

    NYdbGrpc::IQueueClientContextPtr ClientContext;
    NYdbGrpc::IQueueClientContextPtr ConnectContext;
    NYdbGrpc::IQueueClientContextPtr ConnectTimeoutContext;
    NYdbGrpc::IQueueClientContextPtr ConnectDelayContext;
    size_t ConnectionGeneration = 0;

    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    TSingleClusterReadSessionPtr<false> SingleClusterReadSession;
    TServerSessionId ServerSessionId;
    IDirectReadConnection::TPtr Connection;
    IDirectReadConnectionFactoryPtr ConnectionFactory;
    std::shared_ptr<TDirectReadServerMessage> ServerMessage;

    // PartitionSessionId/AssignId -> TPartitionSessionImpl
    // THashMap<TPartitionSessionId, TPartitionStreamImpl<false>::TPtr> PartitionSessions;
    THashMap<TPartitionSessionId, TDirectReadPartitionSession> PartitionSessions;

    EState State;
    TNodeId NodeId;
};


class TDirectReadSessionManager {
public:
    TDirectReadSessionManager(
        const NYdb::NTopic::TReadSessionSettings,
        TSingleClusterReadSessionPtr<false> singleClusterReadSession,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory
    );

    void StartPartitionSession(TDirectReadPartitionSession&&);

    void UpdatePartitionSession(TPartitionSessionId, TPartitionLocation);

    void StopPartitionSession(TPartitionSessionId);

    void SetServerSessionId(TServerSessionId);

private:

    TDirectReadSessionPtr CreateConnection(TNodeId);

private:
    TMutex Lock;
    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    TServerSessionId ServerSessionId;
    TSingleClusterReadSessionPtr<false> SingleClusterReadSession;
    NYdbGrpc::IQueueClientContextPtr ClientContext;
    IDirectReadConnectionFactoryPtr ConnectionFactory;
    TMap<TNodeId, TDirectReadSessionPtr> Sessions;
    TMap<TPartitionSessionId, TNodeId> Locations;
};

}
