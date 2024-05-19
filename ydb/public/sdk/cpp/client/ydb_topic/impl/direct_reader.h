#pragma once

#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/include/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/digest/numeric.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/stream/mem.h>
#include <util/system/env.h>
#include <util/system/condvar.h>

#include <google/protobuf/util/time_util.h>


namespace NYdb::NTopic {

template <bool UseMigrationProtocol>
class TDeferredActions;

template <bool UseMigrationProtocol>
class TSingleClusterReadSessionImpl;

template <bool UseMigrationProtocol>
using TCallbackContextPtr = std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<UseMigrationProtocol>>>;

using TNodeId = i32;
using TGeneration = i64;
using TPartitionSessionId = ui64;

using TDirectReadServerMessage = Ydb::Topic::StreamDirectReadMessage::FromServer;
using TDirectReadClientMessage = Ydb::Topic::StreamDirectReadMessage::FromClient;
using IDirectReadConnectionFactory = ISessionConnectionProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>;
using IDirectReadConnectionFactoryPtr = std::shared_ptr<IDirectReadConnectionFactory>;
using IDirectReadConnection = IDirectReadConnectionFactory::IProcessor;

class TDirectReadConnection : public TEnableSelfContext<TDirectReadConnection> {
public:
    using TSelf = TDirectReadConnection;
    using TPtr = std::shared_ptr<TSelf>;

    TDirectReadConnection(
        TString serverSessionId,
        const NYdb::NTopic::TReadSessionSettings settings,
        TCallbackContextPtr<false> singleClusterReadSession,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory,
        TNodeId nodeId
    );

    void Start();

    void Cancel() {

    }

    bool AddPartitionSession(TPartitionSessionId id, TGeneration generation) {
        PartitionSessionGenerations[id] = generation;
        return true;
    }

    // bool DeletePartitionSession(TPartitionStreamImpl<false>::TPtr partitionSession) {
    //     PartitionSessions.erase(partitionSession->GetPartitionSessionId());
    // }

private:

    bool Reconnect(
        const TPlainStatus& status,
        // [[maybe_unused]] TGeneration generation
    );

    void InitImpl(TDeferredActions<false>& deferred);

    void WriteToProcessorImpl(TDirectReadClientMessage&& req);
    void ReadFromProcessorImpl(TDeferredActions<false>& deferred);
    void OnReadDone(NYdbGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration);

    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::InitDirectReadResponse&& msg, TDeferredActions<false>& deferred);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionResponse&& msg, TDeferredActions<false>& deferred);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::DirectReadResponse&& msg, TDeferredActions<false>& deferred);
    void OnReadDoneImpl(Ydb::Topic::StreamDirectReadMessage::StopDirectReadPartitionSession&& msg, TDeferredActions<false>& deferred);
    void OnReadDoneImpl(Ydb::Topic::UpdateTokenResponse&& msg, TDeferredActions<false>& deferred);


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
    TCallbackContextPtr<false> SingleClusterReadSession;
    TString ServerSessionId;
    IDirectReadConnection::TPtr Connection;
    IDirectReadConnectionFactoryPtr ConnectionFactory;
    std::shared_ptr<TDirectReadServerMessage> ServerMessage;

    // PartitionSessionId/AssignId -> TPartitionSessionImpl
    // THashMap<TPartitionSessionId, TPartitionStreamImpl<false>::TPtr> PartitionSessions;
    THashMap<TPartitionSessionId, TGeneration> PartitionSessionGenerations;

    EState State;
    [[maybe_unused]] TNodeId NodeId;
};

class TDirectReadConnectionManager {
public:
    TDirectReadConnectionManager(
        const NYdb::NTopic::TReadSessionSettings settings,
        TCallbackContextPtr<false> singleClusterReadSession,
        NYdbGrpc::IQueueClientContextPtr clientContext,
        IDirectReadConnectionFactoryPtr connectionFactory
    )
        : ReadSessionSettings(settings)
        , SingleClusterReadSession(singleClusterReadSession)
        , ClientContext(clientContext)
        , ConnectionFactory(connectionFactory)
        {}

    void StartPartitionSession(TNodeId nodeId, [[maybe_unused]] TGeneration generation, TPartitionSessionId partitionSessionId);

    void StopPartitionSession(TNodeId nodeId, TPartitionSessionId partitionSessionId);

    void SetServerSessionId(TString id) {
        ServerSessionId = id;
    }

private:

    std::shared_ptr<TCallbackContext<TDirectReadConnection>> CreateConnection(TNodeId nodeId);

private:
    TMutex Lock;
    const NYdb::NTopic::TReadSessionSettings ReadSessionSettings;
    TString ServerSessionId;
    TCallbackContextPtr<false> SingleClusterReadSession;
    NYdbGrpc::IQueueClientContextPtr ClientContext;
    IDirectReadConnectionFactoryPtr ConnectionFactory;
    std::unordered_map<TNodeId, std::shared_ptr<TCallbackContext<TDirectReadConnection>>> Connections;
    std::unordered_map<TNodeId, std::unordered_set<TPartitionSessionId>> NodePartitionSessions;
};

}
