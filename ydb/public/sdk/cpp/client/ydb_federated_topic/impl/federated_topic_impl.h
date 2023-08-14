#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/executor.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federation_observer.h>

namespace NYdb::NFederatedTopic {

class TFederatedTopicClient::TImpl {
public:
    // Constructor for main client.
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TFederatedTopicClientSettings& settings)
        : Connections(std::move(connections))
        , ClientSettings(settings)
    {
        InitObserver();
    }

    ~TImpl() {
        with_lock(Lock) {
            if (Observer) {
                Observer->Stop();
            }
        }
    }

    // Runtime API.
    std::shared_ptr<IFederatedReadSession> CreateFederatedReadSession(const TFederatedReadSessionSettings& settings);

    std::shared_ptr<TFederatedDbObserver> GetObserver() {
        with_lock(Lock) {
            return Observer;
        }
    }

    void InitObserver();

private:
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const TFederatedTopicClientSettings ClientSettings;
    std::shared_ptr<TFederatedDbObserver> Observer;
    TAdaptiveLock Lock;
};

} // namespace NYdb::NFederatedTopic
