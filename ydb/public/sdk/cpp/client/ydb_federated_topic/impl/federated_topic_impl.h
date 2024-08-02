#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/executor_impl.h>
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

    void ProvideCodec(NTopic::ECodec codecId, THolder<NTopic::ICodec>&& codecImpl) {
        with_lock(Lock) {
            if (ProvidedCodecs->contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " already provided";
            }
            (*ProvidedCodecs)[codecId] = std::move(codecImpl);
        }
    }

    void OverrideCodec(NTopic::ECodec codecId, THolder<NTopic::ICodec>&& codecImpl) {
        with_lock(Lock) {
            (*ProvidedCodecs)[codecId] = std::move(codecImpl);
        }
    }

    const NTopic::ICodec* GetCodecImplOrThrow(NTopic::ECodec codecId) const {
        with_lock(Lock) {
            if (!ProvidedCodecs->contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " not provided";
            }
            return ProvidedCodecs->at(codecId).Get();
        }
    }

    std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> GetProvidedCodecs() const {
        return ProvidedCodecs;
    }

    // Runtime API.
    std::shared_ptr<IFederatedReadSession> CreateReadSession(const TFederatedReadSessionSettings& settings);

    std::shared_ptr<NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TFederatedWriteSessionSettings& settings);
    std::shared_ptr<NTopic::IWriteSession> CreateWriteSession(const TFederatedWriteSessionSettings& settings);

    std::shared_ptr<TFederatedDbObserver> GetObserver() {
        with_lock(Lock) {
            return Observer;
        }
    }

    void InitObserver();

private:

    // Use single-threaded executor to prevent deadlocks inside subsession event handlers.
    NTopic::IExecutor::TPtr GetSubsessionHandlersExecutor();

private:
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const TFederatedTopicClientSettings ClientSettings;
    std::shared_ptr<TFederatedDbObserver> Observer;
    std::shared_ptr<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>> ProvidedCodecs =
        std::make_shared<std::unordered_map<NTopic::ECodec, THolder<NTopic::ICodec>>>();

    NTopic::IExecutor::TPtr SubsessionHandlersExecutor;

    TAdaptiveLock Lock;
};

} // namespace NYdb::NFederatedTopic
