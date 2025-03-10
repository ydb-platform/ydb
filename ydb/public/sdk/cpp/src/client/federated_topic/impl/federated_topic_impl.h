#pragma once

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <src/client/topic/impl/common.h>
#include <src/client/topic/common/executor_impl.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <src/client/federated_topic/impl/federation_observer.h>

namespace NYdb::inline Dev::NFederatedTopic {

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
        std::lock_guard guard(Lock);
        if (Observer) {
            Observer->Stop();
        }
    }

    void ProvideCodec(NTopic::ECodec codecId, std::unique_ptr<NTopic::ICodec>&& codecImpl) {
        with_lock(Lock) {
            if (ProvidedCodecs->contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " already provided";
            }
            (*ProvidedCodecs)[codecId] = std::move(codecImpl);
        }
    }

    void OverrideCodec(NTopic::ECodec codecId, std::unique_ptr<NTopic::ICodec>&& codecImpl) {
        with_lock(Lock) {
            (*ProvidedCodecs)[codecId] = std::move(codecImpl);
        }
    }

    const NTopic::ICodec* GetCodecImplOrThrow(NTopic::ECodec codecId) const {
        with_lock(Lock) {
            if (!ProvidedCodecs->contains(codecId)) {
                throw yexception() << "codec with id " << ui32(codecId) << " not provided";
            }
            return ProvidedCodecs->at(codecId).get();
        }
    }

    std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> GetProvidedCodecs() const {
        return ProvidedCodecs;
    }

    // Runtime API.
    std::shared_ptr<IFederatedReadSession> CreateReadSession(const TFederatedReadSessionSettings& settings);

    std::shared_ptr<NTopic::ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TFederatedWriteSessionSettings& settings);
    std::shared_ptr<NTopic::IWriteSession> CreateWriteSession(const TFederatedWriteSessionSettings& settings);

    std::shared_ptr<TFederatedDbObserver> GetObserver() {
        std::lock_guard guard(Lock);
        return Observer;
    }

    void InitObserver();

private:

     // Use single-threaded executor to prevent deadlocks inside subsession event handlers.
     NTopic::IExecutor::TPtr GetSubsessionHandlersExecutor();

private:
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const TFederatedTopicClientSettings ClientSettings;
    std::shared_ptr<TFederatedDbObserver> Observer;
    std::shared_ptr<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>> ProvidedCodecs =
         std::make_shared<std::unordered_map<NTopic::ECodec, std::unique_ptr<NTopic::ICodec>>>();

    NTopic::IExecutor::TPtr SubsessionHandlersExecutor;

    TAdaptiveLock Lock;
};

} // namespace NYdb::NFederatedTopic
