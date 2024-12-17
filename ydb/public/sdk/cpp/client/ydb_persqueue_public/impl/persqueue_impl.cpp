#include "persqueue_impl.h"

#include "read_session.h"
#include "write_session.h"

namespace NYdb::NPersQueue {

std::shared_ptr<IReadSession> TPersQueueClient::TImpl::CreateReadSession(const TReadSessionSettings& settings) {
    TMaybe<TReadSessionSettings> maybeSettings;
    if (!settings.DecompressionExecutor_ || !settings.EventHandlers_.HandlersExecutor_) {
        maybeSettings = settings;
        with_lock (Lock) {
            if (!settings.DecompressionExecutor_) {
                maybeSettings->DecompressionExecutor(Settings.DefaultCompressionExecutor_);
            }
            if (!settings.EventHandlers_.HandlersExecutor_) {
                maybeSettings->EventHandlers_.HandlersExecutor(Settings.DefaultHandlersExecutor_);
            }
        }
    }
    auto session = std::make_shared<TReadSession>(maybeSettings.GetOrElse(settings), shared_from_this(), Connections_, DbDriverState_);
    session->Start();
    return std::move(session);
}

std::shared_ptr<IWriteSession> TPersQueueClient::TImpl::CreateWriteSession(
        const TWriteSessionSettings& settings
) {
    TMaybe<TWriteSessionSettings> maybeSettings;
    if (!settings.CompressionExecutor_ || !settings.EventHandlers_.HandlersExecutor_) {
        maybeSettings = settings;
        with_lock (Lock) {
            if (!settings.CompressionExecutor_) {
                maybeSettings->CompressionExecutor(Settings.DefaultCompressionExecutor_);
            }
            if (!settings.EventHandlers_.HandlersExecutor_) {
                maybeSettings->EventHandlers_.HandlersExecutor(Settings.DefaultHandlersExecutor_);
            }
        }
    }
    auto session = std::make_shared<TWriteSession>(
            maybeSettings.GetOrElse(settings), shared_from_this(), Connections_, DbDriverState_
    );
    session->Start(TDuration::Zero());
    return std::move(session);
}

std::shared_ptr<ISimpleBlockingWriteSession> TPersQueueClient::TImpl::CreateSimpleWriteSession(
        const TWriteSessionSettings& settings
) {
    auto alteredSettings = settings;
    with_lock (Lock) {
        alteredSettings.EventHandlers_.HandlersExecutor(Settings.DefaultHandlersExecutor_);
        if (!settings.CompressionExecutor_) {
            alteredSettings.CompressionExecutor(Settings.DefaultCompressionExecutor_);
        }
    }

    auto session = std::make_shared<TSimpleBlockingWriteSession>(
            alteredSettings, shared_from_this(), Connections_, DbDriverState_
    );
    return std::move(session);
}

std::shared_ptr<TPersQueueClient::TImpl> TPersQueueClient::TImpl::GetClientForEndpoint(const TString& clusterEndoint) {
    with_lock (Lock) {
        Y_ABORT_UNLESS(!CustomEndpoint);
        std::shared_ptr<TImpl>& client = Subclients[clusterEndoint];
        if (!client) {
            client = std::make_shared<TImpl>(clusterEndoint, Connections_, Settings);
        }
        return client;
    }
}

std::shared_ptr<TPersQueueClient::TImpl::IReadSessionConnectionProcessorFactory> TPersQueueClient::TImpl::CreateReadSessionConnectionProcessorFactory() {
    using TService = Ydb::PersQueue::V1::PersQueueService;
    using TRequest = Ydb::PersQueue::V1::MigrationStreamingReadClientMessage;
    using TResponse = Ydb::PersQueue::V1::MigrationStreamingReadServerMessage;
    return CreateConnectionProcessorFactory<TService, TRequest, TResponse>(&TService::Stub::AsyncMigrationStreamingRead, Connections_, DbDriverState_);
}

std::shared_ptr<TPersQueueClient::TImpl::IWriteSessionConnectionProcessorFactory> TPersQueueClient::TImpl::CreateWriteSessionConnectionProcessorFactory() {
    using TService = Ydb::PersQueue::V1::PersQueueService;
    using TRequest = Ydb::PersQueue::V1::StreamingWriteClientMessage;
    using TResponse = Ydb::PersQueue::V1::StreamingWriteServerMessage;
    return CreateConnectionProcessorFactory<TService, TRequest, TResponse>(&TService::Stub::AsyncStreamingWrite, Connections_, DbDriverState_);
}

}  // namespace NYdb::NPersQueue
