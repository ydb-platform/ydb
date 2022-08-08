#include "topic_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

#include "read_session.h"

namespace NYdb::NTopic {

std::shared_ptr<IReadSession> TTopicClient::TImpl::CreateReadSession(const TReadSessionSettings& settings) {
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
    // return std::make_shared<TDummyReadSession>(settings);
}

std::shared_ptr<TTopicClient::TImpl::IReadSessionConnectionProcessorFactory> TTopicClient::TImpl::CreateReadSessionConnectionProcessorFactory() {
    using TService = Ydb::Topic::V1::TopicService;
    using TRequest = Ydb::Topic::StreamReadMessage::FromClient;
    using TResponse = Ydb::Topic::StreamReadMessage::FromServer;
    return NPersQueue::CreateConnectionProcessorFactory<TService, TRequest, TResponse>(&TService::Stub::AsyncStreamRead, Connections_, DbDriverState_);
}

}
