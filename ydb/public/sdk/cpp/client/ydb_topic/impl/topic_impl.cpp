#include "topic_impl.h"
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

namespace NYdb::NTopic {

class TDummyReadSession: public IReadSession, public std::enable_shared_from_this<TDummyReadSession> {
public:
    TDummyReadSession() = default;
    TDummyReadSession(const TReadSessionSettings& settings) {
        (void)settings;
    }
    NThreading::TFuture<void> WaitEvent() override {
        Y_VERIFY(false);

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        return promise.GetFuture();
    }
    TVector<TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override {
        Y_VERIFY(false);

        (void)block;
        (void)maxEventsCount;
        (void)maxByteSize;
        return {};
    }
    TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override {
        Y_VERIFY(false);

        (void)block;
        (void)maxByteSize;
        return {};
    }

    bool Close(TDuration timeout) override {
        Y_VERIFY(false);

        return !(bool)timeout;
    }

    TString GetSessionId() const override {
        Y_VERIFY(false);

        return "dummy_session_id";
    }

    TReaderCounters::TPtr GetCounters() const override {
        Y_VERIFY(false);

        return nullptr;
    }
};

class TReadSession : public IReadSession,
                     public NPersQueue::IUserRetrievedEventCallback,
                     public std::enable_shared_from_this<TReadSession> {
public:
    TReadSession(const TReadSessionSettings& settings,
                 std::shared_ptr<TTopicClient::TImpl> client,
                 std::shared_ptr<TGRpcConnectionsImpl> connections,
                 TDbDriverStatePtr dbDriverState);

    ~TReadSession();

    void Start();

    NThreading::TFuture<void> WaitEvent() override;
    TVector<TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override;
    TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override;

    bool Close(TDuration timeout) override;

    TString GetSessionId() const override {
        return SessionId;
    }

    TReaderCounters::TPtr GetCounters() const override {
        return Settings.Counters_; // Always not nullptr.
    }

    void Abort(TSessionClosedEvent&& closeEvent);

    void WaitAllDecompressionTasks();
    void ClearAllEvents();



private:
    TReadSessionSettings Settings;
    const TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    TLog Log;
    std::shared_ptr<TTopicClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    NPersQueue::IErrorHandler::TPtr ErrorHandler;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<NPersQueue::TReadSessionEventsQueue> EventsQueue;

    NPersQueue::TSingleClusterReadSessionImpl::TPtr Session;
    TVector<TTopicReadSettings> Topics;

};

std::shared_ptr<IReadSession> TTopicClient::TImpl::CreateReadSession(const TReadSessionSettings& settings) {
    // TMaybe<TReadSessionSettings> maybeSettings;
    // if (!settings.DecompressionExecutor_ || !settings.EventHandlers_.HandlersExecutor_) {
    //     maybeSettings = settings;
    //     with_lock (Lock) {
    //         if (!settings.DecompressionExecutor_) {
    //             maybeSettings->DecompressionExecutor(Settings.DefaultCompressionExecutor_);
    //         }
    //         if (!settings.EventHandlers_.HandlersExecutor_) {
    //             maybeSettings->EventHandlers_.HandlersExecutor(Settings.DefaultHandlersExecutor_);
    //         }
    //     }
    // }
    // auto session = std::make_shared<TReadSession>(maybeSettings.GetOrElse(settings), shared_from_this(), Connections_, DbDriverState_);
    // session->Start();
    // return std::move(session);
    return std::make_shared<TDummyReadSession>(settings);
}

std::shared_ptr<TTopicClient::TImpl::IReadSessionConnectionProcessorFactory> TTopicClient::TImpl::CreateReadSessionConnectionProcessorFactory() {
    using TService = Ydb::Topic::V1::TopicService;
    using TRequest = Ydb::Topic::StreamReadMessage::FromClient;
    using TResponse = Ydb::Topic::StreamReadMessage::FromServer;
    return NPersQueue::CreateConnectionProcessorFactory<TService, TRequest, TResponse>(&TService::Stub::AsyncStreamRead, Connections_, DbDriverState_);
}

}
