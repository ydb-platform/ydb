#pragma once

#include "topic_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/impl_tracker.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

namespace NYdb::NTopic {

class TDummyReadSession: public IReadSession, public std::enable_shared_from_this<TDummyReadSession> {
public:
    TDummyReadSession() = default;

    inline TDummyReadSession(const TReadSessionSettings& settings) {
        (void)settings;
    }

    inline NThreading::TFuture<void> WaitEvent() override {
        Y_VERIFY(false);

        NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
        return promise.GetFuture();
    }

    inline TVector<TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override {
        Y_VERIFY(false);

        (void)block;
        (void)maxEventsCount;
        (void)maxByteSize;
        return {};
    }

    inline TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override {
        Y_VERIFY(false);

        (void)block;
        (void)maxByteSize;
        return {};
    }

    inline bool Close(TDuration timeout) override {
        Y_VERIFY(false);

        return !(bool)timeout;
    }

    inline TString GetSessionId() const override {
        Y_VERIFY(false);

        return "dummy_session_id";
    }

    inline TReaderCounters::TPtr GetCounters() const override {
        Y_VERIFY(false);

        return nullptr;
    }
};

class TReadSession : public IReadSession,
                     public NPersQueue::IUserRetrievedEventCallback<false>,
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

    inline TString GetSessionId() const override {
        return SessionId;
    }

    inline TReaderCounters::TPtr GetCounters() const override {
        return Settings.Counters_; // Always not nullptr.
    }

    void Abort(TSessionClosedEvent&& closeEvent);

    void ClearAllEvents();

private:
    TStringBuilder GetLogPrefix() const;

    // Start
    bool ValidateSettings();

    void CreateClusterSessionsImpl(NPersQueue::TDeferredActions<false>& deferred);

    void OnUserRetrievedEvent(i64 decompressedSize, size_t messagesCount) override;

    void MakeCountersIfNeeded();
    void DumpCountersToLog(size_t timeNumber = 0);
    void ScheduleDumpCountersToLog(size_t timeNumber = 0);

    // Shutdown.
    void Abort(EStatus statusCode, NYql::TIssues&& issues);
    void Abort(EStatus statusCode, const TString& message);

    void AbortImpl(NPersQueue::TDeferredActions<false>& deferred);
    void AbortImpl(TSessionClosedEvent&& closeEvent, NPersQueue::TDeferredActions<false>& deferred);
    void AbortImpl(EStatus statusCode, NYql::TIssues&& issues, NPersQueue::TDeferredActions<false>& deferred);
    void AbortImpl(EStatus statusCode, const TString& message, NPersQueue::TDeferredActions<false>& deferred);

private:
    TReadSessionSettings Settings;
    const TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    TLog Log;
    std::shared_ptr<TTopicClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<NPersQueue::TImplTracker> Tracker;
    std::shared_ptr<NPersQueue::TReadSessionEventsQueue<false>> EventsQueue;

    NPersQueue::TSingleClusterReadSessionImpl<false>::TPtr Session;
    TVector<TTopicReadSettings> Topics;

    NGrpc::IQueueClientContextPtr DumpCountersContext;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
};

}
