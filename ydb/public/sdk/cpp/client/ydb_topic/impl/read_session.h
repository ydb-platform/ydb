#pragma once

#include "topic_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/callback_context.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/counters_logger.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

namespace NYdb::NTopic {

class TReadSession : public IReadSession,
                     public std::enable_shared_from_this<TReadSession> {
public:
    TReadSession(const TReadSessionSettings& settings,
                 std::shared_ptr<TTopicClient::TImpl> client,
                 std::shared_ptr<TGRpcConnectionsImpl> connections,
                 TDbDriverStatePtr dbDriverState);

    ~TReadSession();

    void Start();

    NThreading::TFuture<void> WaitEvent() override;
    TVector<TReadSessionEvent::TEvent> GetEvents(bool block,
                                                 TMaybe<size_t> maxEventsCount,
                                                 size_t maxByteSize) override;
    TVector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) override;
    TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block,
                                               size_t maxByteSize) override;
    TMaybe<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) override;

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

    void MakeCountersIfNeeded();
    void SetupCountersLogger();

    // Shutdown.
    void Abort(EStatus statusCode, NYql::TIssues&& issues);
    void Abort(EStatus statusCode, const TString& message);

    void AbortImpl(NPersQueue::TDeferredActions<false>& deferred);
    void AbortImpl(TSessionClosedEvent&& closeEvent, NPersQueue::TDeferredActions<false>& deferred);
    void AbortImpl(EStatus statusCode, NYql::TIssues&& issues, NPersQueue::TDeferredActions<false>& deferred);
    void AbortImpl(EStatus statusCode, const TString& message, NPersQueue::TDeferredActions<false>& deferred);

private:
    using TOffsetRanges = THashMap<TString, THashMap<ui64, TDisjointIntervalTree<ui64>>>;

    void CollectOffsets(NTable::TTransaction& tx,
                        const TReadSessionEvent::TDataReceivedEvent& event);
    void CollectOffsets(NTable::TTransaction& tx,
                        const TString& topicPath, ui32 partitionId, ui64 offset);
    void UpdateOffsets(const NTable::TTransaction& tx);

    //
    // (session, tx) -> topic -> partition -> (begin, end)
    //
    THashMap<std::pair<TString, TString>, TOffsetRanges> OffsetRanges;

    TReadSessionSettings Settings;
    const TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    TLog Log;
    std::shared_ptr<TTopicClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<NPersQueue::TReadSessionEventsQueue<false>> EventsQueue;

    NPersQueue::TSingleClusterReadSessionImpl<false>::TPtr Session;
    std::shared_ptr<NPersQueue::TCallbackContext<NPersQueue::TSingleClusterReadSessionImpl<false>>> CbContext;
    TVector<TTopicReadSettings> Topics;

    std::shared_ptr<NPersQueue::TCountersLogger<false>> CountersLogger;
    std::shared_ptr<NPersQueue::TCallbackContext<NPersQueue::TCountersLogger<false>>> DumpCountersContext;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
};

}
