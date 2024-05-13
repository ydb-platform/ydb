#pragma once

#include "counters_logger.h"
#include "read_session_impl.ipp"
#include "topic_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>

namespace NYdb::NTopic {

class TReadSession : public IReadSession {
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

    void CreateClusterSessionsImpl(TDeferredActions<false>& deferred);

    void MakeCountersIfNeeded();
    void SetupCountersLogger();

    // Shutdown.
    void Abort(EStatus statusCode, NYql::TIssues&& issues);
    void Abort(EStatus statusCode, const TString& message);

    void AbortImpl(TDeferredActions<false>& deferred);
    void AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions<false>& deferred);
    void AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions<false>& deferred);
    void AbortImpl(EStatus statusCode, const TString& message, TDeferredActions<false>& deferred);

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
    std::shared_ptr<TReadSessionEventsQueue<false>> EventsQueue;

    std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<false>>> CbContext;
    TVector<TTopicReadSettings> Topics;

    std::shared_ptr<TCountersLogger<false>> CountersLogger;
    std::shared_ptr<TCallbackContext<TCountersLogger<false>>> DumpCountersContext;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;
};

}  // namespace NYdb::NTopic
