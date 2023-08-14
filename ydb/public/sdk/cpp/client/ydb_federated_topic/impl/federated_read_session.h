#pragma once

#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_topic_impl.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/impl_tracker.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/read_session.h>

namespace NYdb::NFederatedTopic {

class TFederatedReadSession : public IFederatedReadSession,
                     public std::enable_shared_from_this<TFederatedReadSession> {
    friend class TFederatedTopicClient::TImpl;

private:
    struct TSubSession {
        TSubSession(std::shared_ptr<NTopic::IReadSession> session = {}, std::shared_ptr<TDbInfo> dbInfo = {})
            : Session(std::move(session))
            , DbInfo(std::move(dbInfo))
            {}

        std::shared_ptr<NTopic::IReadSession> Session;
        std::shared_ptr<TDbInfo> DbInfo;
    };

public:
    TFederatedReadSession(const TFederatedReadSessionSettings& settings,
                          std::shared_ptr<TGRpcConnectionsImpl> connections,
                          const TFederatedTopicClientSettings& clientSetttings,
                          std::shared_ptr<TFederatedDbObserver> observer);

    ~TFederatedReadSession() = default;

    NThreading::TFuture<void> WaitEvent() override;
    TVector<TReadSessionEvent::TEvent> GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) override;
    TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override;

    bool Close(TDuration timeout) override;

    inline TString GetSessionId() const override {
        return SessionId;
    }

    inline NTopic::TReaderCounters::TPtr GetCounters() const override {
        return Settings.Counters_; // Always not nullptr.
    }

private:
    // TODO logging
    TStringBuilder GetLogPrefix() const;

    void Start();
    bool ValidateSettings();
    void OpenSubSessionsImpl();

    void OnFederatedStateUpdateImpl();

    void Abort();
    void CloseImpl();

    void ClearAllEvents();

    // TODO Counters
    // void MakeCountersIfNeeded();
    // void DumpCountersToLog(size_t timeNumber = 0);
    // void ScheduleDumpCountersToLog(size_t timeNumber = 0);

private:
    // For subsessions creation
    const NTopic::TReadSessionSettings Settings;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    const NTopic::TTopicClientSettings SubClientSetttings;

    std::shared_ptr<TFederatedDbObserver> Observer;
    NThreading::TFuture<void> AsyncInit;
    std::shared_ptr<TFederatedDbState> FederationState;

    // TODO
    // TLog Log;

    const TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();

    TAdaptiveLock Lock;

    std::vector<TSubSession> SubSessions;
    size_t SubsessionIndex = 0;

    // NGrpc::IQueueClientContextPtr DumpCountersContext;

    // Exiting.
    bool Closing = false;
};

} // namespace NYdb::NFederatedTopic
