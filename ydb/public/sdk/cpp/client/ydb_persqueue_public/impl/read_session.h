#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/aliases.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/read_session_impl.ipp>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/callback_context.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/counters_logger.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/persqueue_impl.h>

namespace NYdb::NPersQueue {

// High level class that manages several read session impls.
// Each one of them works with single cluster.
// This class communicates with cluster discovery service and then creates
// sessions to each cluster.
class TReadSession : public IReadSession,
                     public std::enable_shared_from_this<TReadSession> {
    struct TClusterSessionInfo {
        TClusterSessionInfo(const TString& cluster)
            : ClusterName(cluster)
        {
        }

        TString ClusterName; // In lower case
        TSingleClusterReadSessionImpl::TPtr Session;
        TVector<TTopicReadSettings> Topics;
        TString ClusterEndpoint;
    };

public:
    TReadSession(const TReadSessionSettings& settings,
                 std::shared_ptr<TPersQueueClient::TImpl> client,
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

    void AddTopic(const TTopicReadSettings& topicReadSettings) /*override*/ {
        Y_UNUSED(topicReadSettings);
        // TODO: implement.
        ThrowFatalError("Method \"AddTopic\" is not implemented");
    }

    void RemoveTopic(const TString& path) /*override*/ {
        Y_UNUSED(path);
        // TODO: implement.
        ThrowFatalError("Method \"RemoveTopic\" is not implemented");
    }

    void RemoveTopic(const TString& path, const TVector<ui64>& partitionGruops) /*override*/ {
        Y_UNUSED(path);
        Y_UNUSED(partitionGruops);
        // TODO: implement.
        ThrowFatalError("Method \"RemoveTopic\" is not implemented");
    }

    void StopReadingData() override;
    void ResumeReadingData() override;

    void Abort();

    void ClearAllEvents();

private:
    TStringBuilder GetLogPrefix() const;

    // Start
    bool ValidateSettings();

    // Cluster discovery
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest MakeClusterDiscoveryRequest() const;
    void StartClusterDiscovery();
    void OnClusterDiscovery(const TStatus& status, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult& result);
    void ProceedWithoutClusterDiscovery();
    void RestartClusterDiscoveryImpl(TDuration delay, TDeferredActions& deferred);
    void CreateClusterSessionsImpl(TDeferredActions& deferred);

    void AbortImpl(TDeferredActions& deferred);
    void AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions& deferred);
    void AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions& deferred);
    void AbortImpl(EStatus statusCode, const TString& message, TDeferredActions& deferred);

    void MakeCountersIfNeeded();
    void SetupCountersLogger();

private:
    TReadSessionSettings Settings;
    const TString SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    TLog Log;
    std::shared_ptr<TPersQueueClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<TReadSessionEventsQueue> EventsQueue;
    THashMap<TString, TClusterSessionInfo> ClusterSessions; // Cluster name (in lower case) -> TClusterSessionInfo
    NYdbGrpc::IQueueClientContextPtr ClusterDiscoveryDelayContext;
    IRetryPolicy::IRetryState::TPtr ClusterDiscoveryRetryState;
    bool DataReadingSuspended = false;

    // Exiting.
    bool Aborting = false;
    bool Closing = false;

    std::vector<TCallbackContextPtr> CbContexts;

    std::shared_ptr<TCountersLogger> CountersLogger;
    std::shared_ptr<TCallbackContext<TCountersLogger>> DumpCountersContext;
};

}  // namespace NYdb::NPersQueue
