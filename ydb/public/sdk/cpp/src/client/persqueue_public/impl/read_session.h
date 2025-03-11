#pragma once

#include <src/client/persqueue_public/impl/aliases.h>
#include <src/client/topic/impl/read_session_impl.ipp>
#include <src/client/topic/common/callback_context.h>
#include <src/client/topic/impl/counters_logger.h>
#include <src/client/persqueue_public/impl/persqueue_impl.h>

#include <unordered_map>

namespace NYdb::inline Dev::NPersQueue {

// High level class that manages several read session impls.
// Each one of them works with single cluster.
// This class communicates with cluster discovery service and then creates
// sessions to each cluster.
class TReadSession : public IReadSession,
                     public std::enable_shared_from_this<TReadSession> {
    struct TClusterSessionInfo {
        TClusterSessionInfo(const std::string& cluster)
            : ClusterName(cluster)
        {
        }

        std::string ClusterName; // In lower case
        TSingleClusterReadSessionImpl::TPtr Session;
        std::vector<TTopicReadSettings> Topics;
        std::string ClusterEndpoint;
    };

public:
    TReadSession(const TReadSessionSettings& settings,
                 std::shared_ptr<TPersQueueClient::TImpl> client,
                 std::shared_ptr<TGRpcConnectionsImpl> connections,
                 TDbDriverStatePtr dbDriverState);

    ~TReadSession();

    void Start();

    NThreading::TFuture<void> WaitEvent() override;
    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) override;
    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) override;

    bool Close(TDuration timeout) override;

    std::string GetSessionId() const override {
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

    void RemoveTopic(const std::string& path) /*override*/ {
        Y_UNUSED(path);
        // TODO: implement.
        ThrowFatalError("Method \"RemoveTopic\" is not implemented");
    }

    void RemoveTopic(const std::string& path, const std::vector<ui64>& partitionGruops) /*override*/ {
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
    void AbortImpl(EStatus statusCode, NYdb::NIssue::TIssues&& issues, TDeferredActions& deferred);
    void AbortImpl(EStatus statusCode, const std::string& message, TDeferredActions& deferred);

    void MakeCountersIfNeeded();
    void SetupCountersLogger();

private:
    TReadSessionSettings Settings;
    const std::string SessionId;
    const TInstant StartSessionTime = TInstant::Now();
    TLog Log;
    std::shared_ptr<TPersQueueClient::TImpl> Client;
    std::shared_ptr<TGRpcConnectionsImpl> Connections;
    TDbDriverStatePtr DbDriverState;
    TAdaptiveLock Lock;
    std::shared_ptr<TReadSessionEventsQueue> EventsQueue;
    std::unordered_map<std::string, TClusterSessionInfo> ClusterSessions; // Cluster name (in lower case) -> TClusterSessionInfo
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