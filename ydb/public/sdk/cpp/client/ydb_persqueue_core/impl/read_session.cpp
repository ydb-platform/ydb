#include "persqueue_impl.h"
#include "read_session.h"
#include "common.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>
#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/generic/yexception.h>
#include <util/stream/mem.h>
#include <util/system/env.h>

#include <variant>

namespace NYdb::NPersQueue {

static const TString DRIVER_IS_STOPPING_DESCRIPTION = "Driver is stopping";

static const bool RangesMode = !GetEnv("PQ_OFFSET_RANGES_MODE").empty();

std::pair<ui64, ui64> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, ui64 index) {
    if (dataReceivedEvent.IsCompressedMessages()) {
        const auto& msg = dataReceivedEvent.GetCompressedMessages()[index];
        return {msg.GetOffset(0), msg.GetOffset(msg.GetBlocksCount() - 1) + 1};
    }
    const auto& msg = dataReceivedEvent.GetMessages()[index];
    return {msg.GetOffset(), msg.GetOffset() + 1};
}

TString IssuesSingleLineString(const NYql::TIssues& issues) {
    return SubstGlobalCopy(issues.ToString(), '\n', ' ');
}

void MakeCountersNotNull(TReaderCounters& counters);
bool HasNullCounters(TReaderCounters& counters);

class TErrorHandler : public IErrorHandler {
public:
    TErrorHandler(std::weak_ptr<TReadSession> session)
        : Session(std::move(session))
    {
    }

    void AbortSession(TSessionClosedEvent&& closeEvent) override;

private:
    std::weak_ptr<TReadSession> Session;
};

TReadSession::TReadSession(const TReadSessionSettings& settings,
             std::shared_ptr<TPersQueueClient::TImpl> client,
             std::shared_ptr<TGRpcConnectionsImpl> connections,
             TDbDriverStatePtr dbDriverState)
    : Settings(settings)
    , SessionId(CreateGuidAsString())
    , Log(dbDriverState->Log)
    , Client(std::move(client))
    , Connections(std::move(connections))
    , DbDriverState(std::move(dbDriverState))
{
    if (!Settings.RetryPolicy_) {
        Settings.RetryPolicy_ = IRetryPolicy::GetDefaultPolicy();
    }

    MakeCountersIfNeeded();

    {
        TStringBuilder logPrefix;
        logPrefix << GetDatabaseLogPrefix(DbDriverState->Database) << "[" << SessionId << "] ";
        Log.SetFormatter(GetPrefixLogFormatter(logPrefix));
    }
}

TReadSession::~TReadSession() {
    Abort(EStatus::ABORTED, "Aborted");
    WaitAllDecompressionTasks();
    ClearAllEvents();
}

Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest TReadSession::MakeClusterDiscoveryRequest() const {
    Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest req;
    for (const TTopicReadSettings& topic : Settings.Topics_) {
        auto* params = req.add_read_sessions();
        params->set_topic(topic.Path_);
        params->mutable_all_original(); // set all_original
    }
    return req;
}

void TReadSession::Start() {
    ErrorHandler = MakeIntrusive<TErrorHandler>(weak_from_this());
    EventsQueue = std::make_shared<TReadSessionEventsQueue>(Settings, weak_from_this());

    if (!ValidateSettings()) {
        return;
    }

    Log << TLOG_INFO << "Starting read session";
    if (Settings.DisableClusterDiscovery_) {
        ProceedWithoutClusterDiscovery();
    } else {
        StartClusterDiscovery();
    }
}

bool TReadSession::ValidateSettings() {
    NYql::TIssues issues;
    if (Settings.Topics_.empty()) {
        issues.AddIssue("Empty topics list.");
    }

    if (Settings.ConsumerName_.empty()) {
        issues.AddIssue("No consumer specified.");
    }

    if (Settings.MaxMemoryUsageBytes_ < 1024 * 1024) {
        issues.AddIssue("Too small max memory usage. Valid values start from 1 megabyte.");
    }

    if (issues) {
        Abort(EStatus::BAD_REQUEST, MakeIssueWithSubIssues("Invalid read session settings", issues));
        return false;
    } else {
        return true;
    }
}

void TReadSession::StartClusterDiscovery() {
    with_lock (Lock) {
        if (Aborting) {
            return;
        }

        Log << TLOG_DEBUG << "Starting cluster discovery";
        ClusterDiscoveryDelayContext = nullptr;
    }

    auto extractor = [errorHandler = ErrorHandler, self = weak_from_this()]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
        auto selfShared = self.lock();
        if (!selfShared) {
            return;
        }

        Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult result;
        if (any) {
            any->UnpackTo(&result);
        }
        TStatus st(std::move(status));
        selfShared->OnClusterDiscovery(st, result);
    };

    Connections->RunDeferred<Ydb::PersQueue::V1::ClusterDiscoveryService,
                             Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest,
                             Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse>(
        MakeClusterDiscoveryRequest(),
        std::move(extractor),
        &Ydb::PersQueue::V1::ClusterDiscoveryService::Stub::AsyncDiscoverClusters,
        DbDriverState,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(Settings),
        /*ClientTimeout_*/TDuration::Seconds(5)); // TODO: make client timeout setting
}


void TReadSession::ProceedWithoutClusterDiscovery() {
    TDeferredActions deferred;
    with_lock (Lock) {
        if (Aborting) {
            return;
        }

        TString normalizedName = "null";
        THashMap<TString, TClusterSessionInfo>::iterator clusterSessionInfoIter;
        clusterSessionInfoIter = ClusterSessions.emplace(normalizedName, normalizedName).first;
        TClusterSessionInfo& clusterSessionInfo = clusterSessionInfoIter->second;
        clusterSessionInfo.ClusterEndpoint = DbDriverState->DiscoveryEndpoint;
        clusterSessionInfo.Topics = Settings.Topics_;
        CreateClusterSessionsImpl();
    }
    ScheduleDumpCountersToLog();
}

void TReadSession::CreateClusterSessionsImpl() {
    TDeferredActions deferred;
    // Create cluster sessions.
    ui64 partitionStreamIdStart = 1;
    const size_t clusterSessionsCount = ClusterSessions.size();
    for (auto& [clusterName, clusterSessionInfo] : ClusterSessions) {
        TReadSessionSettings sessionSettings = Settings;
        sessionSettings.Topics_ = clusterSessionInfo.Topics;
        if (sessionSettings.MaxMemoryUsageBytes_ > clusterSessionsCount && sessionSettings.MaxMemoryUsageBytes_ != std::numeric_limits<size_t>::max()) {
            sessionSettings.MaxMemoryUsageBytes_ /= clusterSessionsCount;
        }
        Log << TLOG_DEBUG << "Starting session to cluster " << clusterName << " (" << clusterSessionInfo.ClusterEndpoint << ")";
        auto subclient = Client->GetClientForEndpoint(clusterSessionInfo.ClusterEndpoint);
        auto context = subclient->CreateContext();
        if (!context) {
            AbortImpl(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION, deferred);
            return;
        }
        TStringBuilder logPrefix;
        logPrefix << GetDatabaseLogPrefix(DbDriverState->Database) << "[" << SessionId << "] [" << clusterName << "] ";
        TLog log = Log;
        log.SetFormatter(GetPrefixLogFormatter(logPrefix));
        clusterSessionInfo.Session =
            std::make_shared<TSingleClusterReadSessionImpl>(
                sessionSettings,
                clusterName,
                log,
                subclient->CreateReadSessionConnectionProcessorFactory(),
                EventsQueue,
                ErrorHandler,
                context,
                partitionStreamIdStart++, clusterSessionsCount);
        clusterSessionInfo.Session->Start();
    }
}

void TReadSession::OnClusterDiscovery(const TStatus& status, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult& result) {
    TDeferredActions deferred;
    with_lock (Lock) {
        if (Aborting) {
            return;
        }

        if (!status.IsSuccess()) {
            ++*Settings.Counters_->Errors;
            if (!ClusterDiscoveryRetryState) {
                ClusterDiscoveryRetryState = Settings.RetryPolicy_->CreateRetryState();
            }
            TMaybe<TDuration> retryDelay = ClusterDiscoveryRetryState->GetNextRetryDelay(status);
            if (retryDelay) {
                Log << TLOG_INFO << "Cluster discovery request failed. Status: " << status.GetStatus()
                                   << ". Issues: \"" << IssuesSingleLineString(status.GetIssues()) << "\"";
                RestartClusterDiscoveryImpl(*retryDelay, deferred);
            } else {
                AbortImpl(status.GetStatus(), MakeIssueWithSubIssues("Failed to discover clusters", status.GetIssues()), deferred);
            }
            return;
        }

        Log << TLOG_DEBUG << "Cluster discovery request succeeded";
        ClusterDiscoveryRetryState = nullptr;

        // Init ClusterSessions.
        if (static_cast<size_t>(result.read_sessions_clusters_size()) != Settings.Topics_.size()) {
            ++*Settings.Counters_->Errors;
            AbortImpl(EStatus::INTERNAL_ERROR, TStringBuilder() << "Unexpected reply from cluster discovery. Sizes of topics arrays don't match: "
                      << result.read_sessions_clusters_size() << " vs " << Settings.Topics_.size(), deferred);
            return;
        }

        const bool explicitlySpecifiedClusters = !Settings.Clusters_.empty();
        if (explicitlySpecifiedClusters) {
            for (const TString& cluster : Settings.Clusters_) {
                TString normalizedName = cluster;
                normalizedName.to_lower();
                ClusterSessions.emplace(normalizedName, normalizedName);
            }
        }

        NYql::TIssues issues;
        EStatus errorStatus = EStatus::INTERNAL_ERROR;
        for (size_t topicIndex = 0; topicIndex < Settings.Topics_.size(); ++topicIndex) {
            const TTopicReadSettings& topicSettings = Settings.Topics_[topicIndex];
            const Ydb::PersQueue::ClusterDiscovery::ReadSessionClusters& readSessionClusters = result.read_sessions_clusters(topicIndex);
            for (const Ydb::PersQueue::ClusterDiscovery::ClusterInfo& cluster : readSessionClusters.clusters()) {
                TString normalizedName = cluster.name();
                normalizedName.to_lower();
                THashMap<TString, TClusterSessionInfo>::iterator clusterSessionInfoIter;
                if (explicitlySpecifiedClusters) {
                    clusterSessionInfoIter = ClusterSessions.find(normalizedName);
                    if (clusterSessionInfoIter == ClusterSessions.end()) { // User hasn't specified this cluster, so it isn't in our interest.
                        continue;
                    }
                } else {
                    clusterSessionInfoIter = ClusterSessions.emplace(normalizedName, normalizedName).first;
                }
                TClusterSessionInfo& clusterSessionInfo = clusterSessionInfoIter->second;
                if (cluster.endpoint().empty()) {
                    issues.AddIssue(TStringBuilder() << "Unexpected reply from cluster discovery. Empty endpoint for cluster "
                                    << normalizedName);
                }
                if (clusterSessionInfo.ClusterEndpoint && clusterSessionInfo.ClusterEndpoint != cluster.endpoint()) {
                    issues.AddIssue(TStringBuilder() << "Unexpected reply from cluster discovery. Different endpoints for one cluster name. Cluster: "
                                    << normalizedName << ". \"" << clusterSessionInfo.ClusterEndpoint << "\" vs \""
                                    << cluster.endpoint() << "\"");
                }
                if (!clusterSessionInfo.ClusterEndpoint) {
                    clusterSessionInfo.ClusterEndpoint = ApplyClusterEndpoint(DbDriverState->DiscoveryEndpoint, cluster.endpoint());
                }
                clusterSessionInfo.Topics.reserve(Settings.Topics_.size());
                clusterSessionInfo.Topics.push_back(topicSettings);
            }
        }

        // Check clusters.
        for (const auto& [cluster, clusterInfo] : ClusterSessions) {
            if (clusterInfo.Topics.empty()) { // If specified explicitly by user.
                errorStatus = EStatus::BAD_REQUEST;
                issues.AddIssue(TStringBuilder() << "Unsupported cluster: " << cluster);
            }
        }

        if (issues) {
            ++*Settings.Counters_->Errors;
            AbortImpl(errorStatus, std::move(issues), deferred);
            return;
        }

        CreateClusterSessionsImpl();
    }
    ScheduleDumpCountersToLog();
}

void TReadSession::RestartClusterDiscoveryImpl(TDuration delay, TDeferredActions& deferred) {
    Log << TLOG_DEBUG << "Restart cluster discovery in " << delay;
    auto startCallback = [self = weak_from_this()](bool ok) {
        if (ok) {
            if (auto sharedSelf = self.lock()) {
                sharedSelf->StartClusterDiscovery();
            }
        }
    };

    ClusterDiscoveryDelayContext = Connections->CreateContext();
    if (!ClusterDiscoveryDelayContext) {
        AbortImpl(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION, deferred);
        return;
    }
    Connections->ScheduleCallback(delay,
                                  std::move(startCallback),
                                  ClusterDiscoveryDelayContext);
}

bool TReadSession::Close(TDuration timeout) {
    Log << TLOG_INFO << "Closing read session. Close timeout: " << timeout;
    with_lock (Lock) {
        Cancel(ClusterDiscoveryDelayContext);
        Cancel(DumpCountersContext);
    }
    // Log final counters.
    DumpCountersToLog();

    std::vector<TSingleClusterReadSessionImpl::TPtr> sessions;
    NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
    std::shared_ptr<std::atomic<size_t>> count = std::make_shared<std::atomic<size_t>>(0);
    auto callback = [=]() mutable {
        if (--*count == 0) {
            promise.TrySetValue(true);
        }
    };

    TDeferredActions deferred;
    with_lock (Lock) {
        if (Closing || Aborting) {
            return false;
        }

        if (!timeout) {
            AbortImpl(EStatus::ABORTED, "Close with zero timeout", deferred);
            return false;
        }

        Closing = true;
        sessions.reserve(ClusterSessions.size());
        for (auto& [cluster, sessionInfo] : ClusterSessions) {
            if (sessionInfo.Session) {
                sessions.emplace_back(sessionInfo.Session);
            }
        }
    }
    *count = sessions.size() + 1;
    for (const auto& session : sessions) {
        session->Close(callback);
    }

    callback(); // For the case when there are no subsessions yet.

    auto timeoutCallback = [=](bool) mutable {
        promise.TrySetValue(false);
    };

    auto timeoutContext = Connections->CreateContext();
    if (!timeoutContext) {
        AbortImpl(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION, deferred);
        return false;
    }
    Connections->ScheduleCallback(timeout,
                                  std::move(timeoutCallback),
                                  timeoutContext);

    // Wait.
    NThreading::TFuture<bool> resultFuture = promise.GetFuture();
    const bool result = resultFuture.GetValueSync();
    if (result) {
        Cancel(timeoutContext);

        NYql::TIssues issues;
        issues.AddIssue("Session was gracefully closed");
        EventsQueue->Close(TSessionClosedEvent(EStatus::SUCCESS, std::move(issues)), deferred);
    } else {
        ++*Settings.Counters_->Errors;
        for (const auto& session : sessions) {
            session->Abort();
        }

        NYql::TIssues issues;
        issues.AddIssue(TStringBuilder() << "Session was closed after waiting " << timeout);
        EventsQueue->Close(TSessionClosedEvent(EStatus::TIMEOUT, std::move(issues)), deferred);
    }

    with_lock (Lock) {
        Aborting = true; // Set abort flag for doing nothing on destructor.
    }
    return result;
}

void TReadSession::AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions& deferred) {
    if (!Aborting) {
        Aborting = true;
        Log << TLOG_NOTICE << "Aborting read session. Description: " << closeEvent.DebugString();
        Cancel(ClusterDiscoveryDelayContext);
        Cancel(DumpCountersContext);
        for (auto& [cluster, sessionInfo] : ClusterSessions) {
            if (sessionInfo.Session) {
                sessionInfo.Session->Abort();
            }
        }
        EventsQueue->Close(std::move(closeEvent), deferred);
    }
}

void TReadSession::AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions& deferred) {
    AbortImpl(TSessionClosedEvent(statusCode, std::move(issues)), deferred);
}

void TReadSession::AbortImpl(EStatus statusCode, const TString& message, TDeferredActions& deferred) {
    NYql::TIssues issues;
    issues.AddIssue(message);
    AbortImpl(statusCode, std::move(issues), deferred);
}

void TReadSession::Abort(EStatus statusCode, NYql::TIssues&& issues) {
    Abort(TSessionClosedEvent(statusCode, std::move(issues)));
}

void TReadSession::Abort(EStatus statusCode, const TString& message) {
    NYql::TIssues issues;
    issues.AddIssue(message);
    Abort(statusCode, std::move(issues));
}

void TReadSession::Abort(TSessionClosedEvent&& closeEvent) {
    TDeferredActions deferred;
    with_lock (Lock) {
        AbortImpl(std::move(closeEvent), deferred);
    }
}

void TReadSession::WaitAllDecompressionTasks() {
    for (auto& [cluster, sessionInfo] : ClusterSessions) {
        if (sessionInfo.Session) {
            sessionInfo.Session->WaitAllDecompressionTasks();
        }
    }
}

void TReadSession::ClearAllEvents() {
    EventsQueue->ClearAllEvents();
}

NThreading::TFuture<void> TReadSession::WaitEvent() {
    return EventsQueue->WaitEvent();
}

TVector<TReadSessionEvent::TEvent> TReadSession::GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) {
    return EventsQueue->GetEvents(block, maxEventsCount, maxByteSize);
}

TMaybe<TReadSessionEvent::TEvent> TReadSession::GetEvent(bool block, size_t maxByteSize) {
    return EventsQueue->GetEvent(block, maxByteSize);
}

void TReadSession::StopReadingData() {
    Log << TLOG_INFO << "Stop reading data";
    with_lock (Lock) {
        if (!DataReadingSuspended) {
            DataReadingSuspended = true;

            for (auto& [cluster, sessionInfo] : ClusterSessions) {
                if (sessionInfo.Session) {
                    sessionInfo.Session->StopReadingData();
                }
            }
        }
    }
}

void TReadSession::ResumeReadingData() {
    Log << TLOG_INFO << "Resume reading data";
    with_lock (Lock) {
        if (DataReadingSuspended) {
            DataReadingSuspended = false;

            for (auto& [cluster, sessionInfo] : ClusterSessions) {
                if (sessionInfo.Session) {
                    sessionInfo.Session->ResumeReadingData();
                }
            }
        }
    }
}

static ELogPriority GetEventLogPriority(const TReadSessionEvent::TEvent& event) {
    if (std::holds_alternative<TReadSessionEvent::TCreatePartitionStreamEvent>(event)
        || std::holds_alternative<TReadSessionEvent::TDestroyPartitionStreamEvent>(event)
        || std::holds_alternative<TReadSessionEvent::TPartitionStreamClosedEvent>(event)
        || std::holds_alternative<TSessionClosedEvent>(event))
    { // Control event.
        return TLOG_INFO;
    } else {
        return TLOG_DEBUG;
    }
}

void TReadSession::OnUserRetrievedEvent(const TReadSessionEvent::TEvent& event) {
    Log << GetEventLogPriority(event) << "Read session event " << DebugString(event);
}

void TReadSession::MakeCountersIfNeeded() {
    if (!Settings.Counters_ || HasNullCounters(*Settings.Counters_)) {
        TReaderCounters::TPtr counters = MakeIntrusive<TReaderCounters>();
        if (Settings.Counters_) {
            *counters = *Settings.Counters_; // Copy all counters that have been set by user.
        }
        MakeCountersNotNull(*counters);
        Settings.Counters(counters);
    }
}

void TReadSession::DumpCountersToLog(size_t timeNumber) {
    const bool logCounters = timeNumber % 60 == 0; // Every 1 minute.
    const bool dumpSessionsStatistics = timeNumber % 600 == 0; // Every 10 minutes.

    *Settings.Counters_->CurrentSessionLifetimeMs = (TInstant::Now() - StartSessionTime).MilliSeconds();
    std::vector<TSingleClusterReadSessionImpl::TPtr> sessions;
    with_lock (Lock) {
        if (Closing || Aborting) {
            return;
        }

        sessions.reserve(ClusterSessions.size());
        for (auto& [cluster, sessionInfo] : ClusterSessions) {
            if (sessionInfo.Session) {
                sessions.emplace_back(sessionInfo.Session);
            }
        }
    }

    {
        TMaybe<TLogElement> log;
        if (dumpSessionsStatistics) {
            log.ConstructInPlace(&Log, TLOG_INFO);
            (*log) << "Read/commit by partition streams (cluster:topic:partition:stream-id:read-offset:committed-offset):";
        }
        for (const auto& session : sessions) {
            session->UpdateMemoryUsageStatistics();
            if (dumpSessionsStatistics) {
                session->DumpStatisticsToLog(*log);
            }
        }
    }

#define C(counter)                                                      \
    << " " Y_STRINGIZE(counter) ": "                                    \
    << Settings.Counters_->counter->Val()                               \
        /**/

    if (logCounters) {
        Log << TLOG_INFO
            << "Counters: {"
            C(Errors)
            C(CurrentSessionLifetimeMs)
            C(BytesRead)
            C(MessagesRead)
            C(BytesReadCompressed)
            C(BytesInflightUncompressed)
            C(BytesInflightCompressed)
            C(BytesInflightTotal)
            C(MessagesInflight)
            << " }";
    }

#undef C

    ScheduleDumpCountersToLog(timeNumber + 1);
}

void TReadSession::ScheduleDumpCountersToLog(size_t timeNumber) {
    with_lock(Lock) {
        DumpCountersContext = Connections->CreateContext();
    }
    if (DumpCountersContext) {
        auto callback = [self = weak_from_this(), timeNumber](bool ok) {
            if (ok) {
                if (auto sharedSelf = self.lock()) {
                    sharedSelf->DumpCountersToLog(timeNumber);
                }
            }
        };
        Connections->ScheduleCallback(TDuration::Seconds(1),
                                      std::move(callback),
                                      DumpCountersContext);
    }
}

TPartitionStreamImpl::~TPartitionStreamImpl() = default;

TLog TPartitionStreamImpl::GetLog() const {
    if (auto session = Session.lock()) {
        return session->GetLog();
    }
    return {};
}

void TPartitionStreamImpl::Commit(ui64 startOffset, ui64 endOffset) {
    std::vector<std::pair<ui64, ui64>> toCommit;
    if (auto sessionShared = Session.lock()) {
        Y_VERIFY(endOffset > startOffset);
        with_lock(sessionShared->Lock) {
            if (!AddToCommitRanges(startOffset, endOffset, true)) // Add range for real commit always.
                return;

            Y_VERIFY(!Commits.Empty());
            for (auto c : Commits) {
                if (c.first >= endOffset) break; // Commit only gaps before client range.
                toCommit.emplace_back(c);
            }
            Commits.EraseInterval(0, endOffset); // Drop only committed ranges;
        }
        for (auto range: toCommit) {
            sessionShared->Commit(this, range.first, range.second);
        }
    }
}

void TPartitionStreamImpl::RequestStatus() {
    if (auto sessionShared = Session.lock()) {
        sessionShared->RequestPartitionStreamStatus(this);
    }
}

void TPartitionStreamImpl::ConfirmCreate(TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset) {
    if (auto sessionShared = Session.lock()) {
        sessionShared->ConfirmPartitionStreamCreate(this, readOffset, commitOffset);
    }
}

void TPartitionStreamImpl::ConfirmDestroy() {
    if (auto sessionShared = Session.lock()) {
        sessionShared->ConfirmPartitionStreamDestroy(this);
    }
}

void TPartitionStreamImpl::StopReading() {
    Y_FAIL("Not implemented"); // TODO
}

void TPartitionStreamImpl::ResumeReading() {
    Y_FAIL("Not implemented"); // TODO
}

void TPartitionStreamImpl::SignalReadyEvents(TReadSessionEventsQueue* queue, TDeferredActions& deferred) {
    for (auto& event : EventsQueue) {
        event.Signal(this, queue, deferred);

        if (!event.IsReady()) {
            break;
        }
    }
}

void TSingleClusterReadSessionImpl::Start() {
    Settings.DecompressionExecutor_->Start();
    Settings.EventHandlers_.HandlersExecutor_->Start();
    if (!Reconnect(TPlainStatus())) {
        ErrorHandler->AbortSession(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION);
    }
}

bool TSingleClusterReadSessionImpl::Reconnect(const TPlainStatus& status) {
    TDuration delay = TDuration::Zero();
    NGrpc::IQueueClientContextPtr delayContext = nullptr;
    NGrpc::IQueueClientContextPtr connectContext = ClientContext->CreateContext();
    NGrpc::IQueueClientContextPtr connectTimeoutContext = ClientContext->CreateContext();
    if (!connectContext || !connectTimeoutContext) {
        return false;
    }

    // Previous operations contexts.
    NGrpc::IQueueClientContextPtr prevConnectContext;
    NGrpc::IQueueClientContextPtr prevConnectTimeoutContext;
    NGrpc::IQueueClientContextPtr prevConnectDelayContext;

    if (!status.Ok()) {
        Log << TLOG_INFO << "Got error. Status: " << status.Status << ". Description: " << IssuesSingleLineString(status.Issues);
    }

    TDeferredActions deferred;
    with_lock (Lock) {
        if (Aborting) {
            Cancel(connectContext);
            Cancel(connectTimeoutContext);
            return false;
        }
        Processor = nullptr;
        WaitingReadResponse = false;
        ServerMessage = std::make_shared<Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>();
        ++ConnectionGeneration;
        if (RetryState) {
            TMaybe<TDuration> nextDelay = RetryState->GetNextRetryDelay(TPlainStatus(status));
            if (nextDelay) {
                delay = *nextDelay;
                delayContext = ClientContext->CreateContext();
                if (!delayContext) {
                    return false;
                }
                Log << TLOG_DEBUG << "Reconnecting session to cluster " << ClusterName << " in "<< delay;
            } else {
                return false;
            }
        } else {
            RetryState = Settings.RetryPolicy_->CreateRetryState();
        }
        ++ConnectionAttemptsDone;

        // Set new context
        prevConnectContext = std::exchange(ConnectContext, connectContext);
        prevConnectTimeoutContext = std::exchange(ConnectTimeoutContext, connectTimeoutContext);
        prevConnectDelayContext = std::exchange(ConnectDelayContext, delayContext);

        Y_ASSERT(ConnectContext);
        Y_ASSERT(ConnectTimeoutContext);
        Y_ASSERT((delay == TDuration::Zero()) == !ConnectDelayContext);

        // Destroy all partition streams before connecting.
        DestroyAllPartitionStreamsImpl(deferred);
    }

    // Cancel previous operations.
    Cancel(prevConnectContext);
    Cancel(prevConnectTimeoutContext);
    Cancel(prevConnectDelayContext);

    auto connectCallback = [weakThis = weak_from_this(), connectContext = connectContext](TPlainStatus&& st, typename IProcessor::TPtr&& processor) {
        if (auto sharedThis = weakThis.lock()) {
            sharedThis->OnConnect(std::move(st), std::move(processor), connectContext);
        }
    };

    auto connectTimeoutCallback = [weakThis = weak_from_this(), connectTimeoutContext = connectTimeoutContext](bool ok) {
        if (ok) {
            if (auto sharedThis = weakThis.lock()) {
                sharedThis->OnConnectTimeout(connectTimeoutContext);
            }
        }
    };

    Y_ASSERT(connectContext);
    Y_ASSERT(connectTimeoutContext);
    Y_ASSERT((delay == TDuration::Zero()) == !delayContext);
    ConnectionFactory->CreateProcessor(
        std::move(connectCallback),
        TRpcRequestSettings::Make(Settings),
        std::move(connectContext),
        TDuration::Seconds(30) /* connect timeout */, // TODO: make connect timeout setting.
        std::move(connectTimeoutContext),
        std::move(connectTimeoutCallback),
        delay,
        std::move(delayContext));
    return true;
}

void TSingleClusterReadSessionImpl::BreakConnectionAndReconnectImpl(TPlainStatus&& status, TDeferredActions& deferred) {
    Log << TLOG_INFO << "Break connection due to unexpected message from server. Status: " << status.Status << ", Issues: \"" << IssuesSingleLineString(status.Issues) << "\"";

    Processor->Cancel();
    Processor = nullptr;
    RetryState = Settings.RetryPolicy_->CreateRetryState(); // Explicitly create retry state to determine whether we should connect to server again.

    deferred.DeferReconnection(shared_from_this(), ErrorHandler, std::move(status));
}

void TSingleClusterReadSessionImpl::OnConnectTimeout(const NGrpc::IQueueClientContextPtr& connectTimeoutContext) {
    with_lock (Lock) {
        if (ConnectTimeoutContext == connectTimeoutContext) {
            Cancel(ConnectContext);
            ConnectContext = nullptr;
            ConnectTimeoutContext = nullptr;
            ConnectDelayContext = nullptr;

            if (Closing || Aborting) {
                CallCloseCallbackImpl();
                return;
            }
        } else {
            return;
        }
    }

    ++*Settings.Counters_->Errors;
    TStringBuilder description;
    description << "Failed to establish connection to server. Attempts done: " << ConnectionAttemptsDone;
    if (!Reconnect(TPlainStatus(EStatus::TIMEOUT, description))) {
        ErrorHandler->AbortSession(EStatus::TIMEOUT, description);
    }
}

void TSingleClusterReadSessionImpl::OnConnect(TPlainStatus&& st, typename IProcessor::TPtr&& processor, const NGrpc::IQueueClientContextPtr& connectContext) {
    TDeferredActions deferred;
    with_lock (Lock) {
        if (ConnectContext == connectContext) {
            Cancel(ConnectTimeoutContext);
            ConnectContext = nullptr;
            ConnectTimeoutContext = nullptr;
            ConnectDelayContext = nullptr;

            if (Closing || Aborting) {
                CallCloseCallbackImpl();
                return;
            }

            if (st.Ok()) {
                Processor = std::move(processor);
                RetryState = nullptr;
                ConnectionAttemptsDone = 0;
                InitImpl(deferred);
                return;
            }
        } else {
            return;
        }
    }

    if (!st.Ok()) {
        ++*Settings.Counters_->Errors;
        if (!Reconnect(st)) {
            ErrorHandler->AbortSession(st.Status,
                                       MakeIssueWithSubIssues(
                                           TStringBuilder() << "Failed to establish connection to server \"" << st.Endpoint << "\" ( cluster " << ClusterName << "). Attempts done: "
                                               << ConnectionAttemptsDone,
                                           st.Issues));
        }
    }
}

void TSingleClusterReadSessionImpl::InitImpl(TDeferredActions& deferred) { // Assumes that we're under lock.
    Log << TLOG_DEBUG << "Successfully connected. Initializing session";
    Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
    auto& init = *req.mutable_init_request();
    init.set_ranges_mode(RangesMode);
    for (const TTopicReadSettings& topic : Settings.Topics_) {
        auto* topicSettings = init.add_topics_read_settings();
        topicSettings->set_topic(topic.Path_);
        if (topic.StartingMessageTimestamp_) {
            topicSettings->set_start_from_written_at_ms(topic.StartingMessageTimestamp_->MilliSeconds());
        }
        for (ui64 groupId : topic.PartitionGroupIds_) {
            topicSettings->add_partition_group_ids(groupId);
        }
    }
    init.set_consumer(Settings.ConsumerName_);
    init.set_read_only_original(Settings.ReadOnlyOriginal_);
    init.mutable_read_params()->set_max_read_size(Settings.MaxMemoryUsageBytes_);
    if (Settings.MaxTimeLag_) {
        init.set_max_lag_duration_ms(Settings.MaxTimeLag_->MilliSeconds());
    }
    if (Settings.StartingMessageTimestamp_) {
        init.set_start_from_written_at_ms(Settings.StartingMessageTimestamp_->MilliSeconds());
    }

    WriteToProcessorImpl(std::move(req));
    ReadFromProcessorImpl(deferred);
}

void TSingleClusterReadSessionImpl::ContinueReadingDataImpl() { // Assumes that we're under lock.
    if (!Closing
        && !Aborting
        && !WaitingReadResponse
        && !DataReadingSuspended
        && Processor
        && CompressedDataSize < GetCompressedDataSizeLimit()
        && static_cast<size_t>(CompressedDataSize + DecompressedDataSize) < Settings.MaxMemoryUsageBytes_)
    {
        Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
        req.mutable_read();

        WriteToProcessorImpl(std::move(req));
        WaitingReadResponse = true;
    }
}

bool TSingleClusterReadSessionImpl::IsActualPartitionStreamImpl(const TPartitionStreamImpl* partitionStream) { // Assumes that we're under lock.
    auto actualPartitionStreamIt = PartitionStreams.find(partitionStream->GetAssignId());
    return actualPartitionStreamIt != PartitionStreams.end()
        && actualPartitionStreamIt->second->GetPartitionStreamId() == partitionStream->GetPartitionStreamId();
}

void TSingleClusterReadSessionImpl::ConfirmPartitionStreamCreate(const TPartitionStreamImpl* partitionStream, TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset) {
    TStringBuilder commitOffsetLogStr;
    if (commitOffset) {
        commitOffsetLogStr << ". Commit offset: " << *commitOffset;
    }
    Log << TLOG_INFO << "Confirm partition stream create. Partition stream id: " << partitionStream->GetPartitionStreamId()
        << ". Cluster: \"" << partitionStream->GetCluster() << "\". Topic: \"" << partitionStream->GetTopicPath()
        << "\". Partition: " << partitionStream->GetPartitionId()
        << ". Read offset: " << readOffset << commitOffsetLogStr;

    with_lock (Lock) {
        if (Aborting || Closing || !IsActualPartitionStreamImpl(partitionStream)) { // Got previous incarnation.
            Log << TLOG_DEBUG << "Skip partition stream create confirm. Partition stream id: " << partitionStream->GetPartitionStreamId();
            return;
        }

        Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
        auto& startRead = *req.mutable_start_read();
        startRead.mutable_topic()->set_path(partitionStream->GetTopicPath());
        startRead.set_cluster(partitionStream->GetCluster());
        startRead.set_partition(partitionStream->GetPartitionId());
        startRead.set_assign_id(partitionStream->GetAssignId());
        if (readOffset) {
            startRead.set_read_offset(*readOffset);
        }
        if (commitOffset) {
            startRead.set_commit_offset(*commitOffset);
        }

        WriteToProcessorImpl(std::move(req));
    }
}

void TSingleClusterReadSessionImpl::ConfirmPartitionStreamDestroy(TPartitionStreamImpl* partitionStream) {
    Log << TLOG_INFO << "Confirm partition stream destroy. Partition stream id: " << partitionStream->GetPartitionStreamId()
        << ". Cluster: \"" << partitionStream->GetCluster() << "\". Topic: \"" << partitionStream->GetTopicPath()
        << "\". Partition: " << partitionStream->GetPartitionId();

    TDeferredActions deferred;
    with_lock (Lock) {
        if (Aborting || Closing || !IsActualPartitionStreamImpl(partitionStream)) { // Got previous incarnation.
            Log << TLOG_DEBUG << "Skip partition stream destroy confirm. Partition stream id: " << partitionStream->GetPartitionStreamId();
            return;
        }

        CookieMapping.RemoveMapping(partitionStream->GetPartitionStreamId());
        PartitionStreams.erase(partitionStream->GetAssignId());
        EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TPartitionStreamClosedEvent(partitionStream, TReadSessionEvent::TPartitionStreamClosedEvent::EReason::DestroyConfirmedByUser)}, deferred);

        Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
        auto& released = *req.mutable_released();
        released.mutable_topic()->set_path(partitionStream->GetTopicPath());
        released.set_cluster(partitionStream->GetCluster());
        released.set_partition(partitionStream->GetPartitionId());
        released.set_assign_id(partitionStream->GetAssignId());

        WriteToProcessorImpl(std::move(req));
    }
}

void TSingleClusterReadSessionImpl::Commit(const TPartitionStreamImpl* partitionStream, ui64 startOffset, ui64 endOffset) {
    Log << TLOG_DEBUG << "Commit offsets [" << startOffset << ", " << endOffset << "). Partition stream id: " << partitionStream->GetPartitionStreamId();
    with_lock (Lock) {
        if (Aborting || Closing || !IsActualPartitionStreamImpl(partitionStream)) { // Got previous incarnation.
            return;
        }
        Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
        bool hasSomethingToCommit = false;
        if (RangesMode) {
            hasSomethingToCommit = true;
            auto* range = req.mutable_commit()->add_offset_ranges();
            range->set_assign_id(partitionStream->GetAssignId());
            range->set_start_offset(startOffset);
            range->set_end_offset(endOffset);
        } else {
            for (ui64 offset = startOffset; offset < endOffset; ++offset) {
                TPartitionCookieMapping::TCookie::TPtr cookie = CookieMapping.CommitOffset(partitionStream->GetPartitionStreamId(), offset);
                if (cookie) {
                    hasSomethingToCommit = true;
                    auto* cookieInfo = req.mutable_commit()->add_cookies();
                    cookieInfo->set_assign_id(partitionStream->GetAssignId());
                    cookieInfo->set_partition_cookie(cookie->Cookie);
                }
            }
        }
        if (hasSomethingToCommit) {
            WriteToProcessorImpl(std::move(req));
        }
    }
}

void TSingleClusterReadSessionImpl::RequestPartitionStreamStatus(const TPartitionStreamImpl* partitionStream) {
    Log << TLOG_DEBUG << "Requesting status for partition stream id: " << partitionStream->GetPartitionStreamId();
    with_lock (Lock) {
        if (Aborting || Closing || !IsActualPartitionStreamImpl(partitionStream)) { // Got previous incarnation.
            return;
        }

        Ydb::PersQueue::V1::MigrationStreamingReadClientMessage req;
        auto& status = *req.mutable_status();
        status.mutable_topic()->set_path(partitionStream->GetTopicPath());
        status.set_cluster(partitionStream->GetCluster());
        status.set_partition(partitionStream->GetPartitionId());
        status.set_assign_id(partitionStream->GetAssignId());

        WriteToProcessorImpl(std::move(req));
    }
}

void TSingleClusterReadSessionImpl::OnUserRetrievedEvent(const TReadSessionEvent::TEvent& event) {
    Log << TLOG_DEBUG << "Read session event " << DebugString(event);
    const i64 bytesCount = static_cast<i64>(CalcDataSize(event));
    Y_ASSERT(bytesCount >= 0);

    if (!std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) { // Event is not data event.
        return;
    }

    *Settings.Counters_->MessagesInflight -= std::get<TReadSessionEvent::TDataReceivedEvent>(event).GetMessagesCount();
    *Settings.Counters_->BytesInflightTotal -= bytesCount;
    *Settings.Counters_->BytesInflightUncompressed -= bytesCount;

    TDeferredActions deferred;
    with_lock (Lock) {
        UpdateMemoryUsageStatisticsImpl();
        Y_VERIFY(bytesCount <= DecompressedDataSize);
        DecompressedDataSize -= bytesCount;
        ContinueReadingDataImpl();
        StartDecompressionTasksImpl(deferred);
    }
}

void TSingleClusterReadSessionImpl::WriteToProcessorImpl(Ydb::PersQueue::V1::MigrationStreamingReadClientMessage&& req) { // Assumes that we're under lock.
    if (Processor) {
        Processor->Write(std::move(req));
    }
}

bool TSingleClusterReadSessionImpl::HasCommitsInflightImpl() const {
    for (const auto& [id, partitionStream] : PartitionStreams) {
        if (partitionStream->HasCommitsInflight())
            return true;
    }
    return false;
}

void TSingleClusterReadSessionImpl::ReadFromProcessorImpl(TDeferredActions& deferred) { // Assumes that we're under lock.
    if (Closing && !HasCommitsInflightImpl()) {
        Processor->Cancel();
        CallCloseCallbackImpl();
        return;
    }

    if (Processor) {
        ServerMessage->Clear();

        auto callback = [weakThis = weak_from_this(),
                         connectionGeneration = ConnectionGeneration,
                         // Capture message & processor not to read in freed memory.
                         serverMessage = ServerMessage,
                         processor = Processor](NGrpc::TGrpcStatus&& grpcStatus) {
            if (auto sharedThis = weakThis.lock()) {
                sharedThis->OnReadDone(std::move(grpcStatus), connectionGeneration);
            }
        };

        deferred.DeferReadFromProcessor(Processor, ServerMessage.get(), std::move(callback));
    }
}

void TSingleClusterReadSessionImpl::OnReadDone(NGrpc::TGrpcStatus&& grpcStatus, size_t connectionGeneration) {
    TPlainStatus errorStatus;
    if (!grpcStatus.Ok()) {
        errorStatus = TPlainStatus(std::move(grpcStatus));
    }

    TDeferredActions deferred;
    with_lock (Lock) {
        if (Aborting) {
            return;
        }

        if (connectionGeneration != ConnectionGeneration) {
            return; // Message from previous connection. Ignore.
        }
        if (errorStatus.Ok()) {
            if (IsErrorMessage(*ServerMessage)) {
                errorStatus = MakeErrorFromProto(*ServerMessage);
            } else {
                switch (ServerMessage->response_case()) {
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kInitResponse:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_init_response()), deferred);
                    break;
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kDataBatch:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_data_batch()), deferred);
                    break;
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kAssigned:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_assigned()), deferred);
                    break;
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kRelease:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_release()), deferred);
                    break;
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kCommitted:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_committed()), deferred);
                    break;
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::kPartitionStatus:
                    OnReadDoneImpl(std::move(*ServerMessage->mutable_partition_status()), deferred);
                    break;
                case Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::RESPONSE_NOT_SET:
                    errorStatus = TPlainStatus::Internal("Unexpected response from server");
                    break;
                }
                if (errorStatus.Ok()) {
                    ReadFromProcessorImpl(deferred); // Read next.
                }
            }
        }
    }
    if (!errorStatus.Ok()) {
        ++*Settings.Counters_->Errors;
        RetryState = Settings.RetryPolicy_->CreateRetryState(); // Explicitly create retry state to determine whether we should connect to server again.
        if (!Reconnect(errorStatus)) {
            ErrorHandler->AbortSession(std::move(errorStatus));
        }
    }
}

void TSingleClusterReadSessionImpl::OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::InitResponse&& msg, TDeferredActions& deferred) { // Assumes that we're under lock.
    Y_UNUSED(deferred);

    Log << TLOG_INFO << "Server session id: " << msg.session_id();

    // Successful init. Do nothing.
    ContinueReadingDataImpl();
}

void TSingleClusterReadSessionImpl::OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch&& msg, TDeferredActions& deferred) { // Assumes that we're under lock.
    if (Closing || Aborting) {
        return; // Don't process new data.
    }
    UpdateMemoryUsageStatisticsImpl();
    for (Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData& partitionData : *msg.mutable_partition_data()) {
        auto partitionStreamIt = PartitionStreams.find(partitionData.cookie().assign_id());
        if (partitionStreamIt == PartitionStreams.end()) {
            ++*Settings.Counters_->Errors;
            BreakConnectionAndReconnectImpl(EStatus::INTERNAL_ERROR,
                                            TStringBuilder() << "Got unexpected partition stream data message. Topic: "
                                                << partitionData.topic()
                                                << ". Partition: " << partitionData.partition() << " AssignId: " << partitionData.cookie().assign_id(),
                                            deferred);
            return;
        }
        const TIntrusivePtr<TPartitionStreamImpl>& partitionStream = partitionStreamIt->second;

        TPartitionCookieMapping::TCookie::TPtr cookie = MakeIntrusive<TPartitionCookieMapping::TCookie>(partitionData.cookie().partition_cookie(), partitionStream);

        ui64 firstOffset = std::numeric_limits<ui64>::max();
        ui64 currentOffset = std::numeric_limits<ui64>::max();
        ui64 desiredOffset = partitionStream->GetFirstNotReadOffset();
        for (const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch& batch : partitionData.batches()) {
            // Validate messages.
            for (const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::MessageData& messageData : batch.message_data()) {
                // Check offsets continuity.
                if (messageData.offset() != desiredOffset) {
                    bool res = partitionStream->AddToCommitRanges(desiredOffset, messageData.offset(), RangesMode);
                    Y_VERIFY(res);
                }

                if (firstOffset == std::numeric_limits<ui64>::max()) {
                    firstOffset = messageData.offset();
                }
                currentOffset = messageData.offset();
                desiredOffset = currentOffset + 1;
                partitionStream->UpdateMaxReadOffset(currentOffset);
                const i64 messageSize = static_cast<i64>(messageData.data().size());
                CompressedDataSize += messageSize;
                *Settings.Counters_->BytesInflightTotal += messageSize;
                *Settings.Counters_->BytesInflightCompressed += messageSize;
                ++*Settings.Counters_->MessagesInflight;
            }
        }
        if (firstOffset == std::numeric_limits<ui64>::max()) {
            BreakConnectionAndReconnectImpl(EStatus::INTERNAL_ERROR,
                                            TStringBuilder() << "Got empty data message. Topic: "
                                                << partitionData.topic()
                                                << ". Partition: " << partitionData.partition()
                                                << " message: " << msg,
                                            deferred);
            return;
        }
        cookie->SetOffsetRange(std::make_pair(firstOffset, desiredOffset));
        partitionStream->SetFirstNotReadOffset(desiredOffset);
        if (!CookieMapping.AddMapping(cookie)) {
            BreakConnectionAndReconnectImpl(EStatus::INTERNAL_ERROR,
                                            TStringBuilder() << "Got unexpected data message. Topic: "
                                                << partitionData.topic()
                                                << ". Partition: " << partitionData.partition()
                                                << ". Cookie mapping already has such cookie",
                                            deferred);
            return;
        }
        TDataDecompressionInfo* decompressionInfo = EventsQueue->PushDataEvent(partitionStream, std::move(partitionData));
        Y_VERIFY(decompressionInfo);
        if (decompressionInfo) {
            DecompressionQueue.emplace_back(decompressionInfo, partitionStream);
            StartDecompressionTasksImpl(deferred);
        }
    }

    WaitingReadResponse = false;
    ContinueReadingDataImpl();
}

void TSingleClusterReadSessionImpl::OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::Assigned&& msg, TDeferredActions& deferred) { // Assumes that we're under lock.
    auto partitionStream = MakeIntrusive<TPartitionStreamImpl>(NextPartitionStreamId,
                                                               msg.topic().path(),
                                                               msg.cluster(),
                                                               msg.partition() + 1, // Group.
                                                               msg.partition(), // Partition.
                                                               msg.assign_id(),
                                                               msg.read_offset(),
                                                               weak_from_this(),
                                                               ErrorHandler);
    NextPartitionStreamId += PartitionStreamIdStep;

    // Renew partition stream.
    TIntrusivePtr<TPartitionStreamImpl>& currentPartitionStream = PartitionStreams[partitionStream->GetAssignId()];
    if (currentPartitionStream) {
        CookieMapping.RemoveMapping(currentPartitionStream->GetPartitionStreamId());
        EventsQueue->PushEvent({currentPartitionStream, weak_from_this(), TReadSessionEvent::TPartitionStreamClosedEvent(currentPartitionStream, TReadSessionEvent::TPartitionStreamClosedEvent::EReason::Lost)}, deferred);
    }
    currentPartitionStream = partitionStream;

    // Send event to user.
    EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TCreatePartitionStreamEvent(partitionStream, msg.read_offset(), msg.end_offset())}, deferred);
}

void TSingleClusterReadSessionImpl::OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::Release&& msg, TDeferredActions& deferred) { // Assumes that we're under lock.
    auto partitionStreamIt = PartitionStreams.find(msg.assign_id());
    if (partitionStreamIt == PartitionStreams.end()) {
        return;
    }
    TIntrusivePtr<TPartitionStreamImpl> partitionStream = partitionStreamIt->second;
    if (msg.forceful_release()) {
        PartitionStreams.erase(msg.assign_id());
        CookieMapping.RemoveMapping(partitionStream->GetPartitionStreamId());
        EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TPartitionStreamClosedEvent(partitionStream, TReadSessionEvent::TPartitionStreamClosedEvent::EReason::Lost)}, deferred);
    } else {
        EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TDestroyPartitionStreamEvent(std::move(partitionStream), msg.commit_offset())}, deferred);
    }
}

void TSingleClusterReadSessionImpl::OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::Committed&& msg, TDeferredActions& deferred) { // Assumes that we're under lock.

    Log << TLOG_DEBUG << "Committed response: " << msg;

    TMap<ui64, TIntrusivePtr<TPartitionStreamImpl>> partitionStreams;
    for (const Ydb::PersQueue::V1::CommitCookie& cookieProto : msg.cookies()) {
        TPartitionCookieMapping::TCookie::TPtr cookie = CookieMapping.RetrieveCommittedCookie(cookieProto);
        if (cookie) {
            cookie->PartitionStream->UpdateMaxCommittedOffset(cookie->OffsetRange.second);
            partitionStreams[cookie->PartitionStream->GetPartitionStreamId()] = cookie->PartitionStream;
        }
    }
    for (auto& [id, partitionStream] : partitionStreams) {
        EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TCommitAcknowledgementEvent(partitionStream, partitionStream->GetMaxCommittedOffset())}, deferred);
    }

    for (const auto& rangeProto : msg.offset_ranges()) {
        auto partitionStreamIt = PartitionStreams.find(rangeProto.assign_id());
        if (partitionStreamIt != PartitionStreams.end()) {
            auto partitionStream = partitionStreamIt->second;
            partitionStream->UpdateMaxCommittedOffset(rangeProto.end_offset());
            EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TCommitAcknowledgementEvent(partitionStream, rangeProto.end_offset())}, deferred);
        }
    }

}

void TSingleClusterReadSessionImpl::OnReadDoneImpl(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::PartitionStatus&& msg, TDeferredActions& deferred) { // Assumes that we're under lock.
    auto partitionStreamIt = PartitionStreams.find(msg.assign_id());
    if (partitionStreamIt == PartitionStreams.end()) {
        return;
    }
    EventsQueue->PushEvent(
        {partitionStreamIt->second, weak_from_this(), TReadSessionEvent::TPartitionStreamStatusEvent(partitionStreamIt->second,
                                                                                                     msg.committed_offset(),
                                                                                                     0, // TODO: support read offset in status
                                                                                                     msg.end_offset(),
                                                                                                     TInstant::MilliSeconds(msg.write_watermark_ms()))},
        deferred);
}

void TSingleClusterReadSessionImpl::StartDecompressionTasksImpl(TDeferredActions& deferred) {
    UpdateMemoryUsageStatisticsImpl();
    const i64 limit = GetDecompressedDataSizeLimit();
    Y_VERIFY(limit > 0);
    while (DecompressedDataSize < limit
           && (static_cast<size_t>(CompressedDataSize + DecompressedDataSize) < Settings.MaxMemoryUsageBytes_
               || DecompressedDataSize == 0 /* Allow decompression of at least one message even if memory is full. */)
           && !DecompressionQueue.empty())
    {
        TDecompressionQueueItem& current = DecompressionQueue.front();
        auto sentToDecompress = current.BatchInfo->StartDecompressionTasks(Settings.DecompressionExecutor_,
                                                                           Max(limit - DecompressedDataSize, static_cast<i64>(1)),
                                                                           AverageCompressionRatio,
                                                                           current.PartitionStream,
                                                                           deferred);
        DecompressedDataSize += sentToDecompress;
        if (current.BatchInfo->AllDecompressionTasksStarted()) {
            DecompressionQueue.pop_front();
        } else {
            break;
        }
    }
}

void TSingleClusterReadSessionImpl::DestroyAllPartitionStreamsImpl(TDeferredActions& deferred) {
    for (auto&& [key, partitionStream] : PartitionStreams) {
        EventsQueue->PushEvent({partitionStream, weak_from_this(), TReadSessionEvent::TPartitionStreamClosedEvent(std::move(partitionStream), TReadSessionEvent::TPartitionStreamClosedEvent::EReason::ConnectionLost)}, deferred);
    }
    PartitionStreams.clear();
    CookieMapping.ClearMapping();
}

void TSingleClusterReadSessionImpl::OnCreateNewDecompressionTask() {
    ++DecompressionTasksInflight;
}

void TSingleClusterReadSessionImpl::OnDataDecompressed(i64 sourceSize, i64 estimatedDecompressedSize, i64 decompressedSize, size_t messagesCount) {
    TDeferredActions deferred;
    --DecompressionTasksInflight;

    *Settings.Counters_->BytesRead += decompressedSize;
    *Settings.Counters_->BytesReadCompressed += sourceSize;
    *Settings.Counters_->MessagesRead += messagesCount;
    *Settings.Counters_->BytesInflightUncompressed += decompressedSize;
    *Settings.Counters_->BytesInflightCompressed -= sourceSize;
    *Settings.Counters_->BytesInflightTotal += (decompressedSize - sourceSize);

    with_lock (Lock) {
        UpdateMemoryUsageStatisticsImpl();
        CompressedDataSize -= sourceSize;
        DecompressedDataSize += decompressedSize - estimatedDecompressedSize;
        constexpr double weight = 0.6;
        AverageCompressionRatio = weight * static_cast<double>(decompressedSize) / static_cast<double>(sourceSize) + (1 - weight) * AverageCompressionRatio;
        if (Aborting) {
            return;
        }
        ContinueReadingDataImpl();
        StartDecompressionTasksImpl(deferred);
    }
}

void TSingleClusterReadSessionImpl::Abort() {
    Log << TLOG_DEBUG << "Abort session to cluster";

    with_lock (Lock) {
        if (!Aborting) {
            Aborting = true;
            CloseCallback = {};

            // Cancel(ClientContext); // Don't cancel, because this is used only as factory for other contexts.
            Cancel(ConnectContext);
            Cancel(ConnectTimeoutContext);
            Cancel(ConnectDelayContext);

            if (Processor) {
                Processor->Cancel();
            }
        }
    }
}

void TSingleClusterReadSessionImpl::Close(std::function<void()> callback) {
    with_lock (Lock) {
        if (Aborting) {
            callback();
        }

        if (!Closing) {
            Closing = true;

            CloseCallback = std::move(callback);

            Cancel(ConnectContext);
            Cancel(ConnectTimeoutContext);
            Cancel(ConnectDelayContext);

            if (!Processor) {
                CallCloseCallbackImpl();
            } else {
                if (!HasCommitsInflightImpl()) {
                    Processor->Cancel();
                    CallCloseCallbackImpl();
                }
            }
        }
    }
}

void TSingleClusterReadSessionImpl::CallCloseCallbackImpl() {
    if (CloseCallback) {
        CloseCallback();
        CloseCallback = {};
    }
    Aborting = true; // So abort call will have no effect.
}

void TSingleClusterReadSessionImpl::StopReadingData() {
    with_lock (Lock) {
        DataReadingSuspended = true;
    }
}

void TSingleClusterReadSessionImpl::ResumeReadingData() {
    with_lock (Lock) {
        if (DataReadingSuspended) {
            DataReadingSuspended = false;
            ContinueReadingDataImpl();
        }
    }
}

void TSingleClusterReadSessionImpl::WaitAllDecompressionTasks() {
    Y_ASSERT(DecompressionTasksInflight >= 0);
    while (DecompressionTasksInflight > 0) {
        Sleep(TDuration::MilliSeconds(5)); // Perform active wait because this is aborting process and there are no decompression tasks here in normal situation.
    }
}

void TSingleClusterReadSessionImpl::DumpStatisticsToLog(TLogElement& log) {
    with_lock (Lock) {
        // cluster:topic:partition:stream-id:read-offset:committed-offset
        for (auto&& [key, partitionStream] : PartitionStreams) {
            log << " "
                << ClusterName
                << ':' << partitionStream->GetTopicPath()
                << ':' << partitionStream->GetPartitionId()
                << ':' << partitionStream->GetPartitionStreamId()
                << ':' << partitionStream->GetMaxReadOffset()
                << ':' << partitionStream->GetMaxCommittedOffset();
        }
    }
}

void TSingleClusterReadSessionImpl::UpdateMemoryUsageStatisticsImpl() {
    const TInstant now = TInstant::Now();
    const ui64 delta = (now - UsageStatisticsLastUpdateTime).MilliSeconds();
    UsageStatisticsLastUpdateTime = now;
    const double percent = 100.0 / static_cast<double>(Settings.MaxMemoryUsageBytes_);

    Settings.Counters_->TotalBytesInflightUsageByTime->Collect((DecompressedDataSize + CompressedDataSize) * percent, delta);
    Settings.Counters_->UncompressedBytesInflightUsageByTime->Collect(DecompressedDataSize * percent, delta);
    Settings.Counters_->CompressedBytesInflightUsageByTime->Collect(CompressedDataSize * percent, delta);
}

void TSingleClusterReadSessionImpl::UpdateMemoryUsageStatistics() {
    with_lock (Lock) {
        UpdateMemoryUsageStatisticsImpl();
    }
}

bool TSingleClusterReadSessionImpl::TPartitionCookieMapping::AddMapping(const TCookie::TPtr& cookie) {
    if (!Cookies.emplace(cookie->GetKey(), cookie).second) {
        return false;
    }
    for (ui64 offset = cookie->OffsetRange.first; offset < cookie->OffsetRange.second; ++offset) {
        if (!UncommittedOffsetToCookie.emplace(std::make_pair(cookie->PartitionStream->GetPartitionStreamId(), offset), cookie).second) {
            return false;
        }
    }
    PartitionStreamIdToCookie.emplace(cookie->PartitionStream->GetPartitionStreamId(), cookie);
    return true;
}

TSingleClusterReadSessionImpl::TPartitionCookieMapping::TCookie::TPtr TSingleClusterReadSessionImpl::TPartitionCookieMapping::CommitOffset(ui64 partitionStreamId, ui64 offset) {
    auto cookieIt = UncommittedOffsetToCookie.find(std::make_pair(partitionStreamId, offset));
    if (cookieIt != UncommittedOffsetToCookie.end()) {
        TCookie::TPtr cookie;
        if (!--cookieIt->second->UncommittedMessagesLeft) {
            ++CommitInflight;
            cookie = cookieIt->second;
        }
        UncommittedOffsetToCookie.erase(cookieIt);
        return cookie;
    } else {
        ThrowFatalError(TStringBuilder() << "Invalid offset " << offset << ". Partition stream id: " << partitionStreamId << Endl);
    }
    // If offset wasn't found, there might be already hard released partition.
    // This situation is OK.
    return nullptr;
}

TSingleClusterReadSessionImpl::TPartitionCookieMapping::TCookie::TPtr TSingleClusterReadSessionImpl::TPartitionCookieMapping::RetrieveCommittedCookie(const Ydb::PersQueue::V1::CommitCookie& cookieProto) {
    TCookie::TPtr cookieInfo;
    auto cookieIt = Cookies.find(TCookie::TKey(cookieProto.assign_id(), cookieProto.partition_cookie()));
    if (cookieIt != Cookies.end()) {
        --CommitInflight;
        cookieInfo = cookieIt->second;
        Cookies.erase(cookieIt);

        auto [rangeBegin, rangeEnd] = PartitionStreamIdToCookie.equal_range(cookieInfo->PartitionStream->GetPartitionStreamId());
        for (auto i = rangeBegin; i != rangeEnd; ++i) {
            if (i->second == cookieInfo) {
                PartitionStreamIdToCookie.erase(i);
                break;
            }
        }
    }
    return cookieInfo;
}

void TSingleClusterReadSessionImpl::TPartitionCookieMapping::RemoveMapping(ui64 partitionStreamId) {
    auto [rangeBegin, rangeEnd] = PartitionStreamIdToCookie.equal_range(partitionStreamId);
    for (auto i = rangeBegin; i != rangeEnd; ++i) {
        TCookie::TPtr cookie = i->second;
        Cookies.erase(cookie->GetKey());
        for (ui64 offset = cookie->OffsetRange.first; offset < cookie->OffsetRange.second; ++offset) {
            UncommittedOffsetToCookie.erase(std::make_pair(partitionStreamId, offset));
        }
    }
    PartitionStreamIdToCookie.erase(rangeBegin, rangeEnd);
}

void TSingleClusterReadSessionImpl::TPartitionCookieMapping::ClearMapping() {
    Cookies.clear();
    UncommittedOffsetToCookie.clear();
    PartitionStreamIdToCookie.clear();
    CommitInflight = 0;
}

bool TSingleClusterReadSessionImpl::TPartitionCookieMapping::HasUnacknowledgedCookies() const {
    return CommitInflight != 0;
}

TReadSessionEvent::TCreatePartitionStreamEvent::TCreatePartitionStreamEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset, ui64 endOffset)
    : PartitionStream(std::move(partitionStream))
    , CommittedOffset(committedOffset)
    , EndOffset(endOffset)
{
}

void TReadSessionEvent::TCreatePartitionStreamEvent::Confirm(TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset) {
    if (PartitionStream) {
        static_cast<TPartitionStreamImpl*>(PartitionStream.Get())->ConfirmCreate(readOffset, commitOffset);
    }
}

TReadSessionEvent::TDestroyPartitionStreamEvent::TDestroyPartitionStreamEvent(TPartitionStream::TPtr partitionStream, bool committedOffset)
    : PartitionStream(std::move(partitionStream))
    , CommittedOffset(committedOffset)
{
}

void TReadSessionEvent::TDestroyPartitionStreamEvent::Confirm() {
    if (PartitionStream) {
        static_cast<TPartitionStreamImpl*>(PartitionStream.Get())->ConfirmDestroy();
    }
}

TReadSessionEvent::TPartitionStreamClosedEvent::TPartitionStreamClosedEvent(TPartitionStream::TPtr partitionStream, EReason reason)
    : PartitionStream(std::move(partitionStream))
    , Reason(reason)
{
}

TReadSessionEvent::TDataReceivedEvent::TDataReceivedEvent(TVector<TMessage> messages,
                                                          TVector<TCompressedMessage> compressedMessages,
                                                          TPartitionStream::TPtr partitionStream)
    : Messages(std::move(messages))
    , CompressedMessages(std::move(compressedMessages))
    , PartitionStream(std::move(partitionStream))
{
    for (size_t i = 0; i < GetMessagesCount(); ++i) {
        auto [from, to] = GetMessageOffsetRange(*this, i);
        if (OffsetRanges.empty() || OffsetRanges.back().second != from) {
            OffsetRanges.emplace_back(from, to);
        } else {
            OffsetRanges.back().second = to;
        }
    }
}

void TReadSessionEvent::TDataReceivedEvent::Commit() {
    for (auto [from, to] : OffsetRanges) {
        static_cast<TPartitionStreamImpl*>(PartitionStream.Get())->Commit(from, to);
    }
}

TReadSessionEvent::TCommitAcknowledgementEvent::TCommitAcknowledgementEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset)
    : PartitionStream(std::move(partitionStream))
    , CommittedOffset(committedOffset)
{
}

TString DebugString(const TReadSessionEvent::TEvent& event) {
    return std::visit([](const auto& ev) { return ev.DebugString(); }, event);
}

TString TReadSessionEvent::TDataReceivedEvent::DebugString(bool printData) const {
    TStringBuilder ret;
    ret << "DataReceived { PartitionStreamId: " << GetPartitionStream()->GetPartitionStreamId()
        << " PartitionId: " << GetPartitionStream()->GetPartitionId();
    for (const auto& message : Messages) {
        ret << " ";
        message.DebugString(ret, printData);
    }
    for (const auto& message : CompressedMessages) {
        ret << " ";
        message.DebugString(ret, printData);
    }
    ret << " }";
    return std::move(ret);
}

TString TReadSessionEvent::TCommitAcknowledgementEvent::DebugString() const {
    return TStringBuilder() << "CommitAcknowledgement { PartitionStreamId: " << GetPartitionStream()->GetPartitionStreamId()
                            << " PartitionId: " << GetPartitionStream()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset()
                            << " }";
}

TString TReadSessionEvent::TCreatePartitionStreamEvent::DebugString() const {
    return TStringBuilder() << "CreatePartitionStream { PartitionStreamId: " << GetPartitionStream()->GetPartitionStreamId()
                            << " PartitionId: " << GetPartitionStream()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset()
                            << " EndOffset: " << GetEndOffset()
                            << " }";
}

TString TReadSessionEvent::TDestroyPartitionStreamEvent::DebugString() const {
    return TStringBuilder() << "DestroyPartitionStream { PartitionStreamId: " << GetPartitionStream()->GetPartitionStreamId()
                            << " PartitionId: " << GetPartitionStream()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset()
                            << " }";
}

TString TReadSessionEvent::TPartitionStreamStatusEvent::DebugString() const {
    return TStringBuilder() << "PartitionStreamStatus { PartitionStreamId: " << GetPartitionStream()->GetPartitionStreamId()
                            << " PartitionId: " << GetPartitionStream()->GetPartitionId()
                            << " CommittedOffset: " << GetCommittedOffset()
                            << " ReadOffset: " << GetReadOffset()
                            << " EndOffset: " << GetEndOffset()
                            << " WriteWatermark: " << GetWriteWatermark()
                            << " }";
}

TString TReadSessionEvent::TPartitionStreamClosedEvent::DebugString() const {
    return TStringBuilder() << "PartitionStreamClosed { PartitionStreamId: " << GetPartitionStream()->GetPartitionStreamId()
                            << " PartitionId: " << GetPartitionStream()->GetPartitionId()
                            << " Reason: " << GetReason()
                            << " }";
}

TString TSessionClosedEvent::DebugString() const {
    return
        TStringBuilder() << "SessionClosed { Status: " << GetStatus()
                         << " Issues: \"" << IssuesSingleLineString(GetIssues())
                         << "\" }";
}

TReadSessionEvent::TPartitionStreamStatusEvent::TPartitionStreamStatusEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset, ui64 readOffset, ui64 endOffset, TInstant writeWatermark)
    : PartitionStream(std::move(partitionStream))
    , CommittedOffset(committedOffset)
    , ReadOffset(readOffset)
    , EndOffset(endOffset)
    , WriteWatermark(writeWatermark)
{
}

TReadSessionEventInfo::TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl> partitionStream, std::weak_ptr<IUserRetrievedEventCallback> session, TEvent event)
    : PartitionStream(std::move(partitionStream))
    , Event(std::move(event))
    , Session(std::move(session))
{}

TReadSessionEventInfo::TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl> partitionStream, std::weak_ptr<IUserRetrievedEventCallback> session)
    : PartitionStream(std::move(partitionStream))
    , Session(std::move(session))
{}

TReadSessionEventInfo::TReadSessionEventInfo(TIntrusivePtr<TPartitionStreamImpl> partitionStream,
                                             std::weak_ptr<IUserRetrievedEventCallback> session,
                                             TVector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages,
                                             TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> compressedMessages)
    : PartitionStream(std::move(partitionStream))
    , Event(
        NMaybe::TInPlace(),
        std::in_place_type_t<TReadSessionEvent::TDataReceivedEvent>(),
        std::move(messages),
        std::move(compressedMessages),
        PartitionStream
    )
    , Session(std::move(session))
{
}

void TReadSessionEventInfo::MoveToPartitionStream() {
    PartitionStream->InsertEvent(std::move(*Event));
    Event = Nothing();
    Y_ASSERT(PartitionStream->HasEvents());
}

void TReadSessionEventInfo::ExtractFromPartitionStream() {
    if (!Event && !IsEmpty()) {
        Event = std::move(PartitionStream->TopEvent().GetEvent());
        PartitionStream->PopEvent();
    }
}

bool TReadSessionEventInfo::IsEmpty() const {
    return !PartitionStream || !PartitionStream->HasEvents();
}

bool TReadSessionEventInfo::IsDataEvent() const {
    return !IsEmpty() && PartitionStream->TopEvent().IsDataEvent();
}

bool TReadSessionEventInfo::HasMoreData() const {
    return PartitionStream->TopEvent().GetData().HasMoreData();
}

bool TReadSessionEventInfo::HasReadyUnreadData() const {
    return PartitionStream->TopEvent().GetData().HasReadyUnreadData();
}

void TReadSessionEventInfo::OnUserRetrievedEvent() {
    if (auto session = Session.lock()) {
        session->OnUserRetrievedEvent(*Event);
    }
}

bool TReadSessionEventInfo::TakeData(TVector<TReadSessionEvent::TDataReceivedEvent::TMessage>* messages,
                                     TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>* compressedMessages,
                                     size_t* maxByteSize)
{
    return PartitionStream->TopEvent().GetData().TakeData(PartitionStream, messages, compressedMessages, maxByteSize);
}

TReadSessionEventsQueue::TReadSessionEventsQueue(const TSettings& settings, std::weak_ptr<IUserRetrievedEventCallback> session)
    : TParent(settings)
    , Session(std::move(session))
{
    const auto& h = Settings.EventHandlers_;
    if (h.CommonHandler_
        || h.DataReceivedHandler_
        || h.CommitAcknowledgementHandler_
        || h.CreatePartitionStreamHandler_
        || h.DestroyPartitionStreamHandler_
        || h.PartitionStreamStatusHandler_
        || h.PartitionStreamClosedHandler_
        || h.SessionClosedHandler_)
    {
        HasEventCallbacks = true;
    } else {
        HasEventCallbacks = false;
    }
}

void TReadSessionEventsQueue::PushEvent(TReadSessionEventInfo eventInfo, TDeferredActions& deferred) {
    if (Closed) {
        return;
    }

    with_lock (Mutex) {
        auto partitionStream = eventInfo.PartitionStream;
        eventInfo.MoveToPartitionStream();
        SignalReadyEventsImpl(partitionStream.Get(), deferred);
    }
}

void TReadSessionEventsQueue::SignalEventImpl(TIntrusivePtr<TPartitionStreamImpl> partitionStream, TDeferredActions& deferred) {
    if (Closed) {
        return;
    }
    auto session = partitionStream->GetSession();
    Events.emplace(std::move(partitionStream), std::move(session));
    SignalWaiterImpl(deferred);
}

TDataDecompressionInfo* TReadSessionEventsQueue::PushDataEvent(TIntrusivePtr<TPartitionStreamImpl> partitionStream, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData&& msg) {
    if (Closed) {
        return nullptr;
    }

    with_lock (Mutex) {
        return &partitionStream->InsertDataEvent(std::move(msg), Settings.Decompress_);
    }
}

TMaybe<TReadSessionEventsQueue::TEventInfo> TReadSessionEventsQueue::GetDataEventImpl(TEventInfo& srcDataEventInfo, size_t* maxByteSize) { // Assumes that we're under lock.
    TVector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
    TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> compressedMessages;
    TIntrusivePtr<TPartitionStreamImpl> partitionStream = srcDataEventInfo.PartitionStream;
    bool messageExtracted = false;
    while (srcDataEventInfo.HasReadyUnreadData() && *maxByteSize > 0) {
        const bool hasMoreUnpackedData = srcDataEventInfo.TakeData(&messages, &compressedMessages, maxByteSize);
        if (!hasMoreUnpackedData) {
            const bool messageIsFullyRead = !srcDataEventInfo.HasMoreData();
            if (messageIsFullyRead) {
                partitionStream->PopEvent();
                messageExtracted = true;
                break;
            }
        }
    }
    if (!messageExtracted) {
        partitionStream->TopEvent().Signalled = false;
    }

    if (messages.empty() && compressedMessages.empty()) {
        return Nothing();
    }
    return TEventInfo(partitionStream, partitionStream->GetSession(), std::move(messages), std::move(compressedMessages));
}

void TReadSessionEventsQueue::SignalReadyEvents(TPartitionStreamImpl* partitionStream) {
    Y_ASSERT(partitionStream);
    TDeferredActions deferred;
    with_lock (Mutex) {
        SignalReadyEventsImpl(partitionStream, deferred);
    }
}

void TReadSessionEventsQueue::SignalReadyEventsImpl(TPartitionStreamImpl* partitionStream, TDeferredActions& deferred) {
    partitionStream->SignalReadyEvents(this, deferred);
    ApplyCallbacksToReadyEventsImpl(deferred);
}

bool TReadSessionEventsQueue::ApplyCallbacksToReadyEventsImpl(TDeferredActions& deferred) {
    if (!HasEventCallbacks) {
        return false;
    }
    bool applied = false;
    while (HasCallbackForNextEventImpl()) {
        size_t maxSize = std::numeric_limits<size_t>::max();
        TMaybe<TReadSessionEventInfo> eventInfo = GetEventImpl(&maxSize);
        if (!eventInfo) {
            break;
        }
        const TIntrusivePtr<TPartitionStreamImpl> partitionStreamForSignalling = eventInfo->IsDataEvent() ? eventInfo->PartitionStream : nullptr;
        applied = true;
        if (!ApplyHandler(*eventInfo, deferred)) { // Close session event.
            break;
        }
        if (partitionStreamForSignalling) {
            SignalReadyEventsImpl(partitionStreamForSignalling.Get(), deferred);
        }
    }
    return applied;
}

struct THasCallbackForEventVisitor {
    explicit THasCallbackForEventVisitor(const TReadSessionSettings& settings)
        : Settings(settings)
    {
    }

#define DECLARE_HANDLER(type, handler)                  \
    bool operator()(const type&) {                      \
        return bool(Settings.EventHandlers_.handler);   \
    }                                                   \
    /**/

    DECLARE_HANDLER(TReadSessionEvent::TDataReceivedEvent, DataReceivedHandler_);
    DECLARE_HANDLER(TReadSessionEvent::TCommitAcknowledgementEvent, CommitAcknowledgementHandler_);
    DECLARE_HANDLER(TReadSessionEvent::TCreatePartitionStreamEvent, CreatePartitionStreamHandler_);
    DECLARE_HANDLER(TReadSessionEvent::TDestroyPartitionStreamEvent, DestroyPartitionStreamHandler_);
    DECLARE_HANDLER(TReadSessionEvent::TPartitionStreamStatusEvent, PartitionStreamStatusHandler_);
    DECLARE_HANDLER(TReadSessionEvent::TPartitionStreamClosedEvent, PartitionStreamClosedHandler_);
    DECLARE_HANDLER(TSessionClosedEvent, SessionClosedHandler_);

#undef DECLARE_HANDLER

    const TReadSessionSettings& Settings;
};

bool TReadSessionEventsQueue::HasCallbackForNextEventImpl() const {
    if (!HasEventsImpl()) {
        return false;
    }
    if (Settings.EventHandlers_.CommonHandler_) {
        return true;
    }

    if (!Events.empty()) {
        const TEventInfo& topEvent = Events.front();
        const TReadSessionEvent::TEvent* event = nullptr;
        if (topEvent.Event) {
            event = &*topEvent.Event;
        } else if (topEvent.PartitionStream && topEvent.PartitionStream->HasEvents()) {
            const TRawPartitionStreamEvent& partitionStreamTopEvent = topEvent.PartitionStream->TopEvent();
            if (partitionStreamTopEvent.IsDataEvent()) {
                return bool(Settings.EventHandlers_.DataReceivedHandler_);
            } else {
                event = &partitionStreamTopEvent.GetEvent();
            }
        }

        if (!event) {
            return false;
        }

        THasCallbackForEventVisitor visitor(Settings);
        return std::visit(visitor, *event);
    } else if (CloseEvent) {
        return bool(Settings.EventHandlers_.SessionClosedHandler_);
    }
    Y_ASSERT(false);
    return false;
}

void TReadSessionEventsQueue::ClearAllEvents() {
    TDeferredActions deferred;
    with_lock (Mutex) {
        while (!Events.empty()) {
            auto& event = Events.front();
            if (event.PartitionStream && event.PartitionStream->HasEvents()) {
                event.PartitionStream->PopEvent();
            }
            Events.pop();
        }
    }
}

TDataDecompressionInfo::TDataDecompressionInfo(
    Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData&& msg,
    std::weak_ptr<TSingleClusterReadSessionImpl> session,
    bool doDecompress
)
    : ServerMessage(std::move(msg))
    , Session(std::move(session))
    , DoDecompress(doDecompress)
{
    for (const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch& batch : ServerMessage.batches()) {
        for (const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::MessageData& messageData : batch.message_data()) {
            CompressedDataSize += messageData.data().size();
        }
    }
    SourceDataNotProcessed = CompressedDataSize;

    BuildBatchesMeta();
}

void TDataDecompressionInfo::BuildBatchesMeta() {
    BatchesMeta.reserve(ServerMessage.batches_size());
    for (const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch& batch : ServerMessage.batches()) {
        // Extra fields.
        TWriteSessionMeta::TPtr meta = MakeIntrusive<TWriteSessionMeta>();
        meta->Fields.reserve(batch.extra_fields_size());
        for (const Ydb::PersQueue::V1::KeyValue& kv : batch.extra_fields()) {
            meta->Fields.emplace(kv.key(), kv.value());
        }
        BatchesMeta.emplace_back(std::move(meta));
    }
}

void TDataDecompressionInfo::PutDecompressionError(std::exception_ptr error, size_t batch, size_t message) {
    if (!DecompressionErrorsStructCreated) {
        with_lock (DecompressionErrorsStructLock) {
            DecompressionErrors.resize(ServerMessage.batches_size());
            for (size_t batch = 0; batch < static_cast<size_t>(ServerMessage.batches_size()); ++batch) {
                DecompressionErrors[batch].resize(static_cast<size_t>(ServerMessage.batches(batch).message_data_size()));
            }

            // Set barrier.
            DecompressionErrorsStructCreated = true;
        }
    }
    Y_ASSERT(batch < DecompressionErrors.size());
    Y_ASSERT(message < DecompressionErrors[batch].size());
    DecompressionErrors[batch][message] = std::move(error);
}

std::exception_ptr TDataDecompressionInfo::GetDecompressionError(size_t batch, size_t message) {
    if (!DecompressionErrorsStructCreated) {
        return {};
    }
    Y_ASSERT(batch < DecompressionErrors.size());
    Y_ASSERT(message < DecompressionErrors[batch].size());
    return DecompressionErrors[batch][message];
}

i64 TDataDecompressionInfo::StartDecompressionTasks(const IExecutor::TPtr& executor, i64 availableMemory, double averageCompressionRatio, const TIntrusivePtr<TPartitionStreamImpl>& partitionStream, TDeferredActions& deferred) {
    constexpr size_t TASK_LIMIT = 512_KB;
    std::shared_ptr<TSingleClusterReadSessionImpl> session = Session.lock();
    Y_ASSERT(session);
    ReadyThresholds.emplace_back();
    TDecompressionTask task(this, partitionStream, &ReadyThresholds.back());
    i64 used = 0;
    while (availableMemory > 0 && !AllDecompressionTasksStarted()) {
        const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch& batch = ServerMessage.batches(CurrentDecompressingMessage.first);
        if (CurrentDecompressingMessage.second < static_cast<size_t>(batch.message_data_size())) {
            const auto& messageData = batch.message_data(CurrentDecompressingMessage.second);
            const i64 size = static_cast<i64>(messageData.data().size());
            const i64 estimatedDecompressedSize =
                messageData.uncompressed_size() ? static_cast<i64>(messageData.uncompressed_size()) : static_cast<i64>(size * averageCompressionRatio);
            task.Add(CurrentDecompressingMessage.first, CurrentDecompressingMessage.second, size, estimatedDecompressedSize);
            used += estimatedDecompressedSize;
            availableMemory -= estimatedDecompressedSize;
        }
        ++CurrentDecompressingMessage.second;
        if (CurrentDecompressingMessage.second >= static_cast<size_t>(batch.message_data_size())) { // next batch
            ++CurrentDecompressingMessage.first;
            CurrentDecompressingMessage.second = 0;
        }
        if (task.AddedDataSize() >= TASK_LIMIT) {
            session->OnCreateNewDecompressionTask();
            deferred.DeferStartExecutorTask(executor, std::move(task));
            ReadyThresholds.emplace_back();
            task = TDecompressionTask(this, partitionStream, &ReadyThresholds.back());
        }
    }
    if (task.AddedMessagesCount() > 0) {
        session->OnCreateNewDecompressionTask();
        deferred.DeferStartExecutorTask(executor, std::move(task));
    } else {
        ReadyThresholds.pop_back(); // Revert.
    }
    return used;
}

bool TDataDecompressionInfo::TakeData(const TIntrusivePtr<TPartitionStreamImpl>& partitionStream,
                                      TVector<TReadSessionEvent::TDataReceivedEvent::TMessage>* messages,
                                      TVector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>* compressedMessages,
                                      size_t* maxByteSize)
{
    TMaybe<std::pair<size_t, size_t>> readyThreshold = GetReadyThreshold();
    Y_ASSERT(readyThreshold);
    auto& msg = GetServerMessage();
    ui64 minOffset = Max<ui64>();
    ui64 maxOffset = 0;
    const auto prevReadingMessage = CurrentReadingMessage;
    while (HasMoreData() && *maxByteSize > 0 && CurrentReadingMessage <= *readyThreshold) {
        auto& batch = *msg.mutable_batches(CurrentReadingMessage.first);
        if (CurrentReadingMessage.second < static_cast<size_t>(batch.message_data_size())) {
            const auto& meta = GetBatchMeta(CurrentReadingMessage.first);
            const TInstant batchWriteTimestamp = TInstant::MilliSeconds(batch.write_timestamp_ms());
            auto& messageData = *batch.mutable_message_data(CurrentReadingMessage.second);
            minOffset = Min(minOffset, messageData.offset());
            maxOffset = Max(maxOffset, messageData.offset());
            TReadSessionEvent::TDataReceivedEvent::TMessageInformation messageInfo(
                messageData.offset(),
                batch.source_id(),
                messageData.seq_no(),
                TInstant::MilliSeconds(messageData.create_timestamp_ms()),
                batchWriteTimestamp,
                batch.ip(),
                meta,
                messageData.uncompressed_size()
            );
            if (DoDecompress) {
                messages->emplace_back(
                    messageData.data(),
                    GetDecompressionError(CurrentReadingMessage.first, CurrentReadingMessage.second),
                    messageInfo,
                    partitionStream,
                    messageData.partition_key(),
                    messageData.explicit_hash()
                );
            } else {
                compressedMessages->emplace_back(
                    static_cast<ECodec>(messageData.codec()),
                    messageData.data(),
                    TVector<TReadSessionEvent::TDataReceivedEvent::TMessageInformation>{messageInfo},
                    partitionStream,
                    messageData.partition_key(),
                    messageData.explicit_hash()
                );
            }
            *maxByteSize -= Min(*maxByteSize, messageData.data().size());

            // Clear data to free internal session's memory.
            messageData.clear_data();
        }

        ++CurrentReadingMessage.second;
        if (CurrentReadingMessage.second >= static_cast<size_t>(batch.message_data_size())) {
            CurrentReadingMessage.second = 0;
            do {
                ++CurrentReadingMessage.first;
            } while (CurrentReadingMessage.first < static_cast<size_t>(msg.batches_size()) && msg.batches(CurrentReadingMessage.first).message_data_size() == 0);
        }
    }
    partitionStream->GetLog() << TLOG_DEBUG << "Take Data. Partition " << partitionStream->GetPartitionId()
                              << ". Read: {" << prevReadingMessage.first << ", " << prevReadingMessage.second << "} -> {"
                              << CurrentReadingMessage.first << ", " << CurrentReadingMessage.second << "} ("
                              << minOffset << "-" << maxOffset << ")";
    return CurrentReadingMessage <= *readyThreshold;
}

bool TDataDecompressionInfo::HasReadyUnreadData() const {
    TMaybe<std::pair<size_t, size_t>> threshold = GetReadyThreshold();
    if (!threshold) {
        return false;
    }
    return CurrentReadingMessage <= *threshold;
}

void TDataDecompressionInfo::TDecompressionTask::Add(size_t batch, size_t message, size_t sourceDataSize, size_t estimatedDecompressedSize) {
    if (Messages.empty() || Messages.back().Batch != batch) {
        Messages.push_back({ batch, { message, message + 1 } });
    }
    Messages.back().MessageRange.second = message + 1;
    SourceDataSize += sourceDataSize;
    EstimatedDecompressedSize += estimatedDecompressedSize;
    Ready->Batch = batch;
    Ready->Message = message;
}

TDataDecompressionInfo::TDecompressionTask::TDecompressionTask(TDataDecompressionInfo* parent, TIntrusivePtr<TPartitionStreamImpl> partitionStream, TReadyMessageThreshold* ready)
    : Parent(parent)
    , PartitionStream(std::move(partitionStream))
    , Ready(ready)
{
}

// Forward delcaration
namespace NCompressionDetails {
    extern TString Decompress(const Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::MessageData& data);
}

void TDataDecompressionInfo::TDecompressionTask::operator()() {
    ui64 minOffset = Max<ui64>();
    ui64 maxOffset = 0;
    const ui64 partition = Parent->ServerMessage.partition();
    i64 dataProcessed = 0;
    size_t messagesProcessed = 0;
    for (const TMessageRange& messages : Messages) {
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch& batch =
            *Parent->ServerMessage.mutable_batches(messages.Batch);
        for (size_t i = messages.MessageRange.first; i < messages.MessageRange.second; ++i) {
            Ydb::PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::MessageData& data =
                *batch.mutable_message_data(i);

            ++messagesProcessed;
            dataProcessed += static_cast<i64>(data.data().size());
            minOffset = Min(minOffset, data.offset());
            maxOffset = Max(maxOffset, data.offset());

            try {
                if (Parent->DoDecompress
                    && data.codec() != Ydb::PersQueue::V1::CODEC_RAW
                    && data.codec() != Ydb::PersQueue::V1::CODEC_UNSPECIFIED
                ) {
                    TString decompressed = NCompressionDetails::Decompress(data);
                    data.set_data(decompressed);
                    data.set_codec(Ydb::PersQueue::V1::CODEC_RAW);
                }
                DecompressedSize += data.data().size();
            } catch (...) {
                Parent->PutDecompressionError(std::current_exception(), messages.Batch, i);
                data.clear_data(); // Free memory, because we don't count it.

                std::shared_ptr<TSingleClusterReadSessionImpl> session = Parent->Session.lock();
                if (session) {
                    session->GetLog() << TLOG_INFO << "Error decompressing data: " << CurrentExceptionMessage();
                }
            }
        }
    }
    if (auto session = Parent->Session.lock()) {
        session->GetLog() << TLOG_DEBUG << "Decompression task done. Partition: " << partition << " (" << minOffset << "-" << maxOffset << ")";
    }
    Y_ASSERT(dataProcessed == SourceDataSize);
    std::shared_ptr<TSingleClusterReadSessionImpl> session = Parent->Session.lock();

    if (session) {
        session->OnDataDecompressed(SourceDataSize, EstimatedDecompressedSize, DecompressedSize, messagesProcessed);
    }

    Parent->SourceDataNotProcessed -= dataProcessed;
    Ready->Ready = true;

    if (session) {
        session->GetEventsQueue()->SignalReadyEvents(PartitionStream.Get());
    }
}

void TRawPartitionStreamEvent::Signal(TPartitionStreamImpl* partitionStream, TReadSessionEventsQueue* queue, TDeferredActions& deferred) {
    if (!Signalled) {
        Signalled = true;
        queue->SignalEventImpl(partitionStream, deferred);
    }
}

void TDeferredActions::DeferReadFromProcessor(const IProcessor::TPtr& processor,
                                              Ydb::PersQueue::V1::MigrationStreamingReadServerMessage* dst,
                                              IProcessor::TReadCallback callback)
{
    Y_ASSERT(!Processor);
    Y_ASSERT(!ReadDst);
    Y_ASSERT(!ReadCallback);
    Processor = processor;
    ReadDst = dst;
    ReadCallback = std::move(callback);
}

void TDeferredActions::DeferStartExecutorTask(const IExecutor::TPtr& executor, IExecutor::TFunction task) {
    ExecutorsTasks.emplace_back(executor, std::move(task));
}

void TDeferredActions::DeferAbortSession(const IErrorHandler::TPtr& errorHandler, TSessionClosedEvent&& closeEvent) {
    ErrorHandler = errorHandler;
    SessionClosedEvent.ConstructInPlace(std::move(closeEvent));
}

void TDeferredActions::DeferAbortSession(const IErrorHandler::TPtr& errorHandler, EStatus statusCode, NYql::TIssues&& issues) {
    DeferAbortSession(errorHandler, TSessionClosedEvent(statusCode, std::move(issues)));
}

void TDeferredActions::DeferAbortSession(const IErrorHandler::TPtr& errorHandler, EStatus statusCode, const TString& message) {
    NYql::TIssues issues;
    issues.AddIssue(message);
    DeferAbortSession(errorHandler, statusCode, std::move(issues));
}

void TDeferredActions::DeferAbortSession(const IErrorHandler::TPtr& errorHandler, TPlainStatus&& status) {
    DeferAbortSession(errorHandler, TSessionClosedEvent(std::move(status)));
}

void TDeferredActions::DeferReconnection(std::shared_ptr<TSingleClusterReadSessionImpl> session, const IErrorHandler::TPtr& errorHandler, TPlainStatus&& status) {
    Session = std::move(session);
    ErrorHandler = errorHandler;
    ReconnectionStatus = std::move(status);
}

void TDeferredActions::DeferSignalWaiter(TWaiter&& waiter) {
    Waiters.emplace_back(std::move(waiter));
}

void TDeferredActions::DoActions() {
    Read();
    StartExecutorTasks();
    AbortSession();
    Reconnect();
    SignalWaiters();
}

void TDeferredActions::Read() {
    if (ReadDst) {
        Y_ASSERT(Processor);
        Y_ASSERT(ReadCallback);
        Processor->Read(ReadDst, std::move(ReadCallback));
    }
}

void TDeferredActions::StartExecutorTasks() {
    for (auto&& [executor, task] : ExecutorsTasks) {
        executor->Post(std::move(task));
    }
}

void TDeferredActions::AbortSession() {
    if (SessionClosedEvent) {
        Y_ASSERT(ErrorHandler);
        ErrorHandler->AbortSession(std::move(*SessionClosedEvent));
    }
}

void TDeferredActions::Reconnect() {
    if (Session) {
        Y_ASSERT(ErrorHandler);
        if (!Session->Reconnect(ReconnectionStatus)) {
            ErrorHandler->AbortSession(std::move(ReconnectionStatus));
        }
    }
}

void TDeferredActions::SignalWaiters() {
    for (auto& w : Waiters) {
        w.Signal();
    }
}

void TErrorHandler::AbortSession(TSessionClosedEvent&& closeEvent) {
    if (auto session = Session.lock()) {
        session->Abort(std::move(closeEvent));
    }
}

class TGracefulReleasingSimpleDataHandlers : public TThrRefBase {
public:
    explicit TGracefulReleasingSimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler, bool commitAfterProcessing)
        : DataHandler(std::move(dataHandler))
        , CommitAfterProcessing(commitAfterProcessing)
    {
    }

    void OnDataReceived(TReadSessionEvent::TDataReceivedEvent& event) {
        Y_ASSERT(event.GetMessagesCount());
        TDeferredCommit deferredCommit;
        with_lock (Lock) {
            auto& offsetSet = PartitionStreamToUncommittedOffsets[event.GetPartitionStream()->GetPartitionStreamId()];
            // Messages could contain holes in offset, but later commit ack will tell us right border.
            // So we can easily insert the whole interval with holes included.
            // It will be removed from set by specifying proper right border.
            auto firstMessageOffsets = GetMessageOffsetRange(event, 0);
            auto lastMessageOffsets = GetMessageOffsetRange(event, event.GetMessagesCount() - 1);

            offsetSet.InsertInterval(firstMessageOffsets.first, lastMessageOffsets.second);

            if (CommitAfterProcessing) {
                deferredCommit.Add(event);
            }
        }
        DataHandler(event);
        deferredCommit.Commit();
    }

    void OnCommitAcknowledgement(TReadSessionEvent::TCommitAcknowledgementEvent& event) {
        with_lock (Lock) {
            const ui64 partitionStreamId = event.GetPartitionStream()->GetPartitionStreamId();
            auto& offsetSet = PartitionStreamToUncommittedOffsets[partitionStreamId];
            if (offsetSet.EraseInterval(0, event.GetCommittedOffset() + 1)) { // Remove some offsets.
                if (offsetSet.Empty()) { // No offsets left.
                    auto unconfirmedDestroyIt = UnconfirmedDestroys.find(partitionStreamId);
                    if (unconfirmedDestroyIt != UnconfirmedDestroys.end()) {
                        // Confirm and forget about this partition stream.
                        unconfirmedDestroyIt->second.Confirm();
                        UnconfirmedDestroys.erase(unconfirmedDestroyIt);
                        PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
                    }
                }
            }
        }
    }

    void OnCreatePartitionStream(TReadSessionEvent::TCreatePartitionStreamEvent& event) {
        with_lock (Lock) {
            Y_VERIFY(PartitionStreamToUncommittedOffsets[event.GetPartitionStream()->GetPartitionStreamId()].Empty());
        }
        event.Confirm();
    }

    void OnDestroyPartitionStream(TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
        with_lock (Lock) {
            const ui64 partitionStreamId = event.GetPartitionStream()->GetPartitionStreamId();
            Y_VERIFY(UnconfirmedDestroys.find(partitionStreamId) == UnconfirmedDestroys.end());
            if (PartitionStreamToUncommittedOffsets[partitionStreamId].Empty()) {
                PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
                event.Confirm();
            } else {
                UnconfirmedDestroys.emplace(partitionStreamId, std::move(event));
            }
        }
    }

    void OnPartitionStreamClosed(TReadSessionEvent::TPartitionStreamClosedEvent& event) {
        with_lock (Lock) {
            const ui64 partitionStreamId = event.GetPartitionStream()->GetPartitionStreamId();
            PartitionStreamToUncommittedOffsets.erase(partitionStreamId);
            UnconfirmedDestroys.erase(partitionStreamId);
        }
    }

private:
    TAdaptiveLock Lock; // For the case when user gave us multithreaded executor.
    const std::function<void(TReadSessionEvent::TDataReceivedEvent&)> DataHandler;
    const bool CommitAfterProcessing;
    THashMap<ui64, TDisjointIntervalTree<ui64>> PartitionStreamToUncommittedOffsets; // Partition stream id -> set of offsets.
    THashMap<ui64, TReadSessionEvent::TDestroyPartitionStreamEvent> UnconfirmedDestroys; // Partition stream id -> destroy events.
};

TReadSessionSettings::TEventHandlers& TReadSessionSettings::TEventHandlers::SimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler,
                                                                                               bool commitDataAfterProcessing,
                                                                                               bool gracefulReleaseAfterCommit) {
    Y_ASSERT(dataHandler);

    PartitionStreamStatusHandler([](TReadSessionEvent::TPartitionStreamStatusEvent&){});

    if (gracefulReleaseAfterCommit) {
        auto handlers = MakeIntrusive<TGracefulReleasingSimpleDataHandlers>(std::move(dataHandler), commitDataAfterProcessing);
        DataReceivedHandler([handlers](TReadSessionEvent::TDataReceivedEvent& event) {
            handlers->OnDataReceived(event);
        });
        CreatePartitionStreamHandler([handlers](TReadSessionEvent::TCreatePartitionStreamEvent& event) {
            handlers->OnCreatePartitionStream(event);
        });
        DestroyPartitionStreamHandler([handlers](TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
            handlers->OnDestroyPartitionStream(event);
        });
        CommitAcknowledgementHandler([handlers](TReadSessionEvent::TCommitAcknowledgementEvent& event) {
            handlers->OnCommitAcknowledgement(event);
        });
        PartitionStreamClosedHandler([handlers](TReadSessionEvent::TPartitionStreamClosedEvent& event) {
            handlers->OnPartitionStreamClosed(event);
        });
    } else {
        if (commitDataAfterProcessing) {
            DataReceivedHandler([dataHandler = std::move(dataHandler)](TReadSessionEvent::TDataReceivedEvent& event) {
                TDeferredCommit deferredCommit;
                deferredCommit.Add(event);
                dataHandler(event);
                deferredCommit.Commit();
            });
        } else {
            DataReceivedHandler(std::move(dataHandler));
        }
        CreatePartitionStreamHandler([](TReadSessionEvent::TCreatePartitionStreamEvent& event) {
            event.Confirm();
        });
        DestroyPartitionStreamHandler([](TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
            event.Confirm();
        });
        CommitAcknowledgementHandler([](TReadSessionEvent::TCommitAcknowledgementEvent&){});
        PartitionStreamClosedHandler([](TReadSessionEvent::TPartitionStreamClosedEvent&){});
    }
    return *this;
}

class TDeferredCommit::TImpl {
public:

    void Add(const TPartitionStream::TPtr& partitionStream, ui64 startOffset, ui64 endOffset);
    void Add(const TPartitionStream::TPtr& partitionStream, ui64 offset);

    void Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message);
    void Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent);

    void Commit();

private:
    static void Add(const TPartitionStream::TPtr& partitionStream, TDisjointIntervalTree<ui64>& offsetSet, ui64 startOffset, ui64 endOffset);

private:
    THashMap<TPartitionStream::TPtr, TDisjointIntervalTree<ui64>> Offsets; // Partition stream -> offsets set.
};

TDeferredCommit::TDeferredCommit() {
}

TDeferredCommit::TDeferredCommit(TDeferredCommit&&) = default;

TDeferredCommit& TDeferredCommit::operator=(TDeferredCommit&&) = default;

TDeferredCommit::~TDeferredCommit() {
}

#define GET_IMPL()                              \
    if (!Impl) {                                \
        Impl = MakeHolder<TImpl>();             \
    }                                           \
    Impl

void TDeferredCommit::Add(const TPartitionStream::TPtr& partitionStream, ui64 startOffset, ui64 endOffset) {
    GET_IMPL()->Add(partitionStream, startOffset, endOffset);
}

void TDeferredCommit::Add(const TPartitionStream::TPtr& partitionStream, ui64 offset) {
    GET_IMPL()->Add(partitionStream, offset);
}

void TDeferredCommit::Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    GET_IMPL()->Add(message);
}

void TDeferredCommit::Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent) {
    GET_IMPL()->Add(dataReceivedEvent);
}

#undef GET_IMPL

void TDeferredCommit::Commit() {
    if (Impl) {
        Impl->Commit();
    }
}

void TDeferredCommit::TImpl::Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    Y_ASSERT(message.GetPartitionStream());
    Add(message.GetPartitionStream(), message.GetOffset());
}

void TDeferredCommit::TImpl::Add(const TPartitionStream::TPtr& partitionStream, TDisjointIntervalTree<ui64>& offsetSet, ui64 startOffset, ui64 endOffset) {
    if (offsetSet.Intersects(startOffset, endOffset)) {
        ThrowFatalError(TStringBuilder() << "Commit set already has some offsets from half-interval ["
                                         << startOffset << "; " << endOffset
                                         << ") for partition stream with id " << partitionStream->GetPartitionStreamId());
    } else {
        offsetSet.InsertInterval(startOffset, endOffset);
    }
}

void TDeferredCommit::TImpl::Add(const TPartitionStream::TPtr& partitionStream, ui64 startOffset, ui64 endOffset) {
    Y_ASSERT(partitionStream);
    Add(partitionStream, Offsets[partitionStream], startOffset, endOffset);
}

void TDeferredCommit::TImpl::Add(const TPartitionStream::TPtr& partitionStream, ui64 offset) {
    Y_ASSERT(partitionStream);
    auto& offsetSet = Offsets[partitionStream];
    if (offsetSet.Has(offset)) {
        ThrowFatalError(TStringBuilder() << "Commit set already has offset " << offset
                                         << " for partition stream with id " << partitionStream->GetPartitionStreamId());
    } else {
        offsetSet.Insert(offset);
    }
}

void TDeferredCommit::TImpl::Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent) {
    const TPartitionStream::TPtr& partitionStream = dataReceivedEvent.GetPartitionStream();
    Y_ASSERT(partitionStream);
    auto& offsetSet = Offsets[partitionStream];
    auto [startOffset, endOffset] = GetMessageOffsetRange(dataReceivedEvent, 0);
    for (size_t i = 1; i < dataReceivedEvent.GetMessagesCount(); ++i) {
        auto msgOffsetRange = GetMessageOffsetRange(dataReceivedEvent, i);
        if (msgOffsetRange.first == endOffset) {
            endOffset= msgOffsetRange.second;
        } else {
            Add(partitionStream, offsetSet, startOffset, endOffset);
            startOffset = msgOffsetRange.first;
            endOffset = msgOffsetRange.second;
        }
    }
    Add(partitionStream, offsetSet, startOffset, endOffset);
}

void TDeferredCommit::TImpl::Commit() {
    for (auto&& [partitionStream, offsetRanges] : Offsets) {
        for (auto&& [startOffset, endOffset] : offsetRanges) {
            static_cast<TPartitionStreamImpl*>(partitionStream.Get())->Commit(startOffset, endOffset);
        }
    }
    Offsets.clear();
}

#define HISTOGRAM_SETUP NMonitoring::ExplicitHistogram({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})

TReaderCounters::TReaderCounters(const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters) {
    Errors = counters->GetCounter("errors", true);
    CurrentSessionLifetimeMs = counters->GetCounter("currentSessionLifetimeMs", false);
    BytesRead = counters->GetCounter("bytesRead", true);
    MessagesRead = counters->GetCounter("messagesRead", true);
    BytesReadCompressed = counters->GetCounter("bytesReadCompressed", true);
    BytesInflightUncompressed = counters->GetCounter("bytesInflightUncompressed", false);
    BytesInflightCompressed = counters->GetCounter("bytesInflightCompressed", false);
    BytesInflightTotal = counters->GetCounter("bytesInflightTotal", false);
    MessagesInflight = counters->GetCounter("messagesInflight", false);

    TotalBytesInflightUsageByTime = counters->GetHistogram("totalBytesInflightUsageByTime", HISTOGRAM_SETUP);
    UncompressedBytesInflightUsageByTime = counters->GetHistogram("uncompressedBytesInflightUsageByTime", HISTOGRAM_SETUP);
    CompressedBytesInflightUsageByTime = counters->GetHistogram("compressedBytesInflightUsageByTime", HISTOGRAM_SETUP);
}

void MakeCountersNotNull(TReaderCounters& counters) {
    if (!counters.Errors) {
        counters.Errors = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    }

    if (!counters.CurrentSessionLifetimeMs) {
        counters.CurrentSessionLifetimeMs = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
    }

    if (!counters.BytesRead) {
        counters.BytesRead = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    }

    if (!counters.MessagesRead) {
        counters.MessagesRead = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    }

    if (!counters.BytesReadCompressed) {
        counters.BytesReadCompressed = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    }

    if (!counters.BytesInflightUncompressed) {
        counters.BytesInflightUncompressed = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
    }

    if (!counters.BytesInflightCompressed) {
        counters.BytesInflightCompressed = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
    }

    if (!counters.BytesInflightTotal) {
        counters.BytesInflightTotal = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
    }

    if (!counters.MessagesInflight) {
        counters.MessagesInflight = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
    }


    if (!counters.TotalBytesInflightUsageByTime) {
        counters.TotalBytesInflightUsageByTime = MakeIntrusive<NMonitoring::THistogramCounter>(HISTOGRAM_SETUP);
    }

    if (!counters.UncompressedBytesInflightUsageByTime) {
        counters.UncompressedBytesInflightUsageByTime = MakeIntrusive<NMonitoring::THistogramCounter>(HISTOGRAM_SETUP);
    }

    if (!counters.CompressedBytesInflightUsageByTime) {
        counters.CompressedBytesInflightUsageByTime = MakeIntrusive<NMonitoring::THistogramCounter>(HISTOGRAM_SETUP);
    }
}

#undef HISTOGRAM_SETUP

bool HasNullCounters(TReaderCounters& counters) {
    return !counters.Errors
        || !counters.CurrentSessionLifetimeMs
        || !counters.BytesRead
        || !counters.MessagesRead
        || !counters.BytesReadCompressed
        || !counters.BytesInflightUncompressed
        || !counters.BytesInflightCompressed
        || !counters.BytesInflightTotal
        || !counters.MessagesInflight
        || !counters.TotalBytesInflightUsageByTime
        || !counters.UncompressedBytesInflightUsageByTime
        || !counters.CompressedBytesInflightUsageByTime;
}

} // namespace NYdb::NPersQueue
