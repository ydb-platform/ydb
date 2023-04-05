#include "persqueue_impl.h"
#include "read_session.h"
#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/log_lazy.h>

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

std::pair<ui64, ui64> GetMessageOffsetRange(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent, ui64 index) {
    if (dataReceivedEvent.IsCompressedMessages()) {
        const auto& msg = dataReceivedEvent.GetCompressedMessages()[index];
        return {msg.GetOffset(0), msg.GetOffset(msg.GetBlocksCount() - 1) + 1};
    }
    const auto& msg = dataReceivedEvent.GetMessages()[index];
    return {msg.GetOffset(), msg.GetOffset() + 1};
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TReadSession

TStringBuilder TReadSession::GetLogPrefix() const {
     return TStringBuilder() << GetDatabaseLogPrefix(DbDriverState->Database) << "[" << SessionId << "] ";
}

TReadSession::TReadSession(const TReadSessionSettings& settings,
             std::shared_ptr<TPersQueueClient::TImpl> client,
             std::shared_ptr<TGRpcConnectionsImpl> connections,
             TDbDriverStatePtr dbDriverState)
    : Settings(settings)
    , SessionId(CreateGuidAsString())
    , Log(settings.Log_.GetOrElse(dbDriverState->Log))
    , Client(std::move(client))
    , Connections(std::move(connections))
    , DbDriverState(std::move(dbDriverState))
{
    if (!Settings.RetryPolicy_) {
        Settings.RetryPolicy_ = IRetryPolicy::GetDefaultPolicy();
    }

    MakeCountersIfNeeded();
}

TReadSession::~TReadSession() {
    Abort(EStatus::ABORTED, "Aborted");
    ClearAllEvents();

    if (Tracker) {
        Tracker->AsyncComplete().Wait();
    }
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
    ErrorHandler = MakeIntrusive<TErrorHandler<true>>(weak_from_this());
    Tracker = std::make_shared<TImplTracker>();
    EventsQueue = std::make_shared<TReadSessionEventsQueue<true>>(Settings, weak_from_this(), Tracker);

    if (!ValidateSettings()) {
        return;
    }

    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Starting read session");
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

    if (Settings.MaxMemoryUsageBytes_ < 1_MB) {
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

        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Starting cluster discovery");
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

    auto rpcSettings = TRpcRequestSettings::Make(Settings);
    rpcSettings.ClientTimeout = TDuration::Seconds(5); // TODO: make client timeout setting
    Connections->RunDeferred<Ydb::PersQueue::V1::ClusterDiscoveryService,
                             Ydb::PersQueue::ClusterDiscovery::DiscoverClustersRequest,
                             Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResponse>(
        MakeClusterDiscoveryRequest(),
        std::move(extractor),
        &Ydb::PersQueue::V1::ClusterDiscoveryService::Stub::AsyncDiscoverClusters,
        DbDriverState,
        INITIAL_DEFERRED_CALL_DELAY,
        rpcSettings); // TODO: make client timeout setting
}


void TReadSession::ProceedWithoutClusterDiscovery() {
    TDeferredActions<true> deferred;
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
        CreateClusterSessionsImpl(deferred);
    }
    ScheduleDumpCountersToLog();
}

void TReadSession::CreateClusterSessionsImpl(TDeferredActions<true>& deferred) {
    Y_VERIFY(Lock.IsLocked());

    // Create cluster sessions.
    ui64 partitionStreamIdStart = 1;
    const size_t clusterSessionsCount = ClusterSessions.size();
    for (auto& [clusterName, clusterSessionInfo] : ClusterSessions) {
        TReadSessionSettings sessionSettings = Settings;
        sessionSettings.Topics_ = clusterSessionInfo.Topics;
        if (sessionSettings.MaxMemoryUsageBytes_ > clusterSessionsCount && sessionSettings.MaxMemoryUsageBytes_ != std::numeric_limits<size_t>::max()) {
            sessionSettings.MaxMemoryUsageBytes_ /= clusterSessionsCount;
        }
        LOG_LAZY(Log,
            TLOG_DEBUG,
            GetLogPrefix() << "Starting session to cluster " << clusterName
                << " (" << clusterSessionInfo.ClusterEndpoint << ")"
        );
        auto subclient = Client->GetClientForEndpoint(clusterSessionInfo.ClusterEndpoint);
        auto context = subclient->CreateContext();
        if (!context) {
            AbortImpl(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION, deferred);
            return;
        }
        clusterSessionInfo.Session =
            std::make_shared<TSingleClusterReadSessionImpl<true>>(
                sessionSettings,
                DbDriverState->Database,
                SessionId,
                clusterName,
                Log,
                subclient->CreateReadSessionConnectionProcessorFactory(),
                EventsQueue,
                ErrorHandler,
                context,
                partitionStreamIdStart++,
                clusterSessionsCount,
                Tracker);

        deferred.DeferStartSession(clusterSessionInfo.Session);
    }
}

void TReadSession::OnClusterDiscovery(const TStatus& status, const Ydb::PersQueue::ClusterDiscovery::DiscoverClustersResult& result) {
    TDeferredActions<true> deferred;
    with_lock (Lock) {
        if (Aborting) {
            return;
        }

        if (!status.IsSuccess()) {
            ++*Settings.Counters_->Errors;
            if (!ClusterDiscoveryRetryState) {
                ClusterDiscoveryRetryState = Settings.RetryPolicy_->CreateRetryState();
            }
            TMaybe<TDuration> retryDelay = ClusterDiscoveryRetryState->GetNextRetryDelay(status.GetStatus());
            if (retryDelay) {
                LOG_LAZY(Log,
                    TLOG_INFO,
                    GetLogPrefix() << "Cluster discovery request failed. Status: " << status.GetStatus()
                        << ". Issues: \"" << IssuesSingleLineString(status.GetIssues()) << "\""
                );
                RestartClusterDiscoveryImpl(*retryDelay, deferred);
            } else {
                AbortImpl(status.GetStatus(), MakeIssueWithSubIssues("Failed to discover clusters", status.GetIssues()), deferred);
            }
            return;
        }

        LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Cluster discovery request succeeded");
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
                auto fullEndpoint = ApplyClusterEndpoint(DbDriverState->DiscoveryEndpoint, cluster.endpoint());
                if (clusterSessionInfo.ClusterEndpoint && clusterSessionInfo.ClusterEndpoint != fullEndpoint) {
                    issues.AddIssue(TStringBuilder() << "Unexpected reply from cluster discovery. Different endpoints for one cluster name. Cluster: "
                                    << normalizedName << ". \"" << clusterSessionInfo.ClusterEndpoint << "\" vs \""
                                    << fullEndpoint << "\"");
                }
                if (!clusterSessionInfo.ClusterEndpoint) {
                    clusterSessionInfo.ClusterEndpoint = fullEndpoint;
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

        CreateClusterSessionsImpl(deferred);
    }
    ScheduleDumpCountersToLog();
}

void TReadSession::RestartClusterDiscoveryImpl(TDuration delay, TDeferredActions<true>& deferred) {
    Y_VERIFY(Lock.IsLocked());
    if (Aborting || Closing) {
        return;
    }
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix() << "Restart cluster discovery in " << delay);
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
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Closing read session. Close timeout: " << timeout);
    // Log final counters.
    DumpCountersToLog();

    std::vector<TSingleClusterReadSessionImpl<true>::TPtr> sessions;
    NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
    std::shared_ptr<std::atomic<size_t>> count = std::make_shared<std::atomic<size_t>>(0);
    auto callback = [=]() mutable {
        if (--*count == 0) {
            promise.TrySetValue(true);
        }
    };

    TDeferredActions<true> deferred;
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

void TReadSession::AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions<true>& deferred) {
    Y_VERIFY(Lock.IsLocked());

    if (!Aborting) {
        Aborting = true;
        LOG_LAZY(Log, TLOG_NOTICE, GetLogPrefix() << "Aborting read session. Description: " << closeEvent.DebugString());
        if (ClusterDiscoveryDelayContext) {
            ClusterDiscoveryDelayContext->Cancel();
            ClusterDiscoveryDelayContext.reset();
        }
        if (DumpCountersContext) {
            DumpCountersContext->Cancel();
            DumpCountersContext.reset();
        }
        for (auto& [cluster, sessionInfo] : ClusterSessions) {
            if (sessionInfo.Session) {
                sessionInfo.Session->Abort();
            }
        }
        EventsQueue->Close(std::move(closeEvent), deferred);
    }
}

void TReadSession::AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions<true>& deferred) {
    Y_VERIFY(Lock.IsLocked());

    AbortImpl(TSessionClosedEvent(statusCode, std::move(issues)), deferred);
}

void TReadSession::AbortImpl(EStatus statusCode, const TString& message, TDeferredActions<true>& deferred) {
    Y_VERIFY(Lock.IsLocked());

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
    TDeferredActions<true> deferred;
    with_lock (Lock) {
        AbortImpl(std::move(closeEvent), deferred);
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
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Stop reading data");
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
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Resume reading data");
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

void TReadSession::OnUserRetrievedEvent(i64 decompressedSize, size_t messagesCount) {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix()
                          << "The application data is transferred to the client. Number of messages "
                          << messagesCount
                          << ", size "
                          << decompressedSize
                          << " bytes");
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
    std::vector<TSingleClusterReadSessionImpl<true>::TPtr> sessions;
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
        LOG_LAZY(Log, TLOG_INFO,
            GetLogPrefix() << "Counters: {"
            C(Errors)
            C(CurrentSessionLifetimeMs)
            C(BytesRead)
            C(MessagesRead)
            C(BytesReadCompressed)
            C(BytesInflightUncompressed)
            C(BytesInflightCompressed)
            C(BytesInflightTotal)
            C(MessagesInflight)
            << " }"
        );
    }

#undef C

    ScheduleDumpCountersToLog(timeNumber + 1);
}

void TReadSession::ScheduleDumpCountersToLog(size_t timeNumber) {
    with_lock(Lock) {
        if (Aborting || Closing) {
            return;
        }
        DumpCountersContext = Connections->CreateContext();
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// NPersQueue::TReadSessionEvent

TReadSessionEvent::TCreatePartitionStreamEvent::TCreatePartitionStreamEvent(TPartitionStream::TPtr partitionStream, ui64 committedOffset, ui64 endOffset)
    : PartitionStream(std::move(partitionStream))
    , CommittedOffset(committedOffset)
    , EndOffset(endOffset)
{
}

void TReadSessionEvent::TCreatePartitionStreamEvent::Confirm(TMaybe<ui64> readOffset, TMaybe<ui64> commitOffset) {
    if (PartitionStream) {
        static_cast<TPartitionStreamImpl<true>*>(PartitionStream.Get())->ConfirmCreate(readOffset, commitOffset);
    }
}

TReadSessionEvent::TDestroyPartitionStreamEvent::TDestroyPartitionStreamEvent(TPartitionStream::TPtr partitionStream, bool committedOffset)
    : PartitionStream(std::move(partitionStream))
    , CommittedOffset(committedOffset)
{
}

void TReadSessionEvent::TDestroyPartitionStreamEvent::Confirm() {
    if (PartitionStream) {
        static_cast<TPartitionStreamImpl<true>*>(PartitionStream.Get())->ConfirmDestroy();
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
        static_cast<TPartitionStreamImpl<true>*>(PartitionStream.Get())->Commit(from, to);
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
                            << " TopicPath: " << GetPartitionStream()->GetTopicPath()
                            << " Cluster: " << GetPartitionStream()->GetCluster()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDeferredCommit

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
            static_cast<TPartitionStreamImpl<true>*>(partitionStream.Get())->Commit(startOffset, endOffset);
        }
    }
    Offsets.clear();
}

#define HISTOGRAM_SETUP NMonitoring::ExplicitHistogram({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100})

TReaderCounters::TReaderCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
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

#undef HISTOGRAM_SETUP

} // namespace NYdb::NPersQueue
