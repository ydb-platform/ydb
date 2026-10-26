#include "read_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>

#include <limits>
#include <utility>

namespace NYdb::inline Dev::NTopic {

    namespace {

        // Close cannot join work interrupted by an update on this thread for the
        // same reader. A stack also preserves the outer reader during nested
        // backend updates; it does not suppress waits on other threads/readers.
        struct TReaderMetricUpdate {
            explicit TReaderMetricUpdate(const TReaderMetrics* metrics)
                : Metrics(metrics)
                , Previous(Current)
            {
                Current = this;
            }

            ~TReaderMetricUpdate() {
                Current = Previous;
            }

            const TReaderMetrics* Metrics;
            const TReaderMetricUpdate* Previous;
            static thread_local const TReaderMetricUpdate* Current;
        };

        thread_local const TReaderMetricUpdate* TReaderMetricUpdate::Current = nullptr;

    } // namespace

static const std::string DRIVER_IS_STOPPING_DESCRIPTION = "Driver is stopping";

void SetReadInTransaction(TReadSessionEvent::TEvent& event)
{
    if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
        e->SetReadInTransaction();
    }
}

std::shared_ptr<TReaderMetrics> TReaderMetrics::Create(
    std::shared_ptr<NMetrics::IMetricRegistry> registry,
    std::string endpoint,
    std::string database,
    const TReadSessionSettings& settings) noexcept {
    if (!registry || settings.ReaderName_.empty()) {
        return {};
    }

    try {
        return std::shared_ptr<TReaderMetrics>(new TReaderMetrics(
            std::move(registry), std::move(endpoint), std::move(database), settings));
    } catch (...) {
        // A registry failure must not prevent reading.
        return {};
    }
}

TReaderMetrics::TReaderMetrics(
    std::shared_ptr<NMetrics::IMetricRegistry> registry,
    std::string endpoint,
    std::string database,
    const TReadSessionSettings& settings)
    : Registry(std::move(registry))
    , DatabasePath(NormalizeTopicPath(database))
{
    BaseLabels.emplace(
        std::string(NObservability::MetricLabel::kEndpoint), std::move(endpoint));
    BaseLabels.emplace(
        std::string(NObservability::MetricLabel::kDatabase), std::move(database));
    if (!settings.WithoutConsumer_) {
        BaseLabels.emplace(
            std::string(NObservability::MetricLabel::kConsumer), settings.ConsumerName_);
    }
    BaseLabels.emplace(
        std::string(NObservability::MetricLabel::kReaderName), settings.ReaderName_);

    for (const auto& topicSettings : settings.Topics_) {
        auto topic = GetTopicKey(topicSettings.Path_);
        if (topic.empty() || Topics.contains(topic)) {
            continue;
        }
        if (auto metrics = MakeTopicMetrics(topic)) {
            HasCommitQueued_ = HasCommitQueued_ || metrics->CommitQueued != nullptr;
            Topics.emplace(std::move(topic), std::move(metrics));
        }
    }
}

std::string TReaderMetrics::NormalizeTopicPath(std::string_view path) {
    size_t begin = 0;
    while (begin < path.size() && path[begin] == '/') {
        ++begin;
    }

    size_t end = path.size();
    while (end > begin && path[end - 1] == '/') {
        --end;
    }

    return std::string(path.substr(begin, end - begin));
}

void TReaderMetrics::AddSaturated(std::uint64_t& total, std::uint64_t value) noexcept {
    const auto maxValue = std::numeric_limits<std::uint64_t>::max();
    if (maxValue - total < value) {
        total = maxValue;
    } else {
        total += value;
    }
}

TReaderMetrics::TTopicMetricsPtr TReaderMetrics::MakeTopicMetrics(std::string_view topic) noexcept {
    try {
        auto result = std::make_shared<TTopicMetrics>();
        auto labels = BaseLabels;
        labels.emplace(std::string(NObservability::MetricLabel::kTopic), std::string(topic));

        const auto makeCounter = [this, &labels](
                                     std::string_view name,
                                     std::string_view description,
                                     std::string_view unit) noexcept
            -> std::shared_ptr<NMetrics::ICounter> {
            try {
                return Registry->Counter(
                    std::string(name),
                    labels,
                    std::string(description),
                    std::string(unit));
            } catch (...) {
                return {};
            }
        };

        result->DeliveredMessages = makeCounter(
            NObservability::MetricName::kTopicReaderDeliveredMessages,
            NObservability::MetricDescription::kTopicReaderDeliveredMessages,
            NObservability::MetricUnit::kMessage);
        result->ReceivedMessages = makeCounter(
            NObservability::MetricName::kTopicReaderReceivedMessages,
            NObservability::MetricDescription::kTopicReaderReceivedMessages,
            NObservability::MetricUnit::kMessage);
        result->CommitQueued = makeCounter(
            NObservability::MetricName::kTopicReaderCommitQueued,
            NObservability::MetricDescription::kTopicReaderCommitQueued,
            NObservability::MetricUnit::kOffset);
        result->CommitAcknowledged = makeCounter(
            NObservability::MetricName::kTopicReaderCommitAcknowledged,
            NObservability::MetricDescription::kTopicReaderCommitAcknowledged,
            NObservability::MetricUnit::kOffset);
        return result;
    } catch (...) {
        return {};
    }
}

std::string TReaderMetrics::GetTopicKey(std::string_view topic) const {
    const bool absolutePath = !topic.empty() && topic.front() == '/';
    auto key = NormalizeTopicPath(topic);
    // A relative "Root/topic" in database "/Root" names "/Root/Root/topic".
    const bool insideDatabase = key.size() > DatabasePath.size()
        && key.compare(0, DatabasePath.size(), DatabasePath) == 0
        && key[DatabasePath.size()] == '/';
    if (absolutePath && !DatabasePath.empty() && insideDatabase) {
        key.erase(0, DatabasePath.size() + 1);
    }
    return key;
}

TReaderMetrics::TTopicMetricsPtr TReaderMetrics::ResolveTopic(std::string_view topic) noexcept {
    try {
        return ResolveTopicImpl(topic);
    } catch (...) {
        return {};
    }
}

TReaderMetrics::TTopicMetricsPtr TReaderMetrics::ResolveTopicImpl(std::string_view topic) {
    const auto key = GetTopicKey(topic);
    if (const auto it = Topics.find(key); it != Topics.end()) {
        return it->second;
    }
    // Preserve attribution for a server path that differs from the requested
    // alias: with one configured topic there is only one possible series.
    if (Topics.size() == 1) {
        return Topics.begin()->second;
    }

    // For multiple topics, only a unique whole-component suffix can resolve an
    // extra service prefix. Ambiguous or unknown paths have no attributable series.
    TTopicMetricsPtr result;
    for (const auto& [configuredTopic, metrics] : Topics) {
        const bool hasConfiguredSuffix = key.size() > configuredTopic.size()
            && key.compare(key.size() - configuredTopic.size(), configuredTopic.size(), configuredTopic) == 0
            && key[key.size() - configuredTopic.size() - 1] == '/';
        if (hasConfiguredSuffix) {
            if (result) {
                return {};
            }
            result = metrics;
        }
    }
    return result;
}

bool TReaderMetrics::IsUpdatingOnThisThread() const noexcept {
    for (auto update = TReaderMetricUpdate::Current; update; update = update->Previous) {
        if (update->Metrics == this) {
            return true;
        }
    }
    return false;
}

void TReaderMetrics::AddCounter(const std::shared_ptr<NMetrics::ICounter>& counter, std::uint64_t count) noexcept {
    if (!counter || !count) {
        return;
    }
    TReaderMetricUpdate update(this);
    try {
        counter->Add(count);
    } catch (...) {
        // Backend failures must not change delivery or commit behavior.
        return;
    }
}

void TReaderMetrics::RecordReceived(const TReceivedSnapshot& snapshot) noexcept {
    if (snapshot.Topic) {
        AddCounter(snapshot.Topic->ReceivedMessages, snapshot.LogicalMessageCount);
    }
}

void TReaderMetrics::RecordCommitAcknowledged(
    const TTopicMetricsPtr& topic, std::uint64_t count) noexcept {
    if (topic) {
        AddCounter(topic->CommitAcknowledged, count);
    }
}

void TReaderMetrics::RecordCommitQueued(
    const TTopicMetricsPtr& topic, std::uint64_t count) noexcept {
    if (topic) {
        AddCounter(topic->CommitQueued, count);
    }
}

void TReaderMetrics::RecordCommitQueued(const std::vector<TTopicOffsets>& topics) noexcept {
    if (!HasCommitQueued_) {
        return;
    }
    for (const auto& topic : topics) {
        auto topicMetrics = ResolveTopic(topic.Path);
        if (!topicMetrics || !topicMetrics->CommitQueued) {
            continue;
        }

        std::uint64_t count = 0;
        for (const auto& partition : topic.Partitions) {
            for (const auto& range : partition.Offsets) {
                if (range.End > range.Start) {
                    AddSaturated(count, range.End - range.Start);
                }
            }
        }
        RecordCommitQueued(topicMetrics, count);
    }
}

void TReaderMetrics::RecordDelivered(const TReadSessionEvent::TDataReceivedEvent& event) noexcept {
    try {
        // GetMessagesCount is used only to avoid accessor preconditions for an
        // empty event; the increment below always sums logical counts.
        if (event.GetMessagesCount() == 0) {
            return;
        }

        std::uint64_t logicalMessages = 0;
        if (event.HasCompressedMessages()) {
            for (const auto& message : event.GetCompressedMessages()) {
                AddSaturated(logicalMessages, message.GetLogicalMessageCount());
            }
        } else {
            for (const auto& message : event.GetMessages()) {
                AddSaturated(logicalMessages, message.GetLogicalMessageCount());
            }
        }
        if (!logicalMessages) {
            return;
        }

        const auto& partitionSession = event.GetPartitionSession();
        if (!partitionSession) {
            return;
        }

        const auto topicMetrics = ResolveTopic(partitionSession->GetTopicPath());
        if (topicMetrics) {
            AddCounter(topicMetrics->DeliveredMessages, logicalMessages);
        }
    } catch (...) {
        // Metric extraction and export must never interfere with delivery.
        return;
    }
}

TReadSession::TReadSession(const TReadSessionSettings& settings,
             std::shared_ptr<TTopicClient::TImpl> client,
             std::shared_ptr<TGRpcConnectionsImpl> connections,
             TDbDriverStatePtr dbDriverState)
    : Settings(settings)
    , SessionId(CreateGuidAsString())
    , Log(settings.Log_.value_or(dbDriverState->Log))
    , Client(std::move(client))
    , Connections(std::move(connections))
    , DbDriverState(std::move(dbDriverState))
{
    if (!Settings.RetryPolicy_) {
        Settings.RetryPolicy_ = IRetryPolicy::GetDefaultPolicy();
    }

    MakeCountersIfNeeded();
    try {
        ReaderMetrics = TReaderMetrics::Create(
            Connections->GetExternalMetricRegistry(),
            DbDriverState->DiscoveryEndpoint,
            DbDriverState->Database,
            Settings);
    } catch (...) {
        // Keep metric setup fail-open even if the connection facade itself
        // rejects access to the externally supplied registry.
        ReaderMetrics.reset();
    }
}

TReadSession::~TReadSession() {
    Close(TDuration::Zero());

    Abort(EStatus::ABORTED, "Aborted");
    if (CbContext) {
        if (auto session = CbContext->LockShared()) {
            const TInstant closeDeadline = TInstant::Now() + TDuration::Seconds(5);
            if (!session->WaitAllDecompressionTasks(closeDeadline)) {
                LOG_LAZY(Log, TLOG_WARNING, GetLogPrefix() << "Some decompression tasks are still running after read session destroy timeout");
            }
            ClearAllEvents();
            session->ClearAllPartitionStreamEvents();
        } else {
            ClearAllEvents();
        }
    } else {
        ClearAllEvents();
    }

    if (CbContext) {
        CbContext->Cancel();
    }
    if (DumpCountersContext) {
        DumpCountersContext->Cancel();
    }
}

void TReadSession::Start() {
    EventsQueue = std::make_shared<TReadSessionEventsQueue<false>>(Settings, ReaderMetrics);

    if (!ValidateSettings()) {
        return;
    }

    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Starting read session");

    TDeferredActions<false> deferred;
    with_lock(Lock) {
        if (Aborting) {
            return;
        }
        Topics = Settings.Topics_;
        CreateClusterSessionsImpl(deferred);
    }
    SetupCountersLogger();
}

void TReadSession::CreateClusterSessionsImpl(TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    // Create cluster sessions.
    LOG_LAZY(Log,
        TLOG_DEBUG,
        GetLogPrefix() << "Starting single session"
    );
    auto context = Client->CreateContext();
    if (!context) {
        AbortImpl(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION, deferred);
        return;
    }

    CbContext = MakeWithCallbackContext<TSingleClusterReadSessionImpl<false>>(
        Settings,
        DbDriverState->Database,
        SessionId,
        "",  // clusterName parameter is used by ydb_persqueue_public only
        Log,
        Client->CreateReadSessionConnectionProcessorFactory(),
        EventsQueue,
        context,
        1, 1,  // partitionStreamIdStart, partitionStreamIdStep parameters are used by ydb_persqueue_public only
        [connections = Connections](TDuration delay, std::function<void(bool)> cb, NYdbGrpc::IQueueClientContextPtr) {
            connections->ScheduleCallback(delay, cb);
        },
        Client->CreateDirectReadSessionConnectionProcessorFactory(),
        ReaderMetrics);

    deferred.DeferStartSession(CbContext);
}

bool TReadSession::ValidateSettings() {
    NYdb::NIssue::TIssues issues;
    if (Settings.Topics_.empty()) {
        issues.AddIssue("Empty topics list.");
    }

    if (Settings.ConsumerName_.empty() && !Settings.WithoutConsumer_) {
        issues.AddIssue("No consumer specified.");
    }

    if (!Settings.ConsumerName_.empty() && Settings.WithoutConsumer_) {
        issues.AddIssue("No need to specify a consumer when reading without a consumer.");
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

NThreading::TFuture<void> TReadSession::WaitEvent() {
    return EventsQueue->WaitEvent();
}

std::vector<TReadSessionEvent::TEvent> TReadSession::GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) {
    auto res = EventsQueue->GetEvents(block, maxEventsCount, maxByteSize);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    RecordDelivered(res);
    return res;
}

std::vector<TReadSessionEvent::TEvent> TReadSession::GetEvents(const TReadSessionGetEventSettings& settings)
{
    auto events = EventsQueue->GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    if (!events.empty() && settings.Tx_) {
        auto& tx = settings.Tx_->get();
        CbContext->TryGet()->CollectOffsets(tx, events, Client);
        for (auto& event : events) {
            SetReadInTransaction(event);
        }
    }
    RecordDelivered(events);
    return events;
}

std::optional<TReadSessionEvent::TEvent> TReadSession::GetEvent(bool block, size_t maxByteSize) {
    auto res = EventsQueue->GetEvent(block, maxByteSize);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    if (res) {
        RecordDelivered(*res);
    }
    return res;
}

std::optional<TReadSessionEvent::TEvent> TReadSession::GetEvent(const TReadSessionGetEventSettings& settings)
{
    auto event = EventsQueue->GetEvent(settings.Block_, settings.MaxByteSize_);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    if (event && settings.Tx_) {
        auto& tx = settings.Tx_->get();
        CbContext->TryGet()->CollectOffsets(tx, *event, Client);
        SetReadInTransaction(*event);
    }
    if (event) {
        RecordDelivered(*event);
    }
    return event;
}

void TReadSession::RecordDelivered(const TReadSessionEvent::TEvent& event) const noexcept {
    if (!ReaderMetrics) {
        return;
    }
    if (const auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
        ReaderMetrics->RecordDelivered(*data);
    }
}

void TReadSession::RecordDelivered(const std::vector<TReadSessionEvent::TEvent>& events) const noexcept {
    if (!ReaderMetrics) {
        return;
    }
    for (const auto& event : events) {
        RecordDelivered(event);
    }
}

bool TReadSession::Close(TDuration timeout) {
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Closing read session. Close timeout: " << timeout);
    // Log final counters.
    if (CountersLogger) {
        CountersLogger->Stop();
    }
    with_lock(Lock) {
        if (DumpCountersContext) {
            DumpCountersContext->Cancel();
        }
    }

    TSingleClusterReadSessionImpl<false>::TPtr session;
    NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
    auto callback = [=]() mutable {
        promise.TrySetValue(true);
    };

    std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<false>>> cbContextToCancel;
    std::shared_ptr<TCallbackContext<TCountersLogger<false>>> dumpCountersContextToCancel;
    TInstant closeDeadline;
    bool result = false;
    bool zeroTimeout = false;
    {
        TDeferredActions<false> deferred;
        with_lock(Lock) {
            if (Closing || Aborting) {
                return false;
            }

            if (!timeout) {
                AbortImpl(EStatus::ABORTED, "Close with zero timeout", deferred);
                zeroTimeout = true;
            } else {
                Closing = true;
                session = CbContext->TryGet();
            }
        }
        if (zeroTimeout) {
            // deferred posts callbacks as this block ends, then we drop the
            // stream -> session cycle without waiting for ~TReadSession.
        } else {
            session->Close(callback);

            callback(); // For the case when there are no subsessions yet.

            auto timeoutCallback = [=](bool) mutable {
                promise.TrySetValue(false);
            };

            auto timeoutContext = Connections->CreateContext();
            if (!timeoutContext) {
                AbortImpl(EStatus::ABORTED, DRIVER_IS_STOPPING_DESCRIPTION, deferred);
                return false;
            }
            closeDeadline = TInstant::Now() + timeout;
            Connections->ScheduleCallback(timeout,
                                          std::move(timeoutCallback),
                                          timeoutContext);

            // Wait.
            NThreading::TFuture<bool> resultFuture = promise.GetFuture();
            result = resultFuture.GetValueSync();
            if (result) {
                Cancel(timeoutContext);

                NYdb::NIssue::TIssues issues;
                issues.AddIssue("Session was gracefully closed");
                EventsQueue->Close(TSessionClosedEvent(EStatus::SUCCESS, std::move(issues)), deferred);
            } else {
                ++*Settings.Counters_->Errors;
                session->Abort();

                NYdb::NIssue::TIssues issues;
                issues.AddIssue(TStringBuilder() << "Session was closed after waiting " << timeout);
                EventsQueue->Close(TSessionClosedEvent(EStatus::TIMEOUT, std::move(issues)), deferred);
            }
            {
                std::lock_guard guard(Lock);
                Aborting = true; // Set abort flag for doing nothing on destructor.
                cbContextToCancel = CbContext;
                dumpCountersContextToCancel = DumpCountersContext;
            }
            // A backend may close its own reader before deferred tasks are posted,
            // or from a task's inline delivery callback. Those tasks retain their
            // data and will finish after the update returns; joining them here
            // would wait for this very call to return.
            if ((!ReaderMetrics || !ReaderMetrics->IsUpdatingOnThisThread()) && !session->WaitAllDecompressionTasks(closeDeadline)) {
                LOG_LAZY(Log, TLOG_WARNING, GetLogPrefix() << "Some decompression tasks are still running after read session close timeout");
            }
            ClearAllEvents();
            session->ClearAllPartitionStreamEvents();
        }
    }
    if (zeroTimeout) {
        if (CbContext) {
            if (auto abortedSession = CbContext->LockShared()) {
                ClearAllEvents();
                abortedSession->ClearAllPartitionStreamEvents();
            } else {
                ClearAllEvents();
            }
        }
        return false;
    }
    if (cbContextToCancel) {
        cbContextToCancel->Cancel();
    }
    if (dumpCountersContextToCancel) {
        dumpCountersContextToCancel->Cancel();
    }
    return result;
}

void TReadSession::ClearAllEvents() {
    EventsQueue->ClearAllEvents();
}

TStringBuilder TReadSession::GetLogPrefix() const {
     return TStringBuilder() << GetDatabaseLogPrefix(DbDriverState->Database) << "[" << SessionId << "] [" << Settings.TraceId_ << "] ";
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

void TReadSession::SetupCountersLogger() {
    std::lock_guard guard(Lock);
    std::vector<TCallbackContextPtr<false>> sessions{CbContext};

    CountersLogger = std::make_shared<TCountersLogger<false>>(Connections, sessions, Settings.Counters_, Log,
                                                                GetLogPrefix(), StartSessionTime);
    DumpCountersContext = CountersLogger->MakeCallbackContext();
    CountersLogger->Start();
}

void TReadSession::AbortImpl(TDeferredActions<false>&) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    if (!Aborting) {
        Aborting = true;
        if (DumpCountersContext) {
            DumpCountersContext->Cancel();
        }
        if (CbContext) {
            CbContext->TryGet()->Abort();
        }
    }
}

void TReadSession::AbortImpl(TSessionClosedEvent&& closeEvent, TDeferredActions<false>& deferred) {
    LOG_LAZY(Log, TLOG_NOTICE, GetLogPrefix() << "Aborting read session. Description: " << closeEvent.DebugString());

    EventsQueue->Close(std::move(closeEvent), deferred);
    AbortImpl(deferred);
}

void TReadSession::AbortImpl(EStatus statusCode, NYdb::NIssue::TIssues&& issues, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    AbortImpl(TSessionClosedEvent(statusCode, std::move(issues)), deferred);
}

void TReadSession::AbortImpl(EStatus statusCode, const std::string& message, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    NYdb::NIssue::TIssues issues;
    issues.AddIssue(message);
    AbortImpl(statusCode, std::move(issues), deferred);
}

void TReadSession::Abort(EStatus statusCode, NYdb::NIssue::TIssues&& issues) {
    Abort(TSessionClosedEvent(statusCode, std::move(issues)));
}

void TReadSession::Abort(EStatus statusCode, const std::string& message) {
    NYdb::NIssue::TIssues issues;
    issues.AddIssue(message);
    Abort(statusCode, std::move(issues));
}

void TReadSession::Abort(TSessionClosedEvent&& closeEvent) {
    TDeferredActions<false> deferred;
    with_lock(Lock) {
        AbortImpl(std::move(closeEvent), deferred);
    }
}

} // namespace NYdb::NTopic
