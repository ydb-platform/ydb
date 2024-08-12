#include "read_session.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>

namespace NYdb::NTopic {

static const TString DRIVER_IS_STOPPING_DESCRIPTION = "Driver is stopping";

TReadSession::TReadSession(const TReadSessionSettings& settings,
             std::shared_ptr<TTopicClient::TImpl> client,
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
    Close(TDuration::Zero());

    Abort(EStatus::ABORTED, "Aborted");
    ClearAllEvents();

    if (CbContext) {
        CbContext->Cancel();
    }
    if (DumpCountersContext) {
        DumpCountersContext->Cancel();
    }
}

void TReadSession::Start() {
    EventsQueue = std::make_shared<TReadSessionEventsQueue<false>>(Settings);

    if (!ValidateSettings()) {
        return;
    }

    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Starting read session");

    TDeferredActions<false> deferred;
    with_lock (Lock) {
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
        "",
        Log,
        Client->CreateReadSessionConnectionProcessorFactory(),
        EventsQueue,
        context,
        1,
        1
    );

    deferred.DeferStartSession(CbContext);
}

bool TReadSession::ValidateSettings() {
    NYql::TIssues issues;
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

TVector<TReadSessionEvent::TEvent> TReadSession::GetEvents(bool block, TMaybe<size_t> maxEventsCount, size_t maxByteSize) {
    auto res = EventsQueue->GetEvents(block, maxEventsCount, maxByteSize);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    return res;
}

TVector<TReadSessionEvent::TEvent> TReadSession::GetEvents(const TReadSessionGetEventSettings& settings)
{
    auto events = GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    if (!events.empty() && settings.Tx_) {
        auto& tx = settings.Tx_->get();
        for (auto& event : events) {
            if (auto* dataEvent = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
                CollectOffsets(tx, *dataEvent);
            }
        }
        UpdateOffsets(tx);
    }
    return events;
}

TMaybe<TReadSessionEvent::TEvent> TReadSession::GetEvent(bool block, size_t maxByteSize) {
    auto res = EventsQueue->GetEvent(block, maxByteSize);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    return res;
}

TMaybe<TReadSessionEvent::TEvent> TReadSession::GetEvent(const TReadSessionGetEventSettings& settings)
{
    auto event = GetEvent(settings.Block_, settings.MaxByteSize_);
    if (event) {
        auto& tx = settings.Tx_->get();
        if (auto* dataEvent = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            CollectOffsets(tx, *dataEvent);
        }
        UpdateOffsets(tx);
    }
    return event;
}

void TReadSession::CollectOffsets(NTable::TTransaction& tx,
                                  const TReadSessionEvent::TDataReceivedEvent& event)
{
    const auto& session = *event.GetPartitionSession();

    if (event.HasCompressedMessages()) {
        for (auto& message : event.GetCompressedMessages()) {
            CollectOffsets(tx, session.GetTopicPath(), session.GetPartitionId(), message.GetOffset());
        }
    } else {
        for (auto& message : event.GetMessages()) {
            CollectOffsets(tx, session.GetTopicPath(), session.GetPartitionId(), message.GetOffset());
        }
    }
}

void TReadSession::CollectOffsets(NTable::TTransaction& tx,
                                  const TString& topicPath, ui32 partitionId, ui64 offset)
{
    const TString& sessionId = tx.GetSession().GetId();
    const TString& txId = tx.GetId();
    TOffsetRanges& ranges = OffsetRanges[std::make_pair(sessionId, txId)];
    ranges[topicPath][partitionId].InsertInterval(offset, offset + 1);
}

void TReadSession::UpdateOffsets(const NTable::TTransaction& tx)
{
    const TString& sessionId = tx.GetSession().GetId();
    const TString& txId = tx.GetId();

    auto p = OffsetRanges.find(std::make_pair(sessionId, txId));
    if (p == OffsetRanges.end()) {
        return;
    }

    TVector<TTopicOffsets> topics;
    for (auto& [path, partitions] : p->second) {
        TTopicOffsets topic;
        topic.Path = path;

        topics.push_back(std::move(topic));

        for (auto& [id, ranges] : partitions) {
            TPartitionOffsets partition;
            partition.PartitionId = id;

            TTopicOffsets& t = topics.back();
            t.Partitions.push_back(std::move(partition));

            for (auto& range : ranges) {
                TPartitionOffsets& p = t.Partitions.back();

                TOffsetsRange r;
                r.Start = range.first;
                r.End = range.second;

                p.Offsets.push_back(r);
            }
        }
    }

    Y_ABORT_UNLESS(!topics.empty());

    auto result = Client->UpdateOffsetsInTransaction(tx,
                                                     topics,
                                                     Settings.ConsumerName_,
                                                     {}).GetValueSync();
    Y_ABORT_UNLESS(!result.IsTransportError());

    if (!result.IsSuccess()) {
        ythrow yexception() << "error on update offsets: " << result;
    }

    OffsetRanges.erase(std::make_pair(sessionId, txId));
}

bool TReadSession::Close(TDuration timeout) {
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Closing read session. Close timeout: " << timeout);
    // Log final counters.
    if (CountersLogger) {
        CountersLogger->Stop();
    }
    with_lock (Lock) {
        if (DumpCountersContext) {
            DumpCountersContext->Cancel();
        }
    }

    TSingleClusterReadSessionImpl<false>::TPtr session;
    NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
    auto callback = [=]() mutable {
        promise.TrySetValue(true);
    };

    TDeferredActions<false> deferred;
    with_lock (Lock) {
        if (Closing || Aborting) {
            return false;
        }

        if (!timeout) {
            AbortImpl(EStatus::ABORTED, "Close with zero timeout", deferred);
            return false;
        }

        Closing = true;
        session = CbContext->TryGet();
    }
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
        session->Abort();

        NYql::TIssues issues;
        issues.AddIssue(TStringBuilder() << "Session was closed after waiting " << timeout);
        EventsQueue->Close(TSessionClosedEvent(EStatus::TIMEOUT, std::move(issues)), deferred);
    }

    with_lock (Lock) {
        Aborting = true; // Set abort flag for doing nothing on destructor.
    }
    return result;
}

void TReadSession::ClearAllEvents() {
    EventsQueue->ClearAllEvents();
}

TStringBuilder TReadSession::GetLogPrefix() const {
     return TStringBuilder() << GetDatabaseLogPrefix(DbDriverState->Database) << "[" << SessionId << "] ";
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
    with_lock(Lock) {
        std::vector<TCallbackContextPtr<false>> sessions{CbContext};

        CountersLogger = std::make_shared<TCountersLogger<false>>(Connections, sessions, Settings.Counters_, Log,
                                                                 GetLogPrefix(), StartSessionTime);
        DumpCountersContext = CountersLogger->MakeCallbackContext();
        CountersLogger->Start();
    }
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

void TReadSession::AbortImpl(EStatus statusCode, NYql::TIssues&& issues, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

    AbortImpl(TSessionClosedEvent(statusCode, std::move(issues)), deferred);
}

void TReadSession::AbortImpl(EStatus statusCode, const TString& message, TDeferredActions<false>& deferred) {
    Y_ABORT_UNLESS(Lock.IsLocked());

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
    TDeferredActions<false> deferred;
    with_lock (Lock) {
        AbortImpl(std::move(closeEvent), deferred);
    }
}

}  // namespace NYdb::NTopic
