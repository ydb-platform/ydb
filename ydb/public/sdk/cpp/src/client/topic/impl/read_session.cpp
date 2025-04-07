#include "read_session.h"

#include <ydb/public/sdk/cpp/src/client/topic/common/log_lazy.h>
#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>

namespace NYdb::inline Dev::NTopic {

static const std::string DRIVER_IS_STOPPING_DESCRIPTION = "Driver is stopping";

void SetReadInTransaction(TReadSessionEvent::TEvent& event)
{
    if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
        e->SetReadInTransaction();
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
    return res;
}

std::vector<TReadSessionEvent::TEvent> TReadSession::GetEvents(const TReadSessionGetEventSettings& settings)
{
    auto events = GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    if (!events.empty() && settings.Tx_) {
        auto& tx = settings.Tx_->get();
        CbContext->TryGet()->CollectOffsets(tx, events, Client);
        for (auto& event : events) {
            SetReadInTransaction(event);
        }
    }
    return events;
}

std::optional<TReadSessionEvent::TEvent> TReadSession::GetEvent(bool block, size_t maxByteSize) {
    auto res = EventsQueue->GetEvent(block, maxByteSize);
    if (EventsQueue->IsClosed()) {
        Abort(EStatus::ABORTED, "Aborted");
    }
    return res;
}

std::optional<TReadSessionEvent::TEvent> TReadSession::GetEvent(const TReadSessionGetEventSettings& settings)
{
    auto event = GetEvent(settings.Block_, settings.MaxByteSize_);
    if (event && settings.Tx_) {
        auto& tx = settings.Tx_->get();
        CbContext->TryGet()->CollectOffsets(tx, *event, Client);
        SetReadInTransaction(*event);
    }
    return event;
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

    TDeferredActions<false> deferred;
    with_lock(Lock) {
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
