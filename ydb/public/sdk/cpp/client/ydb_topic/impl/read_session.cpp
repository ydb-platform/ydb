#include "read_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/log_lazy.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <util/generic/guid.h>

namespace NYdb::NTopic {

static const TString DRIVER_IS_STOPPING_DESCRIPTION = "Driver is stopping";

void MakeCountersNotNull(TReaderCounters& counters);
bool HasNullCounters(TReaderCounters& counters);

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
    Abort(EStatus::ABORTED, "Aborted");
    ClearAllEvents();

    if (Tracker) {
        Tracker->AsyncComplete().Wait();
    }
}

void TReadSession::Start() {
    ErrorHandler = MakeIntrusive<NPersQueue::TErrorHandler<false>>(weak_from_this());
    Tracker = std::make_shared<NPersQueue::TImplTracker>();
    EventsQueue = std::make_shared<NPersQueue::TReadSessionEventsQueue<false>>(Settings, weak_from_this(), Tracker);

    if (!ValidateSettings()) {
        return;
    }

    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Starting read session");

    NPersQueue::TDeferredActions<false> deferred;
    with_lock (Lock) {
        if (Aborting) {
            return;
        }
        Topics = Settings.Topics_;
        CreateClusterSessionsImpl(deferred);
    }
    ScheduleDumpCountersToLog();
}

void TReadSession::CreateClusterSessionsImpl(NPersQueue::TDeferredActions<false>& deferred) {
    Y_VERIFY(Lock.IsLocked());

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
    Session = std::make_shared<NPersQueue::TSingleClusterReadSessionImpl<false>>(
        Settings,
        DbDriverState->Database,
        SessionId,
        "",
        Log,
        Client->CreateReadSessionConnectionProcessorFactory(),
        EventsQueue,
        ErrorHandler,
        context,
        1, 1,
        Tracker);

    deferred.DeferStartSession(Session);
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
        Abort(EStatus::BAD_REQUEST, NPersQueue::MakeIssueWithSubIssues("Invalid read session settings", issues));
        return false;
    } else {
        return true;
    }
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

bool TReadSession::Close(TDuration timeout) {
    LOG_LAZY(Log, TLOG_INFO, GetLogPrefix() << "Closing read session. Close timeout: " << timeout);
    // Log final counters.
    DumpCountersToLog();
    with_lock (Lock) {
        if (DumpCountersContext) {
            DumpCountersContext->Cancel();
            DumpCountersContext.reset();
        }
    }

    NPersQueue::TSingleClusterReadSessionImpl<false>::TPtr session;
    NThreading::TPromise<bool> promise = NThreading::NewPromise<bool>();
    auto callback = [=]() mutable {
        promise.TrySetValue(true);
    };

    NPersQueue::TDeferredActions<false> deferred;
    with_lock (Lock) {
        if (Closing || Aborting) {
            return false;
        }

        if (!timeout) {
            AbortImpl(EStatus::ABORTED, "Close with zero timeout", deferred);
            return false;
        }

        Closing = true;
        session = Session;
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
        NPersQueue::Cancel(timeoutContext);

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

void TReadSession::OnUserRetrievedEvent(i64 decompressedSize, size_t messagesCount) {
    LOG_LAZY(Log, TLOG_DEBUG, GetLogPrefix()
                          << "The application data is transferred to the client. Number of messages "
                          << messagesCount
                          << ", size "
                          << decompressedSize
                          << " bytes");
}

void TReadSession::MakeCountersIfNeeded() {
    if (!Settings.Counters_ || NPersQueue::HasNullCounters(*Settings.Counters_)) {
        TReaderCounters::TPtr counters = MakeIntrusive<TReaderCounters>();
        if (Settings.Counters_) {
            *counters = *Settings.Counters_; // Copy all counters that have been set by user.
        }
        NPersQueue::MakeCountersNotNull(*counters);
        Settings.Counters(counters);
    }
}

void TReadSession::DumpCountersToLog(size_t timeNumber) {
    const bool logCounters = timeNumber % 60 == 0; // Every 1 minute.
    const bool dumpSessionsStatistics = timeNumber % 600 == 0; // Every 10 minutes.

    *Settings.Counters_->CurrentSessionLifetimeMs = (TInstant::Now() - StartSessionTime).MilliSeconds();
    NPersQueue::TSingleClusterReadSessionImpl<false>::TPtr session;
    with_lock (Lock) {
        if (Closing || Aborting) {
            return;
        }

        session = Session;
    }

    {
        TMaybe<TLogElement> log;
        if (dumpSessionsStatistics) {
            log.ConstructInPlace(&Log, TLOG_INFO);
            (*log) << "Read/commit by partition streams (cluster:topic:partition:stream-id:read-offset:committed-offset):";
        }
        session->UpdateMemoryUsageStatistics();
        if (dumpSessionsStatistics) {
            session->DumpStatisticsToLog(*log);
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

void TReadSession::AbortImpl(TSessionClosedEvent&& closeEvent, NPersQueue::TDeferredActions<false>& deferred) {
    Y_VERIFY(Lock.IsLocked());

    if (!Aborting) {
        Aborting = true;
        LOG_LAZY(Log, TLOG_NOTICE, GetLogPrefix() << "Aborting read session. Description: " << closeEvent.DebugString());
        if (DumpCountersContext) {
            DumpCountersContext->Cancel();
            DumpCountersContext.reset();
        }
        Session->Abort();
        EventsQueue->Close(std::move(closeEvent), deferred);
    }
}

void TReadSession::AbortImpl(EStatus statusCode, NYql::TIssues&& issues, NPersQueue::TDeferredActions<false>& deferred) {
    Y_VERIFY(Lock.IsLocked());

    AbortImpl(TSessionClosedEvent(statusCode, std::move(issues)), deferred);
}

void TReadSession::AbortImpl(EStatus statusCode, const TString& message, NPersQueue::TDeferredActions<false>& deferred) {
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
    NPersQueue::TDeferredActions<false> deferred;
    with_lock (Lock) {
        AbortImpl(std::move(closeEvent), deferred);
    }
}

}
