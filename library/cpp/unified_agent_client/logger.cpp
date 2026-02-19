#include "logger.h"

#include <library/cpp/unified_agent_client/clock.h>
#include <library/cpp/unified_agent_client/helpers.h>

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/stream/str.h>
#include <util/system/getpid.h>
#include <util/system/thread.h>

namespace NUnifiedAgent {
    namespace {
        // Check if we should log this message (throttling logic)
        // Returns: {shouldLog, droppedCount}
        //
        // Guarantees:
        // 1. Prevents log floods - limits logs to ~maxLogsPerSlot per slot
        // 2. Every dropped log is counted, but it can be reported later than next logged line
        //
        // Memory ordering: relaxed should be sufficient here.
        // Logs near slot boundaries may be dropped even though the new slot has started,
        // or may pass even though the old slot quota is exceeded. This is acceptable for
        // throttling - we need approximate rate limiting, not perfect precision.
        // Stronger memory ordering cannot eliminate these inherent race conditions.
        std::pair<bool, ui32> ShouldLogThrottled(TLogger::TThrottleState& state, ui32 maxLogsPerSlot, ui32 slotSeconds) {
            static const ui64 cyclesPerSecond = GetCyclesPerMillisecond() * 1000;
            const ui32 newSlot = GetCycleCount() / cyclesPerSecond / slotSeconds;
            ui32 oldSlot = state.TimeSlot.load(std::memory_order_relaxed);

            // New slot - try to update the time slot
            if (oldSlot != newSlot) {
                // The race winner updates the time slot and resets the Logged counter
                if (state.TimeSlot.compare_exchange_strong(oldSlot, newSlot, std::memory_order_relaxed)) {
                    state.Logged.store(1, std::memory_order_relaxed);
                    const ui32 dropped = state.Dropped.exchange(0, std::memory_order_relaxed);
                    return {true, dropped};
                }
            }

            // Same slot (including the race loosers) - check if we can log
            const ui32 logged = state.Logged.fetch_add(1, std::memory_order_relaxed);
            if (logged < maxLogsPerSlot) {
                // Under limit - log it
                return {true, 0};
            }

            // Over limit - drop it
            state.Dropped.fetch_add(1, std::memory_order_relaxed);
            return {false, 0};
        }

        TString FormatLogLine(ELogPriority logLevel, const TStringBuf message, const TString& scope) {
            TString result;
            {
                TStringOutput output(result);
                output << FormatIsoLocal(TClock::Now())
                     << " " << GetPID()
                     << " " << TThread::CurrentThreadId()
                     << " " << logLevel;
                if (!scope.empty()) {
                    output << " " << scope;
                }
                output << " " << message << "\n";
            }
            return result;
        }
    }

    TLogger::TThrottlerWithLock::TThrottlerWithLock(size_t rateLimitBytes)
        : Throttler_(rateLimitBytes, rateLimitBytes / 2)
        , Lock_()
    {
    }

    bool TLogger::TThrottlerWithLock::TryConsume(double tokens) {
        with_lock(Lock_) {
            return Throttler_.TryConsume(tokens);
        }
    }

    TLogger::TLogger(TLog& log, TFMaybe<size_t> rateLimitBytes, TCounters counters)
        : DefaultLogContext_{log, log.IsNullLog() ? ELogPriority::TLOG_EMERG : log.FiltrationLevel()}
        , TracingLogContexts_()
        , CurrentLogContext_()
        , Counters_(counters)
        , MaxLogsPerSlot_(0)
        , SlotSeconds_(0)
        , Throttler_(rateLimitBytes.Defined() ? MakeHolder<TThrottlerWithLock>(*rateLimitBytes) : nullptr)
        , Lock_()
    {
        SetCurrentLogContext(DefaultLogContext_);
    }

    TLogger::TLogger(TLog& log, TFMaybe<size_t> rateLimitBytes, ui32 maxLogsPerSlot, ui32 slotSeconds, TCounters counters)
        : DefaultLogContext_{log, log.IsNullLog() ? ELogPriority::TLOG_EMERG : log.FiltrationLevel()}
        , TracingLogContexts_()
        , CurrentLogContext_()
        , Counters_(counters)
        , MaxLogsPerSlot_(maxLogsPerSlot)
        , SlotSeconds_(slotSeconds)
        , Throttler_(rateLimitBytes.Defined() ? MakeHolder<TThrottlerWithLock>(*rateLimitBytes) : nullptr)
        , Lock_()
    {
        SetCurrentLogContext(DefaultLogContext_);
    }

    void TLogger::AddLog(TLog& log) {
        with_lock(Lock_) {
            AdditionalLogs_.push_back(log);
        }
    }

    void TLogger::SetCurrentLogContext(TLogContext& logContext) {
        CurrentLogContext_.store(logContext.Log.IsNullLog() ? nullptr : &logContext, std::memory_order_release);
    }

    std::pair<TLog*, ui32> TLogger::Accept(ELogPriority logPriority, NMonitoring::TDeprecatedCounter* errors, TLogger::TThrottleState& state) const {
        // Increment received counter if set
        if (Counters_.RecordsReceived) {
            ++(*Counters_.RecordsReceived);
        }

        // Call base Accept to check log level
        auto* log = Accept(logPriority, errors);
        if (!log) {
            return {nullptr, 0};
        }

        // Skip throttling if not configured
        if (MaxLogsPerSlot_ == 0 || SlotSeconds_ == 0) {
            return {log, 0};
        }

        auto [shouldLog, dropped] = ShouldLogThrottled(state, MaxLogsPerSlot_, SlotSeconds_);
        if (shouldLog) {
            return {log, dropped};
        }

        if (Counters_.RecordsDropped) {
            ++(*Counters_.RecordsDropped);
        }
        return {nullptr, 0};
    }

    void TLogger::Log(TLog& log, ELogPriority logPriority, const TStringBuf message, const TString& scope) const {
        try {
            const auto logLine = FormatLogLine(logPriority, NUnifiedAgent::NPrivate::ReplaceNonUTF(message).Data, scope);
            if (Throttler_ && &log == &DefaultLogContext_.Log && !Throttler_->TryConsume(logLine.size())) {
                if (Counters_.DroppedBytes) {
                    Counters_.DroppedBytes->Add(logLine.size());
                }
                return;
            }
            log.Write(logPriority, logLine);

            // Write to all additional logs
            for (auto& additionalLog : AdditionalLogs_) {
                additionalLog.Write(logPriority, logLine);
            }
        } catch (...) {
        }
    }

    void TLogger::StartTracing(ELogPriority logPriority) noexcept {
        with_lock(Lock_) {
            auto& logContext = GetOrCreateTracingLogContext(logPriority);
            SetTracing(logContext, "started");
        }
    }

    void TLogger::FinishTracing() noexcept {
        with_lock(Lock_) {
            SetTracing(DefaultLogContext_, "finished");
        }
    }

    void TLogger::SetTracing(TLogContext& logContext, const char* action) {
        // Lock must be held

        SetCurrentLogContext(logContext);

        Log(logContext.Log,
            TLOG_INFO,
            Sprintf("tracing %s, log priority is set to [%s]",
                    action, ToString(logContext.Priority).c_str()),
            "");
    }

    auto TLogger::GetOrCreateTracingLogContext(ELogPriority logPriority) -> TLogContext& {
        // Lock must be held

        for (const auto& c: TracingLogContexts_) {
            if (c->Priority == logPriority) {
                return *c;
            }
        }

        auto newLogContext = MakeHolder<TLogContext>();
        newLogContext->Log = TLog("cerr", logPriority);
        newLogContext->Priority = logPriority;
        auto* result = newLogContext.Get();
        TracingLogContexts_.push_back(std::move(newLogContext));
        return *result;
    }

    TScopeLogger TLogger::Child(const TString& v, NMonitoring::TDeprecatedCounter* errors) {
        return TScopeLogger(this, v, errors == nullptr ? Counters_.Errors : errors);
    }


    TScopeLogger::TScopeLogger()
        : Logger_(nullptr)
        , Scope_()
        , Errors_(nullptr)
    {
    }

    TScopeLogger::TScopeLogger(TLogger* logger, TString scope, NMonitoring::TDeprecatedCounter* errors)
        : Logger_(logger)
        , Scope_(std::move(scope))
        , Errors_(errors)
    {
    }

    TScopeLogger TScopeLogger::Child(const TString& v, NMonitoring::TDeprecatedCounter* errors) {
        return Logger_
            ? Logger_->Child(Join('/', Scope_, v), errors == nullptr ? Errors_ : errors)
            : TScopeLogger();
    }
}
