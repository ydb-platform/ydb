#include "logger.h"

#include <library/cpp/unified_agent_client/clock.h>
#include <library/cpp/unified_agent_client/helpers.h>

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/stream/str.h>
#include <util/system/getpid.h>
#include <util/system/thread.h>

namespace NUnifiedAgent {
    namespace {
        TString FormatLogLine(ELogPriority logLevel, const TStringBuf message, const TString& scope) {
            TString result;
            {
                TStringOutput output(result);
                output << FormatIsoLocal(TClock::Now())
                     << " " << GetPID()
                     << " " << TThread::CurrentThreadId()
                     << " " << logLevel;
                if (!scope.Empty()) {
                    output << " " << scope;
                }
                output << " " << message << "\n";
            }
            return result;
        }
    }

    TLogger::TThrottlerWithLock::TThrottlerWithLock(size_t rateLimitBytes)
        : Throttler(rateLimitBytes, rateLimitBytes / 2)
        , Lock()
    {
    }

    bool TLogger::TThrottlerWithLock::TryConsume(double tokens) {
        with_lock(Lock) {
            return Throttler.TryConsume(tokens);
        }
    }

    TLogger::TLogger(TLog& log, TFMaybe<size_t> rateLimitBytes)
        : DefaultLogContext{log, log.IsNullLog() ? ELogPriority::TLOG_EMERG : log.FiltrationLevel()}
        , TracingLogContexts()
        , CurrentLogContext_()
        , Errors(nullptr)
        , DroppedBytes(nullptr)
        , Throttler(rateLimitBytes.Defined() ? MakeHolder<TThrottlerWithLock>(*rateLimitBytes) : nullptr)
        , Lock()
    {
        SetCurrentLogContext(DefaultLogContext);
    }

    void TLogger::SetCurrentLogContext(TLogContext& logContext) {
        CurrentLogContext_.store(logContext.Log.IsNullLog() ? nullptr : &logContext, std::memory_order_release);
    }

    void TLogger::Log(TLog& log, ELogPriority logPriority, const TStringBuf message, const TString& scope) const {
        try {
            const auto logLine = FormatLogLine(logPriority, NUnifiedAgent::NPrivate::ReplaceNonUTF(message).Data, scope);
            if (Throttler && &log == &DefaultLogContext.Log && !Throttler->TryConsume(logLine.size())) {
                if (DroppedBytes) {
                    DroppedBytes->Add(logLine.size());
                }
                return;
            }
            log.Write(logPriority, logLine);
        } catch (...) {
        }
    }

    void TLogger::StartTracing(ELogPriority logPriority) noexcept {
        with_lock(Lock) {
            auto& logContext = GetOrCreateTracingLogContext(logPriority);
            SetTracing(logContext, "started");
        }
    }

    void TLogger::FinishTracing() noexcept {
        with_lock(Lock) {
            SetTracing(DefaultLogContext, "finished");
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

        for (const auto& c: TracingLogContexts) {
            if (c->Priority == logPriority) {
                return *c;
            }
        }

        auto newLogContext = MakeHolder<TLogContext>();
        newLogContext->Log = TLog("cerr", logPriority);
        newLogContext->Priority = logPriority;
        auto* result = newLogContext.Get();
        TracingLogContexts.push_back(std::move(newLogContext));
        return *result;
    }

    TScopeLogger::TScopeLogger()
        : Logger(nullptr)
        , Scope()
        , Errors(nullptr)
    {
    }

    TScopeLogger::TScopeLogger(TLogger* logger,
                               const TString& scope,
                               NMonitoring::TDeprecatedCounter* errors)
        : Logger(logger)
        , Scope(scope)
        , Errors(errors)
    {
    }
}
