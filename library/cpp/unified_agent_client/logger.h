#pragma once

#include <library/cpp/unified_agent_client/f_maybe.h>
#include <library/cpp/unified_agent_client/throttling.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <atomic>

#define YLOG(logPriority, message, logger)                                        \
    do {                                                                          \
        const auto __logPriority = logPriority;                                   \
        if (auto* log = logger.Accept(__logPriority, false); log != nullptr) {    \
            logger.Log(*log, __logPriority, message);                             \
        }                                                                         \
    } while (false)

#define YLOG_EMERG(msg)         YLOG(TLOG_EMERG, msg, Logger)
#define YLOG_ALERT(msg)         YLOG(TLOG_ALERT, msg, Logger)
#define YLOG_CRIT(msg)          YLOG(TLOG_CRIT, msg, Logger)
#define YLOG_ERR(msg)           YLOG(TLOG_ERR, msg, Logger)
#define YLOG_WARNING(msg)       YLOG(TLOG_WARNING, msg, Logger)
#define YLOG_NOTICE(msg)        YLOG(TLOG_NOTICE, msg, Logger)
#define YLOG_INFO(msg)          YLOG(TLOG_INFO, msg, Logger)
#define YLOG_DEBUG(msg)         YLOG(TLOG_DEBUG, msg, Logger)
#define YLOG_RESOURCES(msg)     YLOG(TLOG_RESOURCES , msg, Logger)

#define YLOG_FATAL(msg)            \
    YLOG(TLOG_CRIT, msg, Logger);  \
    _Exit(1);

namespace NUnifiedAgent {
    class TScopeLogger;

    class TLogger {
    public:
        TLogger(TLog& log, TFMaybe<size_t> rateLimitBytes);

        void StartTracing(ELogPriority logPriority) noexcept;

        void FinishTracing() noexcept;

        inline TScopeLogger Child(const TString& v, NMonitoring::TDeprecatedCounter* errors = nullptr);

        inline void SetErrorsCounter(NMonitoring::TDeprecatedCounter* counter) noexcept {
            Errors = counter;
        }

        inline void SetDroppedBytesCounter(NMonitoring::TDeprecatedCounter* counter) noexcept {
            DroppedBytes = counter;
        }

        inline bool HasRateLimit() const noexcept {
            return Throttler != nullptr;
        }

        friend class TScopeLogger;

    private:
        void Log(TLog& log, ELogPriority logPriority, const TStringBuf message, const TString& scope) const;

        inline TLog* Accept(ELogPriority logPriority, NMonitoring::TDeprecatedCounter* errors) const noexcept {
            if ((logPriority <= TLOG_ERR) && (errors != nullptr)) {
                ++(*errors);
            }
            auto* result = CurrentLogContext_.load(std::memory_order_acquire);
            return result != nullptr && static_cast<int>(logPriority) <= static_cast<int>(result->Priority)
                   ? &result->Log
                   : nullptr;
        }

    private:
        struct TLogContext {
            TLog Log;
            ELogPriority Priority;
        };

        class TThrottlerWithLock {
        public:
            explicit TThrottlerWithLock(size_t rateLimitBytes);

            bool TryConsume(double tokens);

        private:
            TThrottler Throttler;
            TAdaptiveLock Lock;
        };

    private:
        void SetCurrentLogContext(TLogContext& logContext);

        TLogContext& GetOrCreateTracingLogContext(ELogPriority logPriority);

        void SetTracing(TLogContext& logContext, const char* action);

    private:
        TLogContext DefaultLogContext;
        TVector<THolder<TLogContext>> TracingLogContexts;
        std::atomic<TLogContext*> CurrentLogContext_;
        NMonitoring::TDeprecatedCounter* Errors;
        NMonitoring::TDeprecatedCounter* DroppedBytes;
        const THolder<TThrottlerWithLock> Throttler;
        TAdaptiveLock Lock;
    };

    class TScopeLogger {
    public:
        TScopeLogger();

        inline void Log(TLog& log, ELogPriority logPriority, const TStringBuf message) const {
            if (Logger) {
                Logger->Log(log, logPriority, message, Scope);
            }
        }

        inline TLog* Accept(ELogPriority logPriority, bool silent) const noexcept {
            return Logger ? Logger->Accept(logPriority, silent ? nullptr : Errors) : nullptr;
        }

        inline TScopeLogger Child(const TString& v, NMonitoring::TDeprecatedCounter* errors = nullptr) {
            return Logger
                ? Logger->Child(Join('/', Scope, v), errors == nullptr ? Errors : errors)
                : TScopeLogger();
        }

        inline TLogger* Unwrap() noexcept {
            return Logger;
        }

        friend class TLogger;

    private:
        TScopeLogger(TLogger* logger,
                     const TString& scope,
                     NMonitoring::TDeprecatedCounter* errors);

    private:
        TLogger* Logger;
        TString Scope;
        NMonitoring::TDeprecatedCounter* Errors;
    };

    inline TScopeLogger TLogger::Child(const TString& v, NMonitoring::TDeprecatedCounter* errors) {
        return TScopeLogger(this, v, errors == nullptr ? Errors : errors);
    }

    inline ELogPriority ToLogPriority(int level) noexcept {
        const auto result = ClampVal(level, 0, static_cast<int>(TLOG_RESOURCES));
        return static_cast<ELogPriority>(result);
    }
}
