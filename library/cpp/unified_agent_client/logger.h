#pragma once

#include <library/cpp/unified_agent_client/f_maybe.h>
#include <library/cpp/unified_agent_client/throttling.h>
#include <library/cpp/unified_agent_client/formatters.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/string.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <atomic>
#include <format>

#define YLOG(logPriority, message, logger)                                        \
    do {                                                                          \
        const auto __logPriority = logPriority;                                   \
        if (auto* log = logger.Accept(__logPriority, false); log != nullptr) {    \
            logger.Log(*log, __logPriority, message);                             \
        }                                                                         \
    } while (false)

// Logging with throttling
#define YLOG_T(logger, logPriority, ...)                                                                     \
    do {                                                                                                     \
        const auto _logPriority = logPriority;                                                               \
        static ::NUnifiedAgent::TLogger::TThrottleState _throttleState;                                      \
        auto [_log, _dropped] = logger.Accept(_logPriority, false, _throttleState);                          \
        if (_log != nullptr) {                                                                               \
            if (_dropped > 0) {                                                                              \
                logger.Log(*_log, _logPriority, std::format("{} [+{} suppressed]",                           \
                                                ::NUnifiedAgent::YLogFormatMessage(__VA_ARGS__), _dropped)); \
            } else {                                                                                         \
                logger.Log(*_log, _logPriority, ::NUnifiedAgent::YLogFormatMessage(__VA_ARGS__));            \
            }                                                                                                \
        }                                                                                                    \
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

#define YLOG_EMERG_F(fmt, ...) YLOG_EMERG(std::format(fmt, __VA_ARGS__))
#define YLOG_ALERT_F(fmt, ...) YLOG_ALERT(std::format(fmt, __VA_ARGS__))
#define YLOG_CRIT_F(fmt, ...) YLOG_CRIT(std::format(fmt, __VA_ARGS__))
#define YLOG_ERR_F(fmt, ...) YLOG_ERR(std::format(fmt, __VA_ARGS__))
#define YLOG_WARNING_F(fmt, ...) YLOG_WARNING(std::format(fmt, __VA_ARGS__))
#define YLOG_NOTICE_F(fmt, ...) YLOG_NOTICE(std::format(fmt, __VA_ARGS__))
#define YLOG_INFO_F(fmt, ...) YLOG_INFO(std::format(fmt, __VA_ARGS__))
#define YLOG_DEBUG_F(fmt, ...) YLOG_DEBUG(std::format(fmt, __VA_ARGS__))
#define YLOG_RESOURCES_F(fmt, ...) YLOG_RESOURCES(std::format(fmt, __VA_ARGS__))
#define YLOG_FATAL_F(fmt, ...) YLOG_FATAL(std::format(fmt, __VA_ARGS__))

// Logging with throttling and metrics increment:
// Errors for ERR+ priorities, RecordsReceived/RecordsDropped
#define YLOG_EMERG_T(...)         YLOG_T(Logger, TLOG_EMERG, __VA_ARGS__)
#define YLOG_ALERT_T(...)         YLOG_T(Logger, TLOG_ALERT, __VA_ARGS__)
#define YLOG_CRITICAL_T(...)      YLOG_T(Logger, TLOG_CRIT, __VA_ARGS__)
#define YLOG_ERROR_T(...)         YLOG_T(Logger, TLOG_ERR, __VA_ARGS__)
#define YLOG_WARNING_T(...)       YLOG_T(Logger, TLOG_WARNING, __VA_ARGS__)
#define YLOG_NOTICE_T(...)        YLOG_T(Logger, TLOG_NOTICE, __VA_ARGS__)
#define YLOG_INFO_T(...)          YLOG_T(Logger, TLOG_INFO, __VA_ARGS__)
#define YLOG_DEBUG_T(...)         YLOG_T(Logger, TLOG_DEBUG, __VA_ARGS__)
#define YLOG_RESOURCES_T(...)     YLOG_T(Logger, TLOG_RESOURCES , __VA_ARGS__)

namespace NUnifiedAgent {
    // Helper functions to format messages
    template <typename T>
    decltype(auto) YLogFormatMessage(T&& msg) {
        return std::forward<T>(msg);
    }

    // Overload for multiple arguments (with formatting)
    template <typename... Args>
    std::string YLogFormatMessage(std::format_string<Args...> fmt, Args&&... args) {
        return std::format(fmt, std::forward<Args>(args)...);
    }

    class TScopeLogger;

    class TLogger {
    public:
        // These counters must outlive the TLogger instance
        struct TCounters {
            NMonitoring::TDeprecatedCounter* Errors;
            NMonitoring::TDeprecatedCounter* DroppedBytes;
            NMonitoring::TDeprecatedCounter* RecordsReceived;
            NMonitoring::TDeprecatedCounter* RecordsDropped;
        };

        // Throttling state for a single log location
        struct TThrottleState {
            std::atomic<ui32> TimeSlot{0};
            std::atomic<ui32> Logged{0};
            std::atomic<ui32> Dropped{0};
        };

        TLogger(TLog& log, TFMaybe<size_t> rateLimitBytes, TCounters counters = {});

        TLogger(TLog& log, TFMaybe<size_t> rateLimitBytes, ui32 maxLogsPerSlot, ui32 slotSeconds, TCounters counters = {});

        virtual ~TLogger() = default;

        void AddLog(TLog& log);

        void StartTracing(ELogPriority logPriority) noexcept;

        void FinishTracing() noexcept;

        TScopeLogger Child(const TString& v, NMonitoring::TDeprecatedCounter* errors = nullptr);

        bool HasRateLimit() const noexcept {
            return Throttler_ != nullptr;
        }

        friend class TScopeLogger;

    protected:
        // Accept with throttling support
        // Returns: pair of (TLog* if should log or nullptr if dropped, number of previously suppressed logs)
        std::pair<TLog*, ui32> Accept(ELogPriority logPriority, NMonitoring::TDeprecatedCounter* errors, TThrottleState& state) const;

        virtual void Log(TLog& log, ELogPriority logPriority, const TStringBuf message, const TString& scope) const;

        TLog* Accept(ELogPriority logPriority, NMonitoring::TDeprecatedCounter* errors) const noexcept {
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
            TThrottler Throttler_;
            TAdaptiveLock Lock_;
        };

    private:
        void SetCurrentLogContext(TLogContext& logContext);

        TLogContext& GetOrCreateTracingLogContext(ELogPriority logPriority);

        void SetTracing(TLogContext& logContext, const char* action);

    private:
        TLogContext DefaultLogContext_;
        TVector<THolder<TLogContext>> TracingLogContexts_;
        std::atomic<TLogContext*> CurrentLogContext_;
        TCounters Counters_;
        const ui32 MaxLogsPerSlot_;
        const ui32 SlotSeconds_;
        const THolder<TThrottlerWithLock> Throttler_;
        TAdaptiveLock Lock_;
        TVector<TLog> AdditionalLogs_;
    };

    class TScopeLogger {
    public:
        TScopeLogger();

        void Log(TLog& log, ELogPriority logPriority, const TStringBuf message) const {
            if (Logger_) {
                Logger_->Log(log, logPriority, message, Scope_);
            }
        }

        TLog* Accept(ELogPriority logPriority, bool silent) const noexcept {
            return Logger_ ? Logger_->Accept(logPriority, silent ? nullptr : Errors_) : nullptr;
        }

        // Accept with throttling support
        std::pair<TLog*, ui32> Accept(ELogPriority logPriority, bool silent, TLogger::TThrottleState& state) const {
            return Logger_
                       ? Logger_->Accept(logPriority, silent ? nullptr : Errors_, state)
                       : std::make_pair(nullptr, 0);
        }

        TScopeLogger Child(const TString& v, NMonitoring::TDeprecatedCounter* errors = nullptr);

        TLogger* Unwrap() noexcept {
            return Logger_;
        }

        ELogPriority FiltrationLevel() const {
            if (Logger_) {
                return Logger_->CurrentLogContext_.load()->Priority;
            }
            return LOG_DEF_PRIORITY;
        }

        friend class TLogger;

    private:
        TScopeLogger(TLogger* logger, TString scope, NMonitoring::TDeprecatedCounter* errors);

    private:
        TLogger* Logger_;
        TString Scope_;
        NMonitoring::TDeprecatedCounter* Errors_;
    };

} // namespace NUnifiedAgent
