#include "metrics.h"

#include <ydb/public/sdk/cpp/src/client/impl/internal/common/log_lazy.h>

#include <exception>

namespace NYdb::inline Dev::NObservability {

namespace {

void SafeLogRequestMetricsError(TLog& log, const char* message, std::exception_ptr exception) noexcept {
    try {
        if (!exception) {
            LOG_LAZY(log, TLOG_ERR, std::string("TRequestMetrics: ") + message + ": (no active exception)");
            return;
        }
        try {
            std::rethrow_exception(exception);
        } catch (const std::exception& e) {
            LOG_LAZY(log, TLOG_ERR, std::string("TRequestMetrics: ") + message + ": " + e.what());
            return;
        } catch (...) {
        }
        LOG_LAZY(log, TLOG_ERR, std::string("TRequestMetrics: ") + message + ": (unknown)");
    } catch (...) {
    }
}

} // namespace

TRequestMetrics::TRequestMetrics(NSdkStats::TStatCollector::TClientOperationStatCollector* operationCollector
    , const std::string& requestName
    , const TLog& log
) : Collector_(operationCollector)
    , RequestName_(requestName)
    , Log_(log)
{
    if (!Collector_) {
        return;
    }
    try {
        Collector_->IncRequestCount(requestName);
        StartTime_ = std::chrono::steady_clock::now();
    } catch (...) {
        SafeLogRequestMetricsError(Log_, "failed to initialize metrics", std::current_exception());
        Collector_ = nullptr;
    }
}

TRequestMetrics::~TRequestMetrics() noexcept {
    End(EStatus::CLIENT_INTERNAL_ERROR);
}

void TRequestMetrics::End(EStatus status) noexcept {
    if (Ended_) {
        return;
    }
    Ended_ = true;

    if (!Collector_) {
        return;
    }

    try {
        auto elapsed = std::chrono::steady_clock::now() - StartTime_;
        double durationSec = std::chrono::duration<double>(elapsed).count();
        Collector_->RecordLatency(RequestName_, durationSec, status);
        Collector_->IncErrorCount(RequestName_, status);
    } catch (...) {
        SafeLogRequestMetricsError(Log_, "failed to record metrics", std::current_exception());
    }
}

} // namespace NYdb::NObservability
