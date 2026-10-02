#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/stats/stats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <library/cpp/logger/log.h>

#include <chrono>
#include <string>

namespace NYdb::inline Dev::NObservability {

class TRequestMetrics {
public:
    TRequestMetrics(NSdkStats::TStatCollector::TClientOperationStatCollector* operationCollector
        , const std::string& requestName
        , const TLog& log
    );
    ~TRequestMetrics() noexcept;

    void End(EStatus status) noexcept;

private:
    NSdkStats::TStatCollector::TClientOperationStatCollector* Collector_ = nullptr;
    std::string RequestName_;
    std::chrono::steady_clock::time_point StartTime_{};
    bool Ended_ = false;
    TLog Log_;
};

} // namespace NYdb::NObservability
