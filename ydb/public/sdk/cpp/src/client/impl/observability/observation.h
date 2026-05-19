#pragma once

#include "metrics.h"
#include "span.h"

#include <memory>
#include <string>
#include <string_view>

namespace NYdb::inline Dev::NObservability {

class TRequestObservation {
public:
    TRequestObservation(std::string_view ydbClientType
        , NSdkStats::TStatCollector::TClientOperationStatCollector* operationCollector
        , std::shared_ptr<NTrace::ITracer> tracer
        , std::string_view operationName
        , const std::shared_ptr<TDbDriverState>& dbDriverState
    );

    void End(EStatus status, std::string_view endpoint = {}) noexcept;
    void EndWithClientInternalError() noexcept;

private:
    std::shared_ptr<TRequestSpan> Span_;
    std::shared_ptr<TRequestMetrics> Metrics_;
    std::weak_ptr<TDbDriverState> DbDriverState_;
};

} // namespace NYdb::NObservability
