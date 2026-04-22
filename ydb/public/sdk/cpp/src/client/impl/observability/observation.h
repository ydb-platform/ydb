#pragma once

#include "metrics.h"
#include "span.h"

#include <memory>
#include <string>

namespace NYdb::inline Dev::NObservability {

class TRequestObservation {
public:
    TRequestObservation(const std::string& ydbClientType
        , NSdkStats::TStatCollector::TClientOperationStatCollector* operationCollector
        , std::shared_ptr<NTrace::ITracer> tracer
        , const std::string& operationName
        , const std::string& discoveryEndpoint
        , const std::string& database
        , const TLog& log
    );

    void SetPeerEndpoint(const std::string& endpoint) noexcept;
    void End(EStatus status) noexcept;
    void EndWithClientInternalError() noexcept;

private:
    std::shared_ptr<TRequestSpan> Span_;
    std::shared_ptr<TRequestMetrics> Metrics_;
};

} // namespace NYdb::NObservability
