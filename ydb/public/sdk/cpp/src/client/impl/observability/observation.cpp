#include "observation.h"

namespace NYdb::inline Dev::NObservability {

TRequestObservation::TRequestObservation(const std::string& ydbClientType
    , NSdkStats::TStatCollector::TClientOperationStatCollector* operationCollector
    , std::shared_ptr<NTrace::ITracer> tracer
    , const std::string& operationName
    , const std::string& discoveryEndpoint
    , const std::string& database
    , const TLog& log
) : Span_(std::make_shared<TRequestSpan>(std::move(tracer), operationName, discoveryEndpoint, database, log, ydbClientType))
    , Metrics_(std::make_shared<TRequestMetrics>(operationCollector, operationName, log))
{}

void TRequestObservation::SetPeerEndpoint(const std::string& endpoint) noexcept {
    if (Span_) {
        Span_->SetPeerEndpoint(endpoint);
    }
}

void TRequestObservation::End(EStatus status) noexcept {
    if (Span_) {
        Span_->End(status);
    }
    if (Metrics_) {
        Metrics_->End(status);
    }
}

void TRequestObservation::EndWithClientInternalError() noexcept {
    End(EStatus::CLIENT_INTERNAL_ERROR);
}

} // namespace NYdb::NObservability
