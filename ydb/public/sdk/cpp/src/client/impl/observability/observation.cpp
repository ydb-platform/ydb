#include "observation.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#undef INCLUDE_YDB_INTERNAL_H

namespace NYdb::inline Dev::NObservability {

TRequestObservation::TRequestObservation(const std::string& ydbClientType
    , NSdkStats::TStatCollector::TClientOperationStatCollector* operationCollector
    , std::shared_ptr<NTrace::ITracer> tracer
    , const std::string& operationName
    , const std::shared_ptr<TDbDriverState>& dbDriverState
) : Span_(
        std::make_shared<TRequestSpan>(ydbClientType
            , std::move(tracer)
            , operationName
            , dbDriverState
        )
    ), Metrics_(
        std::make_shared<TRequestMetrics>(operationCollector, operationName, dbDriverState->Log)
    )
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
