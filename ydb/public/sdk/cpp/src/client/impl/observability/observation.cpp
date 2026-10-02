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
        TRequestSpan::Create(ydbClientType
            , std::move(tracer)
            , operationName
            , dbDriverState->DiscoveryEndpoint
            , dbDriverState->Database
            , dbDriverState->Log
        )
    ), Metrics_(
        std::make_shared<TRequestMetrics>(operationCollector, operationName, dbDriverState->Log)
    ), DbDriverState_(dbDriverState)
{}

void TRequestObservation::End(EStatus status, const std::string& endpoint) noexcept {
    if (Span_) {
        std::uint64_t nodeId = 0;
        std::string location;
        if (!endpoint.empty()) {
            if (auto state = DbDriverState_.lock()) {
                const auto record = state->EndpointPool.GetEndpoint(TEndpointKey(endpoint, 0), /*onlyPreferred=*/true);
                nodeId = record.NodeId;
                location = record.Location;
            }
        }
        Span_->SetPeerEndpoint(endpoint, nodeId, location);
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
