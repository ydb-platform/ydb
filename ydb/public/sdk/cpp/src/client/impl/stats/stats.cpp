#include "stats.h"

#include <string>

namespace NYdb::inline Dev {
namespace NSdkStats {

const NMonitoring::TLabel SESSIONS_ON_KQP_HOST_LABEL = NMonitoring::TLabel {"sensor", "SessionsByYdbHost"};
const NMonitoring::TLabel TRANSPORT_ERRORS_BY_HOST_LABEL = NMonitoring::TLabel {"sensor", "TransportErrorsByYdbHost"};
const NMonitoring::TLabel GRPC_INFLIGHT_BY_HOST_LABEL = NMonitoring::TLabel {"sensor", "Grpc/InFlightByYdbHost"};

void TStatCollector::IncSessionsOnHost(std::string_view host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, {"YdbHost", std::string(host)} })->Inc();
    }
}

void TStatCollector::DecSessionsOnHost(std::string_view host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, {"YdbHost", std::string(host)} })->Dec();
    }
}

void TStatCollector::IncTransportErrorsByHost(std::string_view host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->Rate({ DatabaseLabel_, TRANSPORT_ERRORS_BY_HOST_LABEL, {"YdbHost", std::string(host)} })->Inc();
    }
}

void TStatCollector::IncGRpcInFlightByHost(std::string_view host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, GRPC_INFLIGHT_BY_HOST_LABEL, {"YdbHost", std::string(host)} })->Inc();
    }
}

void TStatCollector::DecGRpcInFlightByHost(std::string_view host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, GRPC_INFLIGHT_BY_HOST_LABEL, {"YdbHost", std::string(host)} })->Dec();
    }
}

} // namespace NSdkStats
} // namespace NYdb
