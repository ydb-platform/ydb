#include "stats.h"

#include <string>

namespace NYdb::inline Dev {
namespace NSdkStats {

using std::string;

const NMonitoring::TLabel SESSIONS_ON_KQP_HOST_LABEL = NMonitoring::TLabel {"sensor", "SessionsByYdbHost"};
const NMonitoring::TLabel TRANSPORT_ERRORS_BY_HOST_LABEL = NMonitoring::TLabel {"sensor", "TransportErrorsByYdbHost"};
const NMonitoring::TLabel GRPC_INFLIGHT_BY_HOST_LABEL = NMonitoring::TLabel {"sensor", "Grpc/InFlightByYdbHost"};

void TStatCollector::IncSessionsOnHost(const string& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, {"YdbHost", host} })->Inc();
    }
}

void TStatCollector::DecSessionsOnHost(const string& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, {"YdbHost", host} })->Dec();
    }
}

void TStatCollector::IncTransportErrorsByHost(const string& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->Rate({ DatabaseLabel_, TRANSPORT_ERRORS_BY_HOST_LABEL, {"YdbHost", host} })->Inc();
    }
}

void TStatCollector::IncGRpcInFlightByHost(const string& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, GRPC_INFLIGHT_BY_HOST_LABEL, {"YdbHost", host} })->Inc();
    }
}

void TStatCollector::DecGRpcInFlightByHost(const string& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) {
        ptr->IntGauge({ DatabaseLabel_, GRPC_INFLIGHT_BY_HOST_LABEL, {"YdbHost", host} })->Dec();
    }
}

} // namespace NSdkStats
} // namespace NYdb
