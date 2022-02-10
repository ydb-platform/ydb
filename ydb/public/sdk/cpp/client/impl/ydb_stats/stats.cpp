#include "stats.h"

namespace NYdb {
namespace NSdkStats {

const NMonitoring::TLabel SESSIONS_ON_KQP_HOST_LABEL = NMonitoring::TLabel {"sensor", "SessionsByYdbHost"};
const NMonitoring::TLabel TRANSPORT_ERRORS_BY_HOST_LABEL = NMonitoring::TLabel {"sensor", "TransportErrorsByYdbHost"};
const NMonitoring::TLabel GRPC_INFLIGHT_BY_HOST_LABEL = NMonitoring::TLabel {"sensor", "Grpc/InFlightByYdbHost"};

void TStatCollector::IncSessionsOnHost(const TStringType& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) { 
        ptr->IntGauge({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, {"YdbHost", host} })->Inc();
    }
}

void TStatCollector::DecSessionsOnHost(const TStringType& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) { 
        ptr->IntGauge({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, {"YdbHost", host} })->Dec();
    }
}

void TStatCollector::IncTransportErrorsByHost(const TStringType& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) { 
        ptr->Rate({ DatabaseLabel_, TRANSPORT_ERRORS_BY_HOST_LABEL, {"YdbHost", host} })->Inc();
    }
}

void TStatCollector::IncGRpcInFlightByHost(const TStringType& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) { 
        ptr->IntGauge({ DatabaseLabel_, GRPC_INFLIGHT_BY_HOST_LABEL, {"YdbHost", host} })->Inc();
    }
}

void TStatCollector::DecGRpcInFlightByHost(const TStringType& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) { 
        ptr->IntGauge({ DatabaseLabel_, GRPC_INFLIGHT_BY_HOST_LABEL, {"YdbHost", host} })->Dec();
    }
}

void TStatCollector::DeleteHost(const TStringType& host) {
    if (TMetricRegistry* ptr = MetricRegistryPtr_.Get()) { 
        const NMonitoring::TLabel hostLabel = NMonitoring::TLabel {"YdbHost", host};

        {
            NMonitoring::TLabels label({ DatabaseLabel_, SESSIONS_ON_KQP_HOST_LABEL, hostLabel });
            ptr->RemoveMetric(label); 
        }
        {
            NMonitoring::TLabels label({ DatabaseLabel_, TRANSPORT_ERRORS_BY_HOST_LABEL, hostLabel });
            ptr->RemoveMetric(label); 
        }
    }
}

} // namespace NSdkStats
} // namespace NYdb
