#pragma once

#include <ydb/core/fq/libs/config/protos/control_plane_proxy.pb.h>

#include <util/datetime/base.h>

namespace NFq {

struct TControlPlaneProxyConfig {
    NConfig::TControlPlaneProxyConfig Proto;
    TDuration RequestTimeout;
    TDuration MetricsTtl;
    TDuration ConfigRetryPeriod;

    TControlPlaneProxyConfig(const NConfig::TControlPlaneProxyConfig& config);
};

} // NFq
