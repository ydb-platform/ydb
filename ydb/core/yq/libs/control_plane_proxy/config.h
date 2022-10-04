#pragma once

#include <ydb/core/yq/libs/config/protos/control_plane_proxy.pb.h>

#include <util/datetime/base.h>

namespace NYq {

struct TControlPlaneProxyConfig {
    NConfig::TControlPlaneProxyConfig Proto;
    TDuration RequestTimeout;
    TDuration MetricsTtl;

    TControlPlaneProxyConfig(const NConfig::TControlPlaneProxyConfig& config);
};

} // NYq
