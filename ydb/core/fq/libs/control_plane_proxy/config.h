#pragma once

#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/config/protos/control_plane_proxy.pb.h>

#include <util/datetime/base.h>

namespace NFq {

struct TControlPlaneProxyConfig {
    NConfig::TControlPlaneProxyConfig Proto;
    TComputeConfig ComputeConfig;
    NConfig::TCommonConfig CommonConfig;
    TDuration RequestTimeout;
    TDuration MetricsTtl;
    TDuration ConfigRetryPeriod;

    TControlPlaneProxyConfig(
        const NConfig::TControlPlaneProxyConfig& config,
        const NConfig::TComputeConfig& computeConfig,
        const NConfig::TCommonConfig& commonConfig);
};

} // NFq
