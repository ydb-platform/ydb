#pragma once

#include <ydb/core/fq/libs/compute/common/config.h>
#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/config/protos/compute.pb.h>
#include <ydb/core/fq/libs/config/protos/control_plane_proxy.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/config.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>

namespace NFq {

struct TControlPlaneProxyConfig {
    NConfig::TControlPlaneProxyConfig Proto;
    TControlPlaneStorageConfig StorageConfig;
    TComputeConfig ComputeConfig;
    NConfig::TCommonConfig CommonConfig;
    TDuration RequestTimeout;
    TDuration MetricsTtl;
    TDuration ConfigRetryPeriod;

    TControlPlaneProxyConfig(
        const NConfig::TControlPlaneProxyConfig& config,
        const NConfig::TControlPlaneStorageConfig& storageConfig,
        const NConfig::TComputeConfig& computeConfig,
        const NConfig::TCommonConfig& commonConfig,
        const NYql::TS3GatewayConfig& s3Config);
};

} // NFq
