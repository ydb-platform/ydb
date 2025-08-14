#pragma once

#include "util.h"

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/config/protos/control_plane_storage.pb.h>
#include <ydb/core/kqp/proxy_service/kqp_script_execution_retries.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/public/api/protos/draft/fq.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NFq {

struct TControlPlaneStorageConfig {
    NConfig::TControlPlaneStorageConfig Proto;
    NConfig::TComputeConfig ComputeConfigProto;
    TString IdsPrefix;
    TDuration IdempotencyKeyTtl;
    TDuration AutomaticQueriesTtl;
    TDuration ResultSetsTtl;
    TDuration AnalyticsRetryCounterUpdateTime;
    TDuration StreamingRetryCounterUpdateTime;
    TDuration TaskLeaseTtl;
    TSet<FederatedQuery::ConnectionSetting::ConnectionCase> AvailableConnections;
    TSet<FederatedQuery::BindingSetting::BindingCase> AvailableBindings;
    ui64 GeneratorPathsLimit;
    THashMap<ui64, NKikimr::NKqp::TRetryPolicyItem> RetryPolicies;
    NKikimr::NKqp::TRetryPolicyItem TaskLeaseRetryPolicy;
    TDuration QuotaTtl;
    TDuration MetricsTtl;
    TSet<FederatedQuery::ConnectionSetting::ConnectionCase> AvailableStreamingConnections;

    TControlPlaneStorageConfig(const NConfig::TControlPlaneStorageConfig& config, const NYql::TS3GatewayConfig& s3Config, const NConfig::TCommonConfig& common, const NConfig::TComputeConfig& computeConfigProto);
};

} // NFq
