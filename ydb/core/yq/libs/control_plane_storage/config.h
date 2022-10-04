#pragma once

#include "util.h"

#include <ydb/core/yq/libs/config/protos/common.pb.h>
#include <ydb/core/yq/libs/config/protos/control_plane_storage.pb.h>
#include <ydb/public/api/protos/yq.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>

namespace NYq {

struct TControlPlaneStorageConfig {
    NConfig::TControlPlaneStorageConfig Proto;
    TString IdsPrefix;
    TDuration IdempotencyKeyTtl;
    TDuration AutomaticQueriesTtl;
    TDuration ResultSetsTtl;
    TDuration AnalyticsRetryCounterUpdateTime;
    TDuration StreamingRetryCounterUpdateTime;
    TDuration TaskLeaseTtl;
    TSet<YandexQuery::ConnectionSetting::ConnectionCase> AvailableConnections;
    TSet<YandexQuery::BindingSetting::BindingCase> AvailableBindings;
    THashMap<ui64, TRetryPolicyItem> RetryPolicies;
    TRetryPolicyItem TaskLeaseRetryPolicy;
    TDuration QuotaTtl;
    TDuration MetricsTtl;

    TControlPlaneStorageConfig(const NConfig::TControlPlaneStorageConfig& config, const NConfig::TCommonConfig& common);
};

} // NYq
