#include "public_counters.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/protos/kesus.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NKesus {

using TMetric = NKikimrKesus::TAccountingConfig::TMetric;

std::optional<TString> GetMetricCategory(const TMetric& cfg) {
    for (const auto& [label, value] : cfg.GetLabels()) {
        if (to_lower(label) == "category") {
            return value;
        }
    }

    return std::nullopt;
}

bool IsPublicMetric(const TMetric& cfg) {
    return cfg.GetEnabled()
        && cfg.GetCloudId()
        && cfg.GetFolderId()
        && cfg.GetResourceId()
        && GetMetricCategory(cfg);
}

::NMonitoring::TDynamicCounterPtr GetPublicCounters(const TMetric& cfg, ::NMonitoring::TDynamicCounterPtr counters) {
    return GetServiceCounters(counters, "ydb_serverless", false)
        ->GetSubgroup("host", "")
        ->GetSubgroup("cloud_id", cfg.GetCloudId())
        ->GetSubgroup("folder_id", cfg.GetFolderId())
        ->GetSubgroup("database_id", cfg.GetResourceId())
        ->GetSubgroup("database", cfg.GetDatabase());
}

} // NKikimr::NKesus
