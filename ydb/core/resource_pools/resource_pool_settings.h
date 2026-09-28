#pragma once

#include "settings_common.h"

#include <contrib/libs/protobuf/src/google/protobuf/map.h>

#include <util/datetime/base.h>


namespace NKikimr::NResourcePool {

inline constexpr char DEFAULT_POOL_ID[] = "default";

inline constexpr i64 POOL_MAX_CONCURRENT_QUERY_LIMIT = 1000;

struct TPoolSettings : public TSettingsBase {
    typedef double TPercent;

    using TBase = TSettingsBase;
    using TProperty = std::variant<i32*, TDuration*, TPercent*>;

    struct TParser : public TBase::TParser {
        void operator()(i32* setting) const;
        void operator()(TDuration* setting) const;
        void operator()(TPercent* setting) const;
    };

    struct TExtractor : public TBase::TExtractor {
        TString operator()(i32* setting) const;
        TString operator()(double* setting) const;
        TString operator()(TDuration* setting) const;
    };

    TPoolSettings() = default;
    TPoolSettings(const google::protobuf::Map<TString, TString>& properties);

    bool operator==(const TPoolSettings& other) const = default;

    std::unordered_map<TString, TProperty> GetPropertiesMap(bool restricted = false);
    [[nodiscard]] std::optional<TString> Validate() const;

    // True iff admission control (concurrency, CPU-threshold, cancel-after) applies to this pool.
    bool IsAdmissionRequired() const {
        return ConcurrentQueryLimit != -1
            || DatabaseLoadCpuThreshold >= 0.0
            || QueryCancelAfter;
    }

    // True iff any pool-scoped setting is configured (admission, per-node caps, resource
    // weight, or per-query caps). Kept intentionally inclusive so that a pool with an
    // admin-set field always takes the SkipAdmission path (or normal WMS admission),
    // preserving its actual PoolId for observability and enforcement propagation.
    //
    // Note: Query{Cpu,Memory}LimitPercentPerNode and ResourceWeight are already listed
    // here even though the current planner path only propagates Total{Cpu,Memory}Limit.
    // Including them now avoids a stale-check hazard when their enforcement plumbing lands.
    bool IsWorkloadServiceRequired() const {
        return IsAdmissionRequired()
            || TotalCpuLimitPercentPerNode > 0
            || TotalMemoryLimitPercentPerNode > 0
            || QueryCpuLimitPercentPerNode > 0
            || QueryMemoryLimitPercentPerNode > 0
            || ResourceWeight >= 0;
    }

    i32 ConcurrentQueryLimit = -1;  // -1 = disabled
    i32 QueueSize = -1;  // -1 = disabled
    TDuration QueryCancelAfter = TDuration::Zero();  // 0 = disabled
    TPercent QueryMemoryLimitPercentPerNode = -1;  // RESERVED for future per-query enforcement, currently ignored
    TPercent TotalMemoryLimitPercentPerNode = -1;  // Percent from node memory capacity for all pool queries combined, -1 = disabled
    TPercent DatabaseLoadCpuThreshold = -1;  // -1 = disabled
    TPercent TotalCpuLimitPercentPerNode = -1;  // -1 = disabled
    TPercent QueryCpuLimitPercentPerNode = -1; // -1 = disabled;
    double ResourceWeight = -1;
};

}  // namespace NKikimr::NResourcePool
