#pragma once

#include "settings_common.h"

#include <util/datetime/base.h>


namespace NKikimr::NResourcePool {

inline constexpr char DEFAULT_POOL_ID[] = "default";

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

    bool operator==(const TPoolSettings& other) const = default;
    std::unordered_map<TString, TProperty> GetPropertiesMap(bool restricted = false);

    i32 ConcurrentQueryLimit = -1;  // -1 = disabled
    i32 QueueSize = -1;  // -1 = disabled
    TDuration QueryCancelAfter = TDuration::Zero();  // 0 = disabled
    TPercent QueryMemoryLimitPercentPerNode = -1;  // Percent from node memory capacity, -1 = disabled
    TPercent DatabaseLoadCpuThreshold = -1;  // -1 = disabled
    TPercent TotalCpuLimitPercentPerNode = -1;  // -1 = disabled
};

}  // namespace NKikimr::NResourcePool
