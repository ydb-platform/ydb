#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <optional>

namespace NKikimrKesus {
    class TAccountingConfig_TMetric;
}

namespace NMonitoring {
    struct TDynamicCounters;
    using TDynamicCounterPtr = TIntrusivePtr<TDynamicCounters>;
}

namespace NKikimr::NKesus {

std::optional<TString> GetMetricCategory(const NKikimrKesus::TAccountingConfig_TMetric& cfg);
bool IsPublicMetric(const NKikimrKesus::TAccountingConfig_TMetric& cfg);
::NMonitoring::TDynamicCounterPtr GetPublicCounters(
    const NKikimrKesus::TAccountingConfig_TMetric& cfg,
    ::NMonitoring::TDynamicCounterPtr counters);

} // NKikimr::NKesus
