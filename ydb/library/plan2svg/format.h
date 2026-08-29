#pragma once

#include "metrics.h"

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NPlan2Svg {

TString FormatDurationMs(ui64 durationMs);
TString FormatDurationUs(ui64 durationUs);
TString FormatUsage(ui64 usec);
TString FormatIntegerValue(ui64 i, ui32 scale = 1000, const TString& suffix = "");
TString FormatBytes(ui64 bytes);
TString FormatInteger(ui64 bytes);
TString FormatTimeMs(ui64 time);
TString FormatTimeAgg(const TAggregation& agg);
TString FormatMCpu(ui64 mCpu);

// Appends the tooltip text to the first argument and also returns it.
TString FormatTooltip(TStringBuilder& builder, const TString& prefix, TSingleMetric* metric, TString (*format)(ui64), ui64 total = 0);
TString FormatTooltip(TString& tooltip, const TString& prefix, TSingleMetric* metric, TString (*format)(ui64), ui64 total = 0);

} // namespace NPlan2Svg
