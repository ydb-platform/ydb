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

// The tooltip every data flow shows above its summary bar: bytes, then the
// optional local share, row count and width, then the optional chunk count and
// mean chunk size. Returns the summary text drawn inside the bar.
//
// localBytes and chunks are 0 for flows that do not report them (egress and
// ingress).
TString FormatDataFlowTooltip(TStringBuilder& tooltip, const TString& label,
    const std::shared_ptr<TSingleMetric>& bytes,
    const std::shared_ptr<TSingleMetric>& rows,
    ui64 localBytes,
    ui64 chunks,
    const std::shared_ptr<TScalarMetric>& chunkSize);

// The data flow's timeline title: its label plus throughput over the window it
// was actually active, label alone when that window is empty.
TString FormatDataFlowRate(const TString& label,
    const std::shared_ptr<TSingleMetric>& bytes,
    const std::shared_ptr<TSingleMetric>& rows);

} // namespace NPlan2Svg
