#pragma once

#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/types.h>

namespace NKikimr::NPQ {

extern const TVector<std::pair<ui64, TString>> FAST_LATENCY_INTERVALS;
extern const TVector<std::pair<ui64, TString>> FAST_LATENCY_MS_INTERVALS;

extern const TVector<std::pair<ui64, TString>> SLOW_LATENCY_INTERVALS;
extern const TVector<std::pair<ui64, TString>> SLOW_LATENCY_MS_INTERVALS;

extern const TVector<std::pair<ui64, TString>> SIZE_INTERVALS;
extern const TVector<std::pair<ui64, TString>> SIZE_KB_INTERVALS;

extern const TTabletPercentileCounter::TRangeDef MLP_LOCKS_INTERVALS[10];
extern const NMonitoring::TBucketBounds MLP_LOCKS_BOUNDS;


}
