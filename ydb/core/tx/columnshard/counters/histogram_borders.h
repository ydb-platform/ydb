#pragma once

#include <library/cpp/monlib/metrics/histogram_collector.h>
#include <util/generic/fwd.h>
#include <util/system/types.h>

#include <map>
#include <set>

namespace NKikimr::NColumnShard {
struct THistorgamBorders {
    static const std::map<i64, TString> BlobSizeBorders;

    static const std::map<i64, TString> BytesBorders;

    static const std::map<i64, TString> TimeBordersMicroseconds;

    static const std::set<i64> PortionRecordBorders;

    static const std::set<i64> PortionWidthBorders;

    /// Histograms for the monitoring system's percentiles (2026-10-28). It interpolates a percentile linearly inside
    /// a bucket, so power-of-two buckets put every task of 1000 portions at a median of 768 and every 60 MiB task at
    /// 48 MiB. These bounds are 1, 1.25 and 1.5 times each power of two, three buckets per doubling with round labels;
    /// a value is read within 25% and, for the counts, exactly up to 8 (every integer is a bound there).
    static NMonitoring::IHistogramCollectorPtr TaskCountHistogram();   // 1 .. 8, 10, 12, 16 .. 16384, +inf
    static NMonitoring::IHistogramCollectorPtr TaskBytesHistogram();   // 1 MiB .. 48 GiB, +inf
};
}   // namespace NKikimr::NColumnShard
