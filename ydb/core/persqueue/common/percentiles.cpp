#include "percentiles.h"

#include <util/generic/size_literals.h>

namespace NKikimr::NPQ {

namespace {

// TODO constexpr
NMonitoring::TBucketBounds ToBucketBounds(const TTabletPercentileCounter::TRangeDef* ranges, size_t rangeCount) {
    NMonitoring::TBucketBounds bounds;
    bounds.reserve(rangeCount);
    for (size_t i = 0; i < rangeCount; ++i) {
        bounds.emplace_back(ranges[i].RangeVal);
    }
    return bounds;
}

// TODO constexpr
TVector<std::pair<ui64, TString>> ToIntervals(const TTabletPercentileCounter::TRangeDef* ranges, size_t rangeCount) {
    TVector<std::pair<ui64, TString>> intervals;
    intervals.reserve(rangeCount);
    for (size_t i = 0; i < rangeCount; ++i) {
        intervals.emplace_back(ranges[i].RangeVal, ranges[i].RangeName);
    }
    return intervals;
}

// TODO constexpr
TVector<std::pair<ui64, TString>> ToIntervals(const TTabletPercentileCounter::TRangeDef* ranges, size_t rangeCount, std::string_view postfix) {
    TVector<std::pair<ui64, TString>> intervals;
    intervals.reserve(rangeCount);
    for (size_t i = 0; i < rangeCount; ++i) {
        intervals.emplace_back(ranges[i].RangeVal, TStringBuilder() << ranges[i].RangeName << postfix);
    }
    return intervals;
}

}

const TTabletPercentileCounter::TRangeDef FAST_LATENCY_RANGES[13] = {
    {0, "0"},
    {1, "1"},
    {5, "5"},
    {10, "10"},
    {20, "20"},
    {50, "50"},
    {100, "100"},
    {500, "500"},
    {1000, "1000"},
    {2500, "2500"},
    {5000, "5000"},
    {10000, "10000"},
    {9999999, "9999999"}
};

const TVector<std::pair<ui64, TString>> FAST_LATENCY_INTERVALS = ToIntervals(FAST_LATENCY_RANGES, std::size(FAST_LATENCY_RANGES));
const TVector<std::pair<ui64, TString>> FAST_LATENCY_MS_INTERVALS = ToIntervals(FAST_LATENCY_RANGES, std::size(FAST_LATENCY_RANGES), "ms");
const NMonitoring::TBucketBounds FAST_LATENCY_BOUNDS = ToBucketBounds(FAST_LATENCY_RANGES, std::size(FAST_LATENCY_RANGES));


const TTabletPercentileCounter::TRangeDef SLOW_LATENCY_RANGES[14] = {
    {10, "10"},
    {20, "20"},
    {50, "50"},
    {100, "100"},
    {200, "200"},
    {500, "500"},
    {1000, "1000"},
    {2000, "2000"},
    {5000, "5000"},
    {10'000, "10000"},
    {30'000, "30000"},
    {60'000, "60000"},
    {180'000, "180000"},
    {9'999'999, "9999999"}
};

const TVector<std::pair<ui64, TString>> SLOW_LATENCY_INTERVALS = ToIntervals(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES));
const TVector<std::pair<ui64, TString>> SLOW_LATENCY_MS_INTERVALS = ToIntervals(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES), "ms");
const NMonitoring::TBucketBounds SLOW_LATENCY_BOUNDS = ToBucketBounds(SLOW_LATENCY_RANGES, std::size(SLOW_LATENCY_RANGES));

const TTabletPercentileCounter::TRangeDef SIZE_RANGES[14] = {
    {1024, "1024"},
    {5120, "5120"},
    {10'240, "10240"},
    {20'480, "20480"},
    {51'200, "51200"},
    {102'400, "102400"},
    {204'800, "204800"},
    {524'288, "524288"},
    {1'048'576, "1048576"},
    {2'097'152, "2097152"},
    {5'242'880, "5242880"},
    {10'485'760, "10485760"},
    {67'108'864, "67108864"},
    {999'999'999, "999999999"}
};

const TVector<std::pair<ui64, TString>> SIZE_INTERVALS = ToIntervals(SIZE_RANGES, std::size(SIZE_RANGES));

const TVector<std::pair<ui64, TString>> SIZE_KB_INTERVALS = {
    {1_KB, "1kb"},
    {5_KB, "5kb"},
    {10_KB, "10kb"},
    {20_KB, "20kb"},
    {50_KB, "50kb"},
    {100_KB, "100kb"},
    {200_KB, "200kb"},
    {512_KB, "512kb"},
    {1024_KB, "1024kb"},
    {2048_KB, "2048kb"},
    {5120_KB, "5120kb"},
    {10240_KB, "10240kb"},
    {65536_KB, "65536kb"},
    {999'999'999, "999999999kb"}
};

const TTabletPercentileCounter::TRangeDef MLP_LOCKS_RANGES[10] = {
    {1, "1"},
    {2, "2"},
    {5, "5"},
    {10, "10"},
    {25, "25"},
    {50, "50"},
    {100, "100"},
    {250, "250"},
    {500, "500"},
    {1000, "1000"}
};

const NMonitoring::TBucketBounds MLP_LOCKS_BOUNDS = ToBucketBounds(MLP_LOCKS_RANGES, std::size(MLP_LOCKS_RANGES));

}
