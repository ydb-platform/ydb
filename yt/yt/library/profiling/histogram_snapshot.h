#pragma once

#include <util/datetime/base.h>

#include <vector>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct THistogramSnapshot
{
    // When Values.size() == Bounds.size() + 1, Values.back() stores "Inf" bucket.
    std::vector<int> Values;
    std::vector<double> Bounds;

    THistogramSnapshot& operator += (const THistogramSnapshot& other);

    bool operator == (const THistogramSnapshot& other) const;
    bool IsEmpty() const;
};

struct TTimeHistogramSnapshot
    : public THistogramSnapshot
{
    TTimeHistogramSnapshot() = default;

    TTimeHistogramSnapshot(const THistogramSnapshot& hist)
        : THistogramSnapshot(hist)
    { }
};

struct TGaugeHistogramSnapshot
    : public THistogramSnapshot
{
    TGaugeHistogramSnapshot() = default;

    TGaugeHistogramSnapshot(const THistogramSnapshot& hist)
        : THistogramSnapshot(hist)
    { }
};

struct TRateHistogramSnapshot
    : public THistogramSnapshot
{
    TRateHistogramSnapshot() = default;

    TRateHistogramSnapshot(const THistogramSnapshot& hist)
        : THistogramSnapshot(hist)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
