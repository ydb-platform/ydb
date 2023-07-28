#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/core/misc/phoenix.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const int DefaultHistogramViewBuckets = 100;

////////////////////////////////////////////////////////////////////////////////

struct THistogramView
{
    i64 Min = 0;
    i64 Max = 0;
    std::vector<i64> Count;
};

struct IHistogram
    : public virtual NPhoenix::IPersistent
{
    virtual void AddValue(i64 value, i64 count = 1) = 0;
    virtual void RemoveValue(i64 value, i64 count = 1) = 0;
    virtual void BuildHistogramView() = 0;
    virtual THistogramView GetHistogramView() const = 0;
};

std::unique_ptr<IHistogram> CreateHistogram(int defaultBuckets = DefaultHistogramViewBuckets);
void Serialize(const IHistogram& histogram, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct THistogramQuartiles
{
    i64 Q25 = 0;
    i64 Q50 = 0;
    i64 Q75 = 0;
};

THistogramQuartiles ComputeHistogramQuartiles(const THistogramView& histogramView);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////
