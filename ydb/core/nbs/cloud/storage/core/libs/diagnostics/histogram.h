#pragma once

//#include "public.h"

#include <library/cpp/histogram/hdr/histogram.h>

#include <util/datetime/base.h>
#include <util/system/sanitizers.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class THistogramBase
{
private:
    NHdr::THistogram Hist;

public:
    THistogramBase(i64 min, i64 max, i32 precision)
        : Hist(min, max, precision)
    {}

    bool RecordValue(i64 value)
    {
        if (NSan::TSanIsOn()) {
            return false;
        }
        return Hist.RecordValue(value);
    }

    bool RecordValues(i64 value, i64 count)
    {
        if (NSan::TSanIsOn()) {
            return false;
        }
        return Hist.RecordValues(value, count);
    }

    i64 GetValueAtPercentile(double percentile) const
    {
        if (NSan::TSanIsOn()) {
            return 0;
        }
        return Hist.GetValueAtPercentile(percentile);
    }

    i64 GetMin() const
    {
        if (NSan::TSanIsOn()) {
            return 0;
        }
        return Hist.GetMin();
    }

    i64 GetMax() const
    {
        if (NSan::TSanIsOn()) {
            return 0;
        }
        return Hist.GetMax();
    }

    double GetMean() const
    {
        if (NSan::TSanIsOn() || !Hist.GetTotalCount()) {
            return 0;
        }
        return Hist.GetMean();
    }

    double GetStdDeviation() const
    {
        if (NSan::TSanIsOn() || !Hist.GetTotalCount()) {
            return 0;
        }
        return Hist.GetStdDeviation();
    }

    void Add(THistogramBase& other)
    {
        if (NSan::TSanIsOn()) {
            return;
        }
        Hist.Add(other.Hist);
    }

    void Reset()
    {
        if (NSan::TSanIsOn()) {
            return;
        }
        Hist.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLatencyHistogram
    : public THistogramBase
{
public:
    TLatencyHistogram(TDuration max = TDuration::Seconds(10), int precision = 3)
        : THistogramBase(1, max.MicroSeconds(), precision)
    {}

    bool RecordValue(TDuration value)
    {
        return THistogramBase::RecordValue(value.MicroSeconds());
    }

    bool RecordValues(TDuration value, i64 count)
    {
        return THistogramBase::RecordValues(value.MicroSeconds(), count);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSizeHistogram
    : public THistogramBase
{
public:
    TSizeHistogram(ui32 max = 64*1024*1024, int precision = 3)
        : THistogramBase(1, max, precision)
    {}
};

}   // namespace NCloud
