#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKiwiAggr {
    class THistogram;
    class THistoRec;

    class IHistogram;
    typedef TAtomicSharedPtr<IHistogram> IHistogramPtr;

    class IHistogram {
    public:
        // Supposed constructors:
        //
        // TSomeHistogram(size_t intervals, ui64 id = 0);  // where intervals is some constant that defines histogram accuracy
        // TSomeHistogram(const THistogram& histo);  // histo must be acceptable for TSomeHistogram, for example, only with HT_FIXED_BIN_HISTOGRAM for TFixedBinHistogram
        // TSomeHistogram(IHistogram* histo); // any kind of IHistogram

        virtual ~IHistogram() {
        }

        virtual void Clear() = 0;

        // zero- or negative-weighted values are skipped
        virtual void Add(double value, double weight) = 0;
        virtual void Add(const THistoRec& histoRec) = 0;

        // Merge some other histos into current
        virtual void Merge(const THistogram& histo, double multiplier) = 0;
        virtual void Merge(const TVector<THistogram>& histogramsToMerge) = 0;
        virtual void Merge(TVector<IHistogramPtr> histogramsToMerge) = 0;

        // factor should be greater then zero
        virtual void Multiply(double factor) = 0;

        virtual void FromProto(const THistogram& histo) = 0; // throws exception in case of wrong histogram type of histo
        virtual void ToProto(THistogram& histo) = 0;

        virtual void SetId(ui64 id) = 0;
        virtual ui64 GetId() = 0;
        virtual bool Empty() = 0;
        virtual double GetMinValue() = 0;
        virtual double GetMaxValue() = 0;
        virtual double GetSum() = 0;
        virtual double GetSumInRange(double leftBound, double rightBound) = 0;
        virtual double GetSumAboveBound(double bound) = 0;
        virtual double GetSumBelowBound(double bound) = 0;
        virtual double CalcUpperBound(double sum) = 0;
        virtual double CalcLowerBound(double sum) = 0;
        virtual double CalcUpperBoundSafe(double sum) = 0;
        virtual double CalcLowerBoundSafe(double sum) = 0;
        double GetValueAtPercentile(double percentile) {
            return CalcUpperBound(percentile * GetSum());
        }
        double GetValueAtPercentileSafe(double percentile) {
            return CalcUpperBoundSafe(percentile * GetSum());
        }

        // Histogram implementation is supposed to clear all precomputed values() if Add() is called after PrecomputePartialSums()
        virtual void PrecomputePartialSums() {
        }
    };

}
