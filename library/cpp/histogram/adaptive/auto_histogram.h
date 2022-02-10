#pragma once

#include "adaptive_histogram.h"
#include "fixed_bin_histogram.h"
#include "histogram.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>

namespace NKiwiAggr {
    class TAutoHistogram: private TNonCopyable, public IHistogram {
    private:
        static const size_t DEFAULT_INTERVALS = 100;

        ui64 Id;
        size_t Intervals;
        THolder<IHistogram> HistogramImpl;

    public:
        TAutoHistogram(size_t intervals, ui64 id = 0) {
            Y_UNUSED(intervals);
            Y_UNUSED(id);
            ythrow yexception() << "Empty constructor is not defined for TAutoHistogram";
        }

        TAutoHistogram(const THistogram& histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0)
            : Id(defaultId)
            , Intervals(defaultIntervals)
        {
            FromProto(histo);
        }

        TAutoHistogram(IHistogram* histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0) {
            Y_UNUSED(histo);
            Y_UNUSED(defaultIntervals);
            Y_UNUSED(defaultId);
            ythrow yexception() << "IHistogram constructor is not defined for TAutoHistogram";
        }

        virtual ~TAutoHistogram() {
        }

        virtual void Clear() {
            HistogramImpl->Clear();
        }

        virtual void Add(double value, double weight) {
            HistogramImpl->Add(value, weight);
        }

        virtual void Add(const THistoRec& histoRec) {
            HistogramImpl->Add(histoRec);
        }

        virtual void Merge(const THistogram& histo, double multiplier) {
            HistogramImpl->Merge(histo, multiplier);
        }

        virtual void Merge(const TVector<THistogram>& histogramsToMerge) {
            HistogramImpl->Merge(histogramsToMerge);
        }

        virtual void Merge(TVector<IHistogramPtr> histogramsToMerge) {
            HistogramImpl->Merge(histogramsToMerge);
        }

        virtual void Multiply(double factor) {
            HistogramImpl->Multiply(factor);
        }

        virtual void FromProto(const THistogram& histo) {
            if (!histo.HasType() || histo.GetType() == HT_FIXED_BIN_HISTOGRAM) {
                HistogramImpl.Reset(new TFixedBinHistogram(histo, Intervals, Id));
            } else if (histo.GetType() == HT_ADAPTIVE_DISTANCE_HISTOGRAM) {
                HistogramImpl.Reset(new TAdaptiveDistanceHistogram(histo, Intervals, Id));
            } else if (histo.GetType() == HT_ADAPTIVE_WEIGHT_HISTOGRAM) {
                HistogramImpl.Reset(new TAdaptiveWeightHistogram(histo, Intervals, Id));
            } else if (histo.GetType() == HT_ADAPTIVE_WARD_HISTOGRAM) {
                HistogramImpl.Reset(new TAdaptiveWardHistogram(histo, Intervals, Id));
            } else {
                ythrow yexception() << "Can't parse TAutoHistogram from a THistogram of type " << (ui32)histo.GetType();
            }
        }

        virtual void ToProto(THistogram& histo) {
            HistogramImpl->ToProto(histo);
        }

        virtual void SetId(ui64 id) {
            HistogramImpl->SetId(id);
        }

        virtual ui64 GetId() {
            return HistogramImpl->GetId();
        }

        virtual bool Empty() {
            return HistogramImpl->Empty();
        }

        virtual double GetMinValue() {
            return HistogramImpl->GetMinValue();
        }

        virtual double GetMaxValue() {
            return HistogramImpl->GetMaxValue();
        }

        virtual double GetSum() {
            return HistogramImpl->GetSum();
        }

        virtual double GetSumInRange(double leftBound, double rightBound) {
            return HistogramImpl->GetSumInRange(leftBound, rightBound);
        }

        virtual double GetSumAboveBound(double bound) {
            return HistogramImpl->GetSumAboveBound(bound);
        }

        virtual double GetSumBelowBound(double bound) {
            return HistogramImpl->GetSumBelowBound(bound);
        }

        virtual double CalcUpperBound(double sum) {
            return HistogramImpl->CalcUpperBound(sum);
        }

        virtual double CalcLowerBound(double sum) {
            return HistogramImpl->CalcLowerBound(sum);
        }

        virtual double CalcUpperBoundSafe(double sum) {
            return HistogramImpl->CalcUpperBoundSafe(sum);
        }

        virtual double CalcLowerBoundSafe(double sum) {
            return HistogramImpl->CalcLowerBoundSafe(sum);
        }

        virtual void PrecomputePartialSums() {
            return HistogramImpl->PrecomputePartialSums();
        }
    };

}
