#pragma once

#include "histogram.h"
#include "common.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NKiwiAggr {
    class TAdaptiveHistogram: private TNonCopyable, public IHistogram {
    protected:
        static const size_t DEFAULT_INTERVALS = 100;

    private:
        using TPairSet = TSet<TWeightedValue>;

        struct TFastBin {
            // these names are for compatibility with TWeightedValue
            double first;
            double second;
            // both sums do not include current bin
            double SumBelow;
            double SumAbove;

            TFastBin(double first_, double second_, double sumBelow = 0, double sumAbove = 0)
                : first(first_)
                , second(second_)
                , SumBelow(sumBelow)
                , SumAbove(sumAbove)
            {
            }

            bool operator<(const TFastBin& rhs) const {
                return first < rhs.first;
            }
        };

        ui64 Id;
        double MinValue;
        double MaxValue;
        double Sum;
        size_t Intervals;
        TPairSet Bins;
        TPairSet BinsByQuality;
        TQualityFunction CalcQuality;

        TVector<TFastBin> PrecomputedBins;

    public:
        TAdaptiveHistogram(size_t intervals, ui64 id = 0, TQualityFunction qualityFunc = CalcWeightQuality);
        TAdaptiveHistogram(const THistogram& histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0, TQualityFunction qualityFunc = nullptr);
        TAdaptiveHistogram(IHistogram* histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0, TQualityFunction qualityFunc = CalcWeightQuality);

        ~TAdaptiveHistogram() override {
        }

        TQualityFunction GetQualityFunc();

        void Clear() override;

        void Add(double value, double weight) override;
        void Add(const THistoRec& histoRec) override;

        void Merge(const THistogram& histo, double multiplier) final;
        void Merge(const TVector<THistogram>& histogramsToMerge) final;
        void Merge(TVector<IHistogramPtr> histogramsToMerge) final;

        void Multiply(double factor) final;

        void FromProto(const THistogram& histo) final;
        void ToProto(THistogram& histo) final;

        void SetId(ui64 id) final;
        ui64 GetId() final;
        bool Empty() final;
        double GetMinValue() final;
        double GetMaxValue() final;
        double GetSum() final;
        double GetSumInRange(double leftBound, double rightBound) final;
        double GetSumAboveBound(double bound) final;
        double GetSumBelowBound(double bound) final;
        double CalcUpperBound(double sum) final;
        double CalcLowerBound(double sum) final;
        double CalcUpperBoundSafe(double sum) final;
        double CalcLowerBoundSafe(double sum) final;

        void PrecomputePartialSums() final;

    private:
        void FromIHistogram(IHistogram* histo);
        void Add(const TWeightedValue& weightedValue, bool initial);
        void Erase(double value);
        void Shrink();

        template <typename TBins, typename TGetSumAbove>
        double GetSumAboveBoundImpl(double bound, const TBins& bins, typename TBins::const_iterator rightBin, const TGetSumAbove& getSumAbove) const;

        template <typename TBins, typename TGetSumBelow>
        double GetSumBelowBoundImpl(double bound, const TBins& bins, typename TBins::const_iterator rightBin, const TGetSumBelow& getSumBelow) const;
    };

    template <TQualityFunction QualityFunction>
    class TDefinedAdaptiveHistogram: public TAdaptiveHistogram {
    public:
        TDefinedAdaptiveHistogram(size_t intervals, ui64 id = 0)
            : TAdaptiveHistogram(intervals, id, QualityFunction)
        {
        }

        TDefinedAdaptiveHistogram(const THistogram& histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0)
            : TAdaptiveHistogram(histo, defaultIntervals, defaultId, QualityFunction)
        {
        }

        TDefinedAdaptiveHistogram(IHistogram* histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0)
            : TAdaptiveHistogram(histo, defaultIntervals, defaultId, QualityFunction)
        {
        }

        ~TDefinedAdaptiveHistogram() override {
        }
    };

    typedef TDefinedAdaptiveHistogram<CalcDistanceQuality> TAdaptiveDistanceHistogram;
    typedef TDefinedAdaptiveHistogram<CalcWeightQuality> TAdaptiveWeightHistogram;
    typedef TDefinedAdaptiveHistogram<CalcWardQuality> TAdaptiveWardHistogram;

}
