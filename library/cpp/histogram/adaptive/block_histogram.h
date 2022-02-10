#pragma once

#include "histogram.h"
#include "common.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <utility>

namespace NKiwiAggr {
    ///////////////////
    // TBlockHistogram
    ///////////////////

    /**
     * Contrary to adaptive histogram, block histogram doesn't rebuild bins
     * after the addition of each point. Instead, it accumulates points and in case the amount
     * of points overflows specified limits, it shrinks all the points at once to produce histogram
     * Indeed, there exist two limits and two shrinkage operations:
     * 1) FastGreedyShrink is fast but coarse. It is used to shrink from upper limit to intermediate limit
     *    (override FastGreedyShrink to set specific behaviour)
     * 2) SlowShrink is slow, but produces finer histogram. It shrinks from the intermediate limit to the
     *    actual number of bins in a manner similar to that in adaptive histogram (set CalcQuality in constuctor)
     * While FastGreedyShrink is used most of the time, SlowShrink is mostly used for histogram finalization
     */
    class TBlockHistogram: private TNonCopyable, public IHistogram {
    protected:
        static const size_t SHRINK_MULTIPLIER = 2;
        static const size_t GREEDY_SHRINK_MULTIPLIER = 4;
        static const size_t DEFAULT_INTERVALS = 100;
        static const size_t DEFAULT_SHRINK_SIZE = DEFAULT_INTERVALS * (SHRINK_MULTIPLIER + GREEDY_SHRINK_MULTIPLIER);

        const EHistogramType Type;
        const TQualityFunction CalcQuality;

        size_t Intervals;
        size_t ShrinkSize;
        size_t PrevSize;

        ui64 Id;

        double Sum;
        double MinValue;
        double MaxValue;

        TVector<TWeightedValue> Bins;

    public:
        TBlockHistogram(EHistogramType type, TQualityFunction calcQuality,
                        size_t intervals, ui64 id = 0, size_t shrinkSize = DEFAULT_SHRINK_SIZE);

        virtual ~TBlockHistogram() {
        }

        virtual void Clear();

        virtual void Add(double value, double weight);
        virtual void Add(const THistoRec& histoRec);

        virtual void Merge(const THistogram& histo, double multiplier);
        virtual void Merge(const TVector<THistogram>& histogramsToMerge);
        virtual void Merge(TVector<IHistogramPtr> histogramsToMerge); // not implemented

        virtual void Multiply(double factor);

        virtual void FromProto(const THistogram& histo);
        virtual void ToProto(THistogram& histo);

        virtual void SetId(ui64 id);
        virtual ui64 GetId();
        virtual bool Empty();
        virtual double GetMinValue();
        virtual double GetMaxValue();
        virtual double GetSum();

        // methods below are not implemented
        virtual double GetSumInRange(double leftBound, double rightBound);
        virtual double GetSumAboveBound(double bound);
        virtual double GetSumBelowBound(double bound);
        virtual double CalcUpperBound(double sum);
        virtual double CalcLowerBound(double sum);
        virtual double CalcUpperBoundSafe(double sum);
        virtual double CalcLowerBoundSafe(double sum);

    private:
        void SortBins();
        void UniquifyBins();
        void CorrectShrinkSize();

        void SortAndShrink(size_t intervals, bool final = false);

        void SlowShrink(size_t intervals);
        virtual void FastGreedyShrink(size_t intervals) = 0;
    };

    /////////////////////////
    // TBlockWeightHistogram
    /////////////////////////

    class TBlockWeightHistogram: public TBlockHistogram {
    public:
        TBlockWeightHistogram(size_t intervals, ui64 id = 0, size_t shrinkSize = DEFAULT_SHRINK_SIZE);

        virtual ~TBlockWeightHistogram() {
        }

    private:
        virtual void FastGreedyShrink(size_t intervals) final;
    };

    ///////////////////////
    // TBlockWardHistogram
    ///////////////////////

    class TBlockWardHistogram: public TBlockHistogram {
    public:
        TBlockWardHistogram(size_t intervals, ui64 id = 0, size_t shrinkSize = DEFAULT_SHRINK_SIZE);

        virtual ~TBlockWardHistogram() {
        }

    private:
        using TCumulative = std::pair<double, double>; // cumulative sum of (weights, weighted centers)
        using TCumulatives = TVector<TCumulative>;

        struct TSplitInfo {
            double profit;

            TCumulatives::const_iterator beg;
            TCumulatives::const_iterator mid;
            TCumulatives::const_iterator end;

            bool operator<(const TSplitInfo& other) const {
                return profit < other.profit;
            }
        };

    private:
        virtual void FastGreedyShrink(size_t intervals) final;

        static bool CalcSplitInfo(const TCumulatives::const_iterator beg,
                                  const TCumulatives::const_iterator end,
                                  TSplitInfo& splitInfo);
    };

}
