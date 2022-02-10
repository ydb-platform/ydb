#pragma once

#include "histogram.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <utility>

namespace NKiwiAggr {
    class TFixedBinHistogram: private TNonCopyable, public IHistogram {
    private:
        static const size_t DEFAULT_TRAINING_SET_SIZE = 10000;
        static const size_t DEFAULT_INTERVALS = 100;

        typedef std::pair<double, double> TWeightedValue; // value, weight
        THolder<TVector<TWeightedValue>> TrainingSet;
        size_t TrainingSetSize;

        bool IsInitialized;
        bool IsEmpty;

        ui64 Id;
        double MinValue;
        double MaxValue;
        double Sum;

        TVector<double> Freqs;
        TVector<double> ReserveFreqs;
        double ReferencePoint;
        double BinRange;
        size_t Intervals;
        i32 FirstUsedBin;
        i32 LastUsedBin;
        i32 BaseIndex;

    public:
        TFixedBinHistogram(size_t intervals, ui64 id = 0, size_t trainingSetSize = DEFAULT_TRAINING_SET_SIZE);
        TFixedBinHistogram(const THistogram& histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0, size_t trainingSetSize = DEFAULT_TRAINING_SET_SIZE);
        TFixedBinHistogram(IHistogram* histo, size_t defaultIntervals = DEFAULT_INTERVALS, ui64 defaultId = 0, size_t trainingSetSize = DEFAULT_TRAINING_SET_SIZE);

        virtual ~TFixedBinHistogram() {
        }

        virtual void Clear();

        virtual void Add(double value, double weight);
        virtual void Add(const THistoRec& histoRec);

        virtual void Merge(const THistogram& histo, double multiplier);
        virtual void Merge(const TVector<THistogram>& histogramsToMerge);
        virtual void Merge(TVector<IHistogramPtr> histogramsToMerge);

        virtual void Multiply(double factor);

        virtual void FromProto(const THistogram& histo);
        virtual void ToProto(THistogram& histo);

        virtual void SetId(ui64 id);
        virtual ui64 GetId();
        virtual bool Empty();
        virtual double GetMinValue();
        virtual double GetMaxValue();
        virtual double GetSum();
        virtual double GetSumInRange(double leftBound, double rightBound);
        virtual double GetSumAboveBound(double bound);
        virtual double GetSumBelowBound(double bound);
        virtual double CalcUpperBound(double sum);
        virtual double CalcLowerBound(double sum);
        virtual double CalcUpperBoundSafe(double sum);
        virtual double CalcLowerBoundSafe(double sum);

        double CalcDensity(double value);

    private:
        double CalcBinRange(double referencePoint, double maxValue);
        void SetFrame(double minValue, double maxValue, bool clear);
        void FromIHistogram(IHistogram* histo);
        void Initialize();
        i32 CalcBin(double value);
        double BinStart(i32 i);
        double BinEnd(i32 i);
        void Shrink(double newMinValue, double newMaxValue);

        static bool CompareWeightedValue(const TWeightedValue& left, const TWeightedValue& right) {
            return left.first < right.first;
        }
    };

}
