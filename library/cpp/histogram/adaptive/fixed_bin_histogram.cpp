#include "fixed_bin_histogram.h"
#include "auto_histogram.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/generic/ymath.h>
#include <util/string/printf.h>

namespace NKiwiAggr {
    TFixedBinHistogram::TFixedBinHistogram(size_t intervals, ui64 id, size_t trainingSetSize)
        : TrainingSetSize(trainingSetSize)
        , IsInitialized(false)
        , IsEmpty(true)
        , Id(id)
        , Sum(0.0)
        , Freqs(0)
        , ReserveFreqs(0)
        , BinRange(0.0)
        , Intervals(intervals)
        , BaseIndex(intervals / 2)
    {
    }

    TFixedBinHistogram::TFixedBinHistogram(const THistogram& histo, size_t defaultIntervals, ui64 defaultId, size_t trainingSetSize)
        : TrainingSetSize(trainingSetSize)
        , IsInitialized(false)
        , IsEmpty(true)
        , Id(defaultId)
        , Sum(0.0)
        , Freqs(0)
        , ReserveFreqs(0)
        , BinRange(0.0)
        , Intervals(defaultIntervals)
        , BaseIndex(defaultIntervals / 2)
    {
        FromProto(histo);
    }

    TFixedBinHistogram::TFixedBinHistogram(IHistogram* histo, size_t defaultIntervals, ui64 defaultId, size_t trainingSetSize)
        : TrainingSetSize(trainingSetSize)
        , IsInitialized(false)
        , IsEmpty(true)
        , Id(defaultId)
        , Sum(0.0)
        , Freqs(0)
        , ReserveFreqs(0)
        , BinRange(0.0)
        , Intervals(defaultIntervals)
        , BaseIndex(defaultIntervals / 2)
    {
        TFixedBinHistogram* fixedBinHisto = dynamic_cast<TFixedBinHistogram*>(histo);
        if (!fixedBinHisto) {
            FromIHistogram(histo);
            return;
        }
        fixedBinHisto->Initialize();
        TrainingSetSize = fixedBinHisto->TrainingSetSize;
        IsInitialized = fixedBinHisto->IsInitialized;
        IsEmpty = fixedBinHisto->IsEmpty;
        Id = fixedBinHisto->Id;
        MinValue = fixedBinHisto->MinValue;
        MaxValue = fixedBinHisto->MaxValue;
        Sum = fixedBinHisto->Sum;
        Freqs.assign(fixedBinHisto->Freqs.begin(), fixedBinHisto->Freqs.end());
        ReserveFreqs.assign(fixedBinHisto->ReserveFreqs.begin(), fixedBinHisto->ReserveFreqs.end());
        ReferencePoint = fixedBinHisto->ReferencePoint;
        BinRange = fixedBinHisto->BinRange;
        Intervals = fixedBinHisto->Intervals;
        FirstUsedBin = fixedBinHisto->FirstUsedBin;
        LastUsedBin = fixedBinHisto->LastUsedBin;
        BaseIndex = fixedBinHisto->BaseIndex;
    }

    void TFixedBinHistogram::Clear() {
        TrainingSet.Destroy();
        IsInitialized = false;
        IsEmpty = true;
        Sum = 0.0;
        Freqs.clear();
        ReserveFreqs.clear();
        BinRange = 0.0;
    }

    void TFixedBinHistogram::Add(const THistoRec& rec) {
        if (!rec.HasId() || rec.GetId() == Id) {
            Add(rec.GetValue(), rec.GetWeight());
        }
    }

    void TFixedBinHistogram::Add(double value, double weight) {
        if (!IsValidFloat(value) || !IsValidFloat(weight)) {
            ythrow yexception() << Sprintf("Histogram id %lu: bad value %f weight %f", Id, value, weight);
        }

        if (weight <= 0.0) {
            return; // all zero-weighted values should be skipped because they don't affect the distribution, negative weights are forbidden
        }

        Sum += weight;

        if (!IsInitialized) {
            if (!TrainingSet) {
                TrainingSet.Reset(new TVector<TWeightedValue>(0));
            }
            TrainingSet->push_back(TWeightedValue(value, weight));
            if (TrainingSet->size() >= TrainingSetSize) {
                Initialize();
            }
            return;
        }

        i32 bin = CalcBin(value);
        if (bin < 0 || bin >= (i32)Freqs.size() || (BinRange == 0.0 && value != ReferencePoint)) {
            Shrink(Min(value, MinValue), Max(value, MaxValue));
            Freqs[CalcBin(value)] += weight;
        } else {
            MinValue = Min(value, MinValue);
            MaxValue = Max(value, MaxValue);
            FirstUsedBin = Min(FirstUsedBin, bin);
            LastUsedBin = Max(LastUsedBin, bin);
            Freqs[bin] += weight;
        }
    }

    void TFixedBinHistogram::Merge(const THistogram& /*histo*/, double /*multiplier*/) {
        ythrow yexception() << "Method is not implemented for TFixedBinHistogram";
    }

    void TFixedBinHistogram::Merge(const TVector<THistogram>& histogramsToMerge) {
        TVector<IHistogramPtr> parsedHistogramsToMerge;
        for (size_t i = 0; i < histogramsToMerge.size(); ++i) {
            parsedHistogramsToMerge.push_back(IHistogramPtr(new TAutoHistogram(histogramsToMerge[i], Intervals, Id)));
        }
        Merge(parsedHistogramsToMerge);
    }

    void TFixedBinHistogram::Merge(TVector<IHistogramPtr> histogramsToMerge) {
        TVector<IHistogramPtr> histogramsToMergeRepacked(0);
        TVector<TFixedBinHistogram*> histograms(0);

        // put current histogram to the vector of histograms to merge and clear self
        if (!Empty()) {
            histogramsToMergeRepacked.push_back(IHistogramPtr(new TFixedBinHistogram(this, Intervals, Id, TrainingSetSize)));
            histograms.push_back(dynamic_cast<TFixedBinHistogram*>(histogramsToMergeRepacked.back().Get()));
        }
        Clear();

        for (size_t i = 0; i < histogramsToMerge.size(); ++i) {
            if (!histogramsToMerge[i] || histogramsToMerge[i]->Empty()) {
                continue;
            }
            TFixedBinHistogram* fixedBinHisto = dynamic_cast<TFixedBinHistogram*>(histogramsToMerge[i].Get());
            if (fixedBinHisto) {
                fixedBinHisto->Initialize();
                histogramsToMergeRepacked.push_back(histogramsToMerge[i]);
            } else {
                histogramsToMergeRepacked.push_back(IHistogramPtr(new TFixedBinHistogram(histogramsToMerge[i].Get(), Intervals, Id, TrainingSetSize))); // Convert histograms that are not of TFixedBinHistogram type
            }
            histograms.push_back(dynamic_cast<TFixedBinHistogram*>(histogramsToMergeRepacked.back().Get()));
        }

        if (histograms.size() == 0) {
            return;
        }

        double minValue = histograms[0]->MinValue;
        double maxValue = histograms[0]->MaxValue;
        Sum = histograms[0]->Sum;
        for (size_t i = 1; i < histograms.size(); ++i) {
            minValue = Min(minValue, histograms[i]->MinValue);
            maxValue = Max(maxValue, histograms[i]->MaxValue);
            Sum += histograms[i]->Sum;
        }
        SetFrame(minValue, maxValue, true);

        if (BinRange == 0.0) {
            Freqs[BaseIndex] = Sum;
            return;
        }

        for (size_t histoIndex = 0; histoIndex < histograms.size(); ++histoIndex) {
            TFixedBinHistogram* histo = histograms[histoIndex];
            for (i32 bin = histo->FirstUsedBin; bin <= histo->LastUsedBin; ++bin) {
                double binStart = histo->BinStart(bin);
                double binEnd = histo->BinEnd(bin);
                double freq = histo->Freqs[bin];
                if (binStart == binEnd) {
                    Freqs[CalcBin(binStart)] += freq;
                    continue;
                }
                size_t firstCross = CalcBin(binStart);
                size_t lastCross = CalcBin(binEnd);
                for (size_t i = firstCross; i <= lastCross; ++i) {
                    double mergedBinStart = BinStart(i);
                    double mergedBinEnd = BinEnd(i);
                    double crossStart = Max(mergedBinStart, binStart);
                    double crossEnd = Min(mergedBinEnd, binEnd);
                    if (binStart == binEnd) {
                    }
                    Freqs[i] += freq * (crossEnd - crossStart) / (binEnd - binStart);
                }
            }
        }
    }

    void TFixedBinHistogram::Multiply(double factor) {
        if (!IsValidFloat(factor) || factor <= 0) {
            ythrow yexception() << "Not valid factor in IHistogram::Multiply(): " << factor;
        }
        if (!IsInitialized) {
            Initialize();
        }
        Sum *= factor;
        for (i32 i = FirstUsedBin; i <= LastUsedBin; ++i) {
            Freqs[i] *= factor;
        }
    }

    void TFixedBinHistogram::FromProto(const THistogram& histo) {
        if (histo.HasType() && histo.GetType() != HT_FIXED_BIN_HISTOGRAM) {
            ythrow yexception() << "Attempt to parse TFixedBinHistogram from THistogram protobuf record of wrong type = " << (ui32)histo.GetType();
        }
        TrainingSet.Destroy();
        IsInitialized = false;
        Sum = 0.0;

        Id = histo.GetId();
        size_t intervals = histo.FreqSize();
        if (intervals == 0) {
            IsEmpty = true;
            return;
        }
        Intervals = intervals;
        TrainingSetSize = Intervals;
        BaseIndex = Intervals / 2;

        if (!IsValidFloat(histo.GetMinValue()) || !IsValidFloat(histo.GetMaxValue()) || !IsValidFloat(histo.GetBinRange())) {
            ythrow yexception() << Sprintf("FromProto in histogram id %lu: skip bad histo with minvalue %f maxvalue %f binrange %f", Id, histo.GetMinValue(), histo.GetMaxValue(), histo.GetBinRange());
        }

        double minValue = histo.GetMinValue();
        double binRange = histo.GetBinRange();
        double maxValue = histo.HasMaxValue() ? histo.GetMaxValue() : minValue + binRange * Intervals;
        SetFrame(minValue, maxValue, true);
        BinRange = binRange;
        for (i32 i = FirstUsedBin; i <= LastUsedBin; ++i) {
            Freqs[i] = histo.GetFreq(i - BaseIndex);
            if (!IsValidFloat(Freqs[i])) {
                ythrow yexception() << Sprintf("FromProto in histogram id %lu: bad value %f", Id, Freqs[i]);
            }
            Sum += Freqs[i];
        }
    }

    void TFixedBinHistogram::ToProto(THistogram& histo) {
        histo.Clear();
        if (!IsInitialized) {
            Initialize();
        }
        histo.SetType(HT_FIXED_BIN_HISTOGRAM);
        histo.SetId(Id);
        if (IsEmpty) {
            return;
        }
        if (FirstUsedBin < (i32)BaseIndex || (LastUsedBin - FirstUsedBin + 1) > (i32)Intervals) {
            Shrink(MinValue, MaxValue);
        }
        histo.SetMinValue(MinValue);
        histo.SetMaxValue(MaxValue);
        histo.SetBinRange(BinRange);
        for (ui32 i = BaseIndex; i < BaseIndex + Intervals; ++i) {
            histo.AddFreq(Freqs[i]);
        }
    }

    void TFixedBinHistogram::SetId(ui64 id) {
        Id = id;
    }

    ui64 TFixedBinHistogram::GetId() {
        return Id;
    }

    bool TFixedBinHistogram::Empty() {
        if (!IsInitialized) {
            Initialize();
        }
        return IsEmpty;
    }

    double TFixedBinHistogram::GetMinValue() {
        if (!IsInitialized) {
            Initialize();
        }
        return MinValue;
    }

    double TFixedBinHistogram::GetMaxValue() {
        if (!IsInitialized) {
            Initialize();
        }
        return MaxValue;
    }

    double TFixedBinHistogram::GetSum() {
        return Sum;
    }

    double TFixedBinHistogram::GetSumInRange(double leftBound, double rightBound) {
        if (!IsInitialized) {
            Initialize();
        }
        if (leftBound > rightBound) {
            return 0.0;
        }
        return GetSumAboveBound(leftBound) + GetSumBelowBound(rightBound) - Sum;
    }

    double TFixedBinHistogram::GetSumAboveBound(double bound) {
        if (!IsInitialized) {
            Initialize();
        }
        if (IsEmpty) {
            return 0.0;
        }
        if (BinRange == 0.0) { // special case - all values added to histogram are the same
            return (bound <= ReferencePoint) ? Sum : 0.0;
        }
        i32 bin = CalcBin(bound);
        if (bin < FirstUsedBin) {
            return Sum;
        }
        if (bin > LastUsedBin) {
            return 0.0;
        }
        double binStart = BinStart(bin);
        double binEnd = BinEnd(bin);
        double result = (bound < binStart) ? Freqs[bin] : Freqs[bin] * (binEnd - bound) / (binEnd - binStart);
        for (i32 i = bin + 1; i <= LastUsedBin; ++i) {
            result += Freqs[i];
        }
        return result;
    }

    double TFixedBinHistogram::GetSumBelowBound(double bound) {
        if (!IsInitialized) {
            Initialize();
        }
        if (IsEmpty) {
            return 0.0;
        }
        if (BinRange == 0.0) { // special case - all values added to histogram are the same
            return (bound > ReferencePoint) ? Sum : 0.0;
        }
        i32 bin = CalcBin(bound);
        if (bin < FirstUsedBin) {
            return 0.0;
        }
        if (bin > LastUsedBin) {
            return Sum;
        }
        double binStart = BinStart(bin);
        double binEnd = BinEnd(bin);
        double result = (bound > binEnd) ? Freqs[bin] : Freqs[bin] * (bound - binStart) / (binEnd - binStart);
        for (i32 i = bin - 1; i >= FirstUsedBin; --i) {
            result += Freqs[i];
        }
        return result;
    }

    double TFixedBinHistogram::CalcUpperBound(double sum) {
        if (!IsInitialized) {
            Initialize();
        }
        if (sum == 0.0) {
            return MinValue;
        }
        if (IsEmpty) {
            return MaxValue;
        }
        i32 currentBin = FirstUsedBin;
        double gatheredSum = 0.0;
        while (gatheredSum < sum && currentBin <= LastUsedBin) {
            gatheredSum += Freqs[currentBin];
            ++currentBin;
        }
        --currentBin;
        if ((gatheredSum <= sum && currentBin == LastUsedBin) || (Freqs[currentBin] == 0)) {
            return MaxValue;
        }
        double binStart = BinStart(currentBin);
        double binEnd = BinEnd(currentBin);
        return binEnd - (binEnd - binStart) * (gatheredSum - sum) / Freqs[currentBin];
    }

    double TFixedBinHistogram::CalcLowerBound(double sum) {
        if (!IsInitialized) {
            Initialize();
        }
        if (sum == 0.0) {
            return MaxValue;
        }
        if (IsEmpty) {
            return MinValue;
        }
        i32 currentBin = LastUsedBin;
        double gatheredSum = 0.0;
        while (gatheredSum < sum && currentBin >= FirstUsedBin) {
            gatheredSum += Freqs[currentBin];
            --currentBin;
        }
        ++currentBin;
        if ((gatheredSum <= sum && currentBin == FirstUsedBin) || (Freqs[currentBin] == 0)) {
            return MinValue;
        }
        double binStart = BinStart(currentBin);
        double binEnd = BinEnd(currentBin);
        return binStart + (binEnd - binStart) * (gatheredSum - sum) / Freqs[currentBin];
    }

    double TFixedBinHistogram::CalcUpperBoundSafe(double sum) {
        if (!Empty()) {
            sum = Max(Freqs[FirstUsedBin], sum);
        }
        return CalcUpperBound(sum);
    }

    double TFixedBinHistogram::CalcLowerBoundSafe(double sum) {
        if (!Empty()) {
            sum = Max(Freqs[LastUsedBin], sum);
        }
        return CalcLowerBound(sum);
    }

    double TFixedBinHistogram::CalcBinRange(double referencePoint, double maxValue) {
        return (maxValue - referencePoint) / ((double)Intervals - 0.02);
    }

    void TFixedBinHistogram::SetFrame(double minValue, double maxValue, bool clear) {
        MinValue = minValue;
        MaxValue = maxValue;
        ReferencePoint = MinValue;
        BinRange = CalcBinRange(ReferencePoint, MaxValue);
        FirstUsedBin = BaseIndex;
        LastUsedBin = (BinRange == 0.0) ? BaseIndex : BaseIndex + Intervals - 1;
        if (clear) {
            Freqs.assign(2 * Intervals, 0.0);
            ReserveFreqs.assign(2 * Intervals, 0.0);
            IsEmpty = false;
            IsInitialized = true;
        }
    }

    void TFixedBinHistogram::FromIHistogram(IHistogram* histo) {
        if (!histo) {
            ythrow yexception() << "Attempt to create TFixedBinFistogram from a NULL pointer";
        }
        Id = histo->GetId();
        if (histo->Empty()) {
            IsInitialized = false;
            IsEmpty = true;
            return;
        }
        SetFrame(histo->GetMinValue(), histo->GetMaxValue(), true);
        Sum = histo->GetSum();
        if (BinRange == 0.0) {
            Freqs[BaseIndex] = Sum;
            return;
        }
        for (i32 i = FirstUsedBin; i <= LastUsedBin; ++i) {
            Freqs[i] = histo->GetSumInRange(BinStart(i), BinEnd(i));
        }
        return;
    }

    void TFixedBinHistogram::Initialize() {
        if (IsInitialized) {
            return;
        }
        if (!TrainingSet || TrainingSet->size() == 0) {
            IsEmpty = true;
            return;
        }
        SetFrame(MinElement(TrainingSet->begin(), TrainingSet->end(), CompareWeightedValue)->first,
                 MaxElement(TrainingSet->begin(), TrainingSet->end(), CompareWeightedValue)->first, true);
        for (TVector<TWeightedValue>::const_iterator it = TrainingSet->begin(); it != TrainingSet->end(); ++it) {
            Freqs[CalcBin(it->first)] += it->second;
        }
        TrainingSet.Destroy();
    }

    i32 TFixedBinHistogram::CalcBin(double value) {
        return (BinRange == 0.0) ? BaseIndex : static_cast<i32>(BaseIndex + (value - ReferencePoint) / BinRange);
    }

    double TFixedBinHistogram::CalcDensity(double value) {
        i32 bin = CalcBin(value);
        if (bin < 0 || bin >= (i32)Freqs.size() || BinRange == 0.0 || GetSum() == 0) {
            return 0.0;
        }
        return Freqs[bin] / GetSum() / BinRange;
    }

    double TFixedBinHistogram::BinStart(i32 i) {
        return Max(ReferencePoint + (i - BaseIndex) * BinRange, MinValue);
    }

    double TFixedBinHistogram::BinEnd(i32 i) {
        return Min(ReferencePoint + (i + 1 - BaseIndex) * BinRange, MaxValue);
    }

    void TFixedBinHistogram::Shrink(double newReferencePoint, double newMaxValue) {
        Y_ABORT_UNLESS(newReferencePoint < newMaxValue, "Invalid Shrink()");
        memset(&(ReserveFreqs[0]), 0, ReserveFreqs.size() * sizeof(double));

        double newBinRange = CalcBinRange(newReferencePoint, newMaxValue);
        for (i32 i = FirstUsedBin; i <= LastUsedBin; ++i) {
            double binStart = BinStart(i);
            double binEnd = BinEnd(i);
            double freq = Freqs[i];
            i32 firstCross = static_cast<i32>(BaseIndex + (binStart - newReferencePoint) / newBinRange);
            i32 lastCross = static_cast<i32>(BaseIndex + (binEnd - newReferencePoint) / newBinRange);
            for (i32 j = firstCross; j <= lastCross; ++j) {
                double newBinStart = newReferencePoint + (j - BaseIndex) * newBinRange;
                double newBinEnd = newReferencePoint + (j + 1 - BaseIndex) * newBinRange;
                double crossStart = Max(newBinStart, binStart);
                double crossEnd = Min(newBinEnd, binEnd);
                ReserveFreqs[j] += (binStart == binEnd) ? freq : freq * (crossEnd - crossStart) / (binEnd - binStart);
            }
        }

        Freqs.swap(ReserveFreqs);
        SetFrame(newReferencePoint, newMaxValue, false);
    }

}
