#include "adaptive_histogram.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/generic/ymath.h>
#include <util/string/printf.h>

#include <util/system/backtrace.h>

namespace NKiwiAggr {
    TAdaptiveHistogram::TAdaptiveHistogram(size_t intervals, ui64 id, TQualityFunction qualityFunc)
        : Id(id)
        , MinValue(0.0)
        , MaxValue(0.0)
        , Sum(0.0)
        , Intervals(intervals)
        , CalcQuality(qualityFunc)
    {
    }

    TAdaptiveHistogram::TAdaptiveHistogram(const THistogram& histo, size_t defaultIntervals, ui64 defaultId, TQualityFunction qualityFunc)
        : TAdaptiveHistogram(defaultIntervals, defaultId, qualityFunc)
    {
        FromProto(histo);
    }

    TAdaptiveHistogram::TAdaptiveHistogram(IHistogram* histo, size_t defaultIntervals, ui64 defaultId, TQualityFunction qualityFunc)
        : TAdaptiveHistogram(defaultIntervals, defaultId, qualityFunc)
    {
        TAdaptiveHistogram* adaptiveHisto = dynamic_cast<TAdaptiveHistogram*>(histo);
        if (!adaptiveHisto) {
            FromIHistogram(histo);
            return;
        }
        Id = adaptiveHisto->Id;
        MinValue = adaptiveHisto->MinValue;
        MaxValue = adaptiveHisto->MaxValue;
        Sum = adaptiveHisto->Sum;
        Intervals = adaptiveHisto->Intervals;
        Bins = adaptiveHisto->Bins;
        BinsByQuality = adaptiveHisto->BinsByQuality;
        if (CalcQuality == nullptr) {
            CalcQuality = adaptiveHisto->CalcQuality;
        }
    }

    TQualityFunction TAdaptiveHistogram::GetQualityFunc() {
        return CalcQuality;
    }

    void TAdaptiveHistogram::Clear() {
        Sum = 0.0;
        Bins.clear();
        BinsByQuality.clear();
    }

    void TAdaptiveHistogram::Add(const THistoRec& histoRec) {
        if (!histoRec.HasId() || histoRec.GetId() == Id) {
            Add(histoRec.GetValue(), histoRec.GetWeight());
        }
    }

    void TAdaptiveHistogram::Add(double value, double weight) {
        if (!IsValidFloat(value) || !IsValidFloat(weight)) {
            ythrow yexception() << Sprintf("Histogram id %lu: bad value %f weight %f", Id, value, weight);
        }
        TWeightedValue weightedValue(value, weight);
        Add(weightedValue, true);
        PrecomputedBins.clear();
    }

    void TAdaptiveHistogram::Merge(const THistogram& histo, double multiplier) {
        if (!IsValidFloat(histo.GetMinValue()) || !IsValidFloat(histo.GetMaxValue())) {
            fprintf(stderr, "Merging in histogram id %lu: skip bad histo with minvalue %f maxvalue %f\n", Id, histo.GetMinValue(), histo.GetMaxValue());
            return;
        }
        if (histo.FreqSize() == 0) {
            return; // skip empty histos
        }
        if (histo.GetType() == HT_ADAPTIVE_DISTANCE_HISTOGRAM ||
            histo.GetType() == HT_ADAPTIVE_WEIGHT_HISTOGRAM ||
            histo.GetType() == HT_ADAPTIVE_WARD_HISTOGRAM ||
            histo.GetType() == HT_ADAPTIVE_HISTOGRAM)
        {
            Y_ABORT_UNLESS(histo.FreqSize() == histo.PositionSize(), "Corrupted histo");
            for (size_t j = 0; j < histo.FreqSize(); ++j) {
                double value = histo.GetPosition(j);
                double weight = histo.GetFreq(j);
                if (!IsValidFloat(value) || !IsValidFloat(weight)) {
                    fprintf(stderr, "Merging in histogram id %lu: skip bad value %f weight %f\n", Id, value, weight);
                    continue;
                }
                Add(value, weight * multiplier);
            }

            MinValue = Min(MinValue, histo.GetMinValue());
            MaxValue = Max(MaxValue, histo.GetMaxValue());
        } else if (histo.GetType() == HT_FIXED_BIN_HISTOGRAM) {
            double pos = histo.GetMinValue() + histo.GetBinRange() / 2.0;
            for (size_t j = 0; j < histo.FreqSize(); ++j) {
                double weight = histo.GetFreq(j);
                if (!IsValidFloat(pos) || !IsValidFloat(weight)) {
                    fprintf(stderr, "Merging in histogram id %lu: skip bad value %f weight %f\n", Id, pos, weight);
                    pos += histo.GetBinRange();
                    continue;
                }
                Add(pos, weight * multiplier);
                pos += histo.GetBinRange();
            }

            MinValue = Min(MinValue, histo.GetMinValue());
            MaxValue = Max(MaxValue, histo.GetMaxValue());
        } else {
            ythrow yexception() << "Unknown THistogram type";
        }
    }

    void TAdaptiveHistogram::Merge(const TVector<THistogram>& histogramsToMerge) {
        for (size_t i = 0; i < histogramsToMerge.size(); ++i) {
            Merge(histogramsToMerge[i], 1.0);
        }
    }

    void TAdaptiveHistogram::Merge(TVector<IHistogramPtr> histogramsToMerge) {
        TVector<IHistogramPtr> histogramsToMergeRepacked(0);
        TVector<TAdaptiveHistogram*> histograms(0);
        for (size_t i = 0; i < histogramsToMerge.size(); ++i) {
            if (!histogramsToMerge[i] || histogramsToMerge[i]->Empty()) {
                continue;
            }
            TAdaptiveHistogram* adaptiveHisto = dynamic_cast<TAdaptiveHistogram*>(histogramsToMerge[i].Get());
            if (adaptiveHisto) {
                histogramsToMergeRepacked.push_back(histogramsToMerge[i]);
            } else {
                histogramsToMergeRepacked.push_back(IHistogramPtr(new TAdaptiveHistogram(histogramsToMerge[i].Get(), Intervals, Id, CalcQuality))); // Convert histograms that are not of TFixedBinHistogram type
            }
            if (histogramsToMergeRepacked.back()->Empty()) {
                continue;
            }
            histograms.push_back(dynamic_cast<TAdaptiveHistogram*>(histogramsToMergeRepacked.back().Get()));
        }

        if (histograms.size() == 0) {
            return;
        }

        for (size_t histoIndex = 0; histoIndex < histograms.size(); ++histoIndex) {
            TAdaptiveHistogram* histo = histograms[histoIndex];
            for (TPairSet::const_iterator it = histo->Bins.begin(); it != histo->Bins.end(); ++it) {
                Add(*it, true);
            }
        }

        for (size_t i = 0; i < histograms.size(); ++i) {
            MinValue = Min(MinValue, histograms[i]->MinValue);
            MaxValue = Max(MaxValue, histograms[i]->MaxValue);
        }
    }

    void TAdaptiveHistogram::Multiply(double factor) {
        if (!IsValidFloat(factor) || factor <= 0) {
            ythrow yexception() << "Not valid factor in IHistogram::Multiply(): " << factor;
        }
        Sum *= factor;

        TPairSet newBins;
        for (TPairSet::iterator it = Bins.begin(); it != Bins.end(); ++it) {
            newBins.insert(TWeightedValue(it->first, it->second * factor));
        }
        Bins = newBins;

        TPairSet newBinsByQuality;
        for (TPairSet::iterator it = Bins.begin(); it != Bins.end(); ++it) {
            TPairSet::iterator rightBin = it;
            ++rightBin;
            if (rightBin == Bins.end()) {
                break;
            }
            newBinsByQuality.insert(CalcQuality(*it, *rightBin));
        }
        BinsByQuality = newBinsByQuality;
    }

    void TAdaptiveHistogram::FromProto(const THistogram& histo) {
        Y_ABORT_UNLESS(histo.HasType(), "Attempt to parse TAdaptiveHistogram from THistogram protobuf with no Type field set");
        ;
        switch (histo.GetType()) { // check that histogram type could be deduced
            case HT_ADAPTIVE_DISTANCE_HISTOGRAM:
            case HT_ADAPTIVE_WEIGHT_HISTOGRAM:
            case HT_ADAPTIVE_WARD_HISTOGRAM:
                break; // ok
            case HT_ADAPTIVE_HISTOGRAM:
                if (CalcQuality != nullptr)
                    break; // ok
                [[fallthrough]];
            default:       // not ok
                ythrow yexception() << "Attempt to parse TAdaptiveHistogram from THistogram protobuf record of type = " << (ui32)histo.GetType();
        }

        if (histo.FreqSize() != histo.PositionSize()) {
            ythrow yexception() << "Attempt to parse TAdaptiveHistogram from THistogram protobuf record where FreqSize != PositionSize. FreqSize == " << (ui32)histo.FreqSize() << ", PositionSize == " << (ui32)histo.PositionSize();
        }
        if (CalcQuality == nullptr) {
            if (histo.GetType() == HT_ADAPTIVE_DISTANCE_HISTOGRAM) {
                CalcQuality = CalcDistanceQuality;
            } else if (histo.GetType() == HT_ADAPTIVE_WEIGHT_HISTOGRAM) {
                CalcQuality = CalcWeightQuality;
            } else if (histo.GetType() == HT_ADAPTIVE_WARD_HISTOGRAM) {
                CalcQuality = CalcWardQuality;
            } else {
                ythrow yexception() << "Attempt to parse an HT_ADAPTIVE_HISTOGRAM without default quality function";
            }
        }
        Id = histo.GetId();
        Sum = 0.0;
        Intervals = Max(Intervals, histo.FreqSize());
        for (size_t i = 0; i < histo.FreqSize(); ++i) {
            double value = histo.GetPosition(i);
            double weight = histo.GetFreq(i);
            if (!IsValidFloat(value) || !IsValidFloat(weight)) {
                fprintf(stderr, "FromProto in histogram id %lu: skip bad value %f weight %f\n", Id, value, weight);
                continue;
            }
            Add(value, weight);
        }

        if (!IsValidFloat(histo.GetMinValue()) || !IsValidFloat(histo.GetMaxValue())) {
            ythrow yexception() << Sprintf("FromProto in histogram id %lu: skip bad histo with minvalue %f maxvalue %f", Id, histo.GetMinValue(), histo.GetMaxValue());
        }

        MinValue = histo.GetMinValue();
        MaxValue = histo.GetMaxValue();
    }

    void TAdaptiveHistogram::ToProto(THistogram& histo) {
        histo.Clear();
        if (CalcQuality == CalcDistanceQuality) {
            histo.SetType(HT_ADAPTIVE_DISTANCE_HISTOGRAM);
        } else if (CalcQuality == CalcWeightQuality) {
            histo.SetType(HT_ADAPTIVE_WEIGHT_HISTOGRAM);
        } else if (CalcQuality == CalcWardQuality) {
            histo.SetType(HT_ADAPTIVE_WARD_HISTOGRAM);
        } else {
            histo.SetType(HT_ADAPTIVE_HISTOGRAM);
        }
        histo.SetId(Id);
        if (Empty()) {
            return;
        }
        histo.SetMinValue(MinValue);
        histo.SetMaxValue(MaxValue);
        for (TPairSet::const_iterator it = Bins.begin(); it != Bins.end(); ++it) {
            histo.AddFreq(it->second);
            histo.AddPosition(it->first);
        }
    }

    void TAdaptiveHistogram::SetId(ui64 id) {
        Id = id;
    }

    ui64 TAdaptiveHistogram::GetId() {
        return Id;
    }

    bool TAdaptiveHistogram::Empty() {
        return Bins.size() == 0;
    }

    double TAdaptiveHistogram::GetMinValue() {
        return MinValue;
    }

    double TAdaptiveHistogram::GetMaxValue() {
        return MaxValue;
    }

    double TAdaptiveHistogram::GetSum() {
        return Sum;
    }

    double TAdaptiveHistogram::GetSumInRange(double leftBound, double rightBound) {
        if (leftBound > rightBound) {
            return 0.0;
        }
        return GetSumAboveBound(leftBound) + GetSumBelowBound(rightBound) - Sum;
    }

    double TAdaptiveHistogram::GetSumAboveBound(double bound) {
        if (Empty()) {
            return 0.0;
        }
        if (bound < MinValue) {
            return Sum;
        }
        if (bound > MaxValue) {
            return 0.0;
        }

        if (!PrecomputedBins.empty()) {
            return GetSumAboveBoundImpl(
                bound,
                PrecomputedBins,
                LowerBound(PrecomputedBins.begin(), PrecomputedBins.end(), TFastBin{bound, -1.0, 0, 0}),
                [](const auto& it) { return it->SumAbove; });
        } else {
            return GetSumAboveBoundImpl(
                bound,
                Bins,
                Bins.lower_bound(TWeightedValue(bound, -1.0)),
                [this](TPairSet::const_iterator rightBin) {
                    ++rightBin;
                    double sum = 0;
                    for (TPairSet::const_iterator it = rightBin; it != Bins.end(); ++it) {
                        sum += it->second;
                    }
                    return sum;
                });
        }
    }

    double TAdaptiveHistogram::GetSumBelowBound(double bound) {
        if (Empty()) {
            return 0.0;
        }
        if (bound < MinValue) {
            return 0.0;
        }
        if (bound > MaxValue) {
            return Sum;
        }

        if (!PrecomputedBins.empty()) {
            return GetSumBelowBoundImpl(
                bound,
                PrecomputedBins,
                LowerBound(PrecomputedBins.begin(), PrecomputedBins.end(), TFastBin{bound, -1.0, 0, 0}),
                [](const auto& it) { return it->SumBelow; });
        } else {
            return GetSumBelowBoundImpl(
                bound,
                Bins,
                Bins.lower_bound(TWeightedValue(bound, -1.0)),
                [this](TPairSet::const_iterator rightBin) {
                    double sum = 0;
                    for (TPairSet::iterator it = Bins.begin(); it != rightBin; ++it) {
                        sum += it->second;
                    }
                    return sum;
                });
        }
    }

    double TAdaptiveHistogram::CalcUpperBound(double sum) {
        Y_ABORT_UNLESS(sum >= 0, "Sum must be >= 0");
        if (sum == 0.0) {
            return MinValue;
        }
        if (Empty()) {
            return MaxValue;
        }
        TPairSet::iterator current = Bins.begin();
        double gatheredSum = 0.0;
        while (current != Bins.end() && gatheredSum < sum) {
            gatheredSum += current->second;
            ++current;
        }
        --current;
        if (gatheredSum < sum) {
            return MaxValue;
        }
        TWeightedValue left(MinValue, 0.0);
        TWeightedValue right(MaxValue, 0.0);
        if (current != Bins.begin()) {
            TPairSet::iterator leftBin = current;
            --leftBin;
            left = *leftBin;
        }
        {
            TPairSet::iterator rightBin = current;
            ++rightBin;
            if (rightBin != Bins.end()) {
                right = *rightBin;
            }
        }
        double sumToAdd = sum - (gatheredSum - current->second - left.second / 2);
        if (sumToAdd <= ((current->second + left.second) / 2)) {
            return left.first + 2 * sumToAdd * (current->first - left.first) / (current->second + left.second);
        } else {
            sumToAdd -= (current->second + left.second) / 2;
            return current->first + 2 * sumToAdd * (right.first - current->first) / (right.second + current->second);
        }
    }

    double TAdaptiveHistogram::CalcLowerBound(double sum) {
        Y_ABORT_UNLESS(sum >= 0, "Sum must be >= 0");
        if (sum == 0.0) {
            return MaxValue;
        }
        if (Empty()) {
            return MinValue;
        }
        TPairSet::iterator current = Bins.end();
        double gatheredSum = 0.0;
        while (current != Bins.begin() && gatheredSum < sum) {
            --current;
            gatheredSum += current->second;
        }
        if (gatheredSum < sum) {
            return MinValue;
        }
        TWeightedValue left(MinValue, 0.0);
        TWeightedValue right(MaxValue, 0.0);
        if (current != Bins.begin()) {
            TPairSet::iterator leftBin = current;
            --leftBin;
            left = *leftBin;
        }
        {
            TPairSet::iterator rightBin = current;
            ++rightBin;
            if (rightBin != Bins.end()) {
                right = *rightBin;
            }
        }
        double sumToAdd = sum - (gatheredSum - current->second - right.second / 2);
        if (sumToAdd <= ((current->second + right.second) / 2)) {
            return right.first - 2 * sumToAdd * (right.first - current->first) / (current->second + right.second);
        } else {
            sumToAdd -= (current->second + right.second) / 2;
            return current->first - 2 * sumToAdd * (current->first - left.first) / (left.second + current->second);
        }
    }

    double TAdaptiveHistogram::CalcUpperBoundSafe(double sum) {
        if (!Empty()) {
            sum = Max(Bins.begin()->second, sum);
        }
        return CalcUpperBound(sum);
    }

    double TAdaptiveHistogram::CalcLowerBoundSafe(double sum) {
        if (!Empty()) {
            sum = Max(Bins.rbegin()->second, sum);
        }
        return CalcLowerBound(sum);
    }

    void TAdaptiveHistogram::FromIHistogram(IHistogram* histo) {
        if (!histo) {
            ythrow yexception() << "Attempt to create TAdaptiveHistogram from a NULL pointer";
        }
        if (CalcQuality == CalcWardQuality) {
            ythrow yexception() << "Not implemented";
        } else if (CalcQuality != CalcDistanceQuality && CalcQuality != CalcWeightQuality) {
            ythrow yexception() << "Attempt to create TAdaptiveHistogram from a pointer without default CalcQuality";
        }
        Id = histo->GetId();
        if (histo->Empty()) {
            return;
        }
        double sum = histo->GetSum();
        double minValue = histo->GetMinValue();
        double maxValue = histo->GetMaxValue();
        if (minValue == maxValue) {
            Add(minValue, sum);
            return;
        }
        if (CalcQuality == CalcDistanceQuality) {
            double binRange = (maxValue - minValue) / (Intervals);
            for (size_t i = 0; i < Intervals; ++i) {
                Add(minValue + binRange * (i + 0.5), histo->GetSumInRange(minValue + binRange * i, minValue + binRange * (i + 1)));
            }
        } else if (CalcQuality == CalcWeightQuality && sum != 0.0) {
            double slab = sum / Intervals;
            double prevBound = minValue;
            for (size_t i = 0; i < Intervals; ++i) {
                double bound = histo->CalcUpperBound(slab * (i + 1));
                Add((bound + prevBound) / 2, slab);
                prevBound = bound;
            }
        }
        MinValue = minValue;
        MaxValue = maxValue;
    }

    void TAdaptiveHistogram::Add(const TWeightedValue& weightedValue, bool initial) {
        const double& value = weightedValue.first;
        const double& weight = weightedValue.second;
        if (weight <= 0.0) {
            return; // all zero-weighted values should be skipped because they don't affect the distribution, negative weights are forbidden
        }
        if (initial) {
            Sum += weight;
        }
        if (Bins.size() == 0) {
            MinValue = value;
            MaxValue = value;
            Bins.insert(weightedValue);
            return;
        }
        if (value < MinValue) {
            MinValue = value;
        }
        if (value > MaxValue) {
            MaxValue = value;
        }
        TPairSet::iterator rightBin = Bins.lower_bound(TWeightedValue(value, -1.0));
        if (rightBin != Bins.end() && rightBin->first == value) {
            TPairSet::iterator currentBin = rightBin;
            ++rightBin;
            TWeightedValue newBin(value, weight + currentBin->second);
            if (rightBin != Bins.end()) {
                Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*currentBin, *rightBin)) == 1, "Erase failed");
                BinsByQuality.insert(CalcQuality(newBin, *rightBin));
            }
            if (currentBin != Bins.begin()) {
                TPairSet::iterator leftBin = currentBin;
                --leftBin;
                Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*leftBin, *currentBin)) == 1, "Erase failed");
                BinsByQuality.insert(CalcQuality(*leftBin, newBin));
            }
            Bins.erase(currentBin);
            Bins.insert(newBin);
            return;
        }
        if (rightBin == Bins.begin()) {
            BinsByQuality.insert(CalcQuality(weightedValue, *rightBin));
        } else {
            TPairSet::iterator leftBin = rightBin;
            --leftBin;
            if (rightBin == Bins.end()) {
                BinsByQuality.insert(CalcQuality(*leftBin, weightedValue));
            } else {
                Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*leftBin, *rightBin)) == 1, "Erase failed");
                BinsByQuality.insert(CalcQuality(*leftBin, weightedValue));
                BinsByQuality.insert(CalcQuality(weightedValue, *rightBin));
            }
        }
        Bins.insert(weightedValue);
        if (Bins.size() > Intervals) {
            Shrink();
        }
    }

    void TAdaptiveHistogram::Erase(double value) {
        TPairSet::iterator currentBin = Bins.lower_bound(TWeightedValue(value, -1.0));
        Y_ABORT_UNLESS(currentBin != Bins.end() && currentBin->first == value, "Can't find bin that should be erased");
        TPairSet::iterator rightBin = currentBin;
        ++rightBin;
        if (currentBin == Bins.begin()) {
            Y_ABORT_UNLESS(rightBin != Bins.end(), "No right bin for the first bin");
            Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*currentBin, *rightBin)) != 0, "Erase failed");
        } else {
            TPairSet::iterator leftBin = currentBin;
            --leftBin;
            if (rightBin == Bins.end()) {
                Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*leftBin, *currentBin)) != 0, "Erase failed");
            } else {
                Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*leftBin, *currentBin)) != 0, "Erase failed");
                Y_ABORT_UNLESS(BinsByQuality.erase(CalcQuality(*currentBin, *rightBin)) != 0, "Erase failed");
                BinsByQuality.insert(CalcQuality(*leftBin, *rightBin));
            }
        }
        Bins.erase(currentBin);
    }

    void TAdaptiveHistogram::Shrink() {
        TPairSet::iterator worstBin = BinsByQuality.begin();
        Y_ABORT_UNLESS(worstBin != BinsByQuality.end(), "No right bin for the first bin");
        TPairSet::iterator leftBin = Bins.lower_bound(TWeightedValue(worstBin->second, -1.0));
        Y_ABORT_UNLESS(leftBin != Bins.end() && leftBin->first == worstBin->second, "Can't find worst bin");
        TPairSet::iterator rightBin = leftBin;
        ++rightBin;
        Y_ABORT_UNLESS(rightBin != Bins.end(), "Can't find right bin");

        TWeightedValue newBin((leftBin->first * leftBin->second + rightBin->first * rightBin->second) / (leftBin->second + rightBin->second), leftBin->second + rightBin->second);
        if (Bins.size() > 2) {
            Erase(leftBin->first);
            Erase(rightBin->first);
        } else {
            Bins.clear();
            BinsByQuality.clear();
        }
        Add(newBin, false);
    }

    void TAdaptiveHistogram::PrecomputePartialSums() {
        PrecomputedBins.clear();
        PrecomputedBins.reserve(Bins.size());
        double currentSum = 0;
        for (const auto& bin : Bins) {
            PrecomputedBins.emplace_back(bin.first, bin.second, currentSum, Sum - currentSum - bin.second);
            currentSum += bin.second;
        }
    }

    template <typename TBins, typename TGetSumAbove>
    double TAdaptiveHistogram::GetSumAboveBoundImpl(double bound, const TBins& bins, typename TBins::const_iterator rightBin, const TGetSumAbove& getSumAbove) const {
        typename TBins::value_type left(MinValue, 0.0);
        typename TBins::value_type right(MaxValue, 0.0);
        if (rightBin != bins.end()) {
            right = *rightBin;
        }
        if (rightBin != bins.begin()) {
            typename TBins::const_iterator leftBin = rightBin;
            --leftBin;
            left = *leftBin;
        }
        double sum = (right.second / 2) + ((right.first == left.first) ? ((left.second + right.second) / 2) : (((left.second + right.second) / 2) * (right.first - bound) / (right.first - left.first)));
        if (rightBin == bins.end()) {
            return sum;
        }
        sum += getSumAbove(rightBin);
        return sum;
    }

    template <typename TBins, typename TGetSumBelow>
    double TAdaptiveHistogram::GetSumBelowBoundImpl(double bound, const TBins& bins, typename TBins::const_iterator rightBin, const TGetSumBelow& getSumBelow) const {
        typename TBins::value_type left(MinValue, 0.0);
        typename TBins::value_type right(MaxValue, 0.0);
        if (rightBin != bins.end()) {
            right = *rightBin;
        }
        if (rightBin != bins.begin()) {
            typename TBins::const_iterator leftBin = rightBin;
            --leftBin;
            left = *leftBin;
        }
        double sum = (left.second / 2) + ((right.first == left.first) ? ((left.second + right.second) / 2) : (((left.second + right.second) / 2) * (bound - left.first) / (right.first - left.first)));
        if (rightBin == bins.begin()) {
            return sum;
        }
        --rightBin;
        sum += getSumBelow(rightBin);
        return sum;
    }

}
