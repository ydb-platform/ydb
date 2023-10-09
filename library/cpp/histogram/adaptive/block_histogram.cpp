#include "block_histogram.h"

#include <library/cpp/histogram/adaptive/protos/histo.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/generic/intrlist.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/ymath.h>
#include <util/string/printf.h>

namespace {
    struct TEmpty {
    };

    class TSmartHeap {
    private:
        TVector<ui32> A;
        TVector<ui32> Pos;
        const TVector<double>& Weights;

    public:
        TSmartHeap(const TVector<double>& weights)
            : A(weights.size())
            , Pos(weights.size())
            , Weights(weights)
        {
            for (ui32 i = 0; i < weights.size(); ++i) {
                A[i] = i;
                Pos[i] = i;
            }
            for (ui32 i = weights.size() / 2; i > 0; --i) {
                Down(i - 1);
            }
        }

        ui32 IdOfMin() {
            return A[0];
        }

        void Pop() {
            A[0] = A.back();
            Pos[A[0]] = 0;
            A.pop_back();
            Down(0);
        }

        void DownElement(ui32 id) {
            Down(Pos[id]);
        }

    private:
        void SwapPositions(ui32 x, ui32 y) {
            std::swap(A[x], A[y]);
            Pos[A[x]] = x;
            Pos[A[y]] = y;
        }

        void Down(ui32 pos) {
            while (1) {
                ui32 left = pos * 2 + 1;
                ui32 right = pos * 2 + 2;
                ui32 min = pos;
                if (left < A.size() && Weights[A[min]] > Weights[A[left]])
                    min = left;
                if (right < A.size() && Weights[A[min]] > Weights[A[right]])
                    min = right;
                if (pos == min)
                    break;
                SwapPositions(min, pos);
                pos = min;
            }
        }
    };

}

namespace NKiwiAggr {
    ///////////////////
    // TBlockHistogram
    ///////////////////

    TBlockHistogram::TBlockHistogram(EHistogramType type, TQualityFunction calcQuality,
                                     size_t intervals, ui64 id, size_t shrinkSize)
        : Type(type)
        , CalcQuality(calcQuality)
        , Intervals(intervals)
        , ShrinkSize(shrinkSize)
        , PrevSize(0)
        , Id(id)
        , Sum(0)
        , MinValue(0)
        , MaxValue(0)
    {
        CorrectShrinkSize();
    }

    void TBlockHistogram::Clear() {
        PrevSize = 0;
        Sum = 0.0;
        MinValue = 0.0;
        MaxValue = 0.0;
        Bins.clear();
    }

    void TBlockHistogram::Add(const THistoRec& rec) {
        if (!rec.HasId() || rec.GetId() == Id) {
            Add(rec.GetValue(), rec.GetWeight());
        }
    }

    void TBlockHistogram::Add(double value, double weight) {
        if (!IsValidFloat(value) || !IsValidFloat(weight)) {
            ythrow yexception() << Sprintf("Histogram id %lu: bad value %f weight %f", Id, value, weight);
        }

        if (weight <= 0.0) {
            return; // all zero-weighted values should be skipped because they don't affect the distribution, negative weights are forbidden
        }

        if (Bins.empty()) {
            MinValue = value;
            MaxValue = value;
        } else {
            MinValue = Min(MinValue, value);
            MaxValue = Max(MaxValue, value);
        }

        Sum += weight;

        if (Bins.size() > ShrinkSize) {
            SortAndShrink(Intervals * SHRINK_MULTIPLIER);
        }

        Bins.push_back(TWeightedValue(value, weight));
    }

    void TBlockHistogram::Merge(const THistogram& histo, double multiplier) {
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

    void TBlockHistogram::Merge(const TVector<THistogram>& histogramsToMerge) {
        for (size_t i = 0; i < histogramsToMerge.size(); ++i) {
            Merge(histogramsToMerge[i], 1.0);
        }
    }

    void TBlockHistogram::Merge(TVector<IHistogramPtr> histogramsToMerge) {
        Y_UNUSED(histogramsToMerge);
        ythrow yexception() << "IHistogram::Merge(TVector<IHistogramPtr>) is not defined for TBlockHistogram";
    }

    void TBlockHistogram::Multiply(double factor) {
        if (!IsValidFloat(factor) || factor <= 0) {
            ythrow yexception() << "Not valid factor in IHistogram::Multiply(): " << factor;
        }
        Sum *= factor;
        for (TVector<TWeightedValue>::iterator it = Bins.begin(); it != Bins.end(); ++it) {
            it->second *= factor;
        }
    }

    void TBlockHistogram::FromProto(const THistogram& histo) {
        Y_ABORT_UNLESS(histo.HasType(), "Attempt to parse TBlockHistogram from THistogram protobuf with no Type field set");
        ;
        switch (histo.GetType()) { // check that histogram type is correct
            case HT_ADAPTIVE_DISTANCE_HISTOGRAM:
            case HT_ADAPTIVE_WEIGHT_HISTOGRAM:
            case HT_ADAPTIVE_WARD_HISTOGRAM:
            case HT_ADAPTIVE_HISTOGRAM:
                break; // ok
            default:   // not ok
                ythrow yexception() << "Attempt to parse TBlockHistogram from THistogram protobuf record of type = " << (ui32)histo.GetType();
        }

        if (histo.FreqSize() != histo.PositionSize()) {
            ythrow yexception() << "Attempt to parse TBlockHistogram from THistogram protobuf record where FreqSize != PositionSize. FreqSize == " << (ui32)histo.FreqSize() << ", PositionSize == " << (ui32)histo.PositionSize();
        }
        Id = histo.GetId();
        Sum = 0;
        Intervals = Max(Intervals, histo.FreqSize());
        CorrectShrinkSize();
        Bins.resize(histo.FreqSize());
        PrevSize = Bins.size();
        for (size_t i = 0; i < histo.FreqSize(); ++i) {
            double value = histo.GetPosition(i);
            double weight = histo.GetFreq(i);
            if (!IsValidFloat(value) || !IsValidFloat(weight)) {
                fprintf(stderr, "FromProto in histogram id %lu: skip bad value %f weight %f\n", Id, value, weight);
                continue;
            }
            Bins[i].first = value;
            Bins[i].second = weight;
            Sum += Bins[i].second;
        }

        if (!IsValidFloat(histo.GetMinValue()) || !IsValidFloat(histo.GetMaxValue())) {
            ythrow yexception() << Sprintf("FromProto in histogram id %lu: skip bad histo with minvalue %f maxvalue %f", Id, histo.GetMinValue(), histo.GetMaxValue());
        }
        MinValue = histo.GetMinValue();
        MaxValue = histo.GetMaxValue();
    }

    void TBlockHistogram::ToProto(THistogram& histo) {
        histo.Clear();
        histo.SetType(Type);
        histo.SetId(Id);
        if (Empty()) {
            return;
        }

        SortAndShrink(Intervals, true);
        histo.SetMinValue(MinValue);
        histo.SetMaxValue(MaxValue);
        for (TVector<TWeightedValue>::const_iterator it = Bins.begin(); it != Bins.end(); ++it) {
            histo.AddFreq(it->second);
            histo.AddPosition(it->first);
        }
    }

    void TBlockHistogram::SetId(ui64 id) {
        Id = id;
    }

    ui64 TBlockHistogram::GetId() {
        return Id;
    }

    bool TBlockHistogram::Empty() {
        return Bins.empty();
    }

    double TBlockHistogram::GetMinValue() {
        return MinValue;
    }

    double TBlockHistogram::GetMaxValue() {
        return MaxValue;
    }

    double TBlockHistogram::GetSum() {
        return Sum;
    }

    void TBlockHistogram::SortAndShrink(size_t intervals, bool final) {
        Y_ABORT_UNLESS(intervals > 0);

        if (Bins.size() <= intervals) {
            return;
        }

        if (Bins.size() >= Intervals * GREEDY_SHRINK_MULTIPLIER) {
            SortBins();
            UniquifyBins();
            FastGreedyShrink(intervals);

            if (final) {
                SlowShrink(intervals);
            }

        } else {
            SortBins();
            UniquifyBins();
            SlowShrink(intervals);
        }
    }

    void TBlockHistogram::SortBins() {
        Sort(Bins.begin() + PrevSize, Bins.end());
        if (PrevSize != 0) {
            TVector<TWeightedValue> temp(Bins.begin(), Bins.begin() + PrevSize);
            std::merge(temp.begin(), temp.end(), Bins.begin() + PrevSize, Bins.end(), Bins.begin());
        }
    }

    void TBlockHistogram::UniquifyBins() {
        if (Bins.empty())
            return;

        auto it1 = Bins.begin();
        auto it2 = Bins.begin();
        while (++it2 != Bins.end()) {
            if (it1->first == it2->first) {
                it1->second += it2->second;
            } else {
                *(++it1) = *it2;
            }
        }

        Bins.erase(++it1, Bins.end());
    }

    void TBlockHistogram::CorrectShrinkSize() {
        ShrinkSize = Max(ShrinkSize, Intervals * (SHRINK_MULTIPLIER + GREEDY_SHRINK_MULTIPLIER));
    }

    void TBlockHistogram::SlowShrink(size_t intervals) {
        {
            size_t pos = 0;
            for (size_t i = 1; i < Bins.size(); ++i)
                if (Bins[i].first - Bins[pos].first < 1e-9) {
                    Bins[pos].second += Bins[i].second;
                } else {
                    ++pos;
                    Bins[pos] = Bins[i];
                }
            Bins.resize(pos + 1);
            PrevSize = pos + 1;
        }

        if (Bins.size() <= intervals) {
            return;
        }

        typedef TIntrusiveListItem<TEmpty> TListItem;

        ui32 n = Bins.size() - 1;
        const ui32 end = (ui32)Bins.size();

        TArrayHolder<TListItem> listElementsHolder(new TListItem[end + 1]);
        TListItem* const bins = listElementsHolder.Get();

        for (ui32 i = 1; i <= end; ++i) {
            bins[i].LinkAfter(&bins[i - 1]);
        }

        TVector<double> pairWeights(n);

        for (ui32 i = 0; i < n; ++i) {
            pairWeights[i] = CalcQuality(Bins[i], Bins[i + 1]).first;
        }

        TSmartHeap heap(pairWeights);

        while (n + 1 > intervals) {
            ui32 b = heap.IdOfMin();
            heap.Pop();

            ui32 a = (ui32)(bins[b].Prev() - bins);
            ui32 c = (ui32)(bins[b].Next() - bins);
            ui32 d = (ui32)(bins[b].Next()->Next() - bins);
            Y_ABORT_UNLESS(Bins[c].second != -1);

            double mass = Bins[b].second + Bins[c].second;
            Bins[c].first = (Bins[b].first * Bins[b].second + Bins[c].first * Bins[c].second) / mass;
            Bins[c].second = mass;

            bins[b].Unlink();
            Bins[b].second = -1;

            if (a != end) {
                pairWeights[a] = CalcQuality(Bins[a], Bins[c]).first;
                heap.DownElement(a);
            }

            if (d != end && c + 1 != Bins.size()) {
                pairWeights[c] = CalcQuality(Bins[c], Bins[d]).first;
                heap.DownElement(c);
            }

            --n;
        }

        size_t pos = 0;
        for (TListItem* it = bins[end].Next(); it != &bins[end]; it = it->Next()) {
            Bins[pos++] = Bins[it - bins];
        }

        Bins.resize(pos);
        PrevSize = pos;
        Y_ABORT_UNLESS(pos == intervals);
    }

    double TBlockHistogram::GetSumInRange(double leftBound, double rightBound) {
        Y_UNUSED(leftBound);
        Y_UNUSED(rightBound);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    double TBlockHistogram::GetSumAboveBound(double bound) {
        Y_UNUSED(bound);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    double TBlockHistogram::GetSumBelowBound(double bound) {
        Y_UNUSED(bound);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    double TBlockHistogram::CalcUpperBound(double sum) {
        Y_UNUSED(sum);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    double TBlockHistogram::CalcLowerBound(double sum) {
        Y_UNUSED(sum);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    double TBlockHistogram::CalcUpperBoundSafe(double sum) {
        Y_UNUSED(sum);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    double TBlockHistogram::CalcLowerBoundSafe(double sum) {
        Y_UNUSED(sum);
        ythrow yexception() << "Method is not implemented for TBlockHistogram";
        return 0;
    }

    /////////////////////////
    // TBlockWeightHistogram
    /////////////////////////

    TBlockWeightHistogram::TBlockWeightHistogram(size_t intervals, ui64 id, size_t shrinkSize)
        : TBlockHistogram(HT_ADAPTIVE_WEIGHT_HISTOGRAM, CalcWeightQuality, intervals, id, shrinkSize)
    {
    }

    void TBlockWeightHistogram::FastGreedyShrink(size_t intervals) {
        if (Bins.size() <= intervals)
            return;

        double slab = Sum / intervals;

        size_t i = 0;
        size_t pos = 0;
        while (i < Bins.size()) {
            double curW = Bins[i].second;
            double curMul = Bins[i].first * Bins[i].second;
            ++i;
            while (i < Bins.size() && curW + Bins[i].second <= slab && pos + Bins.size() - i >= intervals) {
                curW += Bins[i].second;
                curMul += Bins[i].first * Bins[i].second;
                ++i;
            }
            Bins[pos++] = TWeightedValue(curMul / curW, curW);
        }

        Bins.resize(pos);
        PrevSize = pos;
    }

    ///////////////////////
    // TBlockWardHistogram
    ///////////////////////

    TBlockWardHistogram::TBlockWardHistogram(size_t intervals, ui64 id, size_t shrinkSize)
        : TBlockHistogram(HT_ADAPTIVE_WARD_HISTOGRAM, CalcWardQuality, intervals, id, shrinkSize)
    {
    }

    bool TBlockWardHistogram::CalcSplitInfo(
        const TCumulatives::const_iterator beg,
        const TCumulatives::const_iterator end, // (!) points to the final element
        TSplitInfo& splitInfo                   // out
    ) {
        if (end - beg < 2) {
            return false;
        }

        TCumulatives::const_iterator mid = LowerBound(beg, end + 1, TCumulative{(beg->first + end->first) / 2, 0.});

        if (mid == beg) {
            mid++;
        } else if (mid == end) {
            mid--;
        }

        // derived from Ward's minimum variance criterion
        double profit = 0.0;
        profit += (mid->second - beg->second) * (mid->second - beg->second) / (mid->first - beg->first);
        profit += (end->second - mid->second) * (end->second - mid->second) / (end->first - mid->first);
        profit -= (end->second - beg->second) * (end->second - beg->second) / (end->first - beg->first);

        splitInfo = {profit, beg, mid, end};

        return true;
    }

    void TBlockWardHistogram::FastGreedyShrink(size_t intervals) {
        Y_ABORT_UNLESS(intervals > 0);

        if (Bins.size() <= intervals) {
            return;
        }

        // fill cumulative sums
        // sum at index i equals to the sum of all values before i
        // sum at index i+1 equals to the sum of all values before i with the value at i added
        TCumulatives cumulatives;
        cumulatives.reserve(Bins.size() + 1);

        TCumulative cumulative = {0., 0.};
        cumulatives.push_back(cumulative);
        for (size_t i = 0; i < Bins.size(); i++) {
            cumulative.first += Bins[i].second;
            cumulative.second += Bins[i].second * Bins[i].first;
            cumulatives.push_back(cumulative);
        }

        TVector<TCumulatives::const_iterator> splits;
        splits.reserve(intervals + 1);
        splits.push_back(cumulatives.begin());
        splits.push_back(cumulatives.end() - 1);

        TPriorityQueue<TSplitInfo> candidates;

        // explicitly add first split
        TSplitInfo newSplitInfo;
        if (CalcSplitInfo(cumulatives.begin(), cumulatives.end() - 1, newSplitInfo)) {
            candidates.push(newSplitInfo);
        }

        // recursively split until done
        for (size_t split = 0; split < intervals - 1 && !candidates.empty(); split++) {
            TSplitInfo curSplitInfo = candidates.top();
            candidates.pop();

            splits.push_back(curSplitInfo.mid);

            if (CalcSplitInfo(curSplitInfo.beg, curSplitInfo.mid, newSplitInfo)) {
                candidates.push(newSplitInfo);
            }
            if (CalcSplitInfo(curSplitInfo.mid, curSplitInfo.end, newSplitInfo)) {
                candidates.push(newSplitInfo);
            }
        }

        // calclate new bin centers and weights
        Sort(splits.begin(), splits.end());

        Bins.clear();
        for (auto it = splits.begin(); it + 1 != splits.end(); ++it) {
            auto splitBeg = *it;
            auto splitEnd = *(it + 1);
            double cnt = (splitEnd->first - splitBeg->first);
            double mu = (splitEnd->second - splitBeg->second) / cnt;

            Bins.push_back(TWeightedValue(mu, cnt));
        }
    }

}
