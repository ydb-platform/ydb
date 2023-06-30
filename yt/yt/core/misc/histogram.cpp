#include "histogram.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

class THistogram
    : public IHistogram
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    THistogram() = default;

    explicit THistogram(int defaultBuckets)
        : MaxBuckets_(defaultBuckets * HistogramViewReserveFactor)
    { }

    void AddValue(i64 value, i64 count) override
    {
        YT_VERIFY(value >= 0);

        Items_.emplace_back(TItem{value, count});
        ValueMin_ = std::min(ValueMin_, value);
        ValueMax_ = std::max(ValueMax_, value);
        if (IsValid() && HasBucket(value)) {
            View_.Count[GetBucketIndex(value)] += count;
        }
    }

    void RemoveValue(i64 value, i64 count) override
    {
        Items_.emplace_back(TItem{value, -count});
        if (IsValid() && HasBucket(value)) {
            auto& valueCount = View_.Count[GetBucketIndex(value)];
            valueCount -= count;
            YT_VERIFY(valueCount >= 0);
        }
    }

    void BuildHistogramView() override
    {
        if (Items_.size() > 1 && !IsValid()) {
            RebuildView();
        }
    }

    THistogramView GetHistogramView() const override
    {
        THistogramView result;
        if (Items_.empty()) {
            return result;
        }
        if (Items_.size() == 1) {
            result.Min = ValueMin_;
            result.Max = ValueMax_;
            result.Count.assign(1, Items_[0].Count);
            return result;
        }

        YT_VERIFY(IsValid());
        i64 firstBucket = GetBucketIndex(ValueMin_);
        i64 lastBucket = GetBucketIndex(ValueMax_) + 1;
        result.Min = View_.Min + BucketWidth_ * firstBucket;
        result.Max = View_.Min + BucketWidth_ * lastBucket;
        result.Count.assign(View_.Count.begin() + firstBucket, View_.Count.begin() + lastBucket);
        return result;
    }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, MaxBuckets_);
        Persist(context, ValueMin_);
        Persist(context, ValueMax_);
        Persist(context, Items_);
    }

private:
    struct TItem {
        i64 Value;
        i64 Count;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Value);
            Persist(context, Count);
        }
    };

    DECLARE_DYNAMIC_PHOENIX_TYPE(THistogram, 0x636d76d7);

    static const i64 HistogramViewReserveFactor = 2;

    i64 MaxBuckets_ = 0;
    i64 ValueMin_ = std::numeric_limits<i64>::max();
    i64 ValueMax_ = std::numeric_limits<i64>::min();
    i64 BucketWidth_ = 0;
    THistogramView View_;
    std::vector<TItem> Items_;

    bool IsValid() const
    {
        return BucketWidth_ != 0 && HasBucket(ValueMin_) && HasBucket(ValueMax_);
    }

    bool HasBucket(i64 value) const
    {
        return value >= View_.Min && value < View_.Max;
    }

    i64 GetBucketIndex(i64 value) const
    {
        YT_VERIFY(HasBucket(value));
        return (value - View_.Min) / BucketWidth_;
    }

    void RebuildView()
    {
        YT_VERIFY(Items_.size() > 1);
        // Make a view with a range twice largen that current and mean value in place.
        BucketWidth_ = (HistogramViewReserveFactor * (ValueMax_ + 1 - ValueMin_) + MaxBuckets_ - 1) / MaxBuckets_;
        if (BucketWidth_ == 0) {
            BucketWidth_ = 1;
        }
        View_.Min = ValueMin_ - BucketWidth_ * (MaxBuckets_ / 4);
        View_.Max = View_.Min + BucketWidth_ * MaxBuckets_;
        View_.Min = std::max<i64>(View_.Min, 0);
        View_.Count.assign(MaxBuckets_, 0);
        for (const auto& item : Items_) {
            View_.Count[GetBucketIndex(item.Value)] += item.Count;
        }
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(THistogram);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IHistogram> CreateHistogram(int maxBuckets)
{
    return std::unique_ptr<IHistogram>(new THistogram(maxBuckets));
}

void Serialize(const IHistogram& histogram, IYsonConsumer* consumer)
{
    auto view = histogram.GetHistogramView();
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("min").Value(view.Min)
            .Item("max").Value(view.Max)
            .Item("count").Value(view.Count)
        .EndMap();
}

THistogramQuartiles ComputeHistogramQuartiles(const THistogramView& histogramView)
{
    YT_VERIFY(histogramView.Count.size() > 0);

    int currentBucketIndex = 0;
    i64 partialBucketSum = 0;
    i64 totalSum = std::accumulate(histogramView.Count.begin(), histogramView.Count.end(), 0LL);
    i64 bucketSize = (histogramView.Max - histogramView.Min) / histogramView.Count.size();

    auto computeNextQuartile = [&] (double quartile) {
        while (currentBucketIndex < std::ssize(histogramView.Count) && partialBucketSum < quartile * totalSum) {
            partialBucketSum += histogramView.Count[currentBucketIndex];
            ++currentBucketIndex;
        }
        return histogramView.Min + currentBucketIndex * bucketSize;
    };

    THistogramQuartiles result;
    result.Q25 = computeNextQuartile(0.25);
    result.Q50 = computeNextQuartile(0.50);
    result.Q75 = computeNextQuartile(0.75);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
