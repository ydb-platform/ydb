#include "histogram_collector.h"
#include "atomics_array.h"

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

namespace NMonitoring {

    ///////////////////////////////////////////////////////////////////////////
    // TExplicitHistogramCollector
    ///////////////////////////////////////////////////////////////////////////
    class TExplicitHistogramCollector: public IHistogramCollector {
    public:
        TExplicitHistogramCollector(TBucketBounds bounds)
            : Values_(bounds.size() + 1)
            , Bounds_(std::move(bounds))
        {
            // add one bucket as +INF
            Bounds_.push_back(Max<TBucketBound>());
        }

        void Collect(double value, ui64 count) noexcept override {
            auto it = LowerBound(Bounds_.begin(), Bounds_.end(), value);
            auto index = std::distance(Bounds_.begin(), it);
            Values_.Add(index, count);
        }

        void Reset() noexcept override {
            Values_.Reset();
        }

        IHistogramSnapshotPtr Snapshot() const override {
            auto values = Values_.Copy();
            return ExplicitHistogramSnapshot(Bounds_, values);
        }

        THolder<IHistogramCollector> Clone() override {
            TBucketBounds copyBounds(Bounds_);
            return MakeHolder<TExplicitHistogramCollector>(TExplicitHistogramCollector(copyBounds, Values_));
        }

    private:
        TExplicitHistogramCollector(TBucketBounds bounds, TAtomicsArray const& values)
            : Values_(bounds.size())
            , Bounds_(std::move(bounds))
        {
            for(size_t i = 0; i < Values_.Size(); ++i) {
                Values_.Add(i, values[i]);
            }
        }

    private:
        TAtomicsArray Values_;
        TBucketBounds Bounds_;
    };

    IHistogramCollectorPtr ExplicitHistogram(TBucketBounds bounds) {
        Y_ENSURE(bounds.size() >= 1,
                 "explicit histogram must contain at least one bucket");
        Y_ENSURE(bounds.size() <= HISTOGRAM_MAX_BUCKETS_COUNT,
                 "buckets count must be <=" << HISTOGRAM_MAX_BUCKETS_COUNT
                                            << ", but got: " << bounds.size());
        Y_ENSURE(IsSorted(bounds.begin(), bounds.end()),
                 "bounds for explicit histogram must be sorted");

        return MakeHolder<TExplicitHistogramCollector>(std::move(bounds));
    }
}
