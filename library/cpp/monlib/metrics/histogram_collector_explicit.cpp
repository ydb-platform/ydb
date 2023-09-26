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

        void Collect(double value, ui64 count) override {
            auto it = LowerBound(Bounds_.begin(), Bounds_.end(), value);
            auto index = std::distance(Bounds_.begin(), it);
            Values_.Add(index, count);
        }

        void Reset() override {
            Values_.Reset();
        }

        IHistogramSnapshotPtr Snapshot() const override {
            auto values = Values_.Copy();
            return ExplicitHistogramSnapshot(Bounds_, values);
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
