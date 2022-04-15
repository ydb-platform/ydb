#include "histogram_collector.h"
#include "atomics_array.h"

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

namespace NMonitoring {
    ///////////////////////////////////////////////////////////////////////////
    // TExponentialHistogramCollector
    ///////////////////////////////////////////////////////////////////////////
    class TExponentialHistogramCollector: public IHistogramCollector {
    public:
        TExponentialHistogramCollector(ui32 bucketsCount, double base, double scale)
            : Values_(bucketsCount)
            , Base_(base)
            , Scale_(scale)
            , MinValue_(scale)
            , MaxValue_(scale * std::pow(base, bucketsCount - 2))
            , LogOfBase_(std::log(base))
        {
        }

        void Collect(double value, ui64 count) override {
            ui32 index = Max<ui32>();
            if (value <= MinValue_) {
                index = 0;
            } else if (value > MaxValue_) {
                index = Values_.Size() - 1;
            } else {
                double logBase = std::log(value / Scale_) / LogOfBase_;
                index = static_cast<ui32>(std::ceil(logBase));
            }
            Values_.Add(index, count);
        }

        void Reset() override {
            Values_.Reset();
        }

        IHistogramSnapshotPtr Snapshot() const override {
            return new TExponentialHistogramSnapshot(Base_, Scale_, Values_.Copy());
        }

    private:
        TAtomicsArray Values_;
        double Base_;
        double Scale_;
        TBucketBound MinValue_;
        TBucketBound MaxValue_;
        double LogOfBase_;
    };

    IHistogramCollectorPtr ExponentialHistogram(
        ui32 bucketsCount, double base, double scale)
    {
        Y_ENSURE(bucketsCount >= 2,
                 "exponential histogram must contain at least two buckets");
        Y_ENSURE(bucketsCount <= HISTOGRAM_MAX_BUCKETS_COUNT,
                 "buckets count must be <=" << HISTOGRAM_MAX_BUCKETS_COUNT
                                            << ", but got: " << bucketsCount);
        Y_ENSURE(base > 1.0, "base must be > 1.0, got: " << base);
        Y_ENSURE(scale >= 1.0, "scale must be >= 1.0, got: " << scale);

        return MakeHolder<TExponentialHistogramCollector>(bucketsCount, base, scale);
    }
}
