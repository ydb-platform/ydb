#include "histogram_collector.h"
#include "atomics_array.h"

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

#include <cmath>

namespace NMonitoring {
    ///////////////////////////////////////////////////////////////////////////
    // TLinearHistogramCollector
    ///////////////////////////////////////////////////////////////////////////
    class TLinearHistogramCollector: public IHistogramCollector {
    public:
        TLinearHistogramCollector(
                ui32 bucketsCount, TBucketBound startValue, TBucketBound bucketWidth)
            : Values_(bucketsCount)
            , StartValue_(startValue)
            , BucketWidth_(bucketWidth)
            , MaxValue_(startValue + bucketWidth * (bucketsCount - 2))
        {
        }

        void Collect(double value, ui64 count) override {
            ui32 index = Max<ui32>();
            if (value <= StartValue_) {
                index = 0;
            } else if (value > MaxValue_) {
                index = Values_.Size() - 1;
            } else {
                double buckets = (value - StartValue_) / BucketWidth_;
                index = static_cast<ui32>(std::ceil(buckets));
            }
            Values_.Add(index, count);
        }

        void Reset() override {
            Values_.Reset();
        }

        IHistogramSnapshotPtr Snapshot() const override {
            return new TLinearHistogramSnapshot(
                    StartValue_, BucketWidth_, Values_.Copy());
        }

    private:
        TAtomicsArray Values_;
        TBucketBound StartValue_;
        double BucketWidth_;
        TBucketBound MaxValue_;
    };

    IHistogramCollectorPtr LinearHistogram(
        ui32 bucketsCount, TBucketBound startValue, TBucketBound bucketWidth)
    {
        Y_ENSURE(bucketsCount >= 2,
                 "linear histogram must contain at least two buckets");
        Y_ENSURE(bucketsCount <= HISTOGRAM_MAX_BUCKETS_COUNT,
                 "buckets count must be <=" << HISTOGRAM_MAX_BUCKETS_COUNT
                                            << ", but got: " << bucketsCount);
        Y_ENSURE(bucketWidth >= 1, "bucketWidth must be >= 1, got: " << bucketWidth);

        return MakeHolder<TLinearHistogramCollector>(bucketsCount, startValue, bucketWidth);
    }
}
