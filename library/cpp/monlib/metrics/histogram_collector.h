#pragma once

#include "histogram_snapshot.h"

namespace NMonitoring {

    ///////////////////////////////////////////////////////////////////////////
    // IHistogramCollector
    ///////////////////////////////////////////////////////////////////////////
    class IHistogramCollector {
    public:
        virtual ~IHistogramCollector() = default;

        /**
         * Store {@code count} times given {@code value} in this collector.
         */
        virtual void Collect(double value, ui64 count) = 0;

        /**
         * Store given {@code value} in this collector.
         */
        void Collect(double value) {
            Collect(value, 1);
        }

        /**
         * Add counts from snapshot into this collector
         */
        void Collect(const IHistogramSnapshot& snapshot) {
            for (ui32 i = 0; i < snapshot.Count(); i++) {
                Collect(snapshot.UpperBound(i), snapshot.Value(i));
            }
        }

        /**
         * Reset collector values
         */
        virtual void Reset() = 0;

        /**
         * @return snapshot of the state of this collector.
         */
        virtual IHistogramSnapshotPtr Snapshot() const = 0;
    };

    using IHistogramCollectorPtr = THolder<IHistogramCollector>;

    ///////////////////////////////////////////////////////////////////////////
    // free functions
    ///////////////////////////////////////////////////////////////////////////

    /**
     * <p>Creates histogram collector for a set of buckets with arbitrary
     * bounds.</p>
     *
     * <p>Defines {@code bounds.size()  + 1} buckets with these boundaries for
     * bucket i:</p>
     * <ul>
     *     <li>Upper bound (0 <= i < N-1): {@code bounds[i]}</li>
     *     <li>Lower bound (1 <= i < N):   {@code bounds[i - 1]}</li>
     * </ul>
     *
     * <p>For example, if the list of boundaries is:</p>
     * <pre>0, 1, 2, 5, 10, 20</pre>
     *
     * <p>then there are five finite buckets with the following ranges:</p>
     * <pre>(-INF, 0], (0, 1], (1, 2], (2, 5], (5, 10], (10, 20], (20, +INF)</pre>
     *
     * @param bounds array of upper bounds for buckets. Values must be sorted.
     */
    IHistogramCollectorPtr ExplicitHistogram(TBucketBounds bounds);

    /**
     * <p>Creates histogram collector for a sequence of buckets that have a
     * width proportional to the value of the lower bound.</p>
     *
     * <p>Defines {@code bucketsCount} buckets with these boundaries for bucket i:</p>
     * <ul>
     *    <li>Upper bound (0 <= i < N-1):  {@code scale * (base ^ i)}</li>
     *    <li>Lower bound (1 <= i < N):    {@code scale * (base ^ (i - 1))}</li>
     * </ul>
     *
     * <p>For example, if {@code bucketsCount=6}, {@code base=2}, and {@code scale=3},
     * then the bucket ranges are as follows:</p>
     *
     * <pre>(-INF, 3], (3, 6], (6, 12], (12, 24], (24, 48], (48, +INF)</pre>
     *
     * @param bucketsCount the total number of buckets. The value must be >= 2.
     * @param base         the exponential growth factor for the buckets width.
     *                     The value must be >= 1.0.
     * @param scale        the linear scale for the buckets. The value must be >= 1.0.
     */
    IHistogramCollectorPtr ExponentialHistogram(
        ui32 bucketsCount, double base, double scale = 1.0);

    /**
     * <p>Creates histogram collector for a sequence of buckets that all have
     * the same width (except overflow and underflow).</p>
     *
     * <p>Defines {@code bucketsCount} buckets with these boundaries for bucket i:</p>
     * <ul>
     *    <li>Upper bound (0 <= i < N-1):  {@code startValue + bucketWidth * i}</li>
     *    <li>Lower bound (1 <= i < N):    {@code startValue + bucketWidth * (i - 1)}</li>
     * </ul>
     *
     * <p>For example, if {@code bucketsCount=6}, {@code startValue=5}, and
     * {@code bucketWidth=15}, then the bucket ranges are as follows:</p>
     *
     * <pre>(-INF, 5], (5, 20], (20, 35], (35, 50], (50, 65], (65, +INF)</pre>
     *
     * @param bucketsCount the total number of buckets. The value must be >= 2.
     * @param startValue   the upper boundary of the first bucket.
     * @param bucketWidth  the difference between the upper and lower bounds for
     *                     each bucket. The value must be >= 1.
     */
    IHistogramCollectorPtr LinearHistogram(
        ui32 bucketsCount, TBucketBound startValue, TBucketBound bucketWidth);

} // namespace NMonitoring
