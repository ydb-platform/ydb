#pragma once

#include <util/generic/ptr.h>
#include <util/generic/noncopyable.h>
#include <util/memory/alloc.h>

struct hdr_histogram;

namespace NHdr {
    /**
     * A High Dynamic Range (HDR) Histogram
     *
     * THdrHistogram supports the recording and analyzing sampled data value counts
     * across a configurable integer value range with configurable value precision
     * within the range. Value precision is expressed as the number of significant
     * digits in the value recording, and provides control over value quantization
     * behavior across the value range and the subsequent value resolution at any
     * given level.
     */
    class THistogram: public TMoveOnly {
    public:
        /**
         * Construct a histogram given the Highest value to be tracked and a number
         * of significant decimal digits. The histogram will be constructed to
         * implicitly track (distinguish from 0) values as low as 1. Default
         * allocator will be used to allocate underlying memory.
         *
         * @param highestTrackableValue The highest value to be tracked by the
         *        histogram. Must be a positive integer that is literal >= 2.
         *
         * @param numberOfSignificantValueDigits Specifies the precision to use.
         *        This is the number of significant decimal digits to which the
         *        histogram will maintain value resolution and separation. Must be
         *        a non-negative integer between 0 and 5.
         */
        THistogram(i64 highestTrackableValue, i32 numberOfSignificantValueDigits)
            : THistogram(1, highestTrackableValue, numberOfSignificantValueDigits)
        {
        }

        /**
         * Construct a histogram given the Lowest and Highest values to be tracked
         * and a number of significant decimal digits. Providing a
         * lowestDiscernibleValue is useful in situations where the units used for
         * the histogram's values are much smaller that the minimal accuracy
         * required. E.g. when tracking time values stated in nanosecond units,
         * where the minimal accuracy required is a microsecond, the proper value
         * for lowestDiscernibleValue would be 1000.
         *
         * @param lowestDiscernibleValue The lowest value that can be discerned
         *        (distinguished from 0) by the histogram. Must be a positive
         *        integer that is >= 1. May be internally rounded down to nearest
         *        power of 2.
         *
         * @param highestTrackableValue The highest value to be tracked by the
         *        histogram. Must be a positive integer that is
         *        >= (2 * lowestDiscernibleValue).
         *
         * @param numberOfSignificantValueDigits Specifies the precision to use.
         *        This is the number of significant decimal digits to which the
         *        histogram will maintain value resolution and separation. Must be
         *        a non-negative integer between 0 and 5.
         *
         * @param allocator Specifies allocator which will be used to allocate
         *        memory for histogram.
         */
        THistogram(i64 lowestDiscernibleValue, i64 highestTrackableValue,
                   i32 numberOfSignificantValueDigits,
                   IAllocator* allocator = TDefaultAllocator::Instance());

        ~THistogram();

        // Histogram structure querying support -----------------------------------

        /**
         * @return The configured lowestDiscernibleValue
         */
        i64 GetLowestDiscernibleValue() const;

        /**
         * @return The configured highestTrackableValue
         */
        i64 GetHighestTrackableValue() const;

        /**
         * @return The configured numberOfSignificantValueDigits
         */
        i32 GetNumberOfSignificantValueDigits() const;

        /**
         * @return The size of allocated memory for histogram
         */
        size_t GetMemorySize() const;

        /**
         * @return The number of created counters
         */
        i32 GetCountsLen() const;

        /**
         * @return The total count of all recorded values in the histogram
         */
        i64 GetTotalCount() const;

        // Value recording support ------------------------------------------------

        /**
         * Records a value in the histogram, will round this value of to a
         * precision at or better than the NumberOfSignificantValueDigits specified
         * at construction time.
         *
         * @param value Value to add to the histogram
         * @return false if the value is larger than the HighestTrackableValue
         *         and can't be recorded, true otherwise.
         */
        bool RecordValue(i64 value);

        /**
         * Records count values in the histogram, will round this value of to a
         * precision at or better than the NumberOfSignificantValueDigits specified
         * at construction time.
         *
         * @param value Value to add to the histogram
         * @param count Number of values to add to the histogram
         * @return false if the value is larger than the HighestTrackableValue
         *         and can't be recorded, true otherwise.
         */
        bool RecordValues(i64 value, i64 count);

        /**
         * Records a value in the histogram and backfill based on an expected
         * interval. Value will be rounded this to a precision at or better
         * than the NumberOfSignificantValueDigits specified at contruction time.
         * This is specifically used for recording latency. If the value is larger
         * than the expectedInterval then the latency recording system has
         * experienced co-ordinated omission. This method fills in the values that
         *  would have occured had the client providing the load not been blocked.
         *
         * @param value Value to add to the histogram
         * @param expectedInterval The delay between recording values
         * @return false if the value is larger than the HighestTrackableValue
         *         and can't be recorded, true otherwise.
         */
        bool RecordValueWithExpectedInterval(i64 value, i64 expectedInterval);

        /**
         * Record a value in the histogram count times. Applies the same correcting
         * logic as {@link THistogram::RecordValueWithExpectedInterval}.
         *
         * @param value Value to add to the histogram
         * @param count Number of values to add to the histogram
         * @param expectedInterval The delay between recording values.
         * @return false if the value is larger than the HighestTrackableValue
         *         and can't be recorded, true otherwise.
         */
        bool RecordValuesWithExpectedInterval(
            i64 value, i64 count, i64 expectedInterval);

        /**
         * Adds all of the values from rhs to this histogram. Will return the
         * number of values that are dropped when copying. Values will be dropped
         * if they around outside of [LowestDiscernibleValue, GetHighestTrackableValue].
         *
         * @param rhs Histogram to copy values from.
         * @return The number of values dropped when copying.
         */
        i64 Add(const THistogram& rhs);

        /**
         * Adds all of the values from rhs to this histogram. Will return the
         * number of values that are dropped when copying. Values will be dropped
         * if they around outside of [LowestDiscernibleValue, GetHighestTrackableValue].
         * Applies the same correcting logic as
         * {@link THistogram::RecordValueWithExpectedInterval}.
         *
         * @param rhs Histogram to copy values from.
         * @return The number of values dropped when copying.
         */
        i64 AddWithExpectedInterval(const THistogram& rhs, i64 expectedInterval);

        // Histogram Data access support ------------------------------------------

        /**
         * Get the lowest recorded value level in the histogram. If the histogram
         * has no recorded values, the value returned is undefined.
         *
         * @return the Min value recorded in the histogram
         */
        i64 GetMin() const;

        /**
         * Get the highest recorded value level in the histogram. If the histogram
         * has no recorded values, the value returned is undefined.
         *
         * @return the Max value recorded in the histogram
         */
        i64 GetMax() const;

        /**
         * Get the computed mean value of all recorded values in the histogram
         *
         * @return the mean value (in value units) of the histogram data
         */
        double GetMean() const;

        /**
         * Get the computed standard deviation of all recorded values in the histogram
         *
         * @return the standard deviation (in value units) of the histogram data
         */
        double GetStdDeviation() const;

        /**
         * Get the value at a given percentile.
         * Note that two values are "equivalent" in this statement if
         * {@link THistogram::ValuesAreEquivalent} would return true.
         *
         * @param percentile  The percentile for which to return the associated
         *        value
         * @return The value that the given percentage of the overall recorded
         *         value entries in the histogram are either smaller than or
         *         equivalent to. When the percentile is 0.0, returns the value
         *         that all value entries in the histogram are either larger than
         *         or equivalent to.
         */
        i64 GetValueAtPercentile(double percentile) const;

        /**
         * Get the count of recorded values at a specific value (to within the
         * histogram resolution at the value level).
         *
         * @param value The value for which to provide the recorded count
         * @return The total count of values recorded in the histogram within the
         *         value range that is >= GetLowestEquivalentValue(value) and
         *         <= GetHighestEquivalentValue(value)
         */
        i64 GetCountAtValue(i64 value) const;

        /**
         * Determine if two values are equivalent with the histogram's resolution.
         * Where "equivalent" means that value samples recorded for any two
         * equivalent values are counted in a common total count.
         *
         * @param v1 first value to compare
         * @param v2 second value to compare
         * @return True if values are equivalent with the histogram's resolution.
         */
        bool ValuesAreEqual(i64 v1, i64 v2) const;

        /**
         * Get the lowest value that is equivalent to the given value within the
         * histogram's resolution. Where "equivalent" means that value samples
         * recorded for any two equivalent values are counted in a common total
         * count.
         *
         * @param value The given value
         * @return The lowest value that is equivalent to the given value within
         *         the histogram's resolution.
         */
        i64 GetLowestEquivalentValue(i64 value) const;

        /**
         * Get the highest value that is equivalent to the given value within the
         * histogram's resolution. Where "equivalent" means that value samples
         * recorded for any two equivalent values are counted in a common total
         * count.
         *
         * @param value The given value
         * @return The highest value that is equivalent to the given value within
         *         the histogram's resolution.
         */
        i64 GetHighestEquivalentValue(i64 value) const;

        /**
         * Get a value that lies in the middle (rounded up) of the range of values
         * equivalent the given value. Where "equivalent" means that value samples
         * recorded for any two equivalent values are counted in a common total
         * count.
         *
         * @param value The given value
         * @return The value lies in the middle (rounded up) of the range of values
         *         equivalent the given value.
         */
        i64 GetMedianEquivalentValue(i64 value) const;

        // misc functions ---------------------------------------------------------

        /**
         * Reset a histogram to zero - empty out a histogram and re-initialise it.
         * If you want to re-use an existing histogram, but reset everything back
         * to zero, this is the routine to use.
         */
        void Reset();

        const hdr_histogram* GetHdrHistogramImpl() const {
            return Data_.Get();
        }

    private:
        THolder<hdr_histogram> Data_;
        IAllocator* Allocator_;
    };
}
