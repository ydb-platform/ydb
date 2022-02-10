#pragma once

#include <library/cpp/histogram/hdr/histogram.h> 

#include <util/system/spinlock.h>
#include <util/stream/output.h>

namespace NMonitoring {
    /**
     * A statistical snapshot of values recorded in histogram.
     */
    struct THistogramSnapshot {
        double Mean;
        double StdDeviation;
        i64 Min;
        i64 Max;
        i64 Percentile50;
        i64 Percentile75;
        i64 Percentile90;
        i64 Percentile95;
        i64 Percentile98;
        i64 Percentile99;
        i64 Percentile999;
        i64 TotalCount;

        void Print(IOutputStream* out) const;
    };

    /**
     * Special counter which calculates the distribution of a value.
     */
    class THdrHistogram {
    public:
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
         */
        THdrHistogram(i64 lowestDiscernibleValue, i64 highestTrackableValue,
                   i32 numberOfSignificantValueDigits)
            : Data_(lowestDiscernibleValue, highestTrackableValue,
                    numberOfSignificantValueDigits) {
        }

        /**
         * Records a value in the histogram, will round this value of to a
         * precision at or better than the NumberOfSignificantValueDigits specified
         * at construction time.
         *
         * @param value Value to add to the histogram
         * @return false if the value is larger than the HighestTrackableValue
         *         and can't be recorded, true otherwise.
         */
        bool RecordValue(i64 value) {
            with_lock (Lock_) {
                return Data_.RecordValue(value);
            }
        }

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
        bool RecordValues(i64 value, i64 count) {
            with_lock (Lock_) {
                return Data_.RecordValues(value, count);
            }
        }

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
        bool RecordValueWithExpectedInterval(i64 value, i64 expectedInterval) {
            with_lock (Lock_) {
                return Data_.RecordValueWithExpectedInterval(value, expectedInterval);
            }
        }

        /**
         * Record a value in the histogram count times. Applies the same correcting
         * logic as {@link THdrHistogram::RecordValueWithExpectedInterval}.
         *
         * @param value Value to add to the histogram
         * @param count Number of values to add to the histogram
         * @param expectedInterval The delay between recording values.
         * @return false if the value is larger than the HighestTrackableValue
         *         and can't be recorded, true otherwise.
         */
        bool RecordValuesWithExpectedInterval(
            i64 value, i64 count, i64 expectedInterval) {
            with_lock (Lock_) {
                return Data_.RecordValuesWithExpectedInterval(
                    value, count, expectedInterval);
            }
        }

        /**
         * @return The configured lowestDiscernibleValue
         */
        i64 GetLowestDiscernibleValue() const {
            with_lock (Lock_) {
                return Data_.GetLowestDiscernibleValue();
            }
        }

        /**
         * @return The configured highestTrackableValue
         */
        i64 GetHighestTrackableValue() const {
            with_lock (Lock_) {
                return Data_.GetHighestTrackableValue();
            }
        }

        /**
         * @return The configured numberOfSignificantValueDigits
         */
        i32 GetNumberOfSignificantValueDigits() const {
            with_lock (Lock_) {
                return Data_.GetNumberOfSignificantValueDigits();
            }
        }

        /**
         * @return The total count of all recorded values in the histogram
         */
        i64 GetTotalCount() const {
            with_lock (Lock_) {
                return Data_.GetTotalCount();
            }
        }

        /**
         * Place a copy of the value counts accumulated since the last snapshot
         * was taken into {@code snapshot}. Calling this member-function will
         * reset the value counts, and start accumulating value counts for the
         * next interval.
         *
         * @param snapshot the structure into which the values should be copied.
         */
        void TakeSnaphot(THistogramSnapshot* snapshot);

    private:
        mutable TSpinLock Lock_;
        NHdr::THistogram Data_;
    };

}

template <>
inline void Out<NMonitoring::THistogramSnapshot>(
    IOutputStream& out, const NMonitoring::THistogramSnapshot& snapshot) {
    snapshot.Print(&out);
}
