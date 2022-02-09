#pragma once

#include "histogram.h"

struct hdr_iter;

namespace NHdr {
    /**
     * Used for iterating through histogram values.
     */
    class TBaseHistogramIterator {
    public:
        /**
         * Iterate to the next value for the iterator. If there are no more values
         * available return false.
         *
         * @return 'false' if there are no values remaining for this iterator.
         */
        bool Next();

        /**
         * @return Raw index into the counts array.
         */
        i32 GetCountsIndex() const;

        /**
         * @return Snapshot of the length at the time the iterator is created.
         */
        i32 GetTotalCount() const;

        /**
         * @return Value directly from array for the current countsIndex.
         */
        i64 GetCount() const;

        /**
         * @return Sum of all of the counts up to and including the count at
         *         this index.
         */
        i64 GetCumulativeCount() const;

        /**
         * @return The current value based on countsIndex.
         */
        i64 GetValue() const;

        /**
         * @return The highest value that is equivalent to the current value
         *         within the histogram's resolution.
         */
        i64 GetHighestEquivalentValue() const;

        /**
         * @return The lowest value that is equivalent to the current value
         *         within the histogram's resolution.
         */
        i64 GetLowestEquivalentValue() const;

        /**
         * @return The value lies in the middle (rounded up) of the range of
         *         values equivalent the current value.
         */
        i64 GetMedianEquivalentValue() const;

        /**
         * @return The actual value level that was iterated from by the iterator.
         */
        i64 GetValueIteratedFrom() const;

        /**
         * @return The actual value level that was iterated to by the iterator.
         */
        i64 GetValueIteratedTo() const;

    protected:
        // must not be instantiated directly
        TBaseHistogramIterator();
        ~TBaseHistogramIterator();

    protected:
        THolder<hdr_iter> Iter_;
    };

    /**
     * Used for iterating through histogram values using the finest granularity
     * steps supported by the underlying representation. The iteration steps
     * through all possible unit value levels, regardless of whether or not there
     * were recorded values for that value level, and terminates when all recorded
     * histogram values are exhausted.
     */
    class TAllValuesIterator: public TBaseHistogramIterator {
    public:
        /**
         * @param histogram The histogram this iterator will operate on
         */
        explicit TAllValuesIterator(const THistogram& histogram);
    };

    /**
     * Used for iterating through all recorded histogram values using the finest
     * granularity steps supported by the underlying representation. The iteration
     * steps through all non-zero recorded value counts, and terminates when all
     * recorded histogram values are exhausted.
     */
    class TRecordedValuesIterator: public TBaseHistogramIterator {
    public:
        /**
         * @param histogram The histogram this iterator will operate on
         */
        explicit TRecordedValuesIterator(const THistogram& histogram);

        /**
         * @return The count of recorded values in the histogram that were added
         *         to the totalCount as a result on this iteration step. Since
         *         multiple iteration steps may occur with overlapping equivalent
         *         value ranges, the count may be lower than the count found at
         *         the value (e.g. multiple linear steps or percentile levels can
         *         occur within a single equivalent value range).
         */
        i64 GetCountAddedInThisIterationStep() const;
    };

    /**
     * Used for iterating through histogram values according to percentile levels.
     * The iteration is performed in steps that start at 0% and reduce their
     * distance to 100% according to the <i>percentileTicksPerHalfDistance</i>
     * parameter, ultimately reaching 100% when all recorded histogram
     * values are exhausted.
     */
    class TPercentileIterator: public TBaseHistogramIterator {
    public:
        /**
         * @param histogram The histogram this iterator will operate on
         * @param ticksPerHalfDistance The number of equal-sized iteration steps
         *        per half-distance to 100%.
         */
        TPercentileIterator(const THistogram& histogram, ui32 ticksPerHalfDistance);

        /**
         * @return The number of equal-sized iteration steps per half-distance
         *         to 100%.
         */
        i32 GetTicketsPerHalfDistance() const;

        double GetPercentileToIterateTo() const;

        /**
         * @return The percentile of recorded values in the histogram at values
         *         equal or smaller than valueIteratedTo.
         *
         */
        double GetPercentile() const;
    };

    /**
     * Used for iterating through histogram values in linear steps. The iteration
     * is performed in steps of <i>valueUnitsPerBucket</i> in size, terminating
     * when all recorded histogram values are exhausted. Note that each iteration
     * "bucket" includes values up to and including the next bucket boundary value.
     */
    class TLinearIterator: public TBaseHistogramIterator {
    public:
        /**
         * @param histogram The histogram this iterator will operate on
         * @param valueUnitsPerBucket The size (in value units) of each bucket
         *        iteration.
         */
        TLinearIterator(const THistogram& histogram, i64 valueUnitsPerBucket);

        /**
         * @return The size (in value units) of each bucket iteration.
         */
        i64 GetValueUnitsPerBucket() const;

        /**
         * @return The count of recorded values in the histogram that were added
         *         to the totalCount as a result on this iteration step. Since
         *         multiple iteration steps may occur with overlapping equivalent
         *         value ranges, the count may be lower than the count found at
         *         the value (e.g. multiple linear steps or percentile levels can
         *         occur within a single equivalent value range).
         */
        i64 GetCountAddedInThisIterationStep() const;

        i64 GetNextValueReportingLevel() const;

        i64 GetNextValueReportingLevelLowestEquivalent() const;
    };

    /**
     * Used for iterating through histogram values in logarithmically increasing
     * levels. The iteration is performed in steps that start at
     * <i>valueUnitsInFirstBucket</i> and increase exponentially according to
     * <i>logBase</i>, terminating when all recorded histogram values are
     * exhausted. Note that each iteration "bucket" includes values up to and
     * including the next bucket boundary value.
     */
    class TLogarithmicIterator: public TBaseHistogramIterator {
    public:
        /**
         * @param histogram The histogram this iterator will operate on
         * @param valueUnitsInFirstBucket the size (in value units) of the first
         *        value bucket step
         * @param logBase the multiplier by which the bucket size is expanded in
         *        each iteration step.
         */
        TLogarithmicIterator(
            const THistogram& histogram, i64 valueUnitsInFirstBucket,
            double logBase);

        /**
         * @return The multiplier by which the bucket size is expanded in each
         *         iteration step.
         */
        double GetLogBase() const;

        /**
         * @return The count of recorded values in the histogram that were added
         *         to the totalCount as a result on this iteration step. Since
         *         multiple iteration steps may occur with overlapping equivalent
         *         value ranges, the count may be lower than the count found at
         *         the value (e.g. multiple linear steps or percentile levels can
         *         occur within a single equivalent value range).
         */
        i64 GetCountAddedInThisIterationStep() const;

        i64 GetNextValueReportingLevel() const;

        i64 GetNextValueReportingLevelLowestEquivalent() const;
    };
}
