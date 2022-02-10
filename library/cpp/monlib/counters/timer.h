#pragma once

#include "histogram.h"

#include <util/generic/scope.h> 
 
#include <chrono>

namespace NMonitoring {
    /**
     * A timer counter which aggregates timing durations and provides duration
     * statistics in selected time resolution.
     */
    template <typename TResolution>
    class TTimerImpl {
    public:
        /**
         * Construct a timer given the Lowest and Highest values to be tracked
         * and a number of significant decimal digits. Providing a
         * lowestDiscernibleValue is useful in situations where the units used for
         * the timer's values are much smaller that the minimal accuracy
         * required. E.g. when tracking time values stated in nanosecond units,
         * where the minimal accuracy required is a microsecond, the proper value
         * for lowestDiscernibleValue would be 1000.
         *
         * @param min The lowest value that can be discerned (distinguished from
         *        0) by the timer. Must be a positive integer that is >= 1.
         *        May be internally rounded down to nearest power of 2.
         *
         * @param max The highest value to be tracked by the timer. Must be a
         *        positive integer that is >= (2 * min).
         *
         * @param numberOfSignificantValueDigits Specifies the precision to use.
         *        This is the number of significant decimal digits to which the
         *        timer will maintain value resolution and separation. Must be
         *        a non-negative integer between 0 and 5.
         */
        TTimerImpl(ui64 min, ui64 max, i32 numberOfSignificantValueDigits = 3)
            : TTimerImpl(TResolution(min), TResolution(max),
                         numberOfSignificantValueDigits) {
        }

        /**
         * Construct a timer given the Lowest and Highest values to be tracked
         * and a number of significant decimal digits.
         *
         * @param min The lowest value that can be discerned (distinguished from
         *        0) by the timer.
         *
         * @param max The highest value to be tracked by the histogram. Must be a
         *        positive integer that is >= (2 * min).
         *
         * @param numberOfSignificantValueDigits Specifies the precision to use.
         */
        template <typename TDurationMin, typename TDurationMax>
        TTimerImpl(TDurationMin min, TDurationMax max,
                   i32 numberOfSignificantValueDigits = 3)
            : Histogram_(std::chrono::duration_cast<TResolution>(min).count(),
                         std::chrono::duration_cast<TResolution>(max).count(),
                         numberOfSignificantValueDigits) {
        }

        /**
         * Records a value in the timer with timer resulution. Recorded value will
         * be rounded to a precision at or better than the
         * NumberOfSignificantValueDigits specified at construction time.
         *
         * @param duration duration to add to the timer
         * @return false if the value is larger than the max and can't be recorded,
         *         true otherwise.
         */
        bool RecordValue(ui64 duration) {
            return Histogram_.RecordValue(duration);
        }

        /**
         * Records a duration in the timer. Recorded value will be converted to
         * the timer resulution and rounded to a precision at or better than the
         * NumberOfSignificantValueDigits specified at construction time.
         *
         * @param duration duration to add to the timer
         * @return false if the value is larger than the max and can't be recorded,
         *         true otherwise.
         */
        template <typename TDuration>
        bool RecordValue(TDuration duration) {
            auto count = static_cast<ui64>(
                std::chrono::duration_cast<TResolution>(duration).count());
            return RecordValue(count);
        }

        /**
         * Records count values in the timer with timer resulution. Recorded value will
         * be rounded to a precision at or better than the
         * NumberOfSignificantValueDigits specified at construction time.
         *
         * @param duration duration to add to the timer
         * @param count number of values to add to the histogram
         * @return false if the value is larger than the max and can't be recorded,
         *         true otherwise.
         */
        bool RecordValues(ui64 duration, ui64 count) {
            return Histogram_.RecordValues(duration, count);
        }

        /**
         * Measures a time of functor execution.
         *
         * @param fn functor whose duration should be timed
         */
        template <typename TFunc>
        void Measure(TFunc&& fn) {
            using TClock = std::chrono::high_resolution_clock;

            auto start = TClock::now();
 
            Y_SCOPE_EXIT(this, start) { 
                RecordValue(TClock::now() - start);
            };

            fn();
        }

        /**
         * Place a copy of the value counts accumulated since the last snapshot
         * was taken into {@code snapshot}. Calling this member-function will
         * reset the value counts, and start accumulating value counts for the
         * next interval.
         *
         * @param snapshot the structure into which the values should be copied.
         */
        void TakeSnapshot(THistogramSnapshot* snapshot) {
            Histogram_.TakeSnaphot(snapshot);
        }

    private:
        THdrHistogram Histogram_;
    };

    /**
     * Timer template instantiations for certain time resolutions.
     */
    using TTimerNs = TTimerImpl<std::chrono::nanoseconds>;
    using TTimerUs = TTimerImpl<std::chrono::microseconds>;
    using TTimerMs = TTimerImpl<std::chrono::milliseconds>;
    using TTimerS = TTimerImpl<std::chrono::seconds>;

    /**
     * A timing scope to record elapsed time since creation.
     */
    template <typename TTimer, typename TFunc = std::function<void(std::chrono::high_resolution_clock::duration)>>
    class TTimerScope {
        using TClock = std::chrono::high_resolution_clock;

    public:
        explicit TTimerScope(TTimer* timer, TFunc* callback = nullptr)
            : Timer_(timer)
            , StartTime_(TClock::now())
            , Callback_(callback)
        {
        }

        ~TTimerScope() {
            TClock::duration duration = TClock::now() - StartTime_;
            if (Callback_) {
                (*Callback_)(duration);
            }
            Timer_->RecordValue(duration);
        }

    private:
        TTimer* Timer_;
        TClock::time_point StartTime_;
        TFunc* Callback_;
    };
}
