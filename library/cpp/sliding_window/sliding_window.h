#pragma once

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <functional>
#include <limits>

namespace NSlidingWindow {
    namespace NPrivate {
        template <class TValueType_, class TCmp, TValueType_ initialValue> // std::less for max, std::greater for min
        struct TMinMaxOperationImpl {
            using TValueType = TValueType_;
            using TValueVector = TVector<TValueType>;

            static constexpr TValueType InitialValue() {
                return initialValue;
            }

            // Updates value in current bucket and returns window value
            static TValueType UpdateBucket(TValueType windowValue, TValueVector& buckets, size_t index, TValueType newVal) {
                Y_ASSERT(index < buckets.size());
                TCmp cmp;
                TValueType& curVal = buckets[index];
                if (cmp(curVal, newVal)) {
                    curVal = newVal;
                    if (cmp(windowValue, newVal)) {
                        windowValue = newVal;
                    }
                }
                return windowValue;
            }

            static TValueType ClearBuckets(TValueType windowValue, TValueVector& buckets, const size_t firstElemIndex, const size_t bucketsToClear) {
                Y_ASSERT(!buckets.empty());
                Y_ASSERT(firstElemIndex < buckets.size());
                Y_ASSERT(bucketsToClear <= buckets.size());
                TCmp cmp;

                bool needRecalc = false;
                size_t current = firstElemIndex;
                const size_t arraySize = buckets.size();
                for (size_t i = 0; i < bucketsToClear; ++i) {
                    TValueType& curVal = buckets[current];
                    if (!needRecalc && windowValue == curVal) {
                        needRecalc = true;
                    }
                    curVal = InitialValue();
                    current = (current + 1) % arraySize;
                }
                if (needRecalc) {
                    windowValue = InitialValue();
                    for (size_t i = 0; i < firstElemIndex; ++i) {
                        const TValueType val = buckets[i];
                        if (cmp(windowValue, val)) {
                            windowValue = val;
                        }
                    }
                    for (size_t i = current, size = buckets.size(); i < size; ++i) {
                        const TValueType val = buckets[i];
                        if (cmp(windowValue, val)) {
                            windowValue = val;
                        }
                    }
                }
                return windowValue;
            }
        };

    }

    template <class TValueType>
    using TMaxOperation = NPrivate::TMinMaxOperationImpl<TValueType, std::less<TValueType>, std::numeric_limits<TValueType>::min()>;

    template <class TValueType>
    using TMinOperation = NPrivate::TMinMaxOperationImpl<TValueType, std::greater<TValueType>, std::numeric_limits<TValueType>::max()>;

    template <class TValueType_>
    struct TSumOperation {
        using TValueType = TValueType_;
        using TValueVector = TVector<TValueType>;

        static constexpr TValueType InitialValue() {
            return TValueType(); // zero
        }

        // Updates value in current bucket and returns window value
        static TValueType UpdateBucket(TValueType windowValue, TValueVector& buckets, size_t index, TValueType newVal) {
            Y_ASSERT(index < buckets.size());
            buckets[index] += newVal;
            windowValue += newVal;
            return windowValue;
        }

        static TValueType ClearBuckets(TValueType windowValue, TValueVector& buckets, size_t firstElemIndex, size_t bucketsToClear) {
            Y_ASSERT(!buckets.empty());
            Y_ASSERT(firstElemIndex < buckets.size());
            Y_ASSERT(bucketsToClear <= buckets.size());

            const size_t arraySize = buckets.size();
            for (size_t i = 0; i < bucketsToClear; ++i) {
                TValueType& curVal = buckets[firstElemIndex];
                windowValue -= curVal;
                curVal = InitialValue();
                firstElemIndex = (firstElemIndex + 1) % arraySize;
            }
            return windowValue;
        }
    };

    /////////////////////////////////////////////////////////////////////////////////////////
    // TSlidingWindow
    /////////////////////////////////////////////////////////////////////////////////////////
    template <class TOperation, class TMutexImpl = TFakeMutex>
    class TSlidingWindow {
    public:
        using TValueType = typename TOperation::TValueType;
        using TValueVector = TVector<TValueType>;
        using TSizeType = typename TValueVector::size_type;

    public:
        TSlidingWindow(const TDuration& length, TSizeType partsNum)
            : Mutex()
            , Buckets(partsNum, TOperation::InitialValue()) // vector of size partsNum initialized with initial value
            , WindowValue(TOperation::InitialValue())
            , FirstElem(0)
            , PeriodStart()
            , Length(length)
            , MicroSecondsPerBucket(length.MicroSeconds() / partsNum)
        {
        }

        TSlidingWindow(const TSlidingWindow& w)
            : Mutex()
        {
            TGuard<TMutexImpl> guard(&w.Mutex);
            Buckets = w.Buckets;
            WindowValue = w.WindowValue;
            FirstElem = w.FirstElem;
            PeriodStart = w.PeriodStart;
            Length = w.Length;
            MicroSecondsPerBucket = w.MicroSecondsPerBucket;
        }

        TSlidingWindow(TSlidingWindow&&) = default;

        TSlidingWindow& operator=(TSlidingWindow&&) = default;
        TSlidingWindow& operator=(const TSlidingWindow&) = delete;

        // Period of time
        const TDuration& GetDuration() const {
            return Length;
        }

        // Update window with new value and time
        TValueType Update(TValueType val, TInstant t) {
            TGuard<TMutexImpl> guard(&Mutex);
            AdvanceTime(t);
            UpdateCurrentBucket(val);
            return WindowValue;
        }

        // Update just time, without new values
        TValueType Update(TInstant t) {
            TGuard<TMutexImpl> guard(&Mutex);
            AdvanceTime(t);
            return WindowValue;
        }

        // Get current window value (without updating current time)
        TValueType GetValue() const {
            TGuard<TMutexImpl> guard(&Mutex);
            return WindowValue;
        }

    private:
        void UpdateCurrentBucket(TValueType val) {
            const TSizeType arraySize = Buckets.size();
            const TSizeType pos = (FirstElem + arraySize - 1) % arraySize;
            WindowValue = TOperation::UpdateBucket(WindowValue, Buckets, pos, val);
        }

        void AdvanceTime(const TInstant& time) {
            if (time < PeriodStart + Length) {
                return;
            }

            if (PeriodStart.MicroSeconds() == 0) {
                PeriodStart = time - Length;
                return;
            }

            const TInstant& newPeriodStart = time - Length;
            const ui64 tmDiff = (newPeriodStart - PeriodStart).MicroSeconds();
            const TSizeType bucketsDiff = tmDiff / MicroSecondsPerBucket;
            const TSizeType arraySize = Buckets.size();
            const TSizeType buckets = Min(bucketsDiff, arraySize);

            WindowValue = TOperation::ClearBuckets(WindowValue, Buckets, FirstElem, buckets);
            FirstElem = (FirstElem + buckets) % arraySize;
            PeriodStart += TDuration::MicroSeconds(bucketsDiff * MicroSecondsPerBucket);

            // Check that PeriodStart lags behind newPeriodStart
            // (which is actual, uptodate, precise and equal to time - Length) not more
            // then MicroSecondsPerBucket
            Y_ASSERT(newPeriodStart >= PeriodStart);
            Y_ASSERT((newPeriodStart - PeriodStart).MicroSeconds() <= MicroSecondsPerBucket);
        }


        mutable TMutexImpl Mutex;
        TValueVector Buckets;
        TValueType WindowValue;
        TSizeType FirstElem;
        TInstant PeriodStart;
        TDuration Length;
        ui64 MicroSecondsPerBucket;
    };

}
