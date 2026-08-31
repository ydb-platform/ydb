#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/system/types.h>

#include <array>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t DEFAULT_BUCKET_COUNT = 15;

////////////////////////////////////////////////////////////////////////////////

template <bool PerSecond, size_t BucketCount>
struct TMaxCalculatorBase
{
    ITimerPtr Timer;

    TAtomic Current = 0;
    TAtomic Started;

    std::array<ui64, BucketCount> Buckets{};

    size_t BucketIndex = 0;

    TMaxCalculatorBase(ITimerPtr timer)
        : Timer(std::move(timer))
        , Started(Timer->Now().MicroSeconds())
    {}

    void Add(ui64 value)
    {
        if constexpr (PerSecond) {
            AtomicAdd(Current, value);
        } else {
            ui64 current;
            do {
                current = AtomicGet(Current);
            } while (current < value && !AtomicCas(&Current, value, current));
        }
    }

    ui64 NextValue()
    {
        ui64 current = AtomicSwap(&Current, 0);

        if constexpr (PerSecond) {
            ui64 now = Timer->Now().MicroSeconds();
            ui64 started = AtomicSwap(&Started, now);
            if (now > started) {
                current = std::ceil(current * 1000000.0 / (now - started));
            }
        }

        Buckets[BucketIndex] = current;
        BucketIndex = (BucketIndex + 1) % BucketCount;

        return *std::max_element(Buckets.begin(), Buckets.end());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketCount>
using TMaxCalculator = TMaxCalculatorBase<false, BucketCount>;
template <size_t BucketCount>
using TMaxPerSecondCalculator = TMaxCalculatorBase<true, BucketCount>;

}   // namespace NYdb::NBS
