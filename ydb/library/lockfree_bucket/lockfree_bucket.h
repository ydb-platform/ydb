#pragma once

#include <atomic>
#include <limits>

#include <util/datetime/base.h>

template<class TTimer> 
class alignas(128) TLockFreeBucket {  // align to cache line size
public:
    TLockFreeBucket(std::atomic<i64>& maxTokens, std::atomic<i64>& minTokens, std::atomic<ui64>& inflowPerSecond)
        : MaxTokens(maxTokens)
        , MinTokens(minTokens)
        , InflowPerSecond(inflowPerSecond)
        , Tokens(maxTokens.load())
    {
        Y_DEBUG_ABORT_UNLESS(maxTokens > 0);
        Y_DEBUG_ABORT_UNLESS(minTokens < 0);
    }

    bool IsEmpty() {
        FillAndTake(0);
        return Tokens.load(std::memory_order_relaxed) <= 0;
    }

    void FillAndTake(ui64 tokens) {
        TTime prev = LastUpdate.load(std::memory_order_acquire);
        TTime now = TTimer::Now();
        i64 duration = TTimer::Duration(prev, now);

        while (true) {
            if (prev >= now) {
                duration = 0;
                break;
            }

            if (LastUpdate.compare_exchange_weak(prev, now,
                    std::memory_order_release,
                    std::memory_order_acquire)) {
                break;
            }
        }

        i64 currentTokens = Tokens.load(std::memory_order_acquire);

        ui64 rawInflow = InflowPerSecond.load(std::memory_order_relaxed) * duration;
        i64 minTokens = MinTokens.load(std::memory_order_relaxed);
        i64 maxTokens = MaxTokens.load(std::memory_order_relaxed);
        Y_DEBUG_ABORT_UNLESS(minTokens <= maxTokens);

        while (true) {
            i64 newTokens = currentTokens + rawInflow / TTimer::Resolution;
            newTokens = std::min(newTokens, maxTokens);
            newTokens = newTokens - tokens;
            newTokens = std::max(newTokens, minTokens);

            if (newTokens == currentTokens) {
                break;
            }

            if (Tokens.compare_exchange_weak(currentTokens, newTokens, std::memory_order_release,
                    std::memory_order_acquire)) {
                break;
            }
        }
    }

private:
    using TTime = typename TTimer::TTime;

    std::atomic<i64>& MaxTokens;
    std::atomic<i64>& MinTokens;
    std::atomic<ui64>& InflowPerSecond;

    std::atomic<i64> Tokens;
    std::atomic<TTime> LastUpdate;

    constexpr static ui32 CacheLineFillerSize = 128 - sizeof(std::atomic<i64>&) * 2 - sizeof(std::atomic<ui64>&)
            - sizeof(std::atomic<i64>) - sizeof(std::atomic<TTime>);

    std::array<char, CacheLineFillerSize> CacheLineFiller;
};
