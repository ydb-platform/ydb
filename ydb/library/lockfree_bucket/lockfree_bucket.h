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
        FillBucket();
        return Tokens.load() <= 0;
    }

    void FillAndTake(ui64 tokens) {
        FillBucket();
        TakeTokens(tokens);
    }

private:
    void FillBucket() {
        TTime prev = LastUpdate.load(std::memory_order_relaxed);
        TTime now = TTimer::Now();

        while (true) {
            if (prev >= now) {
                return;
            }

            if (LastUpdate.compare_exchange_weak(prev, now,
                    std::memory_order_relaxed,
                    std::memory_order_relaxed)) {
                break;
            }
        }

        ui64 rawInflow = InflowPerSecond.load(std::memory_order_relaxed) * TTimer::Duration(prev, now);
        i64 tokens = Tokens.load(std::memory_order_relaxed);

        if (rawInflow >= TTimer::Resolution) {
            i64 tokensPlusInflow = tokens + rawInflow / TTimer::Resolution;
            i64 maxTokens = MaxTokens.load(std::memory_order_relaxed);
            i64 deltaTokens = std::min(maxTokens, tokensPlusInflow) - tokens;
            if (deltaTokens <= 0) {
                // race occured, currentTokens >= maxTokens
            } else {
                tokens = Tokens.fetch_add(deltaTokens, std::memory_order_relaxed) + deltaTokens;
            }

            while (true) {
                if (tokens <= maxTokens || Tokens.compare_exchange_weak(tokens, maxTokens,
                        std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            }
        }
    }

    void TakeTokens(ui64 tokens) {
        i64 currentTokens = Tokens.load(std::memory_order_relaxed);
        i64 minTokens = MinTokens.load(std::memory_order_relaxed);
        i64 deltaTokens = std::max(minTokens, currentTokens - (i64)tokens) - currentTokens;

        if (deltaTokens >= 0) {
            // race occured, currentTokens <= minTokens
        } else {
            currentTokens = Tokens.fetch_add(deltaTokens, std::memory_order_relaxed) + deltaTokens;
        }

        while (true) {
            if (currentTokens >= minTokens || Tokens.compare_exchange_weak(currentTokens, minTokens,
                    std::memory_order_relaxed, std::memory_order_relaxed)) {
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
