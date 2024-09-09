#pragma once

#include <atomic>
#include <limits>

#include <util/datetime/base.h>

template<class TTimer> 
class TLockFreeBucket {
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

    void FillAndTake(i64 tokens) {
        FillBucket();
        TakeTokens(tokens);
    }

private:
    void FillBucket() {
        TTime prev;
        TTime now;
        for (prev = LastUpdate.load(), now = TTimer::Now(); !LastUpdate.compare_exchange_strong(prev, now); ) {}

        ui64 rawInflow = InflowPerSecond.load() * TTimer::Duration(prev, now);
        if (rawInflow >= TTimer::Resolution) {
            Tokens.fetch_add(rawInflow / TTimer::Resolution);
            for (i64 tokens = Tokens.load(), maxTokens = MaxTokens.load(); tokens > maxTokens; ) {
                if (Tokens.compare_exchange_strong(tokens, maxTokens)) {
                    break;
                }
            }
        }
    }

    void TakeTokens(i64 tokens) {
        Tokens.fetch_sub(tokens);
        for (i64 tokens = Tokens.load(), minTokens = MinTokens.load(); tokens < minTokens; ) {
            if (Tokens.compare_exchange_strong(tokens, minTokens)) {
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
};
