#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/base.h>
#include <util/system/mutex.h>
#include <util/system/hp_timer.h>

/* Token bucket.
 * Makes flow of *inflow* units per second in average, with up to *capacity* bursts.
 * Do not use for STRICT flow control.
 */

/* samples: create and use quoter sending 1000 bytes per second on average,
   with up to 60 seconds quota buildup.

   TBucketQuoter quoter(1000, 60000, NULL, NULL, NULL);

   for (;;) {
      T *msg = get_message();

      quoter.Sleep();
      quoter.Use(msg->GetSize());
      send_message(msg);
   }

   ----------------------------

   TBucketQuoter quoter(1000, 60000, NULL, NULL, NULL);

   for (;;) {
      T *msg = get_message();

      while (! quoter.IsAvail()) {
          // do something else
      }

      quoter.Use(msg->GetSize());
      send_message(msg);
   }

*/

struct TInstantTimerMs {
    using TTime = TInstant;
    static constexpr ui64 Resolution = 1000ull; // milliseconds
    static TTime Now() {
        return TInstant::Now();
    }
    static ui64 Duration(TTime from, TTime to) {
        return (to - from).MilliSeconds();
    }
};

struct THPTimerUs {
    using TTime = NHPTimer::STime;
    static constexpr ui64 Resolution = 1000000ull; // microseconds
    static TTime Now() {
        NHPTimer::STime ret;
        NHPTimer::GetTime(&ret);
        return ret;
    }
    static ui64 Duration(TTime from, TTime to) {
        i64 cycles = to - from;
        if (cycles > 0) {
            return ui64(double(cycles) * double(Resolution) / NHPTimer::GetClockRate());
        } else {
            return 0;
        }
    }
};

template <typename StatCounter, typename Lock = TMutex, typename Timer = TInstantTimerMs>
class TBucketQuoter {
public:
    using TTime = typename Timer::TTime;

    struct TResult {
        i64 Before;
        i64 After;
        ui64 Seqno;
    };

    /* fixed quota */
    TBucketQuoter(ui64 inflow, ui64 capacity, StatCounter* msgPassed = nullptr,
                  StatCounter* bucketUnderflows = nullptr, StatCounter* tokensUsed = nullptr,
                  StatCounter* usecWaited = nullptr, bool fill = false, StatCounter* aggregateInflow = nullptr)
        : MsgPassed(msgPassed)
        , BucketUnderflows(bucketUnderflows)
        , TokensUsed(tokensUsed)
        , UsecWaited(usecWaited)
        , AggregateInflow(aggregateInflow)
        , Bucket(fill ? capacity : 0)
        , LastAdd(Timer::Now())
        , InflowTokensPerSecond(&FixedInflow)
        , BucketTokensCapacity(&FixedCapacity)
        , FixedInflow(inflow)
        , FixedCapacity(capacity)
    {
        /* no-op */
    }

    /* adjustable quotas */
    TBucketQuoter(TAtomic* inflow, TAtomic* capacity, StatCounter* msgPassed = nullptr,
                  StatCounter* bucketUnderflows = nullptr, StatCounter* tokensUsed = nullptr,
                  StatCounter* usecWaited = nullptr, bool fill = false, StatCounter* aggregateInflow = nullptr)
        : MsgPassed(msgPassed)
        , BucketUnderflows(bucketUnderflows)
        , TokensUsed(tokensUsed)
        , UsecWaited(usecWaited)
        , AggregateInflow(aggregateInflow)
        , Bucket(fill ? AtomicGet(*capacity) : 0)
        , LastAdd(Timer::Now())
        , InflowTokensPerSecond(inflow)
        , BucketTokensCapacity(capacity)
    {
        /* no-op */
    }

    bool IsAvail() {
        TGuard<Lock> g(BucketMutex);
        FillBucket();
        if (Bucket < 0) {
            if (BucketUnderflows) {
                (*BucketUnderflows)++;
            }
        }
        return (Bucket >= 0);
    }

    bool IsAvail(TResult& res) {
        TGuard<Lock> g(BucketMutex);
        res.Before = Bucket;
        FillBucket();
        res.After = Bucket;
        res.Seqno = ++Seqno;
        if (Bucket < 0) {
            if (BucketUnderflows) {
                (*BucketUnderflows)++;
            }
        }
        return (Bucket >= 0);
    }

    ui64 GetAvail() {
        TGuard<Lock> g(BucketMutex);
        FillBucket();
        return Max<i64>(0, Bucket);
    }

    ui64 GetAvail(TResult& res) {
        TGuard<Lock> g(BucketMutex);
        res.Before = Bucket;
        FillBucket();
        res.After = Bucket;
        res.Seqno = ++Seqno;
        return Max<i64>(0, Bucket);
    }

    void Use(ui64 tokens, bool sleep = false) {
        TGuard<Lock> g(BucketMutex);
        UseNoLock(tokens, sleep);
    }

    void Use(ui64 tokens, TResult& res, bool sleep = false) {
        TGuard<Lock> g(BucketMutex);
        res.Before = Bucket;
        UseNoLock(tokens, sleep);
        res.After = Bucket;
        res.Seqno = ++Seqno;
    }

    i64 UseAndFill(ui64 tokens) {
        TGuard<Lock> g(BucketMutex);
        UseNoLock(tokens);
        FillBucket();
        return Bucket;
    }

    void Add(ui64 tokens) {
        TGuard<Lock> g(BucketMutex);
        AddNoLock(tokens);
    }

    void Add(ui64 tokens, TResult& res) {
        TGuard<Lock> g(BucketMutex);
        res.Before = Bucket;
        AddNoLock(tokens);
        res.After = Bucket;
        res.Seqno = ++Seqno;
    }

    ui32 GetWaitTime() {
        TGuard<Lock> g(BucketMutex);

        FillBucket();
        if (Bucket >= 0) {
            return 0;
        }

        ui32 usec = (-Bucket * 1000000) / (*InflowTokensPerSecond);
        return usec;
    }

    ui32 GetWaitTime(TResult& res) {
        TGuard<Lock> g(BucketMutex);
        res.Before = Bucket;
        FillBucket();
        res.After = Bucket;
        res.Seqno = ++Seqno;
        if (Bucket >= 0) {
            return 0;
        }
        ui32 usec = (-Bucket * 1000000) / (*InflowTokensPerSecond);
        return usec;
    }

    void Sleep() {
        while (!IsAvail()) {
            ui32 delay = GetWaitTime();
            if (delay != 0) {
                usleep(delay);
                if (UsecWaited) {
                    (*UsecWaited) += delay;
                }
            }
        }
    }

private:
    void FillBucket() {
        TTime now = Timer::Now();

        ui64 elapsed = Timer::Duration(LastAdd, now);
        if (*InflowTokensPerSecond * elapsed >= Timer::Resolution) {
            ui64 inflow = *InflowTokensPerSecond * elapsed / Timer::Resolution;
            if (AggregateInflow) {
                *AggregateInflow += inflow;
            }
            Bucket += inflow;
            if (Bucket > *BucketTokensCapacity) {
                Bucket = *BucketTokensCapacity;
            }

            LastAdd = now;
        }
    }

    void UseNoLock(ui64 tokens, bool sleep = false) {
        if (sleep)
            Sleep();
        Bucket -= tokens;
        if (TokensUsed) {
            (*TokensUsed) += tokens;
        }
        if (MsgPassed) {
            (*MsgPassed)++;
        }
    }

    void AddNoLock(ui64 tokens) {
        Bucket += tokens;
        if (Bucket > *BucketTokensCapacity) {
            Bucket = *BucketTokensCapacity;
        }
    }

    StatCounter* MsgPassed;
    StatCounter* BucketUnderflows;
    StatCounter* TokensUsed;
    StatCounter* UsecWaited;
    StatCounter* AggregateInflow;

    i64 Bucket;
    TTime LastAdd;
    Lock BucketMutex;
    ui64 Seqno = 0;

    TAtomic* InflowTokensPerSecond;
    TAtomic* BucketTokensCapacity;
    TAtomic FixedInflow;
    TAtomic FixedCapacity;
};
