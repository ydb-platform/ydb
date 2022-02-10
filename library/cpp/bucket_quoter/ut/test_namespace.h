#pragma once

#include <library/cpp/bucket_quoter/bucket_quoter.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/thread/pool.h>
#include <util/string/printf.h>
#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/datetime/base.h>
#include <util/digest/fnv.h>
#include <vector>
#include <thread>
#include <numeric>

namespace NBucketQuoterTest {

    // thread unsafe
    struct TMockTimer {
        using TTime = i64;
        static constexpr ui64 Resolution = 1'000'000ull; // microseconds
        static TTime CurrentTime;

        static TMockTimer::TTime Now() {
            return CurrentTime;
        }

        static ui64 Duration(TMockTimer::TTime from, TMockTimer::TTime to) {
            return to - from;
        }

        static void Sleep(TDuration duration) {
            CurrentTime += duration.MicroSeconds();
        }
    };

    template <typename Timer>
    void Sleep(TDuration duration) {
        ::Sleep(duration);
    }

    template <>
    void Sleep<TMockTimer>(TDuration duration);

    template <class Timer>
    using QuoterTemplate = TBucketQuoter<i64, TMutex, Timer>;

    template<class Timer>
    struct TTestScenario {
        void operator () (TBucketQuoter<i64, TMutex, Timer>& quoter, ui64 work_time, i64 wait_time,  ui64& store_result) const {
            typename Timer::TTime start = Timer::Now();
            work_time *= Timer::Resolution;
            while (Timer::Duration(start, Timer::Now()) < work_time) {
                while(!quoter.IsAvail()) {
                    NBucketQuoterTest::Sleep<Timer>(TDuration::MicroSeconds(quoter.GetWaitTime()));
                }
                quoter.Use(1);
                ++store_result;
                if (wait_time != 0) {
                    NBucketQuoterTest::Sleep<Timer>(TDuration::MicroSeconds(wait_time));
                }
            }
        }
    };

    template <class Timer, template <class TT> class Scenario>
    ui32 Run(ui64 thread_count, ui64 rps, ui64 seconds, i64 wait_time = 0) {
        TBucketQuoter<i64, TMutex, Timer> quoter(rps, rps);
        std::vector<std::thread> threads;
        std::vector<ui64> results;
        threads.reserve(thread_count);
        results.reserve(thread_count);
        for (ui32 i = 0; i < thread_count; ++i) {
            results.emplace_back(0);
            threads.emplace_back(Scenario<Timer>{}, std::ref(quoter), seconds, wait_time, std::ref(results.back()));
        }
        for (ui32 i = 0; i < thread_count; ++i) {
            threads[i].join();
        }
        return std::reduce(results.begin(), results.end(), 0, std::plus<>());
    }

}