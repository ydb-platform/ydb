#include "max_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

#include <thread>

//////////
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/output.h>

#include <atomic>
#include <chrono>
#include <random>
#include <thread>


namespace NKikimr {
Y_UNIT_TEST_SUITE(TMaxTracker) {
    Y_UNIT_TEST(basic_test) {
        TMaxTracker tracker(3);
        tracker.Collect(5);
        tracker.Collect(10);
        ::NMonitoring::TDynamicCounters Counters;
        auto counter = Counters.GetCounter("test");
        *counter = 0;
        tracker.Init(counter);
        tracker.Update();
        // should report proper max value
        UNIT_ASSERT_EQUAL(*counter, 10);
        tracker.Update();
        tracker.Update();
        tracker.Update();
        // should reset max after N > slots updates called
        UNIT_ASSERT_EQUAL(*counter, 0);
    }

    Y_UNIT_TEST(two_threads_collect_update_ops_per_sec) {
        TMaxTracker tracker(5);
        ::NMonitoring::TDynamicCounters Counters;
        auto counter = Counters.GetCounter("test");
        *counter = 0;
        tracker.Init(counter);

        std::atomic<bool> stop{false};
        std::atomic<ui64> collectCount{0};

        std::thread collector([&] {
            std::mt19937 rng(42);
            std::uniform_int_distribution<i64> dist(0, 1'000'000);
            while (!stop.load(std::memory_order_relaxed)) {
                tracker.Collect(dist(rng));
                collectCount.fetch_add(1, std::memory_order_relaxed);
            }
        });

        std::thread updater([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                tracker.Update();
            }
        });

        const auto start = std::chrono::steady_clock::now();
        const auto duration = std::chrono::seconds(10);
        std::this_thread::sleep_for(duration);
        stop.store(true, std::memory_order_relaxed);

        collector.join();
        updater.join();

        const auto elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
            std::chrono::steady_clock::now() - start);
        const double opsPerSec = static_cast<double>(collectCount.load(std::memory_order_relaxed))
            / elapsed.count();

        Cerr << "Collect ops/sec: " << opsPerSec << Endl;
        UNIT_ASSERT(collectCount.load(std::memory_order_relaxed) > 0);
    }
}
}
