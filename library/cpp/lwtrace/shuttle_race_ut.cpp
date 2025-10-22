#include "shuttle.h"

#include "probe.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>
#include <util/system/thread.h>

#include <atomic>

using namespace NLWTrace;

class TMockShuttle: public IShuttle
{
public:
    TMockShuttle(ui64 traceIdx, ui64 spanId)
        : IShuttle(traceIdx, spanId)
    {}

protected:
    bool DoAddProbe(TProbe*, const TParams&, ui64) override
    {
        return true;
    }

    void DoEndOfTrack() override
    {}

    void DoDrop() override
    {}

    void DoSerialize(TShuttleTrace&) override
    {}

    bool DoFork(TShuttlePtr&) override
    {
        return true;
    }

    bool DoJoin(const TShuttlePtr&) override
    {
        return true;
    }
};

Y_UNIT_TEST_SUITE(TOrbitMultithreadedUsage)
{
    // Test HasShuttles() calls while modifying orbit
    Y_UNIT_TEST(HasShuttlesAndAddShuttle)
    {
        TOrbit orbit;
        std::atomic<bool> stopFlag{false};

        // user branching and atomic counters to increase race conditions
        // probability
        std::atomic<size_t> hasShuttlesCount{0};

        constexpr size_t numShuttles = 100;

        // Reader thread: continuously calls HasShuttles()
        auto reader = [&]()
        {
            while (!stopFlag.load()) {
                bool result = orbit.HasShuttles();
                if (result) {
                    hasShuttlesCount.fetch_add(1);
                }
            }
        };

        // Writer thread: continuously adds shuttles
        auto writer = [&]()
        {
            for (size_t i = 0; i < numShuttles; ++i) {
                orbit.AddShuttle(TShuttlePtr(new TMockShuttle(1, i)));
            }
        };

        TVector<THolder<TThread>> threads;
        threads.emplace_back(MakeHolder<TThread>(reader));
        threads.emplace_back(MakeHolder<TThread>(reader));
        threads.emplace_back(MakeHolder<TThread>(writer));

        for (auto& t: threads) {
            t->Start();
        }

        // Let writer finish
        threads[2]->Join();

        // Stop readers
        stopFlag.store(true);
        threads[0]->Join();
        threads[1]->Join();

        UNIT_ASSERT_LT(0, hasShuttlesCount.load());
        UNIT_ASSERT(orbit.HasShuttles());
    }

    // Test the race condition from the tsan tests crash: Fork() vs
    // HasShuttles()
    Y_UNIT_TEST(ForkAndHasShuttles)
    {
        TOrbit orbit;

        constexpr size_t numShuttles = 10;
        constexpr size_t numForks = 100;

        // Add some shuttles to orbit
        for (size_t i = 0; i < numShuttles; ++i) {
            orbit.AddShuttle(TShuttlePtr(new TMockShuttle(1, i)));
        }

        std::atomic<bool> stopFlag{false};

        // user branching and atomic counters to increase race conditions
        // probability
        std::atomic<size_t> forkCount{0};
        std::atomic<size_t> checkCount{0};

        // Thread 1: Continuously calls Fork()
        auto forker = [&]()
        {
            for (size_t i = 0; i < numForks; ++i) {
                TOrbit tempOrbit;
                if (orbit.Fork(tempOrbit)) {
                    forkCount.fetch_add(1);
                }
            }
        };

        // Thread 2: Continuously calls HasShuttles()
        auto checker = [&]()
        {
            while (!stopFlag.load()) {
                bool result = orbit.HasShuttles();
                if (result) {
                    checkCount.fetch_add(1);
                }
            }
        };

        TThread t1(forker);
        TThread t2(checker);

        t1.Start();
        t2.Start();

        t1.Join();
        stopFlag.store(true);
        t2.Join();

        UNIT_ASSERT_EQUAL(numForks, forkCount.load());
        UNIT_ASSERT_LT(0, checkCount.load());
        UNIT_ASSERT(orbit.HasShuttles());
    }

    // Test the Serialize() race condition
    Y_UNIT_TEST(SerializeAndHasShuttles)
    {
        TOrbit orbit;

        constexpr size_t numShuttles = 10;
        constexpr size_t numIterations = 100;
        constexpr size_t shuttlesPerIteration = 2;

        // Add shuttles
        for (size_t i = 0; i < numShuttles; ++i) {
            auto shuttle = new TMockShuttle(1, i);
            orbit.AddShuttle(TShuttlePtr(shuttle));
        }

        std::atomic<bool> stopFlag{false};

        // user branching and atomic counters to increase race conditions
        // probability
        std::atomic<size_t> serializeCount{0};
        std::atomic<size_t> checkCount{0};

        // Thread 1: Serialize (modifies shuttle chain via Drop/Detach/Swap)
        auto serializer = [&]()
        {
            for (size_t i = 0; i < numIterations; ++i) {
                TShuttleTrace trace;
                orbit.Serialize(1, trace);
                serializeCount.fetch_add(1);

                // Re-add shuttles for next iteration
                for (size_t j = 0; j < shuttlesPerIteration; ++j) {
                    auto shuttle = new TMockShuttle(1, i * numIterations + j);
                    orbit.AddShuttle(TShuttlePtr(shuttle));
                }
            }
        };

        // Thread 2: Check HasShuttles
        auto checker = [&]()
        {
            while (!stopFlag.load()) {
                bool hasShuttles = orbit.HasShuttles();
                if (hasShuttles) {
                    checkCount.fetch_add(1);
                }
            }
        };

        TThread t1(serializer);
        TThread t2(checker);

        t1.Start();
        t2.Start();

        t1.Join();
        stopFlag.store(true);
        t2.Join();

        UNIT_ASSERT(orbit.HasShuttles());
    }

    // Stress test: Many threads doing many operations
    Y_UNIT_TEST(StressTestMultipleOperations)
    {
        TOrbit orbit;

        // user branching and atomic counters to increase race conditions
        // probability
        std::atomic<size_t> totalOperations{0};
        std::atomic<size_t> addCount{0};
        std::atomic<size_t> checkCount{0};
        std::atomic<size_t> serializeCount{0};
        const size_t numThreads = 8;
        const size_t operationsPerThread = 1000;

        auto worker = [&](size_t threadId)
        {
            for (size_t i = 0; i < operationsPerThread; ++i) {
                // Mix of operations based on iteration
                if (i % 5 == 0) {
                    auto shuttle = new TMockShuttle(threadId, i);
                    orbit.AddShuttle(TShuttlePtr(shuttle));
                    addCount.fetch_add(1);
                } else if (i % 5 == 1) {
                    bool hasShuttles = orbit.HasShuttles();
                    if (hasShuttles) {
                        checkCount.fetch_add(1);
                    }
                } else if (i % 5 == 2) {
                    TOrbit childOrbit;
                    orbit.Fork(childOrbit);
                } else if (i % 5 == 3) {
                    TShuttleTrace trace;
                    orbit.Serialize(threadId, trace);
                    serializeCount.fetch_add(1);
                } else {
                    bool hasShuttle = orbit.HasShuttle(threadId);
                    if (hasShuttle) {
                        checkCount.fetch_add(1);
                    }
                }
                totalOperations.fetch_add(1);
            }
        };

        TVector<THolder<TThread>> threads;
        for (size_t i = 0; i < numThreads; ++i) {
            threads.emplace_back(
                MakeHolder<TThread>([&worker, i]() { worker(i); }));
        }

        for (auto& t: threads) {
            t->Start();
        }

        for (auto& t: threads) {
            t->Join();
        }

        UNIT_ASSERT_EQUAL(
            numThreads * operationsPerThread,
            totalOperations.load());
    }
}
