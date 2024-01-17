#include "unordered_cache.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/system/hp_timer.h>
#include <util/system/sanitizers.h>
#include <util/system/thread.h>

Y_UNIT_TEST_SUITE(UnorderedCache) {

    void DoOnePushOnePop(ui64 count) {
        TUnorderedCache<ui64> queue;

        ui64 readRotation = 0;
        ui64 writeRotation = 0;

        auto popped = queue.Pop(readRotation++);
        UNIT_ASSERT_VALUES_EQUAL(popped, 0u);

        for (ui64 i = 0; i < count; ++i) {
            queue.Push(i + 1, writeRotation++);
            popped = queue.Pop(readRotation++);
            UNIT_ASSERT_VALUES_EQUAL(popped, i + 1);

            popped = queue.Pop(readRotation++);
            UNIT_ASSERT_VALUES_EQUAL(popped, 0u);
        }
    }

    Y_UNIT_TEST(OnePushOnePop) {
        DoOnePushOnePop(1);
    }

    Y_UNIT_TEST(OnePushOnePop_Repeat1M) {
        DoOnePushOnePop(1000000);
    }

    /**
     * Simplified thread spawning for testing
     */
    class TWorkerThread : public ISimpleThread {
    private:
        std::function<void()> Func;
        double Time = 0.0;

    public:
        TWorkerThread(std::function<void()> func)
            : Func(std::move(func))
        { }

        double GetTime() const {
            return Time;
        }

        static THolder<TWorkerThread> Spawn(std::function<void()> func) {
            THolder<TWorkerThread> thread = MakeHolder<TWorkerThread>(std::move(func));
            thread->Start();
            return thread;
        }

    private:
        void* ThreadProc() noexcept override {
            THPTimer timer;
            Func();
            Time = timer.Passed();
            return nullptr;
        }
    };

    void DoConcurrentPushPop(size_t threads, ui64 perThreadCount) {
        // Concurrency factor 4 is up to 16 threads
        TUnorderedCache<ui64, 512, 4> queue;

        auto workerFunc = [&](size_t threadIndex) {
            ui64 readRotation = 0;
            ui64 writeRotation = 0;
            ui64 readsDone = 0;
            ui64 writesDone = 0;
            for (;;) {
                bool canRead = readsDone < writesDone;
                bool canWrite = writesDone < perThreadCount;
                if (!canRead && !canWrite) {
                    break;
                }
                if (canRead && canWrite) {
                    // Randomly choose between read and write
                    if (RandomNumber<ui64>(2)) {
                        canRead = false;
                    } else {
                        canWrite = false;
                    }
                }
                if (canRead) {
                    ui64 popped = queue.Pop(readRotation++);
                    if (popped) {
                        ++readsDone;
                    }
                }
                if (canWrite) {
                    queue.Push(1 + writesDone * threads + threadIndex, writeRotation++);
                    ++writesDone;
                }
            }
        };

        TVector<THolder<TWorkerThread>> workers(threads);
        for (size_t i = 0; i < threads; ++i) {
            workers[i] = TWorkerThread::Spawn([workerFunc, i]() {
                workerFunc(i);
            });
        }

        double maxTime = 0;
        for (size_t i = 0; i < threads; ++i) {
            workers[i]->Join();
            maxTime = Max(maxTime, workers[i]->GetTime());
        }

        auto popped = queue.Pop(0);
        UNIT_ASSERT_VALUES_EQUAL(popped, 0u);

        Cerr << "Concurrent with " << threads << " threads: " << maxTime << " seconds" << Endl;
    }

    void DoConcurrentPushPop_3times(size_t threads, ui64 perThreadCount) {
        for (size_t i = 0; i < 3; ++i) {
            DoConcurrentPushPop(threads, perThreadCount);
        }
    }

    static constexpr ui64 PER_THREAD_COUNT = NSan::PlainOrUnderSanitizer(1000000, 100000);

    Y_UNIT_TEST(ConcurrentPushPop_1thread) { DoConcurrentPushPop_3times(1, PER_THREAD_COUNT); }
    Y_UNIT_TEST(ConcurrentPushPop_2threads) { DoConcurrentPushPop_3times(2, PER_THREAD_COUNT); }
    Y_UNIT_TEST(ConcurrentPushPop_4threads) { DoConcurrentPushPop_3times(4, PER_THREAD_COUNT); }
    Y_UNIT_TEST(ConcurrentPushPop_8threads) { DoConcurrentPushPop_3times(8, PER_THREAD_COUNT); }
    Y_UNIT_TEST(ConcurrentPushPop_16threads) { DoConcurrentPushPop_3times(16, PER_THREAD_COUNT); }
}
