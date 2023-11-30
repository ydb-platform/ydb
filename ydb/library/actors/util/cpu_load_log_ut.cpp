#include "cpu_load_log.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/system/hp_timer.h>
#include <util/system/sanitizers.h>
#include <util/system/thread.h>

Y_UNIT_TEST_SUITE(CpuLoadLog) {

    TString PrintBits(ui64 x) {
        TStringStream str;
        for (ui64 i = 0; i < 64; ++i) {
            if (x & (1ull << i)) {
                str << "1";
            } else {
                str << "0";
            }
        }
        return str.Str();
    }

    Y_UNIT_TEST(FillAll) {
        TCpuLoadLog<5> log(100*BitDurationNs);
        log.RegisterBusyPeriod(101*BitDurationNs);
        log.RegisterBusyPeriod(163*BitDurationNs);
        log.RegisterBusyPeriod(164*BitDurationNs);
        log.RegisterBusyPeriod(165*BitDurationNs);
        log.RegisterBusyPeriod(331*BitDurationNs);
        log.RegisterBusyPeriod(340*BitDurationNs);
        log.RegisterBusyPeriod(420*BitDurationNs);
        log.RegisterBusyPeriod(511*BitDurationNs);
        //for (ui64 i = 0; i < 5; ++i) {
        //    Cerr << "i: " << i << " bits: " << PrintBits(log.Data[i]) << Endl;
        //}
        for (ui64 i = 0; i < 5; ++i) {
            UNIT_ASSERT_C((ui64(log.Data[i]) == ~ui64(0)), "Unequal at " << i << "\n      got: " << PrintBits(log.Data[i])
                    << "\n expected: " << PrintBits(~ui64(0)));
        }
    }

    Y_UNIT_TEST(PartialFill) {
        TCpuLoadLog<5> log(0*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b0ull));
        log.RegisterBusyPeriod(0*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b1ull));
        log.RegisterBusyPeriod(0*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b1ull));
        log.RegisterBusyPeriod(1*BitDurationNs/2);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b1ull));
        log.RegisterBusyPeriod(1*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b11ull));
        log.RegisterIdlePeriod(3*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b11ull));
        log.RegisterBusyPeriod(3*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0b1011ull));
        log.RegisterBusyPeriod(63*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits((~0ull)^0b0100ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0b0ull));
        log.RegisterBusyPeriod(128*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits((~0ull)^0b0100ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(0b1ull));
        log.RegisterBusyPeriod(1*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(~0ull));
        log.RegisterBusyPeriod(2*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(~0ull));
        log.RegisterBusyPeriod(64*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0b1ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(~0ull));
        log.RegisterIdlePeriod(128*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0b1ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(~0ull));
        log.RegisterIdlePeriod(192*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0b1ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(~0ull));
        log.RegisterBusyPeriod(192*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(~0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0b1ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(0b1ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(~0ull));
        log.RegisterIdlePeriod((192+5*64-1)*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(0b1ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(0ull));
        log.RegisterIdlePeriod((192+15*64)*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[0]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[1]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[2]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[3]), PrintBits(0ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log.Data[4]), PrintBits(0ull));
    }

    Y_UNIT_TEST(Estimator) {
        TCpuLoadLog<5> *log[10];
        log[0] = new TCpuLoadLog<5>(0*BitDurationNs);
        log[1] = new TCpuLoadLog<5>(0*BitDurationNs);
        TMinusOneCpuEstimator<5> estimator;


        for (ui64 i = 0; i < 5*64; i+=2) {
            log[0]->RegisterIdlePeriod(i*BitDurationNs);
            log[0]->RegisterBusyPeriod(i*BitDurationNs);
        }
        log[0]->RegisterIdlePeriod((5*64-2)*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log[0]->Data[0]),
                PrintBits(0b0101010101010101010101010101010101010101010101010101010101010101ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log[0]->Data[4]),
                PrintBits(0b0101010101010101010101010101010101010101010101010101010101010101ull));
        for (ui64 i = 0; i < 5*64-1; i+=2) {
            log[1]->RegisterIdlePeriod((i+1)*BitDurationNs);
            log[1]->RegisterBusyPeriod((i+1)*BitDurationNs);
        }
        log[1]->RegisterIdlePeriod((5*64-2+1)*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log[1]->Data[0]),
                PrintBits(0b1010101010101010101010101010101010101010101010101010101010101010ull));
        UNIT_ASSERT_VALUES_EQUAL(PrintBits(log[1]->Data[4]),
                PrintBits(0b1010101010101010101010101010101010101010101010101010101010101010ull));

        ui64 value = estimator.MaxLatencyIncreaseWithOneLessCpu(log, 2, (5*64)*BitDurationNs-1, 3*64*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(value/BitDurationNs, 1);

        value = estimator.MaxLatencyIncreaseWithOneLessCpu(log, 2, (5*64+10)*BitDurationNs, 3*64*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(value/BitDurationNs, 12);

        delete log[0];
        delete log[1];
    }

    Y_UNIT_TEST(Estimator2) {
        TCpuLoadLog<5> *log[2];
        log[0] = new TCpuLoadLog<5>(0*BitDurationNs);
        log[1] = new TCpuLoadLog<5>(0*BitDurationNs);
        TMinusOneCpuEstimator<5> estimator;

        for (ui64 i = 0; i < 5*64; i+=2) {
            log[0]->RegisterIdlePeriod(i*BitDurationNs);
            log[0]->RegisterBusyPeriod(i*BitDurationNs);
        }
        for (ui64 i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(PrintBits(log[0]->Data[i]),
                    PrintBits(0b0101010101010101010101010101010101010101010101010101010101010101ull));
        }
        for (ui64 i = 0; i < 5*64-1; i+=2) {
            log[1]->RegisterIdlePeriod((i+1)*BitDurationNs);
            log[1]->RegisterBusyPeriod((i+1)*BitDurationNs);
        }
        for (ui64 i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(PrintBits(log[1]->Data[i]),
                    PrintBits(0b1010101010101010101010101010101010101010101010101010101010101010ull));
        }

        log[0]->Data[2] = ~0ull;
        ui64 value = estimator.MaxLatencyIncreaseWithOneLessCpu(log, 2, (5*64-1)*BitDurationNs, 3*64*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(value/BitDurationNs, 32);

        delete log[0];
        delete log[1];
    }

    Y_UNIT_TEST(Estimator3) {
        TCpuLoadLog<5> *log[3];
        log[0] = new TCpuLoadLog<5>(0*BitDurationNs);
        log[1] = new TCpuLoadLog<5>(0*BitDurationNs);
        log[2] = new TCpuLoadLog<5>(0*BitDurationNs);
        TMinusOneCpuEstimator<5> estimator;

        for (ui64 i = 0; i < 5*64; i+=8) {
            log[0]->RegisterIdlePeriod(i*BitDurationNs);
            log[0]->RegisterBusyPeriod((i+3)*BitDurationNs);
            log[1]->RegisterIdlePeriod(i*BitDurationNs);
            log[1]->RegisterBusyPeriod((i+3)*BitDurationNs);
            log[2]->RegisterIdlePeriod(i*BitDurationNs);
            log[2]->RegisterBusyPeriod((i+3)*BitDurationNs);
        }
        for (ui64 i = 0; i < 5; ++i) {
            for (ui64 n = 0; n < 3; ++n) {
                UNIT_ASSERT_VALUES_EQUAL_C(PrintBits(log[n]->Data[i]),
                        PrintBits(0b0000111100001111000011110000111100001111000011110000111100001111ull),
                        " i: " << i << " n: " << n);
            }
        }

        ui64 value = estimator.MaxLatencyIncreaseWithOneLessCpu(log, 3, (5*64-5)*BitDurationNs, 3*64*BitDurationNs);
        UNIT_ASSERT_VALUES_EQUAL(value/BitDurationNs, 4);

        delete log[0];
        delete log[1];
        delete log[2];
    }
    /*
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

        auto workerFunc = [&](size_t threadIndex) {
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
    */
}
