#include "actorsystem.h"
#include "executor_pool_basic.h"
#include "hfunc.h"
#include "scheduler_basic.h"

#include <ydb/library/actors/util/should_continue.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(WaitingBenchs) {

    Y_UNIT_TEST(SpinPause) {
        const ui32 count = 1'000'000;
        ui64 startTs = GetCycleCountFast();
        for (ui32 idx = 0; idx < count; ++idx) {
            SpinLockPause();
        }
        ui64 stopTs = GetCycleCountFast();
        Cerr << Ts2Us(stopTs - startTs) / count << Endl;
        Cerr << double(stopTs - startTs) / count << Endl;
    }

    struct TThread : public ISimpleThread {
        static const ui64 CyclesInMicroSecond;
        std::array<ui64, 128> Hist;
        ui64 WakingTime = 0;
        ui64 AwakeningTime = 0;
        ui64 SleepTime = 0;
        ui64 IterationCount = 0;

        std::atomic<ui64> Awakens = 0;
        std::atomic<ui64> *OtherAwaken;

        TThreadParkPad OwnPad;
        TThreadParkPad *OtherPad;

        bool IsWaiting = false;

        void GoToWait() {
            ui64 start = GetCycleCountFast();
            OwnPad.Park();
            ui64 elapsed = GetCycleCountFast() - start;
            AwakeningTime += elapsed;
            ui64 idx = std::min(Hist.size() - 1, (elapsed - 20 * CyclesInMicroSecond) / CyclesInMicroSecond);
            Hist[idx]++;
            Awakens++;
        }

        void GoToWakeUp() {
            ui64 start = GetCycleCountFast();
            OtherPad->Unpark();
            ui64 elapsed = GetCycleCountFast() - start;
            WakingTime += elapsed;
            ui64 idx = std::min(Hist.size() - 1, elapsed / CyclesInMicroSecond);
            Hist[idx]++;
        }

        void GoToSleep() {
            ui64 start = GetCycleCountFast();
            ui64 stop = start;
            while (stop - start < 20 * CyclesInMicroSecond) {
                SpinLockPause();
                stop = GetCycleCountFast();
            }
            SleepTime += stop - start;
        }

        void* ThreadProc() {
            for (ui32 idx = 0; idx < IterationCount; ++idx) {
                if (IsWaiting) {
                    GoToWait();
                } else {
                    GoToSleep();
                    GoToWakeUp();
                    while(OtherAwaken->load() == idx) {
                        SpinLockPause();
                    }
                }
            }
            return nullptr;
        }
    };

    const ui64 TThread::CyclesInMicroSecond =  NHPTimer::GetCyclesPerSecond() * 0.000001;

    Y_UNIT_TEST(WakingUpTest) {
        TThread a, b;
        constexpr ui64 iterations = 100'000;
        std::fill(a.Hist.begin(), a.Hist.end(), 0);
        std::fill(b.Hist.begin(), b.Hist.end(), 0);
        a.IterationCount = iterations;
        b.IterationCount = iterations;
        a.IsWaiting = true;
        b.IsWaiting = false;
        b.OtherAwaken = &a.Awakens;
        a.OtherPad = &b.OwnPad;
        b.OtherPad = &a.OwnPad;
        a.Start();
        b.Start();
        a.Join();
        b.Join();

        ui64 awakeningTime = a.AwakeningTime + b.AwakeningTime - a.SleepTime - b.SleepTime;
        ui64 wakingUpTime = a.WakingTime + b.WakingTime;

        Cerr << "AvgAwakeningCycles: " << double(awakeningTime) / iterations << Endl;
        Cerr << "AvgAwakeningUs: " << Ts2Us(awakeningTime) / iterations  << Endl;
        Cerr << "AvgSleep20usCycles:" << double(b.SleepTime) / iterations << Endl;
        Cerr << "AvgSleep20usUs:" << Ts2Us(b.SleepTime) / iterations << Endl;
        Cerr << "AvgWakingUpCycles: " << double(wakingUpTime) / iterations  << Endl;
        Cerr << "AvgWakingUpUs: " << Ts2Us(wakingUpTime) / iterations  << Endl;

        Cerr << "AwakeningHist:\n";
        for (ui32 idx = 0; idx < a.Hist.size(); ++idx) {
            if (a.Hist[idx]) {
                if (idx + 1 != a.Hist.size()) {
                    Cerr << "  [" << idx << "us - " << idx + 1 << "us] " << a.Hist[idx] << Endl;
                } else {
                    Cerr << "  [" << idx << "us - ...] " << a.Hist[idx] << Endl;
                }
            }
        }

        Cerr << "WakingUpHist:\n";
        for (ui32 idx = 0; idx < b.Hist.size(); ++idx) {
            if (b.Hist[idx]) {
                if (idx + 1 != b.Hist.size()) {
                    Cerr << "  [" << idx << "us - " << idx + 1 << "us] " << b.Hist[idx] << Endl;
                } else {
                    Cerr << "  [" << idx << "us - ...] " << b.Hist[idx] << Endl;
                }
            }
        }
    }
} 