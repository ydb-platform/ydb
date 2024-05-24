#include "kqp_compute_scheduler.h"

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp;
using namespace NMonotonic;

Y_UNIT_TEST_SUITE(TKqpComputeScheduler) {

    struct TProcess {
        TString Group;
        double Weight;
        TDuration Cuanta;
    };

    TVector<TDuration> RunSimulation(TComputeScheduler& scheduler, TVector<TProcess> processes, TDuration time, size_t executionUnits) {
        auto start = TMonotonic::Now();
        TMonotonic now = start;
        TMonotonic deadline = now + time;
        scheduler.AdvanceTime(now);
        TVector<TDuration> runTimes(processes.size());
        struct TEvent {
            enum EEventType {
                Wakeup,
                Sleep,
            } Type;

            TMonotonic Time;
        };

        
        TVector<TMaybe<TEvent>> events(processes.size());
        //TVector<TMaybe<TMonotonic>> wakeup(processes.size());
        //TVector<TMaybe<TMonotonic>> sleep(processes.size());
        TVector<TSchedulerEntityHandle> handles(processes.size());
        TVector<size_t> runQueue;
        TVector<double> groupnow(processes.size());

        for (size_t i = 0; i < processes.size(); ++i) {
            handles[i] =  scheduler.Enroll(processes[i].Group, processes[i].Weight);
            runQueue.push_back(i);
        }

        while (now < deadline) {
            size_t toRun = executionUnits;
            for (size_t i = 0; i < processes.size(); ++i) {
                groupnow[i] = scheduler.GroupNow(*handles[i], now);
                //Cerr << " " << scheduler.Now(*handles[i]) << "<" << scheduler.GroupNow(*handles[i]);
                UNIT_ASSERT(handles[i].VRuntime() <= scheduler.GroupNow(*handles[i], now)   + (processes[i].Cuanta / processes[i].Weight).MicroSeconds());
            }
            Cerr << Endl;

            for (size_t i = 0; i < processes.size(); ++i) {
                if (events[i]) {
                    if (events[i]->Time <= now) {
                        if (events[i]->Type == TEvent::EEventType::Wakeup) {
                            events[i].Clear();
                            runQueue.push_back(i);
                        } else {
                            auto delay = scheduler.CalcDelay(*handles[i], now);
                            if (delay) {
                                events[i] = TEvent{TEvent::EEventType::Wakeup, now + *delay};
                            } else {
                                events[i].Clear();
                                runQueue.push_back(i);
                            }
                        }
                    } else {
                        if (events[i]->Type == TEvent::EEventType::Sleep) {
                            toRun -= 1;
                        } else {
                            //UNIT_ASSERT(scheduler.CalcDelay(*handles[i], now - TDuration::MicroSeconds(1)).GetOrElse(TDuration::Zero()) > TDuration::Zero());
                        }
                    }
                }

            }

            for (size_t i = 0; i < toRun && !runQueue.empty(); ++i) {
                size_t taskToRun = runQueue[0];
                events[taskToRun] = TEvent{TEvent::EEventType::Sleep, now + processes[taskToRun].Cuanta};
                runTimes[taskToRun] += processes[taskToRun].Cuanta;
                scheduler.TrackTime(*handles[taskToRun], processes[taskToRun].Cuanta);
                runQueue.erase(runQueue.begin());
            }


            TMonotonic newDeadline = TMonotonic::Max();
            for (auto& t : events) {
                if (t) {
                    newDeadline = Min(newDeadline, t->Time);
                }
            }
            now = newDeadline;
            scheduler.AdvanceTime(now);
        }

        return runTimes;
    }

    void AssertEq(TDuration first, TDuration second, TDuration delta) {
        UNIT_ASSERT(first >= second - delta);
        UNIT_ASSERT(first <= second + delta);
    }

    Y_UNIT_TEST(SingleCoreSimple) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        THashMap<TString, double> priorities;
        priorities["first"] = 1;
        priorities["second"] = 1;
        scheduler.SetPriorities(priorities, 1);

        TDuration all = TDuration::Seconds(10);

        auto result = RunSimulation(scheduler, {{"first", 1, TDuration::MilliSeconds(10)}, {"first", 1, TDuration::MilliSeconds(10)}}, all, 1);

        for (auto t : result) {
            AssertEq(t, all/4, TDuration::MilliSeconds(20));
        }
    }

    Y_UNIT_TEST(SingleCoreThird) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        THashMap<TString, double> priorities;
        priorities["first"] = 1;
        priorities["second"] = 1;
        scheduler.SetPriorities(priorities, 1);

        TDuration all = TDuration::Seconds(10);


        auto result = RunSimulation(scheduler, {{"first", 1, TDuration::MilliSeconds(10)}, {"first", 2, TDuration::MilliSeconds(10)}}, all, 1);
        all = all/2;

        Cerr << result[0].MicroSeconds() << " " << result[1].MicroSeconds() << Endl;
        AssertEq(result[0], all/3, TDuration::MilliSeconds(20));
        AssertEq(result[1], 2*all/3, TDuration::MilliSeconds(20));
    }

    Y_UNIT_TEST(SingleCoreForth) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        THashMap<TString, double> priorities;
        priorities["first"] = 1;
        priorities["second"] = 1;
        scheduler.SetPriorities(priorities, 1);

        TDuration all = TDuration::Seconds(10);


        auto result = RunSimulation(scheduler, {{"first", 1, TDuration::MilliSeconds(10)}, {"first", 3, TDuration::MilliSeconds(10)}}, all, 1);
        all = all/2;

        Cerr << result[0].MicroSeconds() << " " << result[1].MicroSeconds() << Endl;
        AssertEq(result[0], all/4, TDuration::MilliSeconds(20));
        AssertEq(result[1], 3*all/4, TDuration::MilliSeconds(20));
    }
}
