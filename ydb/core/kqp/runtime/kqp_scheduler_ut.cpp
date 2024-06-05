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

    TVector<TDuration> RunSimulation(TComputeScheduler& scheduler, TVector<TProcess> processes, TDuration time, size_t executionUnits, TMonotonic start) {
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
            handles[i] =  scheduler.Enroll(processes[i].Group, processes[i].Weight, now);
            runQueue.push_back(i);
        }

        while (now < deadline) {
            size_t toRun = executionUnits;
            for (size_t i = 0; i < processes.size(); ++i) {
                groupnow[i] = handles[i].GroupNow(now);
                //Cerr << " " << handles[i].VRuntime() << "<" << scheduler.GroupNow(*handles[i], now) << " + " << (processes[i].Cuanta.MicroSeconds() / processes[i].Weight);
                UNIT_ASSERT_LE(handles[i].VRuntime(), handles[i].GroupNow(now) + (processes[i].Cuanta.MicroSeconds() / processes[i].Weight) + 1e-5);
            }
            //Cerr << Endl;

            for (size_t i = 0; i < processes.size(); ++i) {
                if (events[i]) {
                    if (events[i]->Time <= now) {
                        if (events[i]->Type == TEvent::EEventType::Wakeup) {
                            events[i].Clear();
                            runQueue.push_back(i);
                        } else {
                            auto delay = handles[i].CalcDelay(now);
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
                            UNIT_ASSERT(handles[i].CalcDelay(now).Defined());
                        }
                    }
                }

            }

            for (size_t i = 0; i < toRun && !runQueue.empty(); ++i) {
                size_t taskToRun = runQueue[0];
                events[taskToRun] = TEvent{TEvent::EEventType::Sleep, now + processes[taskToRun].Cuanta};
                runTimes[taskToRun] += processes[taskToRun].Cuanta;
                handles[taskToRun].TrackTime(processes[taskToRun].Cuanta, now);
                runQueue.erase(runQueue.begin());
            }


            TMonotonic newDeadline = TMonotonic::Max();
            for (auto& t : events) {
                if (t) {
                    newDeadline = Min(newDeadline, t->Time);
                }
            }
            now = newDeadline;
            //scheduler.AdvanceTime(now);
        }

        return runTimes;
    }

    void AssertEq(TDuration first, TDuration second, TDuration delta) {
        UNIT_ASSERT(first >= second - delta);
        UNIT_ASSERT(first <= second + delta);
    }

    Y_UNIT_TEST(SingleCoreSimple) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        auto start = TMonotonic::Now();

        TComputeScheduler::TDistributionRule rule{.Share = 0.5};
        rule.SubRules.push_back({.Share = 1, .Name = "first"});
        rule.SubRules.push_back({.Share = 1, .Name = "second"});
        scheduler.SetPriorities(rule, 1, start);

        TDuration all = TDuration::Seconds(10);

        auto result = RunSimulation(scheduler, {{"first", 1, TDuration::MilliSeconds(10)}, {"first", 1, TDuration::MilliSeconds(10)}}, all, 1, start);

        for (auto t : result) {
            AssertEq(t, all/4, TDuration::MilliSeconds(20));
        }
    }

    Y_UNIT_TEST(SingleCoreThird) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        auto start = TMonotonic::Now();

        TComputeScheduler::TDistributionRule rule{.Share = 0.5};
        rule.SubRules.push_back({.Share = 1, .Name = "first"});
        rule.SubRules.push_back({.Share = 1, .Name = "second"});
        scheduler.SetPriorities(rule, 1, start);

        TDuration all = TDuration::Seconds(10);

        auto result = RunSimulation(scheduler, {{"first", 1, TDuration::MilliSeconds(10)}, {"first", 2, TDuration::MilliSeconds(10)}}, all, 1, start);
        all = all/2;

        Cerr << result[0].MicroSeconds() << " " << result[1].MicroSeconds() << Endl;
        AssertEq(result[0], all/3, TDuration::MilliSeconds(20));
        AssertEq(result[1], 2*all/3, TDuration::MilliSeconds(20));
    }

    Y_UNIT_TEST(SingleCoreForth) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        TComputeScheduler::TDistributionRule rule{.Share = 0.5};
        auto start = TMonotonic::Now();
        rule.SubRules.push_back({.Share = 1, .Name = "first"});
        rule.SubRules.push_back({.Share = 1, .Name = "second"});

        scheduler.SetPriorities(rule, 1, start);

        TDuration all = TDuration::Seconds(10);

        auto result = RunSimulation(scheduler, {{"first", 1, TDuration::MilliSeconds(10)}, {"first", 3, TDuration::MilliSeconds(10)}}, all, 1, start);
        all = all/2;

        Cerr << result[0].MicroSeconds() << " " << result[1].MicroSeconds() << Endl;
        AssertEq(result[0], all/4, TDuration::MilliSeconds(20));
        AssertEq(result[1], 3*all/4, TDuration::MilliSeconds(20));
    }

    Y_UNIT_TEST(MultipleClients) {
        NKikimr::NKqp::TComputeScheduler scheduler;
        TComputeScheduler::TDistributionRule rule{.Share = 0.5};
        auto start = TMonotonic::Now();
        rule.SubRules.push_back({.Share = 1, .Name = "first"});
        rule.SubRules.push_back({.Share = 1, .Name = "second"});
        static const size_t executors = 10;

        scheduler.SetPriorities(rule, executors, start);
        TVector<TProcess> processes;
        const size_t processesCount = 3000;

        TDuration cuanta = TDuration::MicroSeconds(200);

        for (size_t i = 0; i < processesCount; ++i) {
            processes.push_back({"first", 1, cuanta});
        }

        TDuration all = TDuration::Seconds(3);


        auto result = RunSimulation(scheduler, processes, all, executors, start);

        for (auto res : result) {
            AssertEq(res, all/processesCount * executors /2, cuanta);
        }
    }
}
