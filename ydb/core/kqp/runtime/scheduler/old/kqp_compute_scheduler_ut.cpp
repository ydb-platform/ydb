#include "kqp_compute_scheduler.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NKqp::NSchedulerOld;

Y_UNIT_TEST_SUITE(TComputeScheduler) {

    TVector<TDuration> RunSimulation(TComputeScheduler& scheduler, TArrayRef<TSchedulerEntityHandle> handles, TMonotonic start, TDuration tick, TDuration length, TDuration batch) {
        TVector<TDuration> results(handles.size());
        for (TDuration t = TDuration::Zero(); t < length; t += tick) {
            auto now = start + t;
            for (size_t i = 0; i < handles.size(); ++i) {
                auto& handle = handles[i];
                if (!handle.Delay(now)) {
                    handle.TrackTime(batch, now);
                    results[i] += batch;
                }
            }
            scheduler.AdvanceTime(now);
        }
        return results;
    }


    Y_UNIT_TEST(TTotalLimits) {
        TComputeScheduler scheduler;
        scheduler.UpdateGroupShare("first", 0.4, TMonotonic::Zero(), std::nullopt);
        scheduler.UpdateGroupShare("second", 0.4, TMonotonic::Zero(), std::nullopt);
        scheduler.SetMaxDeviation(TDuration::MilliSeconds(10));
        scheduler.SetCapacity(2);
        TVector<TSchedulerEntityHandle> handles;
        handles.push_back(scheduler.Enroll("first", 1, TMonotonic::Zero()));
        handles.push_back(scheduler.Enroll("second", 1, TMonotonic::Zero()));
        auto times = RunSimulation(scheduler,
            handles,
            TMonotonic::Zero() + TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(1),
            TDuration::Seconds(2),
            TDuration::MilliSeconds(10));
        for (auto& time : times) {
            Cerr << time.MilliSeconds() << " " << (TDuration::Seconds(2) * 0.8).MilliSeconds() << Endl;
            UNIT_ASSERT_LE(time, TDuration::Seconds(2) * 0.8 + TDuration::MilliSeconds(10));
            UNIT_ASSERT_GE(time, TDuration::Seconds(2) * 0.8 - TDuration::MilliSeconds(10));
        }
    }

    Y_UNIT_TEST(QueryLimits) {
        TComputeScheduler scheduler;
        scheduler.UpdateGroupShare("first", 0.4, TMonotonic::Zero(), std::nullopt);
        scheduler.SetMaxDeviation(TDuration::MilliSeconds(1));
        scheduler.UpdatePerQueryShare("first", 0.5, TMonotonic::Zero());
        scheduler.SetCapacity(2);
        TVector<TSchedulerEntityHandle> handles;
        handles.push_back(scheduler.Enroll("first", 1, TMonotonic::Zero()));
        handles.push_back(scheduler.Enroll("first", 1, TMonotonic::Zero()));
        for (auto& handle : handles) {
            auto group = scheduler.MakePerQueryGroup(TMonotonic::Zero(), 0.5, "first");
            scheduler.AddToGroup(TMonotonic::Zero(), group, handle);
        }
        auto times = RunSimulation(scheduler,
            handles,
            TMonotonic::Zero() + TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(1),
            TDuration::Seconds(2),
            TDuration::MilliSeconds(10));
        for (auto& time : times) {
            Cerr << time.MilliSeconds() << " " << (TDuration::Seconds(2) * 0.4).MilliSeconds() << Endl;
            UNIT_ASSERT_LE(time, TDuration::Seconds(2) * 0.4 + TDuration::MilliSeconds(10));
            UNIT_ASSERT_GE(time, TDuration::Seconds(2) * 0.4 - TDuration::MilliSeconds(10));
        }
    }

    Y_UNIT_TEST(ResourceWeight) {
        TComputeScheduler scheduler;
        scheduler.UpdateGroupShare("first", 1, TMonotonic::Zero(), 1);
        scheduler.UpdateGroupShare("second", 1, TMonotonic::Zero(), 3);
        scheduler.SetMaxDeviation(TDuration::MilliSeconds(1));
        scheduler.SetCapacity(1);
        TVector<TSchedulerEntityHandle> handles;
        handles.push_back(scheduler.Enroll("first", 1, TMonotonic::Zero()));
        handles.push_back(scheduler.Enroll("second", 1, TMonotonic::Zero()));
        scheduler.AdvanceTime(TMonotonic::Zero());
        auto times = RunSimulation(scheduler,
            handles,
            TMonotonic::Zero() + TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(1),
            TDuration::Seconds(2),
            TDuration::MilliSeconds(10));

        Cerr << times[0].MilliSeconds() << " " << (TDuration::Seconds(2) /4).MilliSeconds() << Endl;
        UNIT_ASSERT_LE(times[0], TDuration::Seconds(2) /4 + TDuration::MilliSeconds(10));
        UNIT_ASSERT_GE(times[0], TDuration::Seconds(2) /4 - TDuration::MilliSeconds(10));

        Cerr << times[1].MilliSeconds() << " " << (TDuration::Seconds(2)*3 /4).MilliSeconds() << Endl;
        UNIT_ASSERT_LE(times[1], TDuration::Seconds(2)*3 /4 + TDuration::MilliSeconds(10));
        UNIT_ASSERT_GE(times[1], TDuration::Seconds(2)*3 /4 - TDuration::MilliSeconds(10));

        scheduler.Deregister(handles[1], TMonotonic::Zero() + TDuration::Seconds(2));
        handles.pop_back();

        scheduler.UpdateGroupShare("third", 0.5, TMonotonic::Zero() + TDuration::Seconds(2), 2);
        handles.push_back(scheduler.Enroll("third", 1, TMonotonic::Zero() + TDuration::Seconds(2)));
        times = RunSimulation(scheduler,
            handles,
            TMonotonic::Zero() + TDuration::Seconds(2),
            TDuration::MilliSeconds(1),
            TDuration::Seconds(2),
            TDuration::MilliSeconds(10));

        Cerr << times[0].MilliSeconds() << " " << (TDuration::Seconds(2) /2).MilliSeconds() << Endl;
        UNIT_ASSERT_LE(times[0], TDuration::Seconds(2) /2 + TDuration::MilliSeconds(10));
        UNIT_ASSERT_GE(times[0], TDuration::Seconds(2) /2 - TDuration::MilliSeconds(10));

        Cerr << times[1].MilliSeconds() << " " << (TDuration::Seconds(2) /2).MilliSeconds() << Endl;
        UNIT_ASSERT_LE(times[1], TDuration::Seconds(2) /2 + TDuration::MilliSeconds(10));
        UNIT_ASSERT_GE(times[1], TDuration::Seconds(2) /2 - TDuration::MilliSeconds(10));
    }
}
