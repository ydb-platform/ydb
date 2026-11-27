#include "skeleton_burst_counter_publisher.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBurstCounterPublisherTest) {
    Y_UNIT_TEST(PublishesCappedDebtAndStopsOnPoison) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&, TDuration, TInstant&) {
            return false;
        });
        runtime.Initialize();
        TCostMetricsParameters parameters;
        parameters.TimeProvider = runtime.GetTimeProvider();
        const TIntrusivePtr<NMonitoring::TDynamicCounters> counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        const TIntrusivePtr<TBlobStorageGroupInfo> group = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
        const TIntrusivePtr<TVDiskContext> vctx = MakeIntrusive<TVDiskContext>(TActorId(), group->PickTopology(),
            counters, TVDiskID(0, 1, 0, 0, 0), nullptr, NPDisk::DEVICE_TYPE_ROT);
        vctx->CostTracker = std::make_unique<TBsCostTracker>(group->Type, NPDisk::DEVICE_TYPE_ROT, counters, parameters);
        const TIntrusivePtr<NMonitoring::TDynamicCounters> cost = counters->FindSubgroup("subsystem", "advancedCost");
        const NMonitoring::TDynamicCounters::TCounterPtr red = cost->FindCounter("BurstDetector_redMs");
        const NMonitoring::TDynamicCounters::TCounterPtr green = cost->FindCounter("BurstDetector_greenMs");

        vctx->CostTracker->CountRequest(10'000'000'000);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(250));
        UNIT_ASSERT_VALUES_EQUAL(red->Val(), 0);
        const TActorId publisher = runtime.Register(CreateBurstCounterPublisher(vctx, TDuration::Seconds(1)));
        TDispatchOptions bootstrap;
        bootstrap.OnlyMailboxes.emplace_back(publisher.NodeId(), publisher.Hint());
        bootstrap.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap);
        runtime.DispatchEvents(bootstrap);
        UNIT_ASSERT_VALUES_EQUAL(red->Val(), 200);
        UNIT_ASSERT_VALUES_EQUAL(green->Val(), 50);

        runtime.SimulateSleep(TDuration::MilliSeconds(1100));
        UNIT_ASSERT_VALUES_EQUAL(red->Val(), 200);
        UNIT_ASSERT_VALUES_EQUAL(green->Val(), 1050);
        runtime.Send(publisher, runtime.AllocateEdgeActor(), new TEvents::TEvPoison);
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT(!runtime.FindActor(publisher));
        UNIT_ASSERT_VALUES_EQUAL(green->Val(), 1050);
    }
}

} // namespace NKikimr
