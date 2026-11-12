#include "blobstorage_burst_detector.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TBurstDetectorTest) {
    Y_UNIT_TEST(RecoversFromGapsAndIgnoresLateSamples) {
        TBurstDetector light;
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        light.Initialize(counters);
        auto state = counters->GetCounter("BurstDetector_state");
        auto count = counters->GetCounter("BurstDetector_count");
        auto resyncs = counters->GetCounter("BurstDetector_resyncs", true);

        light.Set(false, 1);
        light.Set(true, 0); // startup inversion
        light.Set(true, 1); // duplicate head
        light.Set(true, 255); // near the end of the window
        light.Set(false, 255); // duplicate pending, first observation wins
        UNIT_ASSERT_VALUES_EQUAL(state->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(resyncs->Val(), 0);
        light.Set(true, 257); // gap beyond the window; forget the missing history
        UNIT_ASSERT_VALUES_EQUAL(state->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resyncs->Val(), 1);
        light.Set(false, 2); // a delayed producer cannot undo recovery
        light.Set(false, 255);
        light.Set(false, 257);
        UNIT_ASSERT_VALUES_EQUAL(state->Val(), 1);
        light.Set(false, 258);
        light.Set(true, 259);
        UNIT_ASSERT_VALUES_EQUAL(count->Val(), 2);

        // No truncation at either 32768 or 65536, nor at 32 bits.
        for (ui64 seqno : {ui64(40000), ui64(65536), ui64(1) << 32}) {
            light.Set(false, seqno);
            UNIT_ASSERT_VALUES_EQUAL(state->Val(), 0);
            light.Set(true, seqno + 1);
            light.Set(false, seqno - 65536); // old (or underflowed) sequence
            UNIT_ASSERT_VALUES_EQUAL(state->Val(), 1);
        }
        UNIT_ASSERT_VALUES_EQUAL(resyncs->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(count->Val(), 5);
    }

    Y_UNIT_TEST(KeepsProgressWithPermanentlyMissingSamples) {
        TBurstDetector light;
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        light.Initialize(counters);
        light.Set(false, 0);
        // Never deliver odd sequence numbers; pass a whole ui16 cycle without
        // unbounded allocation, head collisions, or permanently stopping progress.
        for (ui64 seqno = 2; seqno <= 131072; seqno += 2) {
            light.Set(true, seqno);
        }
        UNIT_ASSERT_GT(counters->GetCounter("BurstDetector_resyncs", true)->Val(), 0);
        light.Set(false, 131073);
        light.Set(true, 131074);
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("BurstDetector_state")->Val(), 1);
        UNIT_ASSERT_GE(light.GetCount(), 2);
    }

    Y_UNIT_TEST(ReorderingAcrossSequenceWrap) {
        TBurstDetector light;
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        light.Initialize(counters);
        light.Set(false, Max<ui64>() - 1);
        light.Set(false, 0);
        light.Set(true, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter("BurstDetector_state")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(light.GetCount(), 1);
        light.Set(true, 1);
        UNIT_ASSERT_VALUES_EQUAL(light.GetCount(), 2);
    }

    Y_UNIT_TEST(GapExpiresWithoutFillingTheBuffer) {
        TBurstDetector onUpdate;
        TBurstDetector onSet;
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto updateCounters = counters->GetSubgroup("test", "update");
        auto setCounters = counters->GetSubgroup("test", "set");
        onUpdate.Initialize(updateCounters);
        onSet.Initialize(setCounters);
        for (auto* light : {&onUpdate, &onSet}) {
            light->Set(false, 0);
            light->Set(false, 3);
            light->Set(true, 4); // newest must win, not the last arrival (2)
            light->Set(false, 2);
        }
        Sleep(TDuration::MilliSeconds(1100));
        onUpdate.Update();
        onSet.Set(false, 1); // timeout recovery before handling the late sample
        for (const auto& group : {updateCounters, setCounters}) {
            UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("BurstDetector_state")->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL(group->GetCounter("BurstDetector_resyncs", true)->Val(), 1);
            UNIT_ASSERT_GE(group->GetCounter("BurstDetector_greenMs", true)->Val(), 1000);
        }
        for (auto* light : {&onUpdate, &onSet}) {
            light->Set(false, 5);
            light->Set(true, 6);
            UNIT_ASSERT_VALUES_EQUAL(light->GetCount(), 2);
        }
    }

}

} // namespace NKikimr
