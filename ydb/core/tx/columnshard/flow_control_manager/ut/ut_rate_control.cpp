#include <ydb/core/tx/columnshard/flow_control_manager/delayed_reject_queue.h>
#include <ydb/core/tx/columnshard/flow_control_manager/drain_rate_controller.h>
#include <ydb/core/tx/columnshard/flow_control_manager/node_state_map.h>
#include <ydb/core/tx/columnshard/flow_control_manager/rate_bucket.h>
#include <ydb/core/tx/columnshard/flow_control_manager/wait_queue.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>

// Unit tests for the parts of the flow control manager that carry no actor state. They run
// without an actor system or a scheduler, so every clock is an explicit TInstant and the
// arithmetic can be checked directly instead of inferred from admitted/rejected writes.
// End-to-end behaviour through the actor is covered by ydb/core/tx/columnshard/ut_rw.

using namespace NKikimr::NColumnShard::NFlowControl;

namespace {

const TInstant T0 = TInstant::Seconds(1'000'000);

TDrainRateController::TDrainRateParams MakeParams() {
    // Explicit values rather than the proto defaults: these tests assert on exact arithmetic, and
    // should not start failing because a production default was retuned.
    TDrainRateController::TDrainRateParams params;
    params.RMin = 1.0;
    params.RMax = 1000.0;
    params.RStart = 10.0;
    params.AimdBeta = 0.5;
    params.CubicRecoveryTargetSec = 10.0;
    params.CubicProbePercent = 10.0;
    params.RMinBytes = 1'000.0;
    params.RMaxBytes = 10'000'000.0;
    params.RStartBytes = 100'000.0;
    params.AimdBetaBytes = 0.5;
    return params;
}

TDrainState MakeState(TInstant now, bool anyHot = false, bool queueEmpty = false) {
    TDrainState state;
    state.Now = now;
    state.AnyHotNode = anyHot;
    state.QueueEmpty = queueEmpty;
    return state;
}

TWaiter MakeWaiter(TVector<ui64> tabletIds, TVector<ui32> nodes, ui64 batchSize) {
    TWaiter waiter;
    waiter.TabletIds = std::move(tabletIds);
    waiter.TargetNodes = std::move(nodes);
    waiter.BatchSize = batchSize;
    waiter.WaitDeadline = T0 + TDuration::Seconds(10);
    waiter.EnqueuedAt = T0;
    return waiter;
}

}   // namespace

Y_UNIT_TEST_SUITE(TRateBucketTest) {
    Y_UNIT_TEST(SeedStartsInProbePhaseWithFullSoftCap) {
        TRateBucket bucket(1.0);
        bucket.Seed(1.0, 100.0, 10.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 10.0, 1e-9);
        // Tokens start at one cohort's worth so the first waiter drains immediately.
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetTokens(), 10.0, 1e-9);
        // No cut to recover from yet: the peak is the seed and there is no curve.
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetWmax(), 10.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetCubicC(), 0.0, 1e-9);
    }

    Y_UNIT_TEST(ZeroBoundsMeanFloorAndNoLimit) {
        TRateBucket bucket(1'000'000.0);
        bucket.Seed(0.0, 0.0, 5'000'000.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.EffectiveMin(), 1'000'000.0, 1e-9);
        UNIT_ASSERT(std::isinf(bucket.EffectiveMax()));
    }

    Y_UNIT_TEST(SoftCapNeverBelowExtraFloor) {
        TRateBucket bucket(1.0);
        bucket.Seed(0.0, 0.0, 100.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.SoftCap(), 100.0, 1e-9);
        // A single request larger than one second of budget must still be able to accumulate.
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.SoftCap(4'000.0), 4'000.0, 1e-9);
    }

    Y_UNIT_TEST(RefillIsPacedAndCapped) {
        TRateBucket bucket(1.0);
        bucket.Seed(1.0, 100.0, 10.0);
        bucket.Pay(8.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetTokens(), 2.0, 1e-9);

        // The very first refill only arms the clock.
        bucket.RefillTokens(T0, 0.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetTokens(), 2.0, 1e-9);

        bucket.RefillTokens(T0 + TDuration::MilliSeconds(500), 0.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetTokens(), 7.0, 1e-9);

        // An hour of idling still yields at most one soft cap.
        bucket.RefillTokens(T0 + TDuration::Hours(1), 0.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetTokens(), 10.0, 1e-9);
    }

    Y_UNIT_TEST(ScaleRateStopsAtTheFloor) {
        TRateBucket bucket(1.0);
        bucket.Seed(5.0, 100.0, 10.0);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.ScaleRate(0.5), 10.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 5.0, 1e-9);
        // Already on the floor: further cuts cannot freeze the queue.
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.ScaleRate(0.01), 5.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 5.0, 1e-9);
    }

    Y_UNIT_TEST(CubicRecoveryReachesWmaxAtKTarget) {
        constexpr double k = 10.0;
        TRateBucket bucket(1.0);
        bucket.Seed(1.0, 1000.0, 100.0);
        const double prev = bucket.ScaleRate(0.5);
        bucket.StartCubicEpoch(prev, k);
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetWmax(), 100.0, 1e-9);

        // W(0) is the post-cut rate, so the first step right after the cut changes nothing.
        UNIT_ASSERT(!bucket.Grow(0.0, k, 0.0, 1000.0));
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 50.0, 1e-9);

        // The concave part of the curve: fast at first, then flattening toward the peak.
        UNIT_ASSERT(bucket.Grow(5.0, k, 0.0, 1000.0));
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 93.75, 1e-9);

        UNIT_ASSERT(bucket.Grow(k, k, 0.0, 1000.0));
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 100.0, 1e-9);
    }

    Y_UNIT_TEST(ProbeAddsFractionOfWmaxPastRecovery) {
        TRateBucket bucket(1.0);
        bucket.Seed(1.0, 1000.0, 100.0);
        bucket.EnterProbePhase();
        // Past KTarget with no curve: lift to Wmax (already there) and probe 10% above it.
        UNIT_ASSERT(bucket.Grow(20.0, 10.0, 10.0, 1000.0));
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 110.0, 1e-9);
        // Wmax is the last loss peak and must not follow the probe up.
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetWmax(), 100.0, 1e-9);
    }

    Y_UNIT_TEST(GrowthNeverExceedsCap) {
        TRateBucket bucket(1.0);
        bucket.Seed(1.0, 1000.0, 100.0);
        bucket.EnterProbePhase();
        UNIT_ASSERT(bucket.Grow(20.0, 10.0, 50.0, 120.0));
        UNIT_ASSERT_DOUBLES_EQUAL(bucket.GetRate(), 120.0, 1e-9);
        // Already at the cap: nothing to do.
        UNIT_ASSERT(!bucket.Grow(30.0, 10.0, 50.0, 120.0));
    }
}

Y_UNIT_TEST_SUITE(TWaitQueueTest) {
    Y_UNIT_TEST(KeepsFifoOrderAcrossOutOfOrderRemoval) {
        TWaitQueue queue;
        const ui64 first = queue.Enqueue(MakeWaiter({ 1 }, { 1 }, 100));
        const ui64 second = queue.Enqueue(MakeWaiter({ 2 }, { 2 }, 200));
        const ui64 third = queue.Enqueue(MakeWaiter({ 3 }, { 3 }, 300));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3);

        const auto erased = queue.Erase(second);
        UNIT_ASSERT(erased.has_value());
        UNIT_ASSERT_VALUES_EQUAL(erased->BatchSize, 200);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);

        TVector<ui64> ids;
        for (const auto& waiter : queue.GetOrder()) {
            ids.push_back(waiter.WaiterId);
        }
        UNIT_ASSERT_VALUES_EQUAL(ids.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ids[0], first);
        UNIT_ASSERT_VALUES_EQUAL(ids[1], third);
    }

    Y_UNIT_TEST(EraseIsIdempotentForUnknownIds) {
        TWaitQueue queue;
        const ui64 id = queue.Enqueue(MakeWaiter({ 1 }, { 1 }, 100));
        UNIT_ASSERT(queue.Erase(id).has_value());
        UNIT_ASSERT(!queue.Erase(id).has_value());
        UNIT_ASSERT(!queue.Contains(id));
        UNIT_ASSERT(queue.Empty());
    }

    Y_UNIT_TEST(PerNodeCountsDriveTheNoJumpRule) {
        TWaitQueue queue;
        // One waiter spanning two nodes, plus another on the second node only.
        const ui64 spanning = queue.Enqueue(MakeWaiter({ 1, 2 }, { 10, 20 }, 100));
        const ui64 single = queue.Enqueue(MakeWaiter({ 2 }, { 20 }, 100));

        UNIT_ASSERT(queue.HasWaitersOnAnyNode({ 10 }));
        UNIT_ASSERT(queue.HasWaitersOnAnyNode({ 20 }));
        UNIT_ASSERT(!queue.HasWaitersOnAnyNode({ 30 }));

        queue.Erase(spanning);
        // Node 10 is now free, but node 20 still has the second waiter.
        UNIT_ASSERT(!queue.HasWaitersOnAnyNode({ 10 }));
        UNIT_ASSERT(queue.HasWaitersOnAnyNode({ 20 }));

        queue.Erase(single);
        UNIT_ASSERT(!queue.HasWaitersOnAnyNode({ 10, 20, 30 }));
    }

    Y_UNIT_TEST(FindReturnsMutableWaiter) {
        TWaitQueue queue;
        const ui64 id = queue.Enqueue(MakeWaiter({ 1 }, { 1 }, 100));
        auto* waiter = queue.Find(id);
        UNIT_ASSERT(waiter);
        waiter->DrainScheduled = true;
        UNIT_ASSERT(queue.Find(id)->DrainScheduled);
        UNIT_ASSERT(!queue.Find(id + 1));
    }
}

Y_UNIT_TEST_SUITE(TDelayedRejectQueueTest) {
    Y_UNIT_TEST(FiresEachEntryExactlyOnce) {
        TDelayedRejectQueue queue;
        const NActors::TActorId replyTo(1, 1, 1, 1);
        const ui64 first = queue.Enqueue(replyTo, T0 + TDuration::Seconds(1));
        const ui64 second = queue.Enqueue(replyTo, T0 + TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        UNIT_ASSERT(first != second);

        auto fired = queue.Erase(first);
        UNIT_ASSERT(fired.has_value());
        UNIT_ASSERT_VALUES_EQUAL(fired->RejectId, first);
        UNIT_ASSERT(fired->ReplyTo == replyTo);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);

        // A late timer for an entry that already fired must not resurrect it.
        UNIT_ASSERT(!queue.Erase(first).has_value());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
    }
}

Y_UNIT_TEST_SUITE(TNodeStateMapTest) {
    Y_UNIT_TEST(HotEdgeIsReportedOnlyOnce) {
        TNodeStateMap nodes;
        UNIT_ASSERT(nodes.MarkHot(1, 1));   // empty -> non-empty: this is what cuts the rate
        UNIT_ASSERT(!nodes.MarkHot(2, 1));   // already hot
        UNIT_ASSERT(!nodes.MarkHot(1, 2));   // same node, newer generation
        UNIT_ASSERT_VALUES_EQUAL(nodes.HotCount(), 2);
    }

    Y_UNIT_TEST(ReadyIgnoresStaleGenerations) {
        TNodeStateMap nodes;
        nodes.MarkHot(1, 5);
        // A READY from before the overload was published must not clear it.
        UNIT_ASSERT(!nodes.MarkReady(1, 4));
        UNIT_ASSERT(nodes.AnyHot());
        UNIT_ASSERT(nodes.MarkReady(1, 5));
        UNIT_ASSERT(!nodes.AnyHot());
    }

    Y_UNIT_TEST(ReadyReportsOnlyGenuineCoolEdge) {
        TNodeStateMap nodes;
        // The overload manager re-publishes the current status to every FCM once a minute, so READY
        // for a node that was never hot is routine. It must not look like a recovery: the caller
        // reacts by clamping tokens and freezing growth for a cooldown period.
        UNIT_ASSERT(!nodes.MarkReady(1, 1));

        nodes.MarkHot(1, 1);
        nodes.MarkHot(2, 1);
        // Still one hot node left, so this is not the edge either.
        UNIT_ASSERT(!nodes.MarkReady(1, 1));
        UNIT_ASSERT(nodes.MarkReady(2, 1));
        // A repeat of the same READY is a re-publication, not a second recovery.
        UNIT_ASSERT(!nodes.MarkReady(2, 1));
    }

    Y_UNIT_TEST(StaleOverloadedAfterReadyIsIgnored) {
        TNodeStateMap nodes;
        UNIT_ASSERT(nodes.MarkHot(1, 5));
        UNIT_ASSERT(nodes.MarkReady(1, 5));
        UNIT_ASSERT(!nodes.AnyHot());
        // Delayed OVERLOADED from before the READY must not re-heat the node: the watermark now
        // survives the cool edge, so generation 4 is rejected.
        UNIT_ASSERT(!nodes.MarkHot(1, 4));
        UNIT_ASSERT(!nodes.AnyHot());
        UNIT_ASSERT(nodes.MarkHot(1, 6));
        UNIT_ASSERT(nodes.AnyHot());
    }

    Y_UNIT_TEST(HighGenerationFromRestartSupersedesOldWatermark) {
        TNodeStateMap nodes;
        // Pre-restart publishes left a high watermark on a surviving FCM.
        UNIT_ASSERT(nodes.MarkHot(1, 10'000));
        UNIT_ASSERT(nodes.MarkReady(1, 10'000));
        // A counter that restarted at zero would be ignored forever on a cool node.
        UNIT_ASSERT(!nodes.MarkHot(1, 1));
        UNIT_ASSERT(!nodes.AnyHot());
        // Seeding the publisher from wall clock (or any value above the old watermark) lets the
        // first post-restart OVERLOADED take effect immediately.
        UNIT_ASSERT(nodes.MarkHot(1, TInstant::Now().GetValue()));
        UNIT_ASSERT(nodes.AnyHot());
    }

    Y_UNIT_TEST(UnknownTabletFailsOpen) {
        TNodeStateMap nodes;
        nodes.MarkHot(1, 1);
        // Location unknown: gating on a guess would stall writes we know nothing about.
        UNIT_ASSERT(nodes.IsAdmitAllowed({ 100 }));

        nodes.SetTabletNode(100, 1);
        UNIT_ASSERT(!nodes.IsAdmitAllowed({ 100 }));
        // One hot node gates the whole request, since the client cannot partially succeed.
        nodes.SetTabletNode(200, 2);
        UNIT_ASSERT(!nodes.IsAdmitAllowed({ 200, 100 }));
        UNIT_ASSERT(nodes.IsAdmitAllowed({ 200 }));

        nodes.ForgetTablet(100);
        UNIT_ASSERT(nodes.IsAdmitAllowed({ 100 }));
    }

    Y_UNIT_TEST(TargetNodesAreDistinctAndOrdered) {
        TNodeStateMap nodes;
        nodes.SetTabletNode(1, 20);
        nodes.SetTabletNode(2, 10);
        nodes.SetTabletNode(3, 20);
        const auto targets = nodes.CollectTargetNodes({ 1, 2, 3, 4 });
        UNIT_ASSERT_VALUES_EQUAL(targets.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(targets[0], 20);
        UNIT_ASSERT_VALUES_EQUAL(targets[1], 10);
    }

    Y_UNIT_TEST(RecheckIsDeduplicatedAndRateLimited) {
        constexpr TDuration period = TDuration::Seconds(5);
        TNodeStateMap nodes;
        nodes.SetTabletNode(1, 10);
        nodes.SetTabletNode(2, 20);
        nodes.MarkHot(10, 1);

        // Only the tablet on the hot node is worth re-resolving.
        auto picked = nodes.PickTabletsForRecheck({ 1, 2 }, T0, period);
        UNIT_ASSERT_VALUES_EQUAL(picked.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(picked[0], 1);

        // Still in flight and within the period: do not duplicate the resolve.
        UNIT_ASSERT(nodes.PickTabletsForRecheck({ 1 }, T0 + TDuration::Seconds(1), period).empty());

        // Period elapsed with no FinishRecheck: treat the in-flight as lost and allow a retry.
        UNIT_ASSERT_VALUES_EQUAL(nodes.PickTabletsForRecheck({ 1 }, T0 + period, period).size(), 1);

        nodes.FinishRecheck(1);
        // Resolve finished, but the per-tablet period (from the retry stamp) has not elapsed.
        UNIT_ASSERT(nodes.PickTabletsForRecheck({ 1 }, T0 + period + TDuration::Seconds(1), period).empty());
        UNIT_ASSERT_VALUES_EQUAL(nodes.PickTabletsForRecheck({ 1 }, T0 + period * 2, period).size(), 1);
    }

    Y_UNIT_TEST(ForgetTabletDropsRecheckBookkeepingToo) {
        constexpr TDuration period = TDuration::Seconds(5);
        TNodeStateMap nodes;
        nodes.SetTabletNode(1, 10);
        nodes.MarkHot(10, 1);
        UNIT_ASSERT_VALUES_EQUAL(nodes.PickTabletsForRecheck({ 1 }, T0, period).size(), 1);

        // The tablet moved away and came back somewhere else. Had the in-flight guard survived, the
        // relearned tablet would never be picked for a recheck again.
        nodes.ForgetTablet(1);
        nodes.SetTabletNode(1, 10);
        UNIT_ASSERT_VALUES_EQUAL(nodes.PickTabletsForRecheck({ 1 }, T0, period).size(), 1);
    }

    Y_UNIT_TEST(TabletMapIsBounded) {
        TNodeStateMap nodes;
        for (ui64 tabletId = 1; tabletId <= TNodeStateMap::MaxTrackedTablets; ++tabletId) {
            nodes.SetTabletNode(tabletId, 10);
        }
        UNIT_ASSERT_VALUES_EQUAL(nodes.TabletCount(), TNodeStateMap::MaxTrackedTablets);

        // One past the cap drops the accumulated history and starts over from the new entry.
        nodes.SetTabletNode(TNodeStateMap::MaxTrackedTablets + 1, 10);
        UNIT_ASSERT_VALUES_EQUAL(nodes.TabletCount(), 1);
        // Re-learning an entry that is already tracked must not trip the cap.
        nodes.SetTabletNode(TNodeStateMap::MaxTrackedTablets + 1, 11);
        UNIT_ASSERT_VALUES_EQUAL(nodes.TabletCount(), 1);
    }
}

Y_UNIT_TEST_SUITE(TDrainRateControllerTest) {
    Y_UNIT_TEST(SeedsBothBucketsFromParams) {
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(MakeParams());
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 10.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateBytes(), 100'000.0, 1e-9);
        UNIT_ASSERT(controller.IsObservationWindowOpen());
        UNIT_ASSERT(!controller.IsAtRateFloor());
    }

    Y_UNIT_TEST(ReserveRequiresBothBuckets) {
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(MakeParams());

        // Bytes bucket holds one second of budget: a bigger batch cannot go yet.
        UNIT_ASSERT(!controller.TryReserve(200'000));
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetTokensBytes(), 100'000.0, 1e-9);

        UNIT_ASSERT(controller.TryReserve(40'000));
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetTokensBytes(), 60'000.0, 1e-9);

        controller.Refund(40'000);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetTokensBytes(), 100'000.0, 1e-9);
    }

    Y_UNIT_TEST(OverloadedCohortCutsProportionally) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        // A cohort targets ceil(rate) == 10 outcomes; make every one of them overloaded so the
        // full AimdBeta applies.
        controller.NoteWaiterReleased();
        for (int i = 0; i < 10; ++i) {
            controller.NoteWriteOutcome(MakeState(T0), params, EWriteOutcome::Overloaded);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 5.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateBytes(), 50'000.0, 1e-9);
    }

    Y_UNIT_TEST(HalfOverloadedCohortCutsHalfAsHard) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        controller.NoteWaiterReleased();
        for (int i = 0; i < 5; ++i) {
            controller.NoteWriteOutcome(MakeState(T0), params, EWriteOutcome::Overloaded);
        }
        for (int i = 0; i < 5; ++i) {
            controller.NoteWriteOutcome(MakeState(T0), params, EWriteOutcome::Ok);
        }
        // effectiveBeta = 1 - 0.5 * (1 - 0.5) = 0.75
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 7.5, 1e-9);
    }

    Y_UNIT_TEST(GrowthIsBlockedUntilTheSystemGoesQuiet) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        controller.NoteWaiterReleased();
        for (int i = 0; i < 10; ++i) {
            controller.NoteWriteOutcome(MakeState(T0), params, EWriteOutcome::Overloaded);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 5.0, 1e-9);

        // A clean cohort immediately after the overload: still inside HotCooldownSec (2s here),
        // so nothing may grow yet.
        controller.NoteWaiterReleased();
        for (int i = 0; i < 5; ++i) {
            controller.NoteWriteOutcome(MakeState(T0 + TDuration::MilliSeconds(100)), params, EWriteOutcome::Ok);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 5.0, 1e-9);

        // Same clean cohort after a quiet window: CUBIC recovers along W(t) toward the pre-cut
        // peak of 10, reaching C*(t-K)^3 + Wmax with C = (10 - 5) / K^3.
        const TInstant quiet = T0 + TDuration::Seconds(5);
        controller.NoteWaiterReleased();
        for (int i = 0; i < 5; ++i) {
            controller.NoteWriteOutcome(MakeState(quiet), params, EWriteOutcome::Ok);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 9.375, 1e-6);
    }

    Y_UNIT_TEST(UnknownOutcomesNeitherCutNorCompleteTheCohort) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        // Writes that ended without ever hearing back from the shard. Counting them as clean would
        // let a shard that stopped answering complete this cohort and grow the rate; counting them
        // as overloaded would invent backpressure out of what may be a network fault.
        const TInstant quiet = T0 + TDuration::Seconds(5);
        controller.NoteWaiterReleased();
        for (int i = 0; i < 20; ++i) {
            controller.NoteWriteOutcome(MakeState(quiet), params, EWriteOutcome::Unknown);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 10.0, 1e-9);

        // The cohort is still open and still empty, so the ten answers that follow are what closes
        // it — the unknowns did not consume any of its budget.
        for (int i = 0; i < 9; ++i) {
            controller.NoteWriteOutcome(MakeState(quiet), params, EWriteOutcome::Ok);
        }
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 10.0, 1e-9);
        controller.NoteWriteOutcome(MakeState(quiet), params, EWriteOutcome::Ok);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 11.0, 1e-9);
    }

    Y_UNIT_TEST(StrayOverloadOutsideACohortCutsOnlyASlice) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        // No cohort open: one overload is treated as 1/ceil(rate) of a dirty round, so the cut is
        // a tenth of the full beta rather than a halving.
        controller.NoteWriteOutcome(MakeState(T0), params, EWriteOutcome::Overloaded);
        // effectiveBeta = 1 - 0.1 * (1 - 0.5) = 0.95
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 9.5, 1e-9);
    }

    Y_UNIT_TEST(SustainedHotDecayKeepsPushingTheRateDown) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        // First cycle only arms the decay clock.
        controller.PrepareDrainCycle(MakeState(T0, /*anyHot=*/true), params);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 10.0, 1e-9);

        // HotDecayTauSec is KTarget/10 == 1s here, so one second applies exactly one beta.
        controller.PrepareDrainCycle(MakeState(T0 + TDuration::Seconds(1), /*anyHot=*/true), params);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 5.0, 1e-9);

        controller.PrepareDrainCycle(MakeState(T0 + TDuration::Seconds(2), /*anyHot=*/true), params);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 2.5, 1e-9);
    }

    Y_UNIT_TEST(DecayStopsAtTheFloor) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        controller.PrepareDrainCycle(MakeState(T0, /*anyHot=*/true), params);
        controller.PrepareDrainCycle(MakeState(T0 + TDuration::Minutes(10), /*anyHot=*/true), params);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), params.RMin, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateBytes(), params.RMinBytes, 1e-9);
        // Nothing left to decay: the actor can stop its hot tick.
        UNIT_ASSERT(controller.IsAtRateFloor());
    }

    Y_UNIT_TEST(FirstHotNodeCutsAndPinsThePeak) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        controller.NoteHotNode(T0);
        controller.NoteFirstHotNode(MakeState(T0, /*anyHot=*/true), params);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 5.0, 1e-9);

        // Compaction overload is not a discovered link limit, so the post-cut rate becomes the new
        // peak: a later quiet window probes up from 5, it does not CUBIC back to 10.
        const TInstant quiet = T0 + TDuration::Seconds(5);
        controller.NoteWaiterReleased();
        for (int i = 0; i < 5; ++i) {
            controller.NoteWriteOutcome(MakeState(quiet), params, EWriteOutcome::Ok);
        }
        // 5 + CubicProbePercent% of Wmax(5)
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), 5.5, 1e-9);
    }

    Y_UNIT_TEST(QueueTransitionSeedsFromObservationOnlyOnce) {
        const auto params = MakeParams();
        TCSFlowControlManagerCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TDrainRateController controller(counters);
        controller.Seed(params);

        // Fast-path traffic, one admit every 100ms, while the queue is empty.
        for (int i = 0; i <= 20; ++i) {
            controller.NoteFastPathAdmit(MakeState(T0 + TDuration::MilliSeconds(100 * i), false, true), 1'000);
        }

        const TInstant transition = T0 + TDuration::Seconds(3);
        controller.NoteQueueBecameNonEmpty(MakeState(transition, false, true), 1'000);
        UNIT_ASSERT(!controller.IsObservationWindowOpen());
        // The seed comes from what the system actually sustained, not from the config start rate.
        const double seeded = controller.GetRateCount();
        UNIT_ASSERT(seeded < 10.0);
        UNIT_ASSERT(seeded >= params.RMin);

        // Already non-empty: a second call must not re-seed.
        controller.NoteQueueBecameNonEmpty(MakeState(transition + TDuration::Seconds(1)), 1'000);
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), seeded, 1e-9);

        // Draining back to empty reopens the window without touching the learned rate.
        controller.NoteQueueEmpty();
        UNIT_ASSERT(controller.IsObservationWindowOpen());
        UNIT_ASSERT_DOUBLES_EQUAL(controller.GetRateCount(), seeded, 1e-9);
    }
}
