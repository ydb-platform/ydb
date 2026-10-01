#include "blob_checker_planner.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBsController {
namespace {

TGroupId GroupId(ui32 value) {
    return TGroupId::FromValue(value);
}

TIntrusivePtr<TBlobStorageGroupInfo> MakeGroup(ui32 groupId, std::initializer_list<ui32> nodeIds) {
    TVector<NActors::TActorId> vdiskIds;
    vdiskIds.reserve(nodeIds.size());
    for (const ui32 nodeId : nodeIds) {
        vdiskIds.emplace_back(nodeId, TStringBuf("planner-ut"));
    }

    return new TBlobStorageGroupInfo(
        TBlobStorageGroupType::ErasureNone,
        1,
        static_cast<ui32>(vdiskIds.size()),
        1,
        &vdiskIds,
        TBlobStorageGroupInfo::EEM_NONE,
        TBlobStorageGroupInfo::ELCP_IN_USE,
        TCypherKey(),
        GroupId(groupId));
}

void Enqueue(TBlobCheckerPlanner& planner, const TIntrusivePtr<TBlobStorageGroupInfo>& group) {
    planner.EnqueueCheck(group.Get());
}

void AssertNextGroup(TBlobCheckerPlanner& planner, ui32 expectedGroupId) {
    const std::optional<TGroupId> groupId = planner.ObtainNextGroupToCheck();
    UNIT_ASSERT_C(groupId, "expected group " << expectedGroupId << " to be ready");
    UNIT_ASSERT_VALUES_EQUAL(groupId->GetRawId(), expectedGroupId);
}

void AssertNoGroupReady(TBlobCheckerPlanner& planner) {
    UNIT_ASSERT(!planner.ObtainNextGroupToCheck());
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TBlobCheckerPlannerTest) {
    Y_UNIT_TEST(SharedNodeLocksAreReleasedInFifoOrder) {
        TBlobCheckerPlanner planner(TDuration::Minutes(1), 4);
        const auto first = MakeGroup(1, {1, 2});
        const auto second = MakeGroup(2, {2, 3});
        const auto third = MakeGroup(3, {2, 4});
        const auto disjoint = MakeGroup(4, {5, 6});

        Enqueue(planner, first);
        Enqueue(planner, second);
        Enqueue(planner, third);
        Enqueue(planner, disjoint);

        AssertNextGroup(planner, 1);
        AssertNextGroup(planner, 4);
        AssertNoGroupReady(planner);

        UNIT_ASSERT(planner.DequeueCheck(GroupId(1)));
        AssertNextGroup(planner, 2);
        AssertNoGroupReady(planner);

        UNIT_ASSERT(planner.DequeueCheck(GroupId(2)));
        AssertNextGroup(planner, 3);
        AssertNoGroupReady(planner);

        UNIT_ASSERT(planner.DequeueCheck(GroupId(3)));
        UNIT_ASSERT(planner.DequeueCheck(GroupId(4)));
    }

    Y_UNIT_TEST(MultipleVDisksOnOneNodeAcquireOneLock) {
        TBlobCheckerPlanner planner(TDuration::Minutes(1), 3);
        const auto first = MakeGroup(1, {1, 1, 2});
        const auto second = MakeGroup(2, {1, 3});
        const auto third = MakeGroup(3, {2, 3});

        Enqueue(planner, first);
        Enqueue(planner, second);
        Enqueue(planner, third);

        AssertNextGroup(planner, 1);
        AssertNoGroupReady(planner);

        UNIT_ASSERT(planner.DequeueCheck(GroupId(1)));
        AssertNextGroup(planner, 2);
        AssertNoGroupReady(planner);

        UNIT_ASSERT(planner.DequeueCheck(GroupId(2)));
        AssertNextGroup(planner, 3);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(3)));
    }

    Y_UNIT_TEST(DuplicateEnqueueCancelAndReset) {
        TBlobCheckerPlanner planner(TDuration::Minutes(1), 3);
        const auto first = MakeGroup(1, {1});
        const auto duplicateWithDifferentNodes = MakeGroup(1, {2});
        const auto disjoint = MakeGroup(2, {2});

        Enqueue(planner, first);
        Enqueue(planner, duplicateWithDifferentNodes);
        Enqueue(planner, disjoint);

        AssertNextGroup(planner, 1);
        AssertNextGroup(planner, 2);
        AssertNoGroupReady(planner);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(1)));
        UNIT_ASSERT(planner.DequeueCheck(GroupId(2)));

        const auto locking = MakeGroup(3, {3});
        const auto cancelled = MakeGroup(4, {3});
        const auto next = MakeGroup(5, {3});
        Enqueue(planner, locking);
        Enqueue(planner, cancelled);
        Enqueue(planner, next);

        AssertNextGroup(planner, 3);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(4)));
        UNIT_ASSERT(!planner.DequeueCheck(GroupId(4)));
        UNIT_ASSERT(!planner.DequeueCheck(GroupId(100)));
        UNIT_ASSERT(planner.DequeueCheck(GroupId(3)));
        AssertNextGroup(planner, 5);

        planner.ResetState();
        AssertNoGroupReady(planner);
        UNIT_ASSERT(!planner.DequeueCheck(GroupId(5)));

        Enqueue(planner, cancelled);
        AssertNextGroup(planner, 4);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(4)));
    }

    Y_UNIT_TEST(SchedulesAtTheConfiguredCadence) {
        TBlobCheckerPlanner planner(TDuration::Seconds(90), 3);
        const TMonotonic now = TMonotonic::Seconds(1'000);

        const TMonotonic first = planner.GetNextAllowedCheckTimestamp(now);
        UNIT_ASSERT_VALUES_EQUAL(first, now);
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(now),
            now + TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(now + TDuration::Seconds(5)),
            now + TDuration::Seconds(60));
    }

    Y_UNIT_TEST(GroupCountControlsDelayAndZeroMeansOneGroup) {
        TBlobCheckerPlanner planner(TDuration::Seconds(120), 4);
        const TMonotonic now = TMonotonic::Seconds(1'000);

        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(now), now);
        TMonotonic next = planner.GetNextAllowedCheckTimestamp(now);
        UNIT_ASSERT_VALUES_EQUAL(next, now + TDuration::Seconds(30));

        planner.SetGroupCount(2);
        next = planner.GetNextAllowedCheckTimestamp(next);
        UNIT_ASSERT_VALUES_EQUAL(next, now + TDuration::Seconds(90));

        planner.SetGroupCount(0);
        next = planner.GetNextAllowedCheckTimestamp(next);
        UNIT_ASSERT_VALUES_EQUAL(next, now + TDuration::Seconds(210));

        planner.SetGroupCount(8);
        next = planner.GetNextAllowedCheckTimestamp(next);
        UNIT_ASSERT_VALUES_EQUAL(next, now + TDuration::Seconds(225));
    }

    Y_UNIT_TEST(PeriodicityChangesFutureDelayAndZeroAllowsImmediateChecks) {
        TBlobCheckerPlanner planner(TDuration::Seconds(120), 4);
        const TMonotonic now = TMonotonic::Seconds(1'000);

        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(now), now);

        planner.SetPeriodicity(TDuration::Seconds(40));
        TMonotonic next = planner.GetNextAllowedCheckTimestamp(now);
        UNIT_ASSERT_VALUES_EQUAL(next, now + TDuration::Seconds(10));

        planner.SetPeriodicity(TDuration::Seconds(80));
        next = planner.GetNextAllowedCheckTimestamp(next);
        UNIT_ASSERT_VALUES_EQUAL(next, now + TDuration::Seconds(30));

        planner.SetPeriodicity(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(next), next);
        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(next), next);
    }

    Y_UNIT_TEST(LateChecksAccelerateUntilTimingDebtIsReset) {
        const TDuration periodicity = TDuration::Seconds(100);
        const TDuration nominalDelay = TDuration::Seconds(10);
        TBlobCheckerPlanner planner(periodicity, 10);
        const TMonotonic now = TMonotonic::Seconds(1'000);

        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(now), now);

        const TMonotonic late = now + TDuration::Seconds(50);
        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(late), late);

        const TMonotonic accelerated = planner.GetNextAllowedCheckTimestamp(late);
        const TDuration acceleratedDelay = accelerated - late;
        UNIT_ASSERT(acceleratedDelay > TDuration::Zero());
        UNIT_ASSERT(acceleratedDelay < nominalDelay);

        const TMonotonic lessAccelerated = planner.GetNextAllowedCheckTimestamp(accelerated);
        const TDuration lessAcceleratedDelay = lessAccelerated - accelerated;
        UNIT_ASSERT(lessAcceleratedDelay > acceleratedDelay);
        UNIT_ASSERT(lessAcceleratedDelay < nominalDelay);

        planner.SetPeriodicity(periodicity);
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(lessAccelerated),
            lessAccelerated + nominalDelay);
    }

    Y_UNIT_TEST(ResetStateClearsTimingDebt) {
        TBlobCheckerPlanner planner(TDuration::Seconds(100), 10);
        const TMonotonic now = TMonotonic::Seconds(1'000);

        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(now), now);
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(now + TDuration::Seconds(50)),
            now + TDuration::Seconds(50));

        planner.ResetState();

        const TMonotonic restarted = TMonotonic::Seconds(2'000);
        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(restarted), restarted);
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(restarted),
            restarted + TDuration::Seconds(10));
    }

    Y_UNIT_TEST(ResetPacingPreservesNodeLocksAndFifoOrder) {
        TBlobCheckerPlanner planner(TDuration::Seconds(100), 10);
        const auto first = MakeGroup(1, {1});
        const auto second = MakeGroup(2, {1});
        const auto third = MakeGroup(3, {1});

        Enqueue(planner, first);
        Enqueue(planner, second);
        Enqueue(planner, third);

        AssertNextGroup(planner, 1);
        AssertNoGroupReady(planner);

        const TMonotonic now = TMonotonic::Seconds(1'000);
        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(now), now);
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(now + TDuration::Seconds(50)),
            now + TDuration::Seconds(50));

        planner.ResetPacing();

        const TMonotonic restarted = TMonotonic::Seconds(2'000);
        UNIT_ASSERT_VALUES_EQUAL(planner.GetNextAllowedCheckTimestamp(restarted), restarted);
        UNIT_ASSERT_VALUES_EQUAL(
            planner.GetNextAllowedCheckTimestamp(restarted),
            restarted + TDuration::Seconds(10));

        AssertNoGroupReady(planner);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(1)));
        AssertNextGroup(planner, 2);
        AssertNoGroupReady(planner);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(2)));
        AssertNextGroup(planner, 3);
        UNIT_ASSERT(planner.DequeueCheck(GroupId(3)));
    }
}

} // namespace NKikimr::NBsController
