#include "limit_budget.h"

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NKikimr::NMemory {

namespace {

using TBudget = TMemoryLimitBudget;
constexpr auto ConsumerRequest = TBudget::ETargetSource::ConsumerRequest;
const TBudget::TKey Cache{EMemoryConsumerKind::SharedCache, TActorId(1, 1)};
const TBudget::TKey Scan{EMemoryConsumerKind::ColumnTablesScanGroupedMemory, TActorId(1, 2)};
const TBudget::TKey Comp{EMemoryConsumerKind::ColumnTablesCompGroupedMemory, TActorId(1, 3)};

void AssertGrant(const TBudget::TGrant& grant, ui64 bytes, bool shouldSend) {
    UNIT_ASSERT_VALUES_EQUAL(grant.Bytes, bytes);
    UNIT_ASSERT_VALUES_EQUAL(grant.ShouldSend, shouldSend);
}

} // namespace

Y_UNIT_TEST_SUITE(TMemoryLimitBudgetTest) {
    Y_UNIT_TEST(BootstrapLimitIsReserved) {
        TBudget budget;
        budget.Register(Cache, 80, 1);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 80);
        auto grants = budget.Allocate({{Cache, 80, 20}, {Scan, 0, 80}}, 100);
        AssertGrant(grants[0], 20, true);
        AssertGrant(grants[1], 0, false);
        grants = budget.Allocate({{Cache, 20, 20}, {Scan, 0, 80}}, 100);
        AssertGrant(grants[1], 80, true);
    }

    Y_UNIT_TEST(UnboundedStartupLimitsAreReducedTogetherWhileOverBudget) {
        TBudget budget;
        budget.Register(Cache, 120, 1);
        budget.Register(Scan, std::numeric_limits<ui64>::max(), 1);
        budget.Register(Comp, std::numeric_limits<ui64>::max(), 4);
        const auto grants = budget.Allocate({{Cache, 120, 20}, {Scan, 0, 20}, {Comp, 0, 10, 4}}, 100);
        AssertGrant(grants[0], 20, true);
        AssertGrant(grants[1], 20, true);
        AssertGrant(grants[2], 10, true);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 180);
    }

    Y_UNIT_TEST(IdleGrantsAreReserved) {
        TBudget budget;
        const auto grants = budget.Allocate({{Cache, 0, 50}, {Scan, 0, 60}}, 100);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 45);
        UNIT_ASSERT_VALUES_EQUAL(grants[1].Bytes, 54);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 99);
    }

    Y_UNIT_TEST(CompactionIncreaseWaitsForTheWholeCacheRelease) {
        TBudget budget;
        budget.Allocate({{Cache, 60, 60}, {Comp, 0, 10, 4}}, 100);
        auto grants = budget.Allocate({{Cache, 60, 40}, {Comp, 0, 15, 4}}, 100);
        AssertGrant(grants[0], 40, true);
        AssertGrant(grants[1], 10, false);
        // Releasing only ten of the required twenty bytes must not send a partial increase.
        grants = budget.Allocate({{Cache, 50, 40}, {Comp, 0, 15, 4}}, 100);
        AssertGrant(grants[1], 10, false);
        grants = budget.Allocate({{Cache, 40, 40}, {Comp, 0, 15, 4}}, 100);
        AssertGrant(grants[1], 15, true);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 100);
    }

    Y_UNIT_TEST(PendingIncreaseUsesTheLatestTarget) {
        TBudget budget;
        budget.Allocate({{Cache, 80, 80}, {Scan, 0, 20}}, 100);
        auto grants = budget.Allocate({{Cache, 80, 40}, {Scan, 0, 60}}, 100);
        UNIT_ASSERT(!grants[1].ShouldSend);
        grants = budget.Allocate({{Cache, 80, 60}, {Scan, 0, 40}}, 100);
        UNIT_ASSERT(!grants[1].ShouldSend);
        grants = budget.Allocate({{Cache, 60, 60}, {Scan, 0, 40}}, 100);
        AssertGrant(grants[1], 40, true);
    }

    Y_UNIT_TEST(ExplicitDemandWaitsWithoutBeingScaled) {
        TBudget budget;
        budget.Allocate({{Cache, 80, 80}, {Scan, 0, 20}}, 100);
        auto grants = budget.Allocate({{Cache, 80, 80}, {Scan, 0, 80, 1, ConsumerRequest}}, 100);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 20);
        AssertGrant(grants[1], 20, false);
        grants = budget.Allocate({{Cache, 50, 80}, {Scan, 0, 80, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[1], 20, false);
        // Replace the waiting request. The old target must never be granted.
        grants = budget.Allocate({{Cache, 50, 80}, {Scan, 0, 50, 1, ConsumerRequest}}, 100);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 50);
        AssertGrant(grants[1], 50, true);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 100);
    }

    Y_UNIT_TEST(ImpossibleExplicitDemandWaitsForBudgetChange) {
        TBudget budget;
        budget.Allocate({{Cache, 0, 60}, {Comp, 0, 10, 4}}, 100);
        // An impossible effective cap retains its old grant while default targets grow.
        auto grants = budget.Allocate({{Cache, 0, 100}, {Comp, 0, 30, 4, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 60, true);
        AssertGrant(grants[1], 10, false);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 100);
        grants = budget.Allocate({{Cache, 0, 60}, {Comp, 0, 30, 4, ConsumerRequest}}, 200);
        AssertGrant(grants[1], 30, true);

        const ui64 max = std::numeric_limits<ui64>::max();
        grants = budget.Allocate({{Cache, 0, 60}, {Comp, 0, max, 4, ConsumerRequest}}, max);
        AssertGrant(grants[1], 30, false); // multiplication cannot wrap into a grant
    }

    Y_UNIT_TEST(HardDecreaseReducesGrantsButKeepsExplicitDemandPending) {
        TBudget budget;
        budget.Allocate({{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 100);
        auto grants = budget.Allocate({{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 35);
        UNIT_ASSERT_VALUES_EQUAL(grants[1].Bytes, 35);
        UNIT_ASSERT(grants[0].ShouldSend && grants[1].ShouldSend);
        grants = budget.Allocate({{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT(!grants[0].ShouldSend && !grants[1].ShouldSend);
        grants = budget.Allocate({{Cache, 0, 0, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 0);
        AssertGrant(grants[1], 50, true);
    }

    Y_UNIT_TEST(CompetingExplicitRequestsLeaveDefaultsTheUnusedBudget) {
        const TBudget::TKey second{EMemoryConsumerKind::ColumnTablesScanGroupedMemory, TActorId(1, 4)};
        TBudget budget;
        budget.Allocate({{Cache, 40, 40}, {Scan, 0, 20}, {second, 0, 20}}, 100);
        auto grants = budget.Allocate({{Cache, 40, 40}, {Scan, 0, 60, 1, ConsumerRequest}, {second, 0, 60, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 20, true);
        AssertGrant(grants[1], 20, false);
        AssertGrant(grants[2], 20, false);
        for (size_t round = 0; round < 3; ++round) {
            grants = budget.Allocate({{Cache, 20, 40}, {Scan, 0, 60, 1, ConsumerRequest}, {second, 0, 60, 1, ConsumerRequest}}, 100);
            AssertGrant(grants[0], 20, true);
            AssertGrant(grants[1], 60, true);
            AssertGrant(grants[2], 20, false);
            UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 100);
        }
        // Releasing the first grant lets the second complete without starving the cache.
        grants = budget.Allocate({{Cache, 20, 40}, {Scan, 0, 0, 1, ConsumerRequest}, {second, 0, 60, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 40, true);
        AssertGrant(grants[1], 0, true);
        AssertGrant(grants[2], 60, true);
    }

    Y_UNIT_TEST(ExplicitPlanningSharesOneBudgetAcrossKindsAndMultipliers) {
        TBudget budget;
        budget.Allocate({{Cache, 0, 40}, {Comp, 0, 10, 4}, {Scan, 0, 20}}, 100);
        const TVector<TBudget::TRequest> requests{
            {Cache, 0, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}};
        const auto planned = budget.PlanExplicitLimits(requests, 100);
        UNIT_ASSERT_VALUES_EQUAL(planned[1], 15);
        UNIT_ASSERT_VALUES_EQUAL(planned[2], 20);
        const auto grants = budget.Allocate(requests, 100);
        AssertGrant(grants[0], 20, true);
        AssertGrant(grants[1], 15, true);
        AssertGrant(grants[2], 20, false);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 100);
    }

    Y_UNIT_TEST(EveryRegistrantKeepsItsOwnReservation) {
        TBudget budget;
        const TBudget::TKey second{EMemoryConsumerKind::SharedCache, TActorId(1, 4)};
        budget.Allocate({{Cache, 80, 80}, {second, 0, 20}}, 100);
        auto grants = budget.Allocate({{Cache, 50, 20}, {second, 0, 80}}, 100);
        AssertGrant(grants[1], 20, false);
        grants = budget.Allocate({{Cache, 20, 20}, {second, 0, 80}}, 100);
        AssertGrant(grants[1], 80, true);
    }

    Y_UNIT_TEST(HardDecreaseKeepsUsageDebtVisible) {
        TBudget budget;
        budget.Allocate({{Cache, 60, 60}, {Scan, 40, 40}}, 100);
        const auto grants = budget.Allocate({{Cache, 60, 20}, {Scan, 40, 80}}, 70);
        AssertGrant(grants[1], 40, false);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 100);
    }

    Y_UNIT_TEST(EffectiveCapMultiplierIsCharged) {
        TBudget budget;
        const auto grants = budget.Allocate({{Cache, 0, 50}, {Comp, 0, 20, 4}}, 90);
        UNIT_ASSERT(grants[0].Bytes + 4 * grants[1].Bytes <= 90);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), grants[0].Bytes + 4 * grants[1].Bytes);
    }

    Y_UNIT_TEST(UsageWithoutGrantIsNeverLost) {
        TBudget budget;
        const auto grants = budget.Allocate({{Cache, 110, 0}, {Scan, 0, 50}}, 100);
        AssertGrant(grants[1], 0, false);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 110);
    }

    Y_UNIT_TEST(UsageAboveHardDoesNotAuthorizeNewGrants) {
        TBudget budget;
        const auto grants = budget.Allocate({{Cache, 110, 100}, {Scan, 110, 100}}, 100);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(grants[1].Bytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 220);
    }

    Y_UNIT_TEST(LargeRequestsDoNotOverflow) {
        TBudget budget;
        const ui64 max = std::numeric_limits<ui64>::max();
        const auto grants = budget.Allocate({{Cache, 0, max}, {Scan, 0, max, 4}}, max);
        UNIT_ASSERT(budget.GetCommitted() <= max);
        UNIT_ASSERT(grants[0].Bytes > 0);
        UNIT_ASSERT(grants[1].Bytes > 0);
    }

    Y_UNIT_TEST(UsageSumBeyondUi64CannotCreateHeadroom) {
        TBudget budget;
        const ui64 max = std::numeric_limits<ui64>::max();
        const auto grants = budget.Allocate({{Cache, max, 1}, {Scan, 1, 1}}, max);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(grants[1].Bytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), max);
    }

    Y_UNIT_TEST(BootstrapMultiplierCannotWrapIntoHeadroom) {
        TBudget budget;
        const ui64 max = std::numeric_limits<ui64>::max();
        budget.Register(Cache, max, 4);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), max);
        const auto grants = budget.Allocate({{Cache, max, 0, 4}, {Scan, 1, 1}}, max);
        UNIT_ASSERT(!grants[1].ShouldSend);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), max);
    }

    Y_UNIT_TEST(LateRegistrantWaitsForReleasedUsage) {
        TBudget budget;
        budget.Allocate({{Cache, 100, 100}}, 100);
        auto grants = budget.Allocate({{Cache, 100, 100}, {Scan, 0, 100}}, 100);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 50);
        AssertGrant(grants[1], 0, false);
        grants = budget.Allocate({{Cache, 50, 100}, {Scan, 0, 100}}, 100);
        AssertGrant(grants[1], 50, true);
    }

    Y_UNIT_TEST(HardDecreaseBoundsLatestLimits) {
        TBudget budget;
        budget.Allocate({{Cache, 0, 50}, {Scan, 0, 50}}, 100);
        const auto grants = budget.Allocate({{Cache, 0, 50}, {Scan, 0, 50}}, 70);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 35);
        UNIT_ASSERT_VALUES_EQUAL(grants[1].Bytes, 35);
        UNIT_ASSERT_VALUES_EQUAL(budget.GetCommitted(), 70);
    }

    Y_UNIT_TEST(DeadRegistrantReturnsPermission) {
        TBudget budget;
        budget.Allocate({{Cache, 0, 100}, {Scan, 0, 0}}, 100);
        budget.Forget(Cache);
        const auto grants = budget.Allocate({{Scan, 0, 100}}, 100);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 100);
    }
}

} // namespace NKikimr::NMemory
