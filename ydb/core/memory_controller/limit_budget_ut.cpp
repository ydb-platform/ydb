#include "limit_budget.h"

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NKikimr::NMemory {

namespace {

using TLedger = TMemoryGrantLedger;
constexpr auto ConsumerRequest = TLedger::ETargetSource::ConsumerRequest;
const TLedger::TKey Cache{EMemoryConsumerKind::SharedCache, TActorId(1, 1)};
const TLedger::TKey Scan{EMemoryConsumerKind::ColumnTablesScanGroupedMemory, TActorId(1, 2)};
const TLedger::TKey Comp{EMemoryConsumerKind::ColumnTablesCompGroupedMemory, TActorId(1, 3)};

void AssertGrant(const TLedger::TGrant& grant, ui64 bytes, bool shouldSend) {
    UNIT_ASSERT_VALUES_EQUAL(grant.Bytes, bytes);
    UNIT_ASSERT_VALUES_EQUAL(grant.ShouldSend, shouldSend);
}

TVector<TLedger::TGrant> Allocate(TLedger& ledger, TVector<TLedger::TRequest> requests,
    ui64 available, ui64 automaticMinimum = 0)
{
    const auto planned = ledger.PlanExplicitLimits(requests, available, automaticMinimum);
    return ledger.Allocate(std::move(requests), available, planned);
}

} // namespace

Y_UNIT_TEST_SUITE(TMemoryGrantLedgerTest) {
    Y_UNIT_TEST(StartupLimitsAndUsageDebtGateLaterGrants) {
        const TLedger::TKey sibling{EMemoryConsumerKind::SharedCache, TActorId(1, 4)};
        TLedger ledger;
        ledger.Register(Cache, 120, 1);
        ledger.Register(Comp, std::numeric_limits<ui64>::max(), 4);
        // Scale the 200-byte target to 100, but charge usage above the reduced grants.
        auto grants = Allocate(ledger, {{Cache, 120, 80}, {Comp, 0, 20, 4}, {sibling, 0, 40}}, 100);
        AssertGrant(grants[0], 40, true);
        AssertGrant(grants[1], 10, true);
        AssertGrant(grants[2], 0, false);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 160);
        grants = Allocate(ledger, {{Cache, 60, 80}, {Comp, 0, 20, 4}, {sibling, 0, 40}}, 100);
        AssertGrant(grants[2], 0, false); // partial release cannot fund the full increase
        grants = Allocate(ledger, {{Cache, 40, 80}, {Comp, 0, 20, 4}, {sibling, 0, 40}}, 100);
        AssertGrant(grants[2], 20, true);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 100);
        ledger.Forget(Cache);
        grants = Allocate(ledger, {{Comp, 0, 10, 4}, {sibling, 0, 60}}, 100);
        AssertGrant(grants[1], 60, true);
    }

    Y_UNIT_TEST(ExplicitRequestsShareBudgetAndWaitForFullGrant) {
        TLedger ledger;
        Allocate(ledger, {{Cache, 40, 40}, {Comp, 0, 10, 4}, {Scan, 0, 20}}, 100);
        const TVector<TLedger::TRequest> requests{
            {Cache, 40, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}};
        const auto planned = ledger.PlanExplicitLimits(requests, 100);
        UNIT_ASSERT_VALUES_EQUAL(planned.at(Comp), 15);
        UNIT_ASSERT_VALUES_EQUAL(planned.at(Scan), 20);
        auto grants = Allocate(ledger, requests, 100);
        AssertGrant(grants[0], 20, true); // waiting targets must leave defaults funded
        AssertGrant(grants[1], 10, false);
        AssertGrant(grants[2], 20, false);
        // Comp needs 4 x 5 bytes; releasing ten is not enough.
        grants = Allocate(ledger, {{Cache, 30, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[1], 10, false);
        grants = Allocate(ledger, {{Cache, 10, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[1], 15, true);
        AssertGrant(grants[2], 20, false);
        // Only the replacement target may complete the pending request.
        grants = Allocate(ledger, {{Cache, 10, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 30, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 10, true);
        AssertGrant(grants[2], 30, true);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 100);

        ledger.Forget(Cache);
        ledger.Forget(Comp);
        ledger.Forget(Scan);
        ledger.Register(Cache, 40, 1);
        ledger.Register(Scan, 20, 1);
        ledger.Register(Comp, 20, 1);
        TVector<TLedger::TRequest> ordered{
            {Cache, 40, 40}, {Scan, 20, 40, 1, ConsumerRequest, 2}, {Comp, 20, 60, 1, ConsumerRequest, 1}};
        const auto protectedPlan = ledger.PlanExplicitLimits(ordered, 100, 40);
        UNIT_ASSERT_VALUES_EQUAL(protectedPlan.at(Comp), 20);
        UNIT_ASSERT_VALUES_EQUAL(protectedPlan.at(Scan), 20); // no bypass of the older feasible waiter
        grants = ledger.Allocate(ordered, 100, protectedPlan);
        AssertGrant(grants[0], 40, true);
        const auto orderedPlan = ledger.PlanExplicitLimits(ordered, 100, 20);
        UNIT_ASSERT_VALUES_EQUAL(orderedPlan.at(Comp), 60);
        UNIT_ASSERT_VALUES_EQUAL(orderedPlan.at(Scan), 20);
        grants = ledger.Allocate(ordered, 100, orderedPlan);
        AssertGrant(grants[2], 20, false); // cache has not released the reduction yet
        ordered[0].Used = 20;
        grants = Allocate(ledger, ordered, 100, 20);
        AssertGrant(grants[2], 60, true);
        AssertGrant(grants[1], 20, false);
    }

    Y_UNIT_TEST(ImpossibleExplicitDemandWaitsForBudgetChange) {
        TLedger ledger;
        Allocate(ledger, {{Cache, 0, 60}, {Comp, 0, 10, 4}}, 100);
        // An impossible effective cap retains its old grant while default targets grow.
        auto grants = Allocate(ledger, {{Cache, 0, 100}, {Comp, 0, 30, 4, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 60, true);
        AssertGrant(grants[1], 10, false);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 100);
        grants = Allocate(ledger, {{Cache, 0, 60}, {Comp, 0, 30, 4, ConsumerRequest}}, 200);
        AssertGrant(grants[1], 30, true);

        const ui64 max = std::numeric_limits<ui64>::max();
        grants = Allocate(ledger, {{Cache, 0, 60}, {Comp, 0, max, 4, ConsumerRequest}}, max);
        AssertGrant(grants[0], 60, true);
        AssertGrant(grants[1], 30, false); // multiplication cannot wrap into a grant
    }

    Y_UNIT_TEST(HardDecreaseReducesGrantsButKeepsExplicitDemandPending) {
        TLedger ledger;
        Allocate(ledger, {{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 100);
        auto grants = Allocate(ledger, {{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        AssertGrant(grants[0], 35, true);
        AssertGrant(grants[1], 35, true);
        grants = Allocate(ledger, {{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT(!grants[0].ShouldSend && !grants[1].ShouldSend);
        grants = Allocate(ledger, {{Cache, 0, 0, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 0);
        AssertGrant(grants[1], 50, true);
    }

    Y_UNIT_TEST(HugeValuesSaturateInsteadOfWrapping) {
        const ui64 max = std::numeric_limits<ui64>::max();
        TLedger ledger;
        ledger.Register(Cache, max, 4);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), max);
        const auto grants = Allocate(ledger, {{Cache, max, max, 4}, {Scan, 1, max}}, max);
        AssertGrant(grants[0], max / 5, true);
        AssertGrant(grants[1], 0, false);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), max);
    }
}

} // namespace NKikimr::NMemory
