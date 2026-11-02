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

} // namespace

Y_UNIT_TEST_SUITE(TMemoryGrantLedgerTest) {
    Y_UNIT_TEST(StartupLimitsAndUsageDebtGateLaterGrants) {
        const TLedger::TKey sibling{EMemoryConsumerKind::SharedCache, TActorId(1, 4)};
        TLedger ledger;
        ledger.Register(Cache, 120, 1);
        ledger.Register(Comp, std::numeric_limits<ui64>::max(), 4);
        // Scale the 200-byte target to 100, but charge usage above the reduced grants.
        auto grants = ledger.Allocate({{Cache, 120, 80}, {Comp, 0, 20, 4}, {sibling, 0, 40}}, 100);
        AssertGrant(grants[0], 40, true);
        AssertGrant(grants[1], 10, true);
        AssertGrant(grants[2], 0, false);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 160);
        grants = ledger.Allocate({{Cache, 60, 80}, {Comp, 0, 20, 4}, {sibling, 0, 40}}, 100);
        AssertGrant(grants[2], 0, false); // partial release cannot fund the full increase
        grants = ledger.Allocate({{Cache, 40, 80}, {Comp, 0, 20, 4}, {sibling, 0, 40}}, 100);
        AssertGrant(grants[2], 20, true);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 100);
        ledger.Forget(Cache);
        grants = ledger.Allocate({{Comp, 0, 10, 4}, {sibling, 0, 60}}, 100);
        AssertGrant(grants[1], 60, true);
    }

    Y_UNIT_TEST(ExplicitRequestsShareBudgetAndWaitForFullGrant) {
        TLedger ledger;
        ledger.Allocate({{Cache, 40, 40}, {Comp, 0, 10, 4}, {Scan, 0, 20}}, 100);
        const TVector<TLedger::TRequest> requests{
            {Cache, 40, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}};
        const auto planned = ledger.PlanExplicitLimits(requests, 100);
        UNIT_ASSERT_VALUES_EQUAL(planned[1], 15);
        UNIT_ASSERT_VALUES_EQUAL(planned[2], 20);
        auto grants = ledger.Allocate(requests, 100);
        AssertGrant(grants[0], 20, true); // waiting targets must leave defaults funded
        AssertGrant(grants[1], 10, false);
        AssertGrant(grants[2], 20, false);
        // Comp needs 4 x 5 bytes; releasing ten is not enough.
        grants = ledger.Allocate({{Cache, 30, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[1], 10, false);
        grants = ledger.Allocate({{Cache, 10, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 60, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[1], 15, true);
        AssertGrant(grants[2], 20, false);
        // Only the replacement target may complete the pending request.
        grants = ledger.Allocate({{Cache, 10, 40}, {Comp, 0, 15, 4, ConsumerRequest}, {Scan, 0, 30, 1, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 10, true);
        AssertGrant(grants[2], 30, true);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 100);
    }

    Y_UNIT_TEST(ImpossibleExplicitDemandWaitsForBudgetChange) {
        TLedger ledger;
        ledger.Allocate({{Cache, 0, 60}, {Comp, 0, 10, 4}}, 100);
        // An impossible effective cap retains its old grant while default targets grow.
        auto grants = ledger.Allocate({{Cache, 0, 100}, {Comp, 0, 30, 4, ConsumerRequest}}, 100);
        AssertGrant(grants[0], 60, true);
        AssertGrant(grants[1], 10, false);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), 100);
        grants = ledger.Allocate({{Cache, 0, 60}, {Comp, 0, 30, 4, ConsumerRequest}}, 200);
        AssertGrant(grants[1], 30, true);

        const ui64 max = std::numeric_limits<ui64>::max();
        grants = ledger.Allocate({{Cache, 0, 60}, {Comp, 0, max, 4, ConsumerRequest}}, max);
        AssertGrant(grants[0], 60, true);
        AssertGrant(grants[1], 30, false); // multiplication cannot wrap into a grant
    }

    Y_UNIT_TEST(HardDecreaseReducesGrantsButKeepsExplicitDemandPending) {
        TLedger ledger;
        ledger.Allocate({{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 100);
        auto grants = ledger.Allocate({{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        AssertGrant(grants[0], 35, true);
        AssertGrant(grants[1], 35, true);
        grants = ledger.Allocate({{Cache, 0, 50, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT(!grants[0].ShouldSend && !grants[1].ShouldSend);
        grants = ledger.Allocate({{Cache, 0, 0, 1, ConsumerRequest}, {Scan, 0, 50, 1, ConsumerRequest}}, 70);
        UNIT_ASSERT_VALUES_EQUAL(grants[0].Bytes, 0);
        AssertGrant(grants[1], 50, true);
    }

    Y_UNIT_TEST(HugeValuesSaturateInsteadOfWrapping) {
        const ui64 max = std::numeric_limits<ui64>::max();
        TLedger ledger;
        ledger.Register(Cache, max, 4);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), max);
        const auto grants = ledger.Allocate({{Cache, max, max, 4}, {Scan, 1, max}}, max);
        AssertGrant(grants[0], max / 5, true);
        AssertGrant(grants[1], 0, false);
        UNIT_ASSERT_VALUES_EQUAL(ledger.GetCommitted(), max);
    }
}

} // namespace NKikimr::NMemory
