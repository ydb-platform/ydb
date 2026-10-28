#include "database_space.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NBsController;

namespace {

    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;

    const TDatabaseSpaceTracker::TScope Db1(72075186224037888ull, 2);
    const TDatabaseSpaceTracker::TScope Db2(72075186224037888ull, 3);
    const TBoxStoragePoolId Pool1{1, 1};
    const TBoxStoragePoolId Pool2{1, 2};
    const TBoxStoragePoolId Pool3{1, 3};

    TGroupId G(ui32 id) {
        return TGroupId::FromValue(0x80000000 + id);
    }

    std::vector<TDatabaseSpaceTracker::TScope> Changed(TDatabaseSpaceTracker& tracker) {
        auto res = tracker.TakeChangedScopes();
        std::sort(res.begin(), res.end());
        return res;
    }

} // anonymous

Y_UNIT_TEST_SUITE(DatabaseSpaceTracker) {

    Y_UNIT_TEST(DisabledByDefault) {
        TDatabaseSpaceTracker tracker;
        tracker.Subscribe(1, {Db1});
        tracker.SetPool(Pool1, Db1);
        tracker.SetGroup(G(1), Pool1, TColor::BLACK);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
    }

    Y_UNIT_TEST(BlockAndUnblockWithHysteresis) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
        tracker.Subscribe(1, {Db1});
        tracker.SetPool(Pool1, Db1);
        tracker.SetGroup(G(1), Pool1, TColor::GREEN);
        tracker.SetGroup(G(2), Pool1, TColor::GREEN);
        Changed(tracker); // pool was added to the scope
        UNIT_ASSERT(!tracker.IsExhausted(Db1));

        // one group only -- not the whole pool
        tracker.SetGroup(G(1), Pool1, TColor::ORANGE);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        UNIT_ASSERT(Changed(tracker).empty());

        // every group is at the block color or worse
        tracker.SetGroup(G(2), Pool1, TColor::YELLOW);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        UNIT_ASSERT_VALUES_EQUAL(Changed(tracker).size(), 1);

        // between unblock and block colors -- state is kept
        tracker.SetGroup(G(2), Pool1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        UNIT_ASSERT(Changed(tracker).empty());

        // better than the unblock color -- unblocked
        tracker.SetGroup(G(2), Pool1, TColor::CYAN);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        UNIT_ASSERT_VALUES_EQUAL(Changed(tracker).size(), 1);

        // back to LIGHT_YELLOW does not block again
        tracker.SetGroup(G(2), Pool1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
    }

    Y_UNIT_TEST(NoHysteresis) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::GREEN); // GREEN unblock color means the same as the block one
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetEffectiveUnblockColor(), TColor::YELLOW);
        tracker.SetPool(Pool1, Db1);
        tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        tracker.SetGroup(G(1), Pool1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
    }

    Y_UNIT_TEST(AnyPoolBlocksDatabase) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::YELLOW);
        tracker.SetPool(Pool1, Db1);
        tracker.SetPool(Pool2, Db1);
        tracker.SetPool(Pool3, Db2);
        tracker.SetGroup(G(1), Pool1, TColor::RED);
        tracker.SetGroup(G(2), Pool2, TColor::GREEN);
        tracker.SetGroup(G(3), Pool3, TColor::GREEN);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        UNIT_ASSERT(!tracker.IsExhausted(Db2));

        const auto pool1 = tracker.GetPoolState(Pool1);
        UNIT_ASSERT(pool1 && pool1->Exhausted);
        UNIT_ASSERT_VALUES_EQUAL(*pool1->BestColor, TColor::RED);
        const auto pool2 = tracker.GetPoolState(Pool2);
        UNIT_ASSERT(pool2 && !pool2->Exhausted);
        UNIT_ASSERT_VALUES_EQUAL(pool2->NumGroups, 1);
    }

    Y_UNIT_TEST(ThresholdChanges) {
        TDatabaseSpaceTracker tracker;
        tracker.SetPool(Pool1, Db1);
        tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));

        tracker.SetThresholds(TColor::YELLOW, TColor::YELLOW); // enabling the feature recalculates pools
        UNIT_ASSERT(tracker.IsExhausted(Db1));

        tracker.SetGroup(G(3), Pool1, TColor::GREEN); // new group -- pool is not exhausted anymore
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        tracker.RemoveGroup(G(3));
        UNIT_ASSERT(tracker.IsExhausted(Db1));

        tracker.SetThresholds(TColor::GREEN, TColor::GREEN); // disabling
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
    }

    Y_UNIT_TEST(UnknownColors) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
        tracker.SetPool(Pool1, Db1);

        // a group with unknown color doesn't let the pool get exhausted
        tracker.SetGroup(G(2), Pool1, std::nullopt);
        tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        tracker.SetGroup(G(2), Pool1, TColor::RED);
        UNIT_ASSERT(tracker.IsExhausted(Db1));

        // nor does it unblock the pool: colors are lost (e.g. group reconfigured and BS_CONTROLLER restarted)
        tracker.SetGroup(G(1), Pool1, std::nullopt);
        tracker.SetGroup(G(2), Pool1, std::nullopt);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        auto state = tracker.GetPoolState(Pool1);
        UNIT_ASSERT(state);
        UNIT_ASSERT_VALUES_EQUAL(state->NumGroups, 2);
        UNIT_ASSERT_VALUES_EQUAL(state->NumUnknownGroups, 2);
        UNIT_ASSERT(!state->BestColor);

        // colors refreshed within the hysteresis band -- still exhausted
        tracker.SetGroup(G(1), Pool1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        tracker.SetGroup(G(2), Pool1, TColor::LIGHT_YELLOW);
        UNIT_ASSERT(tracker.IsExhausted(Db1));

        // some group known to be better than the unblock color unblocks it, even if others are not known
        tracker.SetGroup(G(2), Pool1, std::nullopt);
        tracker.SetGroup(G(1), Pool1, TColor::CYAN);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
    }

    Y_UNIT_TEST(RestoredLatchWithUnknownColors) {
        TDatabaseSpaceTracker tracker;
        {
            TDatabaseSpaceTracker::TBatch batch(tracker);
            tracker.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
            tracker.SetPool(Pool1, Db1);
            tracker.SetGroup(G(1), Pool1, std::nullopt); // metrics were dropped by reassignment before restart
            tracker.SetGroup(G(2), Pool1, TColor::LIGHT_YELLOW);
            tracker.RestorePoolExhausted(Pool1, true);
        }
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        UNIT_ASSERT(tracker.TakeChangedLatches().empty());
    }

    Y_UNIT_TEST(PartiallyReportedGroups) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
        tracker.SetPool(Pool1, Db1);

        // group color is the worst of the reported VDisks, so it is only a lower bound until every VDisk reports; a
        // lower bound at the block color or worse is enough to block
        tracker.SetGroup(G(1), Pool1, TColor::YELLOW, false);
        tracker.SetGroup(G(2), Pool1, TColor::RED, false);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        auto state = tracker.GetPoolState(Pool1);
        UNIT_ASSERT(state);
        UNIT_ASSERT_VALUES_EQUAL(state->NumIncompleteGroups, 2);
        UNIT_ASSERT_VALUES_EQUAL(state->NumUnknownGroups, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.TakeChangedLatches().size(), 1);

        // but a partially reported group seemingly better than the unblock color doesn't unblock: the VDisks that
        // haven't reported yet may be worse (e.g. metrics were lost by reassignment and BS_CONTROLLER restart)
        tracker.SetGroup(G(1), Pool1, TColor::CYAN, false);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        UNIT_ASSERT(tracker.TakeChangedLatches().empty());

        // the same fully reported color does
        tracker.SetGroup(G(1), Pool1, TColor::CYAN);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        state = tracker.GetPoolState(Pool1);
        UNIT_ASSERT(state);
        UNIT_ASSERT_VALUES_EQUAL(state->NumIncompleteGroups, 1);

        // and a group missing the color entirely is not complete, whatever the caller says
        tracker.SetGroup(G(1), Pool1, std::nullopt, true);
        state = tracker.GetPoolState(Pool1);
        UNIT_ASSERT(state);
        UNIT_ASSERT_VALUES_EQUAL(state->NumUnknownGroups, 1);
        UNIT_ASSERT_VALUES_EQUAL(state->NumIncompleteGroups, 2);
    }

    Y_UNIT_TEST(GroupMovesAndPoolChanges) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::YELLOW);
        tracker.Subscribe(1, {Db1, Db2});
        tracker.SetPool(Pool1, Db1);
        tracker.SetPool(Pool2, Db2);
        tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        Changed(tracker);

        // group moves to another pool
        tracker.SetGroup(G(1), Pool2, TColor::YELLOW);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        UNIT_ASSERT(tracker.IsExhausted(Db2));
        UNIT_ASSERT_VALUES_EQUAL(Changed(tracker).size(), 2);

        // pool is rebound to another database
        tracker.SetPool(Pool2, Db1);
        UNIT_ASSERT(tracker.IsExhausted(Db1));
        UNIT_ASSERT(!tracker.IsExhausted(Db2));
        UNIT_ASSERT_VALUES_EQUAL(Changed(tracker).size(), 2);

        // pool is deleted
        tracker.RemovePool(Pool2);
        UNIT_ASSERT(!tracker.IsExhausted(Db1));
        UNIT_ASSERT_VALUES_EQUAL(Changed(tracker).size(), 1);
    }

    Y_UNIT_TEST(BatchIsOrderIndependent) {
        // groups {YELLOW, LIGHT_YELLOW} are between unblock and block colors taken together; evaluating YELLOW alone
        // first would latch the pool as exhausted
        for (const bool yellowFirst : {true, false}) {
            TDatabaseSpaceTracker tracker;
            tracker.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
            {
                TDatabaseSpaceTracker::TBatch batch(tracker);
                tracker.SetPool(Pool1, Db1);
                if (yellowFirst) {
                    tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
                    tracker.SetGroup(G(2), Pool1, TColor::LIGHT_YELLOW);
                } else {
                    tracker.SetGroup(G(2), Pool1, TColor::LIGHT_YELLOW);
                    tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
                }
                UNIT_ASSERT(!tracker.IsExhausted(Db1));
            }
            UNIT_ASSERT_C(!tracker.IsExhausted(Db1), "yellowFirst# " << yellowFirst);
        }
    }

    Y_UNIT_TEST(RestoredLatch) {
        auto restore = [](TColor::E color1, TColor::E color2) {
            auto tracker = std::make_unique<TDatabaseSpaceTracker>();
            TDatabaseSpaceTracker::TBatch batch(*tracker);
            tracker->SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
            tracker->SetPool(Pool1, Db1);
            tracker->SetGroup(G(1), Pool1, color1);
            tracker->SetGroup(G(2), Pool1, color2);
            tracker->RestorePoolExhausted(Pool1, true);
            return tracker;
        };

        // between unblock and block colors -- the restored latch holds
        auto tracker = restore(TColor::LIGHT_YELLOW, TColor::YELLOW);
        UNIT_ASSERT(tracker->IsExhausted(Db1));
        UNIT_ASSERT(tracker->TakeChangedLatches().empty());

        // some group is better than the unblock color -- unblocked, and the change is to be persisted
        tracker = restore(TColor::CYAN, TColor::YELLOW);
        UNIT_ASSERT(!tracker->IsExhausted(Db1));
        const auto latches = tracker->TakeChangedLatches();
        UNIT_ASSERT_VALUES_EQUAL(latches.size(), 1);
        UNIT_ASSERT(!latches.at(Pool1));
    }

    Y_UNIT_TEST(LatchChanges) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::YELLOW);
        tracker.SetPool(Pool1, Db1);
        tracker.SetGroup(G(1), Pool1, TColor::GREEN);
        UNIT_ASSERT(tracker.TakeChangedLatches().empty());

        tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
        auto latches = tracker.TakeChangedLatches();
        UNIT_ASSERT_VALUES_EQUAL(latches.size(), 1);
        UNIT_ASSERT(latches.at(Pool1));

        // the latch is stored along with the pool, so nothing is to be persisted for a removed one, even if it changed
        tracker.SetGroup(G(1), Pool1, TColor::GREEN);
        tracker.RemovePool(Pool1);
        UNIT_ASSERT(tracker.TakeChangedLatches().empty());
    }

    Y_UNIT_TEST(Subscriptions) {
        TDatabaseSpaceTracker tracker;
        tracker.SetThresholds(TColor::YELLOW, TColor::YELLOW);
        tracker.SetPool(Pool1, Db1);
        tracker.SetGroup(G(1), Pool1, TColor::YELLOW);
        UNIT_ASSERT(Changed(tracker).empty()); // nobody is subscribed

        tracker.Subscribe(1, {Db1});
        tracker.Subscribe(2, {Db1});
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSubscribers(Db1)->size(), 2);

        tracker.UnsubscribeNode(1);
        UNIT_ASSERT_VALUES_EQUAL(tracker.GetSubscribers(Db1)->size(), 1);
        tracker.Unsubscribe(2, {Db1});
        UNIT_ASSERT(!tracker.GetSubscribers(Db1));

        tracker.SetGroup(G(1), Pool1, TColor::GREEN);
        UNIT_ASSERT(Changed(tracker).empty());
    }

}
