#include "pipe_tracker.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPipeTrackerTest) {
    const ui64 txid1 = 1;
    const ui64 txid2 = 2;
    const ui64 txid3 = 3;
    const ui64 tablet1 = 100;
    const ui64 tablet2 = 101;

    Y_UNIT_TEST(TestSimpleAdd) {
        TPipeTrackerBase tracker;
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 0);
        tracker.AttachTablet(txid1, tablet1);
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet1).begin(), txid1);
        UNIT_ASSERT_EQUAL(tracker.FindTablets(txid1).begin()->second, tablet1);
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.DetachTablet(txid1, tablet1));
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 0);
    }

    Y_UNIT_TEST(TestAddSameTabletTwice) {
        TPipeTrackerBase tracker;
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        tracker.AttachTablet(txid1, tablet1, 1);
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet1).begin(), txid1);
        tracker.AttachTablet(txid1, tablet1, 2);
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet1).begin(), txid1);
        UNIT_ASSERT(!tracker.DetachTablet(txid1, tablet1, 2));
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet1).begin(), txid1);
        UNIT_ASSERT(tracker.DetachTablet(txid1, tablet1, 1));
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
    }

    Y_UNIT_TEST(TestAddTwoTablets) {
        TPipeTrackerBase tracker;
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        UNIT_ASSERT(tracker.FindTx(tablet2).size() == 0);
        tracker.AttachTablet(txid1, tablet1);
        tracker.AttachTablet(txid1, tablet2);
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT(tracker.FindTx(tablet2).size() == 1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet1).begin(), txid1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet2).begin(), txid1);
        UNIT_ASSERT(tracker.DetachTablet(txid1, tablet1));
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        UNIT_ASSERT(tracker.FindTx(tablet2).size() == 1);
        UNIT_ASSERT_EQUAL(*tracker.FindTx(tablet2).begin(), txid1);
        UNIT_ASSERT(tracker.DetachTablet(txid1, tablet2));
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        UNIT_ASSERT(tracker.FindTx(tablet2).size() == 0);
    }

    Y_UNIT_TEST(TestShareTablet) {
        TPipeTrackerBase tracker;
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
        tracker.AttachTablet(txid1, tablet1);
        tracker.AttachTablet(txid2, tablet1);
        UNIT_ASSERT(tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.IsTxAlive(txid2));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 2);
        UNIT_ASSERT(tracker.FindTx(tablet1).count(txid1) == 1);
        UNIT_ASSERT(tracker.FindTx(tablet1).count(txid2) == 1);
        UNIT_ASSERT(!tracker.DetachTablet(txid1, tablet1));
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.IsTxAlive(txid2));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT(tracker.FindTx(tablet1).count(txid2) == 1);
        tracker.AttachTablet(txid3, tablet1);
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.IsTxAlive(txid2));
        UNIT_ASSERT(tracker.IsTxAlive(txid3));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 2);
        UNIT_ASSERT(tracker.FindTx(tablet1).count(txid2) == 1);
        UNIT_ASSERT(tracker.FindTx(tablet1).count(txid3) == 1);
        UNIT_ASSERT(!tracker.DetachTablet(txid3, tablet1));
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(tracker.IsTxAlive(txid2));
        UNIT_ASSERT(!tracker.IsTxAlive(txid3));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 1);
        UNIT_ASSERT(tracker.FindTx(tablet1).count(txid2) == 1);
        UNIT_ASSERT(tracker.DetachTablet(txid2, tablet1));
        UNIT_ASSERT(!tracker.IsTxAlive(txid1));
        UNIT_ASSERT(!tracker.IsTxAlive(txid2));
        UNIT_ASSERT(!tracker.IsTxAlive(txid3));
        UNIT_ASSERT(tracker.FindTx(tablet1).size() == 0);
    }

    Y_UNIT_TEST(TestIdempotentAttachDetach) {
        TPipeTrackerBase tracker;
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 0);
        tracker.AttachTablet(txid1, tablet1, 5);
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 1);
        tracker.AttachTablet(txid1, tablet1, 5);
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 1);
        tracker.AttachTablet(txid1, tablet1, 6);
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 2);
        UNIT_ASSERT(!tracker.DetachTablet(txid1, tablet1, 5));
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 1);
        UNIT_ASSERT(!tracker.DetachTablet(txid1, tablet1, 5));
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 1);
        UNIT_ASSERT(tracker.DetachTablet(txid1, tablet1, 6));
        UNIT_ASSERT(tracker.FindTablets(txid1).size() == 0);
    }
}

}
