#include <ydb/core/base/scan_admission.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
Y_UNIT_TEST_SUITE(TScanAdmission) {
    Y_UNIT_TEST(CountAndByteLimitsAreIndependent) {
        TScanAdmissionControl quota;
        auto first = quota.TryAcquire(40, 2, 100);
        auto second = quota.TryAcquire(40, 2, 100);
        UNIT_ASSERT(first && second);
        UNIT_ASSERT(!quota.TryAcquire(1, 2, 100));
        UNIT_ASSERT(!quota.TryAcquire(21, 100, 100));
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Count, 2);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Bytes, 80);
        auto copy = first;
        first.reset();
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Bytes, 80);
        copy.reset();
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Bytes, 40);
        UNIT_ASSERT(quota.TryAcquire(60, 100, 100));
        second.reset();
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Bytes, 0);
    }

    Y_UNIT_TEST(ShrinkingLimitDrainsAndLeasesOutliveController) {
        std::shared_ptr<TScanAdmissionControl::TLease> lease;
        {
            TScanAdmissionControl quota;
            quota.SetTotalLimit(800);
            lease = quota.TryAcquire(100, 100, 1000);
            UNIT_ASSERT(lease);
            quota.SetTotalLimit(400);
            UNIT_ASSERT(!quota.TryAcquire(1, 100, 1000));
            lease.reset();
            UNIT_ASSERT(!quota.TryAcquire(51, 100, 1000));
            lease = quota.TryAcquire(50, 100, 1000);
            UNIT_ASSERT(lease);
        }
        lease.reset();
    }

    Y_UNIT_TEST(OverflowCannotBypassLimit) {
        TScanAdmissionControl quota;
        auto lease = quota.TryAcquire(1, 1, 100);
        UNIT_ASSERT(lease);
        UNIT_ASSERT(!quota.TryAcquire(Max<ui64>(), 100, 100));
        UNIT_ASSERT_VALUES_EQUAL(quota.GetUsage().Bytes, 1);
    }
}
}
