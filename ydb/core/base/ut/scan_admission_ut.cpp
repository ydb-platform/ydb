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
            lease = quota.TryAcquire(200, 100, 1000);
            UNIT_ASSERT(lease);
            quota.SetTotalLimit(400);
            UNIT_ASSERT(!quota.TryAcquire(1, 100, 1000));
            lease.reset();
            UNIT_ASSERT(!quota.TryAcquire(101, 100, 1000));
            lease = quota.TryAcquire(100, 100, 1000);
            UNIT_ASSERT(lease);
        }
        lease.reset();
    }

    Y_UNIT_TEST(ActiveBudgetRespectsBrokerLimitAndPendingSlice) {
        TScanAdmissionControl quota;
        quota.SetTotalLimit(256, 192);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetByteLimit(64), 64);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 192);
        quota.RestrictBrokerLimit(96, quota.GetBrokerLimitGeneration());
        quota.SetTotalLimit(256, 192);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 96);
        quota.ConfirmBrokerLimit(192);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 192);
        quota.SetTotalLimit(256, 32);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 32);
        quota.SetTotalLimit(128);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetByteLimit(64), 32);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 96);
        quota.SetTotalLimit(128, 0);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 0);
    }

    Y_UNIT_TEST(StaleRejectionCannotOverrideConfirmedBrokerLimit) {
        TScanAdmissionControl quota;
        quota.SetTotalLimit(256, 192, true);
        const ui64 generation = quota.GetBrokerLimitGeneration();
        quota.RestrictBrokerLimit(64, generation);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 64);
        quota.ConfirmBrokerLimit(128);
        UNIT_ASSERT(!quota.IsBrokerConfigurationPending());
        quota.RestrictBrokerLimit(64, generation);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 128);
        quota.RestrictBrokerLimit(96, quota.GetBrokerLimitGeneration());
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 96);
    }

    Y_UNIT_TEST(StaleRejectionCannotOverrideResetBrokerLimit) {
        TScanAdmissionControl quota;
        quota.SetTotalLimit(256, 64, true);
        const ui64 generation = quota.GetBrokerLimitGeneration();
        quota.RestrictBrokerLimit(32, generation);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 32);

        quota.SetTotalLimit(256, 128, true);
        quota.RestrictBrokerLimit(32, generation);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 128);
        UNIT_ASSERT(quota.IsBrokerConfigurationPending());

        const ui64 resetGeneration = quota.GetBrokerLimitGeneration();
        quota.RestrictBrokerLimit(96, resetGeneration);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 96);
        // A tick which keeps the active limit must preserve its current hint.
        quota.SetTotalLimit(512, 128, true);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetBrokerLimitGeneration(), resetGeneration);
        UNIT_ASSERT_VALUES_EQUAL(quota.GetActiveByteLimit(64), 96);
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
