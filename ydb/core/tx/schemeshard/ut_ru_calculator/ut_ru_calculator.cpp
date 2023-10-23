#include "schemeshard_billing_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TRUCalculatorTests) {
    using TRUCalculator = NSchemeShard::TRUCalculator;

    Y_UNIT_TEST(TestReadTable) {
        const ui64 costPerMB = 128; // 1 MB = 128 RU

        UNIT_ASSERT_VALUES_EQUAL(0  * costPerMB, TRUCalculator::ReadTable(0));
        UNIT_ASSERT_VALUES_EQUAL(1  * costPerMB, TRUCalculator::ReadTable(1));

        UNIT_ASSERT_VALUES_EQUAL(1  * costPerMB, TRUCalculator::ReadTable(1_MB - 1));
        UNIT_ASSERT_VALUES_EQUAL(1  * costPerMB, TRUCalculator::ReadTable(1_MB));
        UNIT_ASSERT_VALUES_EQUAL(2  * costPerMB, TRUCalculator::ReadTable(1_MB + 1));

        UNIT_ASSERT_VALUES_EQUAL(2  * costPerMB, TRUCalculator::ReadTable(2_MB));
        UNIT_ASSERT_VALUES_EQUAL(10 * costPerMB, TRUCalculator::ReadTable(10_MB));
    }

    Y_UNIT_TEST(TestBulkUpsert) {
        const ui64 x2CostPerKB = 1; // 1 KB = 0.5 RU

        UNIT_ASSERT_VALUES_EQUAL(0 * x2CostPerKB, TRUCalculator::BulkUpsert(0, 0)); // 0 RU
        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(0, 1)); // 0.5 RU

        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB - 1, 1)); // 0.5 RU
        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB - 1, 2)); // 1 RU

        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB, 1)); // 0.5 RU
        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB, 2)); // 1 RU

        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB + 1, 1)); // 1 RU
        UNIT_ASSERT_VALUES_EQUAL(1 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB + 1, 2)); // 1 RU
        UNIT_ASSERT_VALUES_EQUAL(2 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB + 1, 3)); // 1.5 RU

        UNIT_ASSERT_VALUES_EQUAL(5 * x2CostPerKB, TRUCalculator::BulkUpsert(1_KB, 10)); // 5 RU
        UNIT_ASSERT_VALUES_EQUAL(5 * x2CostPerKB, TRUCalculator::BulkUpsert(10_KB, 1)); // 5 RU
        UNIT_ASSERT_VALUES_EQUAL(5 * x2CostPerKB, TRUCalculator::BulkUpsert(10_KB, 10)); // 5 RU
    }
}

} // NKikimr
