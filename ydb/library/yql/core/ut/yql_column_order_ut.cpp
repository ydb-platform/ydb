#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/mem.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TYqlColumnOrder) {

    Y_UNIT_TEST(ColumnOrderIgnoreCase) {
        TColumnOrder order;
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a");
        UNIT_ASSERT_EQUAL(order.AddColumn("A"), "A_generated_2");
    }

    Y_UNIT_TEST(ColumnOrderShrink) {
        TColumnOrder order;
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a");
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a_generated_2");
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a_generated_3");
        order.Shrink(2);
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a_generated_3");
    }

    Y_UNIT_TEST(ColumnOrderGeneratedMatch) {
        TColumnOrder order;
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a");
        UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a_generated_2");
        UNIT_ASSERT_EQUAL(order.AddColumn("a_generated_2"), "a_generated_2_generated_2");
        UNIT_ASSERT_EQUAL(order.AddColumn("a_generated_2_generated_2"), "a_generated_2_generated_2_generated_2");
    }

    Y_UNIT_TEST(ColumnOrderGeneratedMatchOverVectorCtor) {
        TColumnOrder order(TVector<TString>{"a", "a", "a_generated_2", "a_generated_2_generated_2"});
        TVector<TColumnOrder::TOrderedItem> got(order.begin(), order.end());
        TVector<TColumnOrder::TOrderedItem> ex{{"a", "a"}, {"a", "a_generated_2"}, {"a_generated_2", "a_generated_2_generated_2"}, {"a_generated_2_generated_2", "a_generated_2_generated_2_generated_2"}};
        UNIT_ASSERT_EQUAL(got, ex);
    }
}

