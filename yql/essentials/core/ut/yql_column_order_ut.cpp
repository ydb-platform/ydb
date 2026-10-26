#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/mem.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TYqlColumnOrder) {

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

Y_UNIT_TEST(ColumnOrderCaseSensetive) {
    TColumnOrder order;
    UNIT_ASSERT_EQUAL(order.AddColumn("a"), "a");
    UNIT_ASSERT_EQUAL(order.AddColumn("A"), "A");
    UNIT_ASSERT_EQUAL(order.IsDuplicatedIgnoreCase("a"), true);
}

Y_UNIT_TEST(ColumnOrderGeneratedNameAlreadyPresent) {
    TColumnOrder order;
    UNIT_ASSERT_VALUES_EQUAL(order.AddColumn("x_generated_2"), "x_generated_2");
    UNIT_ASSERT_VALUES_EQUAL(order.AddColumn("x"), "x");
    UNIT_ASSERT_VALUES_EQUAL(order.AddColumn("x"), "x_generated_3");
    UNIT_ASSERT_VALUES_EQUAL(order.Find("x_generated_2"), "x_generated_2");
    UNIT_ASSERT_VALUES_EQUAL(order.Find("x_generated_3"), "x");
    order.Shrink(2);
    UNIT_ASSERT_VALUES_EQUAL(order.AddColumn("x"), "x_generated_3");
}

Y_UNIT_TEST(ColumnOrderMultipleGeneratedNamesAlreadyPresent) {
    TColumnOrder order(TVector<TString>{"x_generated_2", "x_generated_3", "x", "x", "x"});
    UNIT_ASSERT_VALUES_EQUAL(order.GetPhysicalNames(), (TVector<TString>{"x_generated_2", "x_generated_3", "x", "x_generated_4", "x_generated_5"}));
    UNIT_ASSERT_VALUES_EQUAL(order.GetLogicalNames(), (TVector<TString>{"x_generated_2", "x_generated_3", "x", "x", "x"}));
    UNIT_ASSERT_VALUES_EQUAL(order.Find("x_generated_2"), "x_generated_2");
    UNIT_ASSERT_VALUES_EQUAL(order.Find("x_generated_4"), "x");
}

Y_UNIT_TEST(ColumnOrderGeneratedMatchOverVectorCtor) {
    TColumnOrder order(TVector<TString>{"a", "a", "a_generated_2", "a_generated_2_generated_2"});
    TVector<TColumnOrder::TOrderedItem> got(order.begin(), order.end());
    TVector<TColumnOrder::TOrderedItem> ex{{"a", "a"}, {"a", "a_generated_2"}, {"a_generated_2", "a_generated_2_generated_2"}, {"a_generated_2_generated_2", "a_generated_2_generated_2_generated_2"}};
    UNIT_ASSERT_EQUAL(got, ex);
}
} // Y_UNIT_TEST_SUITE(TYqlColumnOrder)
