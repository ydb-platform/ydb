#define HIDE_DELETED_ABI_FUNCTIONS
#include "udf_value_builder.h"
#include "udf_ut_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TUdfValueBuilder) {
    Y_UNIT_TEST(LockMethodsTable) {
        using NYql::GetMethodIndex;
        UNIT_ASSERT_VALUES_EQUAL(2, GetMethodIndex(&IValueBuilder::NewStringNotFilled));
        UNIT_ASSERT_VALUES_EQUAL(3, GetMethodIndex(&IValueBuilder::NewString));
        UNIT_ASSERT_VALUES_EQUAL(4, GetMethodIndex(&IValueBuilder::ConcatStrings));
        UNIT_ASSERT_VALUES_EQUAL(5, GetMethodIndex(&IValueBuilder::AppendString));
        UNIT_ASSERT_VALUES_EQUAL(6, GetMethodIndex(&IValueBuilder::PrependString));
        UNIT_ASSERT_VALUES_EQUAL(7, GetMethodIndex(&IValueBuilder::SubString));
        UNIT_ASSERT_VALUES_EQUAL(8, GetMethodIndex(&IValueBuilder::NewDict));
        UNIT_ASSERT_VALUES_EQUAL(9, GetMethodIndex(&IValueBuilder::NewList));
        UNIT_ASSERT_VALUES_EQUAL(10, GetMethodIndex(&IValueBuilder::ReverseList));
        UNIT_ASSERT_VALUES_EQUAL(11, GetMethodIndex(&IValueBuilder::SkipList));
        UNIT_ASSERT_VALUES_EQUAL(12, GetMethodIndex(&IValueBuilder::TakeList));
        UNIT_ASSERT_VALUES_EQUAL(13, GetMethodIndex(&IValueBuilder::ToIndexDict));
        UNIT_ASSERT_VALUES_EQUAL(14, GetMethodIndex(&IValueBuilder::NewArray32));
        UNIT_ASSERT_VALUES_EQUAL(15, GetMethodIndex(&IValueBuilder::NewVariant));
        UNIT_ASSERT_VALUES_EQUAL(16, GetMethodIndex(&IValueBuilder::GetDateBuilder));
        UNIT_ASSERT_VALUES_EQUAL(17, GetMethodIndex(&IValueBuilder::GetSecureParam));
        UNIT_ASSERT_VALUES_EQUAL(18, GetMethodIndex(&IValueBuilder::CalleePosition));
        UNIT_ASSERT_VALUES_EQUAL(19, GetMethodIndex(&IValueBuilder::Run));
        UNIT_ASSERT_VALUES_EQUAL(20, GetMethodIndex(&IValueBuilder::ExportArrowBlock));
        UNIT_ASSERT_VALUES_EQUAL(21, GetMethodIndex(&IValueBuilder::ImportArrowBlock));
        UNIT_ASSERT_VALUES_EQUAL(22, GetMethodIndex(&IValueBuilder::GetArrowBlockChunks));
        UNIT_ASSERT_VALUES_EQUAL(23, GetMethodIndex(&IValueBuilder::GetPgBuilder));
        UNIT_ASSERT_VALUES_EQUAL(24, GetMethodIndex(&IValueBuilder::NewArray64));
    }
}
