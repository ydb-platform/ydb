#include "udf_registrator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cstddef>
#include <type_traits>

namespace NYql::NUdf {

Y_UNIT_TEST_SUITE(TUdfRegistrator) {
Y_UNIT_TEST(LockStaticSymbolsLayout) {
    static_assert(std::is_same_v<decltype(TStaticSymbols::Reserved1), void* (*)(ui64)>);
    static_assert(std::is_same_v<decltype(TStaticSymbols::Reserved2), void (*)(const void*)>);
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, Reserved1), 0);
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, Reserved2), sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfTerminate), 2 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfRegisterObject), 3 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfUnregisterObject), 4 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfAllocateWithSizeFunc), 5 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfFreeWithSizeFunc), 6 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfArrowAllocateFunc), 7 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfArrowReallocateFunc), 8 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfArrowFreeFunc), 9 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(sizeof(TStaticSymbols), 10 * sizeof(void*));

    const auto symbols = GetStaticSymbols();
    UNIT_ASSERT(!symbols.Reserved1);
    UNIT_ASSERT(!symbols.Reserved2);
}

} // Y_UNIT_TEST_SUITE(TUdfRegistrator)

} // namespace NYql::NUdf
