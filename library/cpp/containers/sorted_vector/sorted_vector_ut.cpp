#include "sorted_vector.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

Y_UNIT_TEST_SUITE(TestSimpleMap) {

    Y_UNIT_TEST(TestFindPrt) {
        NSorted::TSimpleMap<TString, TString> map(
            {std::make_pair(TString("a"), TString("a")), std::make_pair(TString("b"), TString("b"))});

        UNIT_ASSERT_VALUES_UNEQUAL(map.FindPtr(TString("a")), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(map.FindPtr(TString("c")), nullptr);

        UNIT_ASSERT_VALUES_UNEQUAL(map.FindPtr(TStringBuf("a")), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(map.FindPtr(TStringBuf("c")), nullptr);

        UNIT_ASSERT_VALUES_UNEQUAL(map.FindPtr("a"), nullptr);
        UNIT_ASSERT_VALUES_EQUAL(map.FindPtr("c"), nullptr);
    }

}
