#include <library/cpp/testing/unittest/registar.h>
#include "util_string.h"

namespace NKikimr::NUtil {

Y_UNIT_TEST_SUITE(UtilString) {
    Y_UNIT_TEST(ShrinkToFit) {
        { // don't shrink if not much changes
            TString s;
            s.reserve(150);
            s += std::string(120, 'x');
            auto before = s.data();
            ShrinkToFit(s);
            UNIT_ASSERT_GE(s.capacity(), 150);
            UNIT_ASSERT_VALUES_EQUAL((size_t)s.data(), (size_t)before);
        }

        { // shrink if much changes
            TString s, copy;
            s.reserve(150);
            s += std::string(80, 'x');
            copy = s.copy();
            auto before = s.data();
            ShrinkToFit(s);
            UNIT_ASSERT_LE(s.capacity(), 100);
            UNIT_ASSERT_VALUES_UNEQUAL((size_t)s.data(), (size_t)before);
            UNIT_ASSERT_VALUES_EQUAL(copy, s);
        }
    }
}

}
