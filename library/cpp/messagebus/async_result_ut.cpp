
#include <library/cpp/testing/unittest/registar.h>

#include "async_result.h"

namespace {
    void SetValue(int* location, const int& value) {
        *location = value;
    }

}

Y_UNIT_TEST_SUITE(TAsyncResult) {
    Y_UNIT_TEST(AndThen_Here) {
        TAsyncResult<int> r;

        int var = 1;

        r.SetResult(17);

        r.AndThen(std::bind(&SetValue, &var, std::placeholders::_1));

        UNIT_ASSERT_VALUES_EQUAL(17, var);
    }

    Y_UNIT_TEST(AndThen_Later) {
        TAsyncResult<int> r;

        int var = 1;

        r.AndThen(std::bind(&SetValue, &var, std::placeholders::_1));

        r.SetResult(17);

        UNIT_ASSERT_VALUES_EQUAL(17, var);
    }
}
