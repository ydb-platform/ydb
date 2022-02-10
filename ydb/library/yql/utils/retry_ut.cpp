#include "retry.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

namespace {

class TMyError : public yexception {
};

}

Y_UNIT_TEST_SUITE(TRetryTests) {
    Y_UNIT_TEST(ZeroAttempts) {
        auto r = WithRetry<TMyError>(0,
            []() { return TString("abc"); },
            [](auto, auto, auto) { UNIT_FAIL("Exception handler invoked"); });

        UNIT_ASSERT_VALUES_EQUAL("abc", r);
    }

    Y_UNIT_TEST(NoRetries) {
        auto r = WithRetry<TMyError>(5,
            []() { return TString("abc"); },
            [](auto, auto, auto) { UNIT_FAIL("Exception handler invoked"); });

        UNIT_ASSERT_VALUES_EQUAL("abc", r);
    }

    Y_UNIT_TEST(NoRetriesButException) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(WithRetry<TMyError>(5,
            []() { throw yexception() << "xxxx"; },
            [](auto, auto, auto) { UNIT_FAIL("Exception handler invoked"); }), yexception, "xxxx");
    }

    Y_UNIT_TEST(FewRetries) {
        int counter = 0;
        int exceptions = 0;
        auto r = WithRetry<TMyError>(3, [&]() {
            if (counter++ < 2) {
                throw TMyError() << "yyyy";
            }

            return counter;
        }, [&](const auto& e, int attempt, int attemptCount) {
            ++exceptions;
            UNIT_ASSERT_VALUES_EQUAL(e.what(), "yyyy");
            UNIT_ASSERT_VALUES_EQUAL(attempt, counter);
            UNIT_ASSERT_VALUES_EQUAL(attemptCount, 3);
        });

        UNIT_ASSERT_VALUES_EQUAL(2, exceptions);
        UNIT_ASSERT_VALUES_EQUAL(3, r);
        UNIT_ASSERT_VALUES_EQUAL(3, counter);
    }

    Y_UNIT_TEST(ManyRetries) {
        int counter = 0;
        int exceptions = 0;
        UNIT_ASSERT_EXCEPTION_CONTAINS(WithRetry<TMyError>(3, [&]() {
            throw TMyError() << "yyyy" << counter++;
        }, [&](const auto& e, int attempt, int attemptCount) {
            ++exceptions;
            UNIT_ASSERT_STRING_CONTAINS(e.what(), "yyyy");
            UNIT_ASSERT_VALUES_EQUAL(attempt, counter);
            UNIT_ASSERT_VALUES_EQUAL(attemptCount, 3);
        }), TMyError, "yyyy2");

        UNIT_ASSERT_VALUES_EQUAL(2, exceptions);
        UNIT_ASSERT_VALUES_EQUAL(3, counter);
    }
}
