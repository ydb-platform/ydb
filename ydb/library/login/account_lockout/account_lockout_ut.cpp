#include <library/cpp/testing/unittest/registar.h>
#include <util/string/builder.h>
#include "account_lockout.h"

using namespace NLogin;

Y_UNIT_TEST_SUITE(AccountLockoutTest) {
    Y_UNIT_TEST(InitCorrectValue) {
        TAccountLockout accountLockout({.AttemptThreshold = 5, .AttemptResetDuration = "5h"});
        UNIT_ASSERT_VALUES_EQUAL(accountLockout.AttemptThreshold, 5);
        UNIT_ASSERT_EQUAL(accountLockout.AttemptResetDuration, std::chrono::seconds(5 * 60 * 60));
    }

    Y_UNIT_TEST(InitZeroValue) {
        TAccountLockout accountLockout({.AttemptThreshold = 5, .AttemptResetDuration = "0"});
        UNIT_ASSERT_VALUES_EQUAL(accountLockout.AttemptThreshold, 5);
        UNIT_ASSERT_EQUAL(accountLockout.AttemptResetDuration, std::chrono::seconds(0));
    }

    Y_UNIT_TEST(InitEmptyValue) {
        TAccountLockout accountLockout({.AttemptThreshold = 5, .AttemptResetDuration = ""});
        UNIT_ASSERT_VALUES_EQUAL(accountLockout.AttemptThreshold, 5);
        UNIT_ASSERT_EQUAL(accountLockout.AttemptResetDuration, std::chrono::seconds(0));
    }

    Y_UNIT_TEST(InitWrongValue) {
        TAccountLockout accountLockout({.AttemptThreshold = 5, .AttemptResetDuration = "balablabla"});
        UNIT_ASSERT_VALUES_EQUAL(accountLockout.AttemptThreshold, 5);
        UNIT_ASSERT_EQUAL(accountLockout.AttemptResetDuration, std::chrono::seconds(0));
    }
}
