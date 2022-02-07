#include <library/cpp/testing/unittest/registar.h>

#include "local_flags.h"

using namespace NBus;
using namespace NBus::NPrivate;

Y_UNIT_TEST_SUITE(EMessageLocalFlags) {
    Y_UNIT_TEST(TestLocalFlagSetToString) {
        UNIT_ASSERT_VALUES_EQUAL("0", LocalFlagSetToString(0));
        UNIT_ASSERT_VALUES_EQUAL("MESSAGE_REPLY_INTERNAL",
                                 LocalFlagSetToString(MESSAGE_REPLY_INTERNAL));
        UNIT_ASSERT_VALUES_EQUAL("MESSAGE_IN_WORK|MESSAGE_IN_FLIGHT_ON_CLIENT",
                                 LocalFlagSetToString(MESSAGE_IN_WORK | MESSAGE_IN_FLIGHT_ON_CLIENT));
        UNIT_ASSERT_VALUES_EQUAL("0xff3456",
                                 LocalFlagSetToString(0xff3456));
    }
}
