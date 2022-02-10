#include <ydb/core/ymq/actor/message_delay_stats.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(MessageDelayStatsTest) {
    Y_UNIT_TEST(All) {
        TMessageDelayStatistics s;
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(42)), 0);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(40)), 0);

        s.AddDelayedMessage(TInstant::MilliSeconds(150), TInstant::MilliSeconds(100)); // start == 100
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(100)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(120)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(200)), 1);

        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(1099)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(1100)), 0);
        s.AddDelayedMessage(TInstant::MilliSeconds(5000), TInstant::MilliSeconds(2000));
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(2000)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(2200)), 1);

        s.AddDelayedMessage(TInstant::MilliSeconds(5000), TInstant::MilliSeconds(3000));
        s.AddDelayedMessage(TInstant::MilliSeconds(5000), TInstant::MilliSeconds(2900)); // decrease "now" is OK
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(3000)), 3);

        s.AddDelayedMessage(TInstant::MilliSeconds(6000), TInstant::MilliSeconds(3000));
        s.AddDelayedMessage(TInstant::MilliSeconds(7000), TInstant::MilliSeconds(3000));
        s.AddDelayedMessage(TInstant::MilliSeconds(8000), TInstant::MilliSeconds(3000));
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(3000)), 6);

        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(5500)), 3);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(6500)), 2);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(7500)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::MilliSeconds(8500)), 0);
    }

    Y_UNIT_TEST(BigTimeDiff) {
        TMessageDelayStatistics s;
        s.AddDelayedMessage(TInstant::Seconds(1000), TInstant::Seconds(500)); // start from 500
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::Seconds(500)), 1);

        s.AddDelayedMessage(TInstant::Seconds(100000), TInstant::Seconds(99990));
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::Seconds(99990)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::Seconds(101000)), 0);
    }

    Y_UNIT_TEST(MaxMessageDelay) {
        TMessageDelayStatistics s;
        s.AddDelayedMessage(TInstant::Seconds(1900), TInstant::Seconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::Seconds(1000)), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.UpdateAndGetMessagesDelayed(TInstant::Seconds(1001)), 1);
    }
}

} // namespace NKikimr::NSQS
