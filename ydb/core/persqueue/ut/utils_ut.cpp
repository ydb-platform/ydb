#include <ydb/core/persqueue/utils.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPQUtilsTest) {
    Y_UNIT_TEST(TLastCounter) {
        TLastCounter counter;

        TInstant now = TInstant::Now();

        {
            auto r = counter.Count(now);
            UNIT_ASSERT_VALUES_EQUAL(r, 0);
        }

        {
            counter.Use("v-1", now);
            auto r = counter.Count(now);
            UNIT_ASSERT_VALUES_EQUAL(r, 1);
        }

        {
            counter.Use("v-1", now);
            auto r = counter.Count(now);
            UNIT_ASSERT_VALUES_EQUAL(r, 1);
        }

        now += TDuration::Seconds(1);

        {
            counter.Use("v-1", now);
            auto r = counter.Count(now - TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(r, 1);
        }

        {
            auto r = counter.Count(now);
            UNIT_ASSERT_VALUES_EQUAL(r, 1);
        }

        {
            counter.Use("v-2", now);
            auto r = counter.Count(now - TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(r, 2);
        }

        {
            counter.Use("v-1", now);
            auto r = counter.Count(now - TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(r, 2);
        }

        now += TDuration::Seconds(1);

        {
            counter.Use("v-3", now);
            auto r = counter.Count(now - TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(r, 2);
        }

        now += TDuration::Seconds(1);

        {
            counter.Use("v-3", now);
            auto r = counter.Count(now - TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(r, 2);
        }

        now += TDuration::Seconds(1);

        {
            counter.Use("v-2", now);
            auto r = counter.Count(now - TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(r, 2);
        }
    }
}

}
