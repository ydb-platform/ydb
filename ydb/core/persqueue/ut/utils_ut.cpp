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

    Y_UNIT_TEST(Migration_Lifetime) {
        {
            NKikimrPQ::TPQTabletConfig config;
            config.MutablePartitionConfig()->SetLifetimeSeconds(123);
            NKikimr::NPQ::Migrate(config);

            UNIT_ASSERT_VALUES_EQUAL(true, config.GetMigrations().GetLifetime());
            UNIT_ASSERT_VALUES_EQUAL(false, config.GetPartitionConfig().HasStorageLimitBytes());
            UNIT_ASSERT_VALUES_EQUAL(123, config.GetPartitionConfig().GetLifetimeSeconds());
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            config.MutablePartitionConfig()->SetLifetimeSeconds(123);
            config.MutablePartitionConfig()->SetStorageLimitBytes(456);
            NKikimr::NPQ::Migrate(config);

            UNIT_ASSERT_VALUES_EQUAL(true, config.GetMigrations().GetLifetime());
            UNIT_ASSERT_VALUES_EQUAL(456, config.GetPartitionConfig().GetStorageLimitBytes());
            UNIT_ASSERT_VALUES_EQUAL(TDuration::Days(3650).Seconds(), config.GetPartitionConfig().GetLifetimeSeconds());
        }
        {
            NKikimrPQ::TPQTabletConfig config;
            config.MutableMigrations()->SetLifetime(true);
            config.MutablePartitionConfig()->SetLifetimeSeconds(123);
            config.MutablePartitionConfig()->SetStorageLimitBytes(456);
            NKikimr::NPQ::Migrate(config);

            UNIT_ASSERT_VALUES_EQUAL(true, config.GetMigrations().GetLifetime());
            UNIT_ASSERT_VALUES_EQUAL(456, config.GetPartitionConfig().GetStorageLimitBytes());
            UNIT_ASSERT_VALUES_EQUAL(123, config.GetPartitionConfig().GetLifetimeSeconds());
        }
    }
}

}
