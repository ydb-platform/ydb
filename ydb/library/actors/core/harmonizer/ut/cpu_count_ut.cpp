#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/core/executor_pool_basic.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(CpuCountTest) {
    Y_UNIT_TEST(TestWithoutSharedThread) {
        TBasicExecutorPoolConfig config;
        config.MinThreadCount = 1;
        config.DefaultThreadCount = 2;
        config.MaxThreadCount = 3;
        config.Threads = 3;

        TBasicExecutorPool pool(config, nullptr, nullptr);

        UNIT_ASSERT_EQUAL(pool.GetFullThreadCount(), 3);
        UNIT_ASSERT_EQUAL(pool.GetMinFullThreadCount(), 1);
        UNIT_ASSERT_EQUAL(pool.GetMaxFullThreadCount(), 3);
        UNIT_ASSERT_EQUAL(pool.GetDefaultFullThreadCount(), 2);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 3, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetMinThreadCount(), 1, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetMaxThreadCount(), 3, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetDefaultThreadCount(), 2, 1e-6);

        pool.SetFullThreadCount(2);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 2);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 2, 1e-6);

        pool.SetFullThreadCount(1);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 1, 1e-6);

        pool.SetFullThreadCount(0);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 1, 1e-6);

        pool.SetFullThreadCount(10);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 3);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 3, 1e-6);

        pool.SetSharedCpuQuota(0.3);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 3.3, 1e-6);

        pool.SetFullThreadCount(1);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 1.3, 1e-6);
    }

    Y_UNIT_TEST(TestWithSharedThread) {
        TBasicExecutorPoolConfig config;
        config.MinThreadCount = 1;
        config.DefaultThreadCount = 2;
        config.MaxThreadCount = 3;
        config.Threads = 3;
        config.HasSharedThread = true;

        TBasicExecutorPool pool(config, nullptr, nullptr);

        UNIT_ASSERT_EQUAL(pool.GetFullThreadCount(), 2);
        UNIT_ASSERT_EQUAL(pool.GetDefaultFullThreadCount(), 1);
        UNIT_ASSERT_EQUAL(pool.GetMinFullThreadCount(), 0);
        UNIT_ASSERT_EQUAL(pool.GetMaxFullThreadCount(), 2);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 2, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetDefaultThreadCount(), 2, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetMinThreadCount(), 1, 1e-6);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetMaxThreadCount(), 3, 1e-6);

        pool.SetFullThreadCount(1);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 1, 1e-6);

        pool.SetFullThreadCount(0);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 0);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 0, 1e-6);

        pool.SetFullThreadCount(10);
        UNIT_ASSERT_VALUES_EQUAL(pool.GetFullThreadCount(), 2);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 2, 1e-6);

        pool.SetSharedCpuQuota(0.3);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 2.3, 1e-6);

        pool.SetFullThreadCount(0);
        UNIT_ASSERT_DOUBLES_EQUAL(pool.GetThreadCount(), 0.3, 1e-6);
    }
}
