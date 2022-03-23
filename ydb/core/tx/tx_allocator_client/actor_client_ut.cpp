#include "ut_helpers.h"
#include "actor_client.h"

#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <util/generic/xrange.h>

using namespace NKikimr;
using namespace NTxAllocatorUT_Private;

Y_UNIT_TEST_SUITE(TTxAllocatorClientTest) {
    Y_UNIT_TEST(Boot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
    }

    Y_UNIT_TEST(InitiatingRequest) {
        TTestBasicRuntime runtime;

        TMsgCounter initiatingCounter(runtime, TEvTxAllocator::TEvAllocate::EventType);

        TTestEnv env(runtime);
        env.AllocateAndCheck(1);

        initiatingCounter.Wait(1);
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 1, "one request for first batch");
    }

    Y_UNIT_TEST(ZeroRange) {
        TTestBasicRuntime runtime;

        TMsgCounter initiatingCounter(runtime, TEvTxAllocator::TEvAllocate::EventType);

        TTestEnv env(runtime);
        env.AllocateAndCheck(1);

        initiatingCounter.Wait(1);
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 1, "one request for first batch");

        {
            TMsgCounter counter(runtime, TEvTxAllocator::TEvAllocate::EventType);
            for (int i = 0; i < 5500; ++i) {
                env.AllocateAndCheck(0);
                UNIT_ASSERT_VALUES_EQUAL_C(counter.Get(), 0, "no msg expected");
            }
        }
    }

    Y_UNIT_TEST(AllocateOverTheEdge) {
        TTestBasicRuntime runtime;

        TMsgCounter initiatingCounter(runtime, TEvTxAllocator::TEvAllocate::EventType);

        TTestEnv env(runtime);
        env.AllocateAndCheck(1);

        initiatingCounter.Wait(1);
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 1, "one request for first batch");

        env.AllocateAndCheck(1000);
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 1, "no additional request");

        env.AllocateAndCheck(1000); // -2000
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 1, "no additional request");

        env.AllocateAndCheck(1000); // -3000
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 1, "no additional request");

        env.AllocateAndCheck(1000); // -4000
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 2, "one additional request");

        env.AllocateAndCheck(500); // -4500
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 2, "no additional request");

        env.AllocateAndCheck(1000); // -5000 -500
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 2, "no additional request");

        env.AllocateAndCheck(2500); // -5000 -3000
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 2, "no additional request");

        env.AllocateAndCheck(1000); // -5000 -4000
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 3, "one additional request");

        env.AllocateAndCheck(3000); // -5000 -5000 -2000
        UNIT_ASSERT_VALUES_EQUAL_C(initiatingCounter.Get(), 3, "one additional request");
    }
}
