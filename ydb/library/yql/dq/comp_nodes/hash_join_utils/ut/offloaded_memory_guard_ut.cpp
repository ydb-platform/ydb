#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/alloc.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NMiniKQL;

Y_UNIT_TEST_SUITE(TOffloadedMemoryGuardTest) {

    Y_UNIT_TEST(Balance) {
        TScopedAlloc alloc(__LOCATION__);
        const ui64 before = alloc.GetAllocated();
        {
            TOffloadedMemoryGuard guard(12345, /* enabled = */ true);
            UNIT_ASSERT_VALUES_EQUAL(guard.Bytes(), 12345);
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before + 12345);

            TOffloadedMemoryGuard moved = std::move(guard);
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before + 12345);
            guard.Release(); // moved-from: no-op
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before + 12345);

            TOffloadedMemoryGuard assigned;
            assigned = std::move(moved);
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before + 12345);
            assigned.Release();
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before);
            assigned.Release(); // idempotent
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before);
        }
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before);
    }

    Y_UNIT_TEST(Disabled) {
        TScopedAlloc alloc(__LOCATION__);
        const ui64 before = alloc.GetAllocated();
        {
            TOffloadedMemoryGuard guard(12345, /* enabled = */ false);
            UNIT_ASSERT_VALUES_EQUAL(guard.Bytes(), 12345); // the size is still carried
            UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before);
        }
        UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), before);
    }
}
