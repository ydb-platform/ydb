#include "blobstorage_executor_pool_mapping.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NStorage {
namespace {

void AssertPool(const TBlobStorageExecutorPoolMapping& mapping, ui32 pdiskId, ui32 expectedPoolId) {
    const std::optional<ui32> poolId = mapping.FindPoolId(pdiskId);
    UNIT_ASSERT_C(poolId, "PDisk is missing from executor pool mapping");
    UNIT_ASSERT_VALUES_EQUAL(*poolId, expectedPoolId);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobStorageExecutorPoolMapping) {

Y_UNIT_TEST(BalancesPDisksAcrossPools) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2, 3, 4});

    THashMap<ui32, ui32> countByPool;
    for (const ui32 pdiskId : {1, 2, 3, 4}) {
        const std::optional<ui32> poolId = mapping.FindPoolId(pdiskId);
        UNIT_ASSERT_C(poolId, "PDisk is missing from executor pool mapping");
        UNIT_ASSERT(*poolId == 10 || *poolId == 11);
        ++countByPool[*poolId];
    }
    UNIT_ASSERT_VALUES_EQUAL(countByPool[10], 2);
    UNIT_ASSERT_VALUES_EQUAL(countByPool[11], 2);
    UNIT_ASSERT(!mapping.FindPoolId(5));
}

Y_UNIT_TEST(PreservesAssignmentsWhilePoolsExist) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2, 3});
    const std::optional<ui32> pool2 = mapping.FindPoolId(2);
    const std::optional<ui32> pool3 = mapping.FindPoolId(3);
    UNIT_ASSERT(pool2);
    UNIT_ASSERT(pool3);

    mapping.Update({11, 10}, {3, 2, 4});

    AssertPool(mapping, 2, *pool2);
    AssertPool(mapping, 3, *pool3);
    UNIT_ASSERT(mapping.FindPoolId(4));
    UNIT_ASSERT(!mapping.FindPoolId(1));
}

Y_UNIT_TEST(BalancesNewPDisksAgainstRetainedAssignments) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1});
    AssertPool(mapping, 1, 10);

    mapping.Update({10, 11}, {1, 2});
    AssertPool(mapping, 1, 10);
    AssertPool(mapping, 2, 11);
}

Y_UNIT_TEST(ReassignsPDisksWhenExecutorPoolsChangeAndClearsEmptyMapping) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2, 3});
    mapping.Update({20, 21}, {1, 2, 3});

    THashMap<ui32, ui32> countByPool;
    for (const ui32 pdiskId : {1, 2, 3}) {
        const std::optional<ui32> poolId = mapping.FindPoolId(pdiskId);
        UNIT_ASSERT_C(poolId, "PDisk is missing from executor pool mapping");
        UNIT_ASSERT(*poolId == 20 || *poolId == 21);
        ++countByPool[*poolId];
    }
    UNIT_ASSERT_VALUES_EQUAL(countByPool[20] + countByPool[21], 3);
    UNIT_ASSERT(countByPool[20] == 1 || countByPool[20] == 2);
    UNIT_ASSERT(countByPool[21] == 1 || countByPool[21] == 2);

    mapping.Update({}, {1, 2, 3});
    UNIT_ASSERT(!mapping.FindPoolId(1));
    UNIT_ASSERT(!mapping.FindPoolId(2));
    UNIT_ASSERT(!mapping.FindPoolId(3));
}

Y_UNIT_TEST(ClearsMappingWhenNoPDisksRemain) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2});
    mapping.Update({10, 11}, {});

    UNIT_ASSERT(!mapping.FindPoolId(1));
    UNIT_ASSERT(!mapping.FindPoolId(2));
}

} // Y_UNIT_TEST_SUITE(BlobStorageExecutorPoolMapping)
} // namespace NKikimr::NStorage
