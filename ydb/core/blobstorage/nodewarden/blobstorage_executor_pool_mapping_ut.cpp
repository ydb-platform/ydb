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

Y_UNIT_TEST(AssignsPDisksRoundRobinAndIgnoresDuplicates) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2, 1, 3, 4});

    AssertPool(mapping, 1, 10);
    AssertPool(mapping, 2, 11);
    AssertPool(mapping, 3, 10);
    AssertPool(mapping, 4, 11);
    UNIT_ASSERT(!mapping.FindPoolId(5));
}

Y_UNIT_TEST(PreservesAssignmentsWhilePoolsExist) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2, 3});
    mapping.Update({11, 10}, {3, 2, 4});

    AssertPool(mapping, 3, 10);
    AssertPool(mapping, 2, 11);
    AssertPool(mapping, 4, 11);
    UNIT_ASSERT(!mapping.FindPoolId(1));
}

Y_UNIT_TEST(BalancesNewPDisksAgainstRetainedAssignments) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {2, 1});
    AssertPool(mapping, 2, 10);
    AssertPool(mapping, 1, 11);

    // PDisk 2 is gone, pdisk 3 is new; pdisk 1 keeps pool 11, so pdisk 3 must go to pool 10.
    mapping.Update({10, 11}, {1, 3});
    AssertPool(mapping, 1, 11);
    AssertPool(mapping, 3, 10);
}

Y_UNIT_TEST(ReassignsPDisksWhenExecutorPoolsChangeAndClearsEmptyMapping) {
    TBlobStorageExecutorPoolMapping mapping;
    mapping.Update({10, 11}, {1, 2, 3});
    mapping.Update({20, 21}, {1, 2, 3});

    AssertPool(mapping, 1, 20);
    AssertPool(mapping, 2, 21);
    AssertPool(mapping, 3, 20);

    mapping.Update({}, {1, 2, 3});
    UNIT_ASSERT(!mapping.FindPoolId(1));
    UNIT_ASSERT(!mapping.FindPoolId(2));
    UNIT_ASSERT(!mapping.FindPoolId(3));
}

} // Y_UNIT_TEST_SUITE(BlobStorageExecutorPoolMapping)
} // namespace NKikimr::NStorage
