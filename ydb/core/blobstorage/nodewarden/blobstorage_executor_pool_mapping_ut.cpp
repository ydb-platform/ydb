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
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 3), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 4), 11);
    UNIT_ASSERT(!mapping.FindPoolId(5));
}

Y_UNIT_TEST(TieBreaksByConfiguredPoolOrder) {
    const TVector<ui32> pools{11, 10};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 11);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 10);
}

Y_UNIT_TEST(RetainsAssignmentOnRepeatedAcquire) {
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);

    // A running PDisk (e.g. on restart) must keep its pool even though the load
    // computation could now prefer another pool.
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);
    AssertPool(mapping, 1, 10);
    AssertPool(mapping, 2, 11);
}

Y_UNIT_TEST(ReleaseFreesPoolForNewPDisks) {
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);

    mapping.ReleasePoolId(2);
    UNIT_ASSERT(!mapping.FindPoolId(2));
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 3), 11);
    AssertPool(mapping, 1, 10);
    AssertPool(mapping, 3, 11);
}

Y_UNIT_TEST(ReplacementInProductionOrderBalances) {
    // Production ordering when one service-set update replaces PDisk 2 with PDisk 3:
    // the merge drops PDisk 2 from the configuration, then ApplyServiceSetPDisks
    // STARTS PDisk 3 before PDisk 2 is destroyed (destruction can even wait for later
    // updates while its VDisks drain). PDisk 3 must take the vacated slot on pool 11
    // instead of doubling up with PDisk 1 on pool 10.
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);

    mapping.RetainConfiguredPDisks({1, 3});                     // PDisk 2 leaves the config
    UNIT_ASSERT(!mapping.FindPoolId(2));
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 3), 11);

    mapping.ReleasePoolId(2);                                   // DestroyLocalPDisk, possibly much later
    AssertPool(mapping, 1, 10);
    AssertPool(mapping, 3, 11);
}

Y_UNIT_TEST(RetainKeepsConfiguredAssignments) {
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);

    mapping.RetainConfiguredPDisks({2});

    UNIT_ASSERT(!mapping.FindPoolId(1));
    AssertPool(mapping, 2, 11);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 3), 10);
}

Y_UNIT_TEST(ReleaseOfUnknownPDiskIsNoop) {
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    mapping.ReleasePoolId(2);
    AssertPool(mapping, 1, 10);
}

Y_UNIT_TEST(ReassignsReleasedPDiskByCurrentLoad) {
    const TVector<ui32> pools{10, 11};
    TBlobStorageExecutorPoolMapping mapping;

    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 2), 11);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 3), 10);

    // A PDisk destroyed and later re-added is a fresh assignment.
    mapping.ReleasePoolId(1);
    UNIT_ASSERT_VALUES_EQUAL(mapping.AcquirePoolId(pools, 1), 10);
}

} // Y_UNIT_TEST_SUITE(BlobStorageExecutorPoolMapping)
} // namespace NKikimr::NStorage
