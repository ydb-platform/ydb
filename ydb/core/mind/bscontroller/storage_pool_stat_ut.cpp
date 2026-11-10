#include "impl.h" // includes storage_pool_stat.h along with the definitions it needs

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NBsController;

Y_UNIT_TEST_SUITE(StoragePoolStat) {

    // the counters of a deleted storage pool are not exported any longer, and the pool defined again with the same id
    // gets new ones
    Y_UNIT_TEST(DeletedPoolCountersAreRemoved) {
        const auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        TStoragePoolStat stat(counters);
        const TString id = TStoragePoolStat::ConvertId({1, 2});

        stat.AddStoragePool(id, "pool", 100);
        UNIT_ASSERT(counters->FindSubgroup("storagePoolId", id));
        stat.DeleteStoragePool(id);
        UNIT_ASSERT(!counters->FindSubgroup("storagePoolId", id));

        stat.AddStoragePool(id, "pool", 0);
        const auto pool = counters->FindSubgroup("storagePoolId", id);
        UNIT_ASSERT(pool);
        const auto subgroup = pool->FindSubgroup("storagePool", "pool");
        UNIT_ASSERT(subgroup);
        UNIT_ASSERT_VALUES_EQUAL(subgroup->GetCounter("allocatedSize")->Val(), 0);
    }

}
