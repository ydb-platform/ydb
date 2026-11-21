#include "impl.h" // includes storage_pool_stat.h along with the definitions it needs

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NBsController;
using NKikimr::TErasureType;

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

    Y_UNIT_TEST(GroupErasureLifecycle) {
        const auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        const TString id = TStoragePoolStat::ConvertId({1, 2});
        const TString otherId = TStoragePoolStat::ConvertId({1, 3});
        const ui32 group = 0x80000001;
        auto findPool = [&](const TString& poolId, const TString& name) {
            return counters->FindSubgroup("storagePoolId", poolId)->FindSubgroup("storagePool", name);
        };
        auto check = [&](const TString& poolId, const TString& name, const TString& erasure) {
            const auto mapping = findPool(poolId, name)->FindSubgroup("group", ToString(group));
            UNIT_ASSERT(mapping);
            UNIT_ASSERT_VALUES_EQUAL(mapping->ReadSnapshot().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(mapping->FindSubgroup("erasureSpecies", erasure)
                ->GetCounter("GroupErasureSpecies")->Val(), 1);
        };
        {
            TStoragePoolStat stat(counters);
            stat.AddStoragePool(id, "pool", 0);
            stat.AddStoragePool(otherId, "other", 0);
            UNIT_ASSERT(!findPool(id, "pool")->FindSubgroup("group", "0"));
            UNIT_ASSERT(!findPool(id, "pool")->FindSubgroup("group", ToString(group)));
            stat.SetGroupErasure(id, group, TErasureType::Erasure4Plus2Block);
            check(id, "pool", "block-4-2");
            stat.SetGroupErasure(id, group, TErasureType::Erasure8Plus2Block);
            check(id, "pool", "block-8-2");
            stat.RenameStoragePool(id, "renamed");
            UNIT_ASSERT(!counters->FindSubgroup("storagePoolId", id)->FindSubgroup("storagePool", "pool"));
            check(id, "renamed", "block-8-2");
            stat.SetGroupErasure(id, group, std::nullopt);
            stat.SetGroupErasure(otherId, group, TErasureType::Erasure8Plus2Block);
            UNIT_ASSERT(!findPool(id, "renamed")->FindSubgroup("group", ToString(group)));
            check(otherId, "other", "block-8-2");
            stat.DeleteStoragePool(otherId);
            UNIT_ASSERT(!counters->FindSubgroup("storagePoolId", otherId));
        }
        UNIT_ASSERT(!counters->FindSubgroup("storagePoolId", id));
        TStoragePoolStat reloaded(counters);
        reloaded.AddStoragePool(id, "renamed", 0);
        reloaded.SetGroupErasure(id, group, TErasureType::Erasure8Plus2Block);
        check(id, "renamed", "block-8-2");
    }

}
