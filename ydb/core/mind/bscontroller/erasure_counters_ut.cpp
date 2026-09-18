#include "erasure_counters.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBsController {
Y_UNIT_TEST_SUITE(StorageErasureCounters) {
    Y_UNIT_TEST(Block82MappingLifecycle) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        {
            TStorageErasureCounters counters(root);
            const auto mapping = root->FindSubgroup("subsystem", "erasureMapping");
            counters.SetGroup(0, "static", TErasureType::Erasure8Plus2Block);
            counters.SetGroup(123, "/Root/a:ssd", TErasureType::Erasure4Plus2Block);
            counters.SetPool("1:2", "/Root/empty:ssd", TErasureType::Erasure8Plus2Block);
            UNIT_ASSERT(mapping->FindSubgroup("group", "000000000")->FindSubgroup("storagePool", "static"));
            UNIT_ASSERT(!mapping->FindSubgroup("storagePoolId", "0:0"));
            counters.SetGroup(123, "/Root/b:ssd", TErasureType::Erasure8Plus2Block);
            counters.SetGroup(123, "/Root/b:ssd", TErasureType::Erasure8Plus2Block);
            auto group = mapping->FindSubgroup("group", "000000123");
            UNIT_ASSERT(!group->FindSubgroup("storagePool", "/Root/a:ssd"));
            const auto pool = group->FindSubgroup("storagePool", "/Root/b:ssd");
            UNIT_ASSERT(!pool->FindSubgroup("erasureSpecies", "block-4-2"));
            UNIT_ASSERT_VALUES_EQUAL(pool->FindSubgroup("erasureSpecies", "block-8-2")->FindCounter("GroupErasureInfo")->Val(), 1);
            counters.SetGroup(123, "/Root/b:ssd", TErasureType::ErasureSpeciesCount);
            UNIT_ASSERT(!mapping->FindSubgroup("group", "000000123"));
            counters.SetGroup(123, "", TErasureType::Erasure8Plus2Block);
            UNIT_ASSERT(!mapping->FindSubgroup("group", "000000123"));
            counters.EraseGroup(0);
            counters.ErasePool("1:2");
            UNIT_ASSERT(!mapping->FindSubgroup("group", "000000000"));
            UNIT_ASSERT(!mapping->FindSubgroup("storagePoolId", "1:2"));
        }
        UNIT_ASSERT(!root->FindSubgroup("subsystem", "erasureMapping"));
    }
}
} // namespace NKikimr::NBsController
