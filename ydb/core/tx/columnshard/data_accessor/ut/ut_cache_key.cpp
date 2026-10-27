#include <ydb/core/tx/columnshard/blobs_action/counters/request_priority.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>

#include <ydb/library/actors/core/actorid.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NOlap;

Y_UNIT_TEST_SUITE(TPortionMetadataCacheKey) {
    Y_UNIT_TEST(PriorityPutsScansAheadOfCompactionAndPrefetch) {
        using NBlobOperations::EConsumer;
        const ui32 scan = NBlobOperations::GetCacheRequestPriority(EConsumer::SCAN);
        UNIT_ASSERT_VALUES_EQUAL(scan, NBlobOperations::GetCacheRequestPriority(EConsumer::SYS_VIEW_SCAN));
        UNIT_ASSERT(scan > NBlobOperations::GetCacheRequestPriority(EConsumer::GENERAL_COMPACTION));
        UNIT_ASSERT_VALUES_EQUAL(
            NBlobOperations::GetCacheRequestPriority(EConsumer::GENERAL_COMPACTION), NBlobOperations::GetCacheRequestPriority(EConsumer::TTL));
        UNIT_ASSERT(NBlobOperations::GetCacheRequestPriority(EConsumer::GENERAL_COMPACTION) >
                    NBlobOperations::GetCacheRequestPriority(EConsumer::FETCH_ON_LOAD));
    }

    Y_UNIT_TEST(PortionAddressIgnoresActorId) {
        const auto pathId = NKikimr::NColumnShard::TInternalPathId::FromRawValue(7);
        const TPortionAddress portion(pathId, 9);
        const NActors::TActorId first(1, 1);
        const NActors::TActorId second(2, 2);
        const NGeneralCache::TGlobalPortionAddress sameTablet(first, 100, portion);
        const NGeneralCache::TGlobalPortionAddress restarted(second, 100, portion);
        const NGeneralCache::TGlobalPortionAddress otherTablet(first, 101, portion);
        const NGeneralCache::TGlobalPortionAddress otherPortion(first, 100, TPortionAddress(pathId, 10));

        UNIT_ASSERT(sameTablet == restarted);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(sameTablet), static_cast<size_t>(restarted));
        const NGeneralCache::TGlobalPortionAddress changed(first, 100, portion, 5);
        UNIT_ASSERT(!(sameTablet == otherTablet));
        UNIT_ASSERT(!(sameTablet == otherPortion));
        UNIT_ASSERT(!(sameTablet == changed));
    }

    Y_UNIT_TEST(ColumnAddressKeepsActorId) {
        const auto pathId = NKikimr::NColumnShard::TInternalPathId::FromRawValue(7);
        const TPortionAddress portion(pathId, 9);
        const NActors::TActorId first(1, 1);
        const NActors::TActorId second(2, 2);
        const NGeneralCache::TGlobalColumnAddress left(first, portion, 3);
        const NGeneralCache::TGlobalColumnAddress restarted(second, portion, 3);
        const NGeneralCache::TGlobalColumnAddress otherColumn(first, portion, 4);

        UNIT_ASSERT(!(left == restarted));
        UNIT_ASSERT(!(left == otherColumn));
        UNIT_ASSERT(left == NGeneralCache::TGlobalColumnAddress(first, portion, 3));
    }
}
