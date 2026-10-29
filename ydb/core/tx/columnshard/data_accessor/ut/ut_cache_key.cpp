#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/request_priority.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/general_cache/usage/config.h>

#include <ydb/library/actors/core/actorid.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NOlap;

Y_UNIT_TEST_SUITE(TPortionMetadataCacheKey) {
    Y_UNIT_TEST(ConsumerQuotasComeFromConfig) {
        using NBlobOperations::EConsumer;
        using NKikimr::NGeneralCache::NPublic::TConfig;
        const auto defaults = TConfig::BuildDefault();
        const auto atDefaults = [&](const EConsumer consumer) {
            return NBlobOperations::GetCacheRequestClass(consumer, defaults);
        };
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::SCAN).Priority, 0);
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::SCAN).Weight, 0);
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::GENERAL_COMPACTION).Weight, 0);
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::FETCH_ON_LOAD).Priority, 0);
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::TTL).Weight, 0);

        NKikimrConfig::TGeneralCacheConfig proto;
        auto* scan = proto.AddConsumers();
        scan->SetName("SCAN");
        scan->SetPriority(2);
        scan->SetWeight(4);
        auto* compaction = proto.AddConsumers();
        compaction->SetName("GENERAL_COMPACTION");
        compaction->SetPriority(1);
        compaction->SetWeight(1);
        auto* any = proto.AddConsumers();
        any->SetName("*");
        any->SetPriority(0);
        any->SetWeight(1);
        const auto configured = TConfig::BuildFromProto(proto);
        UNIT_ASSERT_C(configured.IsSuccess(), configured.GetErrorMessage());
        UNIT_ASSERT(NBlobOperations::ValidateCacheConsumerQuotas(configured.GetResult()).IsSuccess());
        const auto atConfigured = [&](const EConsumer consumer) {
            return NBlobOperations::GetCacheRequestClass(consumer, configured.GetResult());
        };
        UNIT_ASSERT_VALUES_EQUAL(atConfigured(EConsumer::SCAN).Priority, 2);
        UNIT_ASSERT_VALUES_EQUAL(atConfigured(EConsumer::SCAN).Weight, 4);
        UNIT_ASSERT(atConfigured(EConsumer::SCAN).Priority > atConfigured(EConsumer::GENERAL_COMPACTION).Priority);
        UNIT_ASSERT_VALUES_EQUAL(atConfigured(EConsumer::SYS_VIEW_SCAN).Weight, atConfigured(EConsumer::TTL).Weight);
        UNIT_ASSERT_VALUES_EQUAL(atConfigured(EConsumer::SYS_VIEW_SCAN).QueueId, atConfigured(EConsumer::FETCH_ON_LOAD).QueueId);
        UNIT_ASSERT(atConfigured(EConsumer::SCAN).QueueId != atConfigured(EConsumer::FETCH_ON_LOAD).QueueId);

        NKikimrConfig::TGeneralCacheConfig duplicated;
        duplicated.AddConsumers()->SetName("SCAN");
        duplicated.AddConsumers()->SetName("SCAN");
        UNIT_ASSERT(TConfig::BuildFromProto(duplicated).IsFail());

        NKikimrConfig::TGeneralCacheConfig unknown;
        unknown.AddConsumers()->SetName("NOT_A_CONSUMER");
        const auto parsedUnknown = TConfig::BuildFromProto(unknown);
        UNIT_ASSERT(parsedUnknown.IsSuccess());
        UNIT_ASSERT(NBlobOperations::ValidateCacheConsumerQuotas(parsedUnknown.GetResult()).IsFail());
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
