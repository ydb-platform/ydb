#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/request_priority.h>
#include <ydb/core/tx/general_cache/usage/config.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NOlap;

Y_UNIT_TEST_SUITE(TGeneralCacheRequestPriority) {
    Y_UNIT_TEST(ConsumerQuotasComeFromConfig) {
        using NBlobOperations::EConsumer;
        using NKikimr::NGeneralCache::NPublic::TConfig;
        const auto defaults = TConfig::BuildDefault();
        const auto atDefaults = [&](const EConsumer consumer) {
            return NBlobOperations::GetCacheRequestClass(consumer, defaults);
        };
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::SCAN).Weight, 0);
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::GENERAL_COMPACTION).Weight, 0);
        UNIT_ASSERT_VALUES_EQUAL(atDefaults(EConsumer::TTL).Weight, 0);

        NKikimrConfig::TGeneralCacheConfig proto;
        auto* scan = proto.AddConsumers();
        scan->SetName("SCAN");
        scan->SetWeight(4);
        auto* compaction = proto.AddConsumers();
        compaction->SetName("GENERAL_COMPACTION");
        compaction->SetWeight(1);
        auto* any = proto.AddConsumers();
        any->SetName("*");
        any->SetWeight(1);
        const auto configured = TConfig::BuildFromProto(proto);
        UNIT_ASSERT_C(configured.IsSuccess(), configured.GetErrorMessage());
        UNIT_ASSERT(NBlobOperations::ValidateCacheConsumerQuotas(configured.GetResult()).IsSuccess());
        const auto atConfigured = [&](const EConsumer consumer) {
            return NBlobOperations::GetCacheRequestClass(consumer, configured.GetResult());
        };
        UNIT_ASSERT_VALUES_EQUAL(atConfigured(EConsumer::SCAN).Weight, 4);
        UNIT_ASSERT_VALUES_EQUAL(atConfigured(EConsumer::GENERAL_COMPACTION).Weight, 1);
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
}
