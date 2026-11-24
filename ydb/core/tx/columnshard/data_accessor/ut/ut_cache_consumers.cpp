#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/consumers.h>
#include <ydb/core/tx/general_cache/usage/config.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NOlap;

namespace {

using NBlobOperations::EConsumer;
using NKikimr::NGeneralCache::NPublic::TConfig;

ui32 WeightOf(const TConfig& config, const EConsumer consumer) {
    return config.Classify(::ToString(consumer)).Weight;
}

NKikimrConfig::TGeneralCacheConfig WithConsumer(const TString& name, const ui32 weight) {
    NKikimrConfig::TGeneralCacheConfig proto;
    proto.SetMemoryLimit(123);
    proto.SetDirectInflightSourceLimit(7);
    auto& item = *proto.AddConsumers();
    item.SetName(name);
    item.SetWeight(weight);
    return proto;
}

}   // namespace

Y_UNIT_TEST_SUITE(TGeneralCacheConsumers) {
    Y_UNIT_TEST(ConsumerQuotasComeFromConfig) {
        const auto defaults = TConfig::BuildDefault();
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(defaults, EConsumer::SCAN), 0);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(defaults, EConsumer::GENERAL_COMPACTION), 0);
        UNIT_ASSERT(!defaults.GetHasWeights());

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
        const auto configured = TConfig::BuildFromProto(proto, NGeneralCache::GetAllowedCacheConsumers());
        UNIT_ASSERT_C(configured.IsSuccess(), configured.GetErrorMessage());
        UNIT_ASSERT(configured.GetResult().GetHasWeights());
        const auto classOf = [&](const EConsumer consumer) {
            return configured.GetResult().Classify(::ToString(consumer));
        };
        UNIT_ASSERT_VALUES_EQUAL(classOf(EConsumer::SCAN).Weight, 4);
        UNIT_ASSERT_VALUES_EQUAL(classOf(EConsumer::GENERAL_COMPACTION).Weight, 1);
        UNIT_ASSERT_VALUES_EQUAL(classOf(EConsumer::SYS_VIEW_SCAN).Weight, classOf(EConsumer::TTL).Weight);
        UNIT_ASSERT_VALUES_EQUAL(classOf(EConsumer::SYS_VIEW_SCAN).QueueId, classOf(EConsumer::FETCH_ON_LOAD).QueueId);
        UNIT_ASSERT(classOf(EConsumer::SCAN).QueueId != classOf(EConsumer::FETCH_ON_LOAD).QueueId);

        NKikimrConfig::TGeneralCacheConfig duplicated;
        duplicated.AddConsumers()->SetName("SCAN");
        duplicated.AddConsumers()->SetName("SCAN");
        UNIT_ASSERT(TConfig::BuildFromProto(duplicated).IsFail());

        const auto allowed = NGeneralCache::GetAllowedCacheConsumers();
        UNIT_ASSERT(TConfig::BuildFromProto(WithConsumer("NOT_A_CONSUMER", 1), allowed).IsFail());
        UNIT_ASSERT(TConfig::BuildFromProto(WithConsumer("UNDEFINED", 1), allowed).IsFail());
        UNIT_ASSERT(TConfig::BuildFromProto(WithConsumer("NOT_A_CONSUMER", 1)).IsSuccess());
    }

    Y_UNIT_TEST(ConsumersAreClearedWhenWeightsAreOff) {
        const auto config = NGeneralCache::BuildCacheConfig("ut", WithConsumer("SCAN", 4), false, true);
        UNIT_ASSERT(!config.GetHasWeights());
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::SCAN), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMemoryLimit().value_or(0), 123);
    }

    Y_UNIT_TEST(DefaultsArePassedByValidation) {
        const auto config = NGeneralCache::BuildCacheConfig("ut", NKikimrConfig::TGeneralCacheConfig(), true, true);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::SCAN), 8);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::SYS_VIEW_SCAN), 8);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::DUPLICATE_FILTERING), 8);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::GENERAL_COMPACTION), 2);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::FETCH_ON_LOAD), 1);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::TTL), 1);
    }

    Y_UNIT_TEST(NoDefaultsForColumnData) {
        const auto config = NGeneralCache::BuildCacheConfig("ut", NKikimrConfig::TGeneralCacheConfig(), true, false);
        UNIT_ASSERT(!config.GetHasWeights());
    }

    Y_UNIT_TEST(PartialConfigKeepsMissingDefaults) {
        auto proto = WithConsumer("SCAN", 4);
        auto& zero = *proto.AddConsumers();
        zero.SetName("FETCH_ON_LOAD");
        zero.SetWeight(0);
        const auto config = NGeneralCache::BuildCacheConfig("ut", proto, true, true);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::SCAN), 4);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::GENERAL_COMPACTION), 2);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::DUPLICATE_FILTERING), 8);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::FETCH_ON_LOAD), 0);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::TTL), 1);
    }

    Y_UNIT_TEST(PartialConfigWithoutDefaultsKeepsOthersWeighted) {
        const auto config = NGeneralCache::BuildCacheConfig("ut", WithConsumer("SCAN", 4), true, false);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::SCAN), 4);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::DUPLICATE_FILTERING), 1);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::GENERAL_COMPACTION), 1);

        auto proto = WithConsumer("SCAN", 4);
        auto& any = *proto.AddConsumers();
        any.SetName("*");
        any.SetWeight(0);
        const auto explicitZero = NGeneralCache::BuildCacheConfig("ut", proto, true, false);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(explicitZero, EConsumer::SCAN), 4);
        UNIT_ASSERT_VALUES_EQUAL(WeightOf(explicitZero, EConsumer::DUPLICATE_FILTERING), 0);
    }

    Y_UNIT_TEST(ListedConsumerWithoutWeightGetsOne) {
        NKikimrConfig::TGeneralCacheConfig proto;
        proto.AddConsumers()->SetName("GENERAL_COMPACTION");
        for (const bool useDefaultConsumers : { true, false }) {
            const auto config = NGeneralCache::BuildCacheConfig("ut", proto, true, useDefaultConsumers);
            UNIT_ASSERT_VALUES_EQUAL(WeightOf(config, EConsumer::GENERAL_COMPACTION), 1);
        }
    }

    Y_UNIT_TEST(InvalidConsumersKeepTheLimits) {
        const auto config = NGeneralCache::BuildCacheConfig("ut", WithConsumer("NOT_A_CONSUMER", 4), true, false);
        UNIT_ASSERT(!config.GetHasWeights());
        UNIT_ASSERT_VALUES_EQUAL(config.GetMemoryLimit().value_or(0), 123);
        UNIT_ASSERT_VALUES_EQUAL(config.GetDirectInflightSourceLimit(), 7);
    }
}
