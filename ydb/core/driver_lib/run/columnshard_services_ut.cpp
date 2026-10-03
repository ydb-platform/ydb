#include "kikimr_services_initializers.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/tx/conveyor_composite/common/config/config.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/priorities/usage/service.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/library/signals/owner.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKikimrServicesInitializers {
namespace {
NConveyorComposite::NConfig::TConfig Build(NKikimrConfig::TAppConfig& config) {
    TKikimrRunConfig run(config, 17);
    return TCompositeConveyorInitializer(run).BuildServiceConfig();
}

using ECategory = NConveyorComposite::ESpecialTaskCategory;

const NConveyorComposite::NConfig::TWorkersPool& Pool(
    const NConveyorComposite::NConfig::TConfig& config, ECategory category)
{
    // A category also uses the shared schedulable pool. Check the legacy
    // configuration on its named service pool, independent of vector order.
    const TString poolName = "WP::" + ::ToString(category);
    for (const auto& pool : config.GetWorkerPools()) {
        if (pool.GetName() != poolName) {
            continue;
        }
        UNIT_ASSERT_C(pool.GetSchedulingMode() ==
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool::NonSchedulable,
            pool.DebugString());
        for (const auto& link : pool.GetLinks()) {
            if (link.GetCategory() == category) {
                return pool;
            }
        }
    }
    UNIT_FAIL("Missing worker pool for " + ::ToString(category));
    Y_UNREACHABLE();
}

void AssertFractions(NKikimrConfig::TAppConfig& config, double comp, double insert, double scan) {
    const auto built = Build(config);
    const std::pair<ECategory, double> expected[] = {
        {ECategory::Compaction, comp}, {ECategory::Insert, insert}, {ECategory::Scan, scan}};
    for (const auto& [name, fraction] : expected) {
        const auto& workers = Pool(built, name).GetWorkersCountInfo();
        UNIT_ASSERT(!workers.GetCount());
        UNIT_ASSERT(workers.GetFraction());
        UNIT_ASSERT_DOUBLES_EQUAL(*workers.GetFraction(), fraction, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(Pool(built, name).GetWorkersCount(100), static_cast<ui64>(std::ceil(fraction * 100)));
    }
}
}

Y_UNIT_TEST_SUITE(ColumnShardServiceConfiguration) {

    Y_UNIT_TEST(PreserveManagedAndLegacyCategoryPools) {
        NKikimrConfig::TAppConfig config;
        const auto built = Build(config);
        const NConveyorComposite::NConfig::TWorkersPool* managed = nullptr;
        for (const auto& pool : built.GetWorkerPools()) {
            if (pool.GetName() == "WP::DEFAULT_SCHEDULABLE") {
                UNIT_ASSERT(!managed);
                managed = &pool;
            }
        }
        UNIT_ASSERT(managed);
        UNIT_ASSERT_C(managed->GetSchedulingMode() ==
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool::Schedulable,
            managed->DebugString());
        UNIT_ASSERT(!managed->GetWorkersCountInfo().GetCount());
        UNIT_ASSERT(managed->GetWorkersCountInfo().GetFraction());
        UNIT_ASSERT_DOUBLES_EQUAL(*managed->GetWorkersCountInfo().GetFraction(), 1.0, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(managed->GetWorkersCount(100), 100);
        for (const auto category : {ECategory::Compaction, ECategory::Insert,
                ECategory::Scan, ECategory::Deduplication, ECategory::Normalizer}) {
            const auto& legacy = Pool(built, category);
            const auto& categoryConfig = built.GetCategoryConfig(category);
            UNIT_ASSERT_VALUES_EQUAL(categoryConfig.GetWorkerPools().size(), 2);
            bool usesManaged = false;
            bool usesLegacy = false;
            for (const auto poolId : categoryConfig.GetWorkerPools()) {
                usesManaged |= poolId == managed->GetWorkersPoolId();
                usesLegacy |= poolId == legacy.GetWorkersPoolId();
            }
            UNIT_ASSERT(usesManaged);
            UNIT_ASSERT(usesLegacy);
        }
    }

    Y_UNIT_TEST(ApplyConfiguredCacheLimitsToRunningServices) {
        for (ui64 limit : {0ull, 1048576ull}) {
            NActors::TTestBasicRuntime runtime;
            TAppPrepare app;
            app.InitIcb(1);
            runtime.Initialize(app.Unwrap());
            NKikimrConfig::TAppConfig config;
            config.MutablePortionsMetadataCache()->SetMemoryLimit(limit);
            config.MutableColumnDataCache()->SetMemoryLimit(limit + 1024);
            TKikimrRunConfig run(config, runtime.GetNodeId());
            TActorSystemSetup setup;
            TGeneralCachePortionsMetadataInitializer(run).InitializeServices(&setup, &runtime.GetAppData());
            TGeneralCacheColumnDataInitializer(run).InitializeServices(&setup, &runtime.GetAppData());
            UNIT_ASSERT_VALUES_EQUAL(setup.LocalServices.size(), 2);
            for (auto& service : setup.LocalServices) {
                runtime.RegisterService(service.first, runtime.Register(service.second.Actor.release()));
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
            auto tablets = GetServiceCounters(runtime.GetAppData().Counters, "tablets");
            const auto observedLimit = [&](const TString& type, const TString& cacheName) {
                NColumnShard::TCommonCountersOwner owner("general_cache", tablets->GetSubgroup("type", type));
                owner.DeepSubGroup("cache_name", cacheName);
                owner.DeepSubGroup("signals_owner", "manager");
                return owner.GetValue("Cache/ConfigSizeLimit/Bytes")->Val();
            };
            UNIT_ASSERT_VALUES_EQUAL(observedLimit("TX_GENERAL_CACHE_PORTIONS_METADATA",
                NOlap::NGeneralCache::TPortionsMetadataCachePolicy::GetCacheName()), limit);
            UNIT_ASSERT_VALUES_EQUAL(observedLimit("TX_GENERAL_CACHE_COLUMN_DATA",
                NOlap::NGeneralCache::TColumnDataCachePolicy::GetCacheName()), limit + 1024);
        }
    }

    Y_UNIT_TEST(PreserveLegacyDefaultsAndWorkerCountPriority) {
        NKikimrConfig::TAppConfig config;
        AssertFractions(config, 0.33, 0.2, 0.4);
        auto* comp = config.MutableCompConveyorConfig();
        auto* insert = config.MutableInsertConveyorConfig();
        auto* scan = config.MutableScanConveyorConfig();
        AssertFractions(config, 0.33, 0.2, 0.4);
        for (auto* legacy : {comp, insert, scan}) {
            legacy->SetWorkersCount(11);
            legacy->SetDefaultFractionOfThreadsCount(0.9);
        }
        for (const ECategory name : {ECategory::Compaction, ECategory::Insert, ECategory::Scan}) {
            const auto built = Build(config);
            UNIT_ASSERT_VALUES_EQUAL(Pool(built, name).GetWorkersCount(100), 11);
        }
        for (auto* legacy : {comp, insert, scan}) {
            legacy->SetWorkersCountDouble(2.5);
        }
        const auto bothCounts = Build(config);
        for (const ECategory category : {ECategory::Compaction, ECategory::Insert, ECategory::Scan}) {
            UNIT_ASSERT_VALUES_EQUAL(Pool(bothCounts, category).GetWorkersCount(100), 3);
        }
        for (auto* legacy : {comp, insert, scan}) {
            legacy->ClearWorkersCount();
        }
        const auto built = Build(config);
        for (const ECategory name : {ECategory::Compaction, ECategory::Insert, ECategory::Scan}) {
            UNIT_ASSERT(Pool(built, name).GetWorkersCountInfo().GetCount());
            UNIT_ASSERT_DOUBLES_EQUAL(*Pool(built, name).GetWorkersCountInfo().GetCount(), 2.5, 1e-9);
            UNIT_ASSERT_VALUES_EQUAL(Pool(built, name).GetWorkersCount(100), 3);
        }
    }

    Y_UNIT_TEST(PreserveRepositorySpecificFractionSemantics) {
        NKikimrConfig::TAppConfig config;
        config.MutableCompConveyorConfig()->SetDefaultFractionOfThreadsCount(0.1);
        config.MutableInsertConveyorConfig()->SetDefaultFractionOfThreadsCount(0.6);
        config.MutableScanConveyorConfig()->SetDefaultFractionOfThreadsCount(0.7);
        AssertFractions(config, 0.1, 0.6, 0.7);
        config.ClearCompConveyorConfig();
        AssertFractions(config, 0.33, 0.6, 0.7);
    }

    Y_UNIT_TEST(RespectExplicitDisableAndRegistrationPools) {
        NKikimrConfig::TAppConfig config;
        TAppData appData(1, 2, 3, 4, {}, nullptr, nullptr, nullptr, nullptr);
        appData.Counters = new NMonitoring::TDynamicCounters;
        TKikimrRunConfig run(config, 17);
        TActorSystemSetup enabled;
        TCompositeConveyorInitializer(run).InitializeServices(&enabled, &appData);
        UNIT_ASSERT_VALUES_EQUAL(enabled.LocalServices.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(enabled.LocalServices.front().first, NConveyorComposite::TServiceOperator::MakeServiceId(17));
        UNIT_ASSERT_VALUES_EQUAL(enabled.LocalServices.front().second.PoolId, 2);
        UNIT_ASSERT(enabled.LocalServices.front().second.Actor);
        config.MutableCompositeConveyorConfig()->SetEnabled(false);
        UNIT_ASSERT(!Build(config).IsEnabled());
        TActorSystemSetup disabled;
        TCompositeConveyorInitializer(run).InitializeServices(&disabled, &appData);
        UNIT_ASSERT(disabled.LocalServices.empty());

        config.MutableCompPrioritiesConfig()->SetEnabled(false);
        TActorSystemSetup prioritiesOff;
        TCompPrioritiesInitializer(run).InitializeServices(&prioritiesOff, &appData);
        UNIT_ASSERT(prioritiesOff.LocalServices.empty());
        config.MutableCompPrioritiesConfig()->SetEnabled(true);
        TActorSystemSetup prioritiesOn;
        TCompPrioritiesInitializer(run).InitializeServices(&prioritiesOn, &appData);
        UNIT_ASSERT_VALUES_EQUAL(prioritiesOn.LocalServices.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(prioritiesOn.LocalServices.front().first, NPrioritiesQueue::TCompServiceOperator::MakeServiceId(17));
        UNIT_ASSERT_VALUES_EQUAL(prioritiesOn.LocalServices.front().second.PoolId, 2);
        UNIT_ASSERT(prioritiesOn.LocalServices.front().second.Actor);
    }

    Y_UNIT_TEST(OverlaySuccessFailureAndParseFallback) {
        NKikimrConfig::TAppConfig config;
        auto* pool = config.MutableCompositeConveyorConfig()->AddWorkerPools();
        pool->SetName("WP::" + ::ToString(ECategory::Scan));
        pool->SetWorkersCount(5);
        auto overlay = Build(config);
        UNIT_ASSERT_VALUES_EQUAL(Pool(overlay, ECategory::Scan).GetWorkersCount(100), 5);
        UNIT_ASSERT_VALUES_EQUAL(Pool(overlay, ECategory::Insert).GetWorkersCount(100), 20);

        // A nameless overlay cannot be applied: keep the synthesized defaults.
        pool->ClearName();
        AssertFractions(config, 0.33, 0.2, 0.4);
        // An applied but invalid worker count falls back to the default configuration.
        pool->SetName("WP::" + ::ToString(ECategory::Scan));
        pool->SetWorkersCount(-1);
        UNIT_ASSERT_VALUES_EQUAL(Build(config).DebugString(),
            NConveyorComposite::NConfig::TConfig::BuildDefault().DebugString());
    }

}
}
