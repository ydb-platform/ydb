#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>
#include <limits>
#include <algorithm>

namespace NKikimr::NConveyorComposite {

namespace {

using TLinkConfig = std::pair<ESpecialTaskCategory, double>;

void AddPool(NKikimrConfig::TCompositeConveyorConfig& config, const std::optional<TString>& name,
    const std::vector<TLinkConfig>& links, const std::optional<double> workersCount = 1,
    const std::optional<double> fraction = std::nullopt) {
    auto* pool = config.AddWorkerPools();
    if (name) {
        pool->SetName(*name);
    }
    if (workersCount) {
        pool->SetWorkersCount(*workersCount);
    }
    if (fraction) {
        pool->SetDefaultFractionOfThreadsCount(*fraction);
    }
    for (const auto& [category, weight] : links) {
        auto* link = pool->AddLinks();
        link->SetCategory(::ToString(category));
        link->SetWeight(weight);
    }
}

NKikimrConfig::TCompositeConveyorConfig BuildPoolWithCPU(
    const std::optional<double> workersCount, const std::optional<double> fraction) {
    NKikimrConfig::TCompositeConveyorConfig result;
    result.SetEnabled(true);
    AddPool(result, "pool", {{ESpecialTaskCategory::Scan, 1}}, workersCount, fraction);
    return result;
}

void AssertCPUConfig(const NKikimrConfig::TCompositeConveyorConfig& proto, const ui64 totalThreadsCount,
    const std::vector<double>& expectedLimits) {
    auto config = NConfig::TConfig::BuildFromProto(proto);
    UNIT_ASSERT_C(!config.IsFail(), config.GetErrorMessage());
    const auto parsedConfig = config.DetachResult();
    const auto& pool = parsedConfig.GetWorkerPools()[2];
    UNIT_ASSERT_VALUES_EQUAL(pool.GetWorkersCount(totalThreadsCount), expectedLimits.size());
    for (ui64 workerIdx = 0; workerIdx < expectedLimits.size(); ++workerIdx) {
        UNIT_ASSERT_C(std::abs(pool.GetWorkerCPUUsage(workerIdx, totalThreadsCount) - expectedLimits[workerIdx]) < 1e-9,
            "unexpected CPU limit for worker " << workerIdx);
    }
}

void AssertInvalid(const NKikimrConfig::TCompositeConveyorConfig& proto) {
    UNIT_ASSERT(NConfig::TConfig::BuildFromProto(proto).IsFail());
}

Y_UNIT_TEST_SUITE(TCompositeConveyorConfig) {
    /* Scenario:
        Specialized and All pools cover each identity type independently.
        Both fallback pools always exist, including when every category is covered.
     */
    Y_UNIT_TEST(SchedulingModeFallbackMatrix) {
        using TPool = NKikimrConfig::TCompositeConveyorConfig::TWorkersPool;
        const std::vector<std::vector<TPool::ESchedulingMode>> cases{
            {}, {TPool::NonSchedulable}, {TPool::Schedulable}, {TPool::All}, {TPool::NonSchedulable, TPool::Schedulable}};
        for (const auto& modes : cases) {
            for (const bool allCategories : {false, true}) {
                NKikimrConfig::TCompositeConveyorConfig proto;
                bool serviceCovered = false;
                bool managedCovered = false;
                for (const auto mode : modes) {
                    auto* pool = proto.AddWorkerPools();
                    pool->SetSchedulingMode(mode);
                    pool->SetWorkersCount(1);
                    for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
                        if (allCategories || category == ESpecialTaskCategory::Scan) {
                            pool->AddLinks()->SetCategory(::ToString(category));
                        }
                    }
                    serviceCovered |= mode != TPool::Schedulable;
                    managedCovered |= mode != TPool::NonSchedulable;
                }
                const auto config = NConfig::TConfig::BuildFromProto(proto).DetachResult();
                const auto& pools = config.GetWorkerPools();
                UNIT_ASSERT_VALUES_EQUAL(pools.size(), modes.size() + 2);
                UNIT_ASSERT_VALUES_EQUAL(pools[0].GetName(), "WP::DEFAULT");
                UNIT_ASSERT_VALUES_EQUAL(pools[1].GetName(), "WP::DEFAULT_SCHEDULABLE");
                UNIT_ASSERT(pools[0].GetSchedulingMode() == NConfig::ESchedulingMode::NonSchedulable);
                UNIT_ASSERT(pools[1].GetSchedulingMode() == NConfig::ESchedulingMode::Schedulable);
                UNIT_ASSERT_VALUES_EQUAL(*pools[1].GetWorkersCountInfo().GetFraction(), 1);
                UNIT_ASSERT(pools[0].GetHeavyLimits() == pools[1].GetHeavyLimits());
                UNIT_ASSERT_VALUES_EQUAL(pools[1].GetMaxBatchSize(),
                    pools[0].GetMaxBatchSize() * std::max<size_t>(1, pools[1].GetLinks().size()));
                for (const auto category : GetEnumAllValues<ESpecialTaskCategory>()) {
                    for (ui64 id : {0, 1}) {
                        const bool covered = (allCategories || category == ESpecialTaskCategory::Scan)
                            && (id ? managedCovered : serviceCovered);
                        const auto& links = pools[id].GetLinks();
                        UNIT_ASSERT_VALUES_EQUAL(std::ranges::count_if(links, [&](const auto& link) {
                            return link.GetCategory() == category && link.GetWeight() == 1;
                        }), !covered);
                        const auto& reverseLinks = config.GetCategoryConfig(category).GetWorkerPools();
                        UNIT_ASSERT_VALUES_EQUAL(std::ranges::count(reverseLinks, id), !covered);
                    }
                }
            }
        }
    }

    /* Scenario:
        Generated names include the mode; explicit names are stable and fallback names are reserved.
        Missing mode is NonSchedulable, including in a full replacement snapshot.
     */
    Y_UNIT_TEST(SchedulingModeNamesAndDefaults) {
        using TPool = NKikimrConfig::TCompositeConveyorConfig::TWorkersPool;
        auto proto = BuildPoolWithCPU(1, std::nullopt);
        auto config = NConfig::TConfig::BuildFromProto(proto).DetachResult();
        UNIT_ASSERT(config.GetWorkerPools()[2].GetSchedulingMode() == NConfig::ESchedulingMode::NonSchedulable);
        for (const auto mode : {TPool::NonSchedulable, TPool::Schedulable, TPool::All}) {
            proto.MutableWorkerPools(0)->SetSchedulingMode(mode);
            config = NConfig::TConfig::BuildFromProto(proto).DetachResult();
            UNIT_ASSERT_VALUES_EQUAL(config.GetWorkerPools()[2].GetName(), "pool");
            UNIT_ASSERT(config.GetWorkerPools()[2].DebugString().Contains(TPool::ESchedulingMode_Name(mode)));
        }
        proto.MutableWorkerPools(0)->SetName("WP::DEFAULT_SCHEDULABLE");
        AssertInvalid(proto);
        proto.MutableWorkerPools(0)->ClearName();
        *proto.AddWorkerPools() = proto.GetWorkerPools(0);
        AssertInvalid(proto);
        proto.MutableWorkerPools(1)->SetSchedulingMode(TPool::Schedulable);
        config = NConfig::TConfig::BuildFromProto(proto).DetachResult();
        UNIT_ASSERT_VALUES_EQUAL(config.GetWorkerPools()[2].GetName(), "WP::scan-All");
        UNIT_ASSERT_VALUES_EQUAL(config.GetWorkerPools()[3].GetName(), "WP::scan-Schedulable");
    }

    /* Scenario:
        Both partial overlay branches preserve an absent mode and replace an explicit zero value.
        A full list is a replacement, not a patch.
     */
    Y_UNIT_TEST(SchedulingModeOverlayPresence) {
        using TPool = NKikimrConfig::TCompositeConveyorConfig::TWorkersPool;
        auto defaults = BuildPoolWithCPU(1, std::nullopt);
        defaults.MutableWorkerPools(0)->SetSchedulingMode(TPool::All);
        AddPool(defaults, "other", {{ESpecialTaskCategory::Insert, 1}});
        for (const bool withLinks : {false, true}) {
            for (const auto mode : {std::optional<TPool::ESchedulingMode>{}, {TPool::NonSchedulable}, {TPool::Schedulable}}) {
                NKikimrConfig::TCompositeConveyorConfig yaml;
                auto* pool = yaml.AddWorkerPools();
                pool->SetName("pool");
                if (mode) {
                    pool->SetSchedulingMode(*mode);
                }
                if (withLinks) {
                    pool->AddLinks()->SetCategory("scan");
                    yaml.AddWorkerPools()->SetName("other"); // Keep the partial-overlay path.
                }
                auto merged = NConfig::TConfig::OverlayYamlOnDefaults(defaults, yaml).DetachResult();
                UNIT_ASSERT(merged.GetWorkerPools(0).GetSchedulingMode() == mode.value_or(TPool::All));
            }
        }
        auto snapshot = defaults;
        snapshot.MutableWorkerPools(0)->ClearSchedulingMode();
        auto replaced = NConfig::TConfig::OverlayYamlOnDefaults(defaults, snapshot).DetachResult();
        UNIT_ASSERT(replaced.GetWorkerPools(0).GetSchedulingMode() == TPool::NonSchedulable);
        UNIT_ASSERT(NConfig::TConfig::BuildFromProto(replaced).DetachResult().GetWorkerPools()[2].GetSchedulingMode()
            == NConfig::ESchedulingMode::NonSchedulable);
    }

    Y_UNIT_TEST(NormalizationMatrix) {
        // WorkersCount wins over a simultaneously specified fraction.
        AssertCPUConfig(BuildPoolWithCPU(2.5, 0.1), 10, {1, 1, 0.5});

        // absent fields use 0.33, while capacity below one remains fractional.
        AssertCPUConfig(BuildPoolWithCPU(std::nullopt, std::nullopt), 10, {1, 1, 1, 0.3});
        AssertCPUConfig(BuildPoolWithCPU(0.2, std::nullopt), 10, {0.2});

        // ceil changes only above the integer boundary.
        AssertCPUConfig(BuildPoolWithCPU(1.999, std::nullopt), 10, {1, 0.999});
        AssertCPUConfig(BuildPoolWithCPU(2, std::nullopt), 10, {1, 1});
        AssertCPUConfig(BuildPoolWithCPU(2.001, std::nullopt), 10, {1, 1, 0.001});

        // a fraction crossing an integer boundary changes the number of workers.
        AssertCPUConfig(BuildPoolWithCPU(std::nullopt, 0.19), 10, {1, 0.9});
        AssertCPUConfig(BuildPoolWithCPU(std::nullopt, 0.21), 10, {1, 1, 0.1});

        // the two protobuf representations normalize to the same limits.
        AssertCPUConfig(BuildPoolWithCPU(2.5, std::nullopt), 10, {1, 1, 0.5});
        AssertCPUConfig(BuildPoolWithCPU(std::nullopt, 0.25), 10, {1, 1, 0.5});

        // empty and reserved names are replaced with the category-derived name.
        for (const TString& name : {TString(), TString("WP::DEFAULT")}) {
            NKikimrConfig::TCompositeConveyorConfig proto;
            AddPool(proto, name, {{ESpecialTaskCategory::Scan, 1}});
            auto config = NConfig::TConfig::BuildFromProto(proto).DetachResult();
            UNIT_ASSERT_VALUES_EQUAL(config.GetWorkerPools()[2].GetName(), "WP::scan-NonSchedulable");
        }
    }

    Y_UNIT_TEST(ValidationMatrix) {
        for (const double count : {0.0, -1.0}) {
            AssertInvalid(BuildPoolWithCPU(count, std::nullopt));
        }
        for (const double fraction : {0.0, -0.1, 1.1}) {
            AssertInvalid(BuildPoolWithCPU(std::nullopt, fraction));
        }

        // unknown top-level category.
        {
            auto proto = BuildPoolWithCPU(1, std::nullopt);
            proto.AddCategories()->SetName("UNKNOWN");
            AssertInvalid(proto);
        }

        // unknown link category.
        {
            NKikimrConfig::TCompositeConveyorConfig proto;
            auto* pool = proto.AddWorkerPools();
            pool->SetWorkersCount(1);
            pool->AddLinks()->SetCategory("UNKNOWN");
            AssertInvalid(proto);
        }

        // duplicate top-level category.
        {
            auto proto = BuildPoolWithCPU(1, std::nullopt);
            proto.AddCategories()->SetName(::ToString(ESpecialTaskCategory::Scan));
            proto.AddCategories()->SetName(::ToString(ESpecialTaskCategory::Scan));
            AssertInvalid(proto);
        }

        // duplicate link in one pool.
        {
            NKikimrConfig::TCompositeConveyorConfig proto;
            AddPool(proto, "pool", {{ESpecialTaskCategory::Scan, 1}, {ESpecialTaskCategory::Scan, 2}});
            AssertInvalid(proto);
        }

        // an explicit pool must not be empty.
        {
            NKikimrConfig::TCompositeConveyorConfig proto;
            AddPool(proto, "pool", {});
            AssertInvalid(proto);
        }

        // effective pool names must be unique.
        {
            NKikimrConfig::TCompositeConveyorConfig proto;
            AddPool(proto, "pool", {{ESpecialTaskCategory::Scan, 1}});
            AddPool(proto, "pool", {{ESpecialTaskCategory::Insert, 1}});
            AssertInvalid(proto);
        }

        // weights must be positive and finite.
        for (const double weight : {0.0, -1.0}) {
            NKikimrConfig::TCompositeConveyorConfig proto;
            AddPool(proto, "pool", {{ESpecialTaskCategory::Scan, weight}});
            AssertInvalid(proto);
        }

    }
}

}   // namespace

}   // namespace NKikimr::NConveyorComposite
