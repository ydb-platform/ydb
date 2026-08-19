#include <ydb/core/tx/conveyor_composite/usage/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>
#include <limits>

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
    const auto& pool = parsedConfig.GetWorkerPools()[1];
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
            UNIT_ASSERT_VALUES_EQUAL(config.GetWorkerPools()[1].GetName(), "WP::scan");
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
