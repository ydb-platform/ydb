#include <ydb/core/config/validation/validators.h>
#include <ydb/core/tx/conveyor_composite/common/config/config.h>

#include <ydb/library/actors/core/defs.h>

#include <library/cpp/testing/unittest/registar.h>

#include <cmath>
#include <limits>

namespace NKikimr::NConfig {

    namespace {

        NKikimrConfig::TAppConfig BuildValidConfig() {
            NKikimrConfig::TAppConfig result;
            auto* pool = result.MutableCompositeConveyorConfig()->AddWorkerPools();
            pool->SetWorkersCount(1);
            auto* link = pool->AddLinks();
            link->SetCategory("scan");
            link->SetWeight(1);
            return result;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TCompositeConveyorConfigValidationTest) {
        /* Scenario:
            Generated pool names include sorted categories and non-default scheduling modes.
            Same-mode duplicates and both reserved fallback names are rejected.
         */
        Y_UNIT_TEST(SchedulingModeNames) {
            auto config = BuildValidConfig();
            auto* conveyor = config.MutableCompositeConveyorConfig();
            conveyor->MutableWorkerPools(0)->AddLinks()->SetCategory("insert");
            *conveyor->AddWorkerPools() = conveyor->GetWorkerPools(0);
            conveyor->MutableWorkerPools(1)->MutableLinks()->SwapElements(0, 1);
            conveyor->MutableWorkerPools(1)->SetSchedulingMode(NConveyorComposite::NConfig::TProtoWorkerPool::Schedulable);
            std::vector<TString> errors;
            UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Ok);
            conveyor->MutableWorkerPools(1)->SetSchedulingMode(NConveyorComposite::NConfig::TProtoWorkerPool::NonSchedulable);
            UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Error);
            UNIT_ASSERT_VALUES_EQUAL(errors.back(), "duplicate composite conveyor worker pool name: WP::insert-scan");
            errors.clear();
            for (const auto mode : {NConveyorComposite::NConfig::TProtoWorkerPool::NonSchedulable, NConveyorComposite::NConfig::TProtoWorkerPool::Schedulable, NConveyorComposite::NConfig::TProtoWorkerPool::All}) {
                conveyor->MutableWorkerPools(1)->SetSchedulingMode(mode);
                for (const auto& pool : NConveyorComposite::NConfig::GetDefaultWorkersPoolTemplates()) {
                    conveyor->MutableWorkerPools(1)->SetName(pool.GetName());
                    UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Error);
                    errors.clear();
                }
            }
            conveyor->MutableWorkerPools(1)->SetName("explicit");
            UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Ok);
        }

        Y_UNIT_TEST(AcceptsValidConfig) {
            std::vector<TString> errors;
            UNIT_ASSERT(ValidateConfig(BuildValidConfig(), errors) == EValidationResult::Ok);
            UNIT_ASSERT(errors.empty());
            auto config = BuildValidConfig();
            config.MutableCompositeConveyorConfig()->MutableWorkerPools(0)->SetName("");
            UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Ok);
            UNIT_ASSERT(errors.empty());
        }

        Y_UNIT_TEST(RejectsInvalidConfig) {
            auto config = BuildValidConfig();
            config.MutableCompositeConveyorConfig()->MutableWorkerPools(0)->SetWorkersCount(
                std::numeric_limits<double>::infinity());
            std::vector<TString> errors;
            UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Error);
            UNIT_ASSERT_VALUES_EQUAL(errors.size(), 1);
        }

        Y_UNIT_TEST(WorkerCapacitySanityLimit) {
            for (const double count : {0.25, 1.0, NActors::MaxWorkers - 0.5, static_cast<double>(NActors::MaxWorkers)}) {
                auto config = BuildValidConfig();
                config.MutableCompositeConveyorConfig()->MutableWorkerPools(0)->SetWorkersCount(count);
                std::vector<TString> errors;
                UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Ok);
                UNIT_ASSERT(errors.empty());
            }

            for (const double count : {std::nextafter(static_cast<double>(NActors::MaxWorkers), std::numeric_limits<double>::infinity()),
                                       NActors::MaxWorkers + 1.0, 1e6, std::numeric_limits<double>::max()}) {
                auto config = BuildValidConfig();
                auto* pool = config.MutableCompositeConveyorConfig()->MutableWorkerPools(0);
                pool->SetWorkersCount(count);
                pool->SetDefaultFractionOfThreadsCount(0.1);
                std::vector<TString> errors;
                UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Error);
                UNIT_ASSERT_VALUES_EQUAL(errors.size(), 1);
                UNIT_ASSERT_STRING_CONTAINS(errors.front(), "exceeds limit " + ::ToString(NActors::MaxWorkers));
            }

            auto config = BuildValidConfig();
            auto* pool = config.MutableCompositeConveyorConfig()->MutableWorkerPools(0);
            pool->ClearWorkersCount();
            pool->SetDefaultFractionOfThreadsCount(1.0);
            std::vector<TString> errors;
            UNIT_ASSERT(ValidateConfig(config, errors) == EValidationResult::Ok);
            UNIT_ASSERT(errors.empty());
        }
    } // Y_UNIT_TEST_SUITE(TCompositeConveyorConfigValidationTest)

} // namespace NKikimr::NConfig
