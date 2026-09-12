#include <ydb/core/config/validation/validators.h>

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
        Y_UNIT_TEST(AcceptsValidConfig) {
            std::vector<TString> errors;
            UNIT_ASSERT(ValidateConfig(BuildValidConfig(), errors) == EValidationResult::Ok);
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
