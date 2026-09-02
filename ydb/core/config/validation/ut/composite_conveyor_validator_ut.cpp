#include "../validators.h"

#include <library/cpp/testing/unittest/registar.h>

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
    } // Y_UNIT_TEST_SUITE(TCompositeConveyorConfigValidationTest)

} // namespace NKikimr::NConfig
