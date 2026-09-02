#include "../validator_composite_conveyor.h"

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NKikimr::NConsole {

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

bool Validate(const NKikimrConfig::TAppConfig& config, TVector<Ydb::Issue::IssueMessage>& issues) {
    TCompositeConveyorConfigValidator validator;
    return validator.CheckConfig({}, config, issues);
}

}   // namespace

Y_UNIT_TEST_SUITE(TCompositeConveyorConfigValidatorTest) {
    Y_UNIT_TEST(AcceptsAbsentAndValidConfig) {
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(Validate({}, issues));
        UNIT_ASSERT(issues.empty());

        UNIT_ASSERT(Validate(BuildValidConfig(), issues));
        UNIT_ASSERT(issues.empty());
    }

    Y_UNIT_TEST(RejectsInvalidWeight) {
        auto config = BuildValidConfig();
        config.MutableCompositeConveyorConfig()->MutableWorkerPools(0)->MutableLinks(0)->SetWeight(0);
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(!Validate(config, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);

        config = BuildValidConfig();
        config.MutableCompositeConveyorConfig()->MutableWorkerPools(0)->MutableLinks(0)->SetWeight(
            std::numeric_limits<double>::infinity());
        issues.clear();
        UNIT_ASSERT(!Validate(config, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
    }

    Y_UNIT_TEST(RejectsInvalidWorkerCapacity) {
        auto config = BuildValidConfig();
        config.MutableCompositeConveyorConfig()->MutableWorkerPools(0)->SetWorkersCount(
            std::numeric_limits<double>::infinity());
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(!Validate(config, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);

        config = BuildValidConfig();
        auto* pool = config.MutableCompositeConveyorConfig()->MutableWorkerPools(0);
        pool->ClearWorkersCount();
        pool->SetDefaultFractionOfThreadsCount(2);
        issues.clear();
        UNIT_ASSERT(!Validate(config, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
    }
}

}   // namespace NKikimr::NConsole
