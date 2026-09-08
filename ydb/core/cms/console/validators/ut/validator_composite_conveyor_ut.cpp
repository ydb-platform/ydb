#include "../validator_composite_conveyor.h"

#include <ydb/library/actors/core/defs.h>

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

    Y_UNIT_TEST(RejectsConfigRemoval) {
        TCompositeConveyorConfigValidator validator;
        for (const bool enabled : {true, false}) {
            auto oldConfig = BuildValidConfig();
            oldConfig.MutableCompositeConveyorConfig()->SetEnabled(enabled);
            auto newConfig = oldConfig;
            newConfig.ClearCompositeConveyorConfig();
            TVector<Ydb::Issue::IssueMessage> issues;
            UNIT_ASSERT(!validator.CheckConfig(oldConfig, newConfig, issues));
            UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
            UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "removing CompositeConveyorConfig is not supported");
        }
    }

    Y_UNIT_TEST(RejectsDisabling) {
        auto oldConfig = BuildValidConfig();
        oldConfig.MutableCompositeConveyorConfig()->SetEnabled(true);
        auto newConfig = oldConfig;
        newConfig.MutableCompositeConveyorConfig()->SetEnabled(false);
        TCompositeConveyorConfigValidator validator;
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(!validator.CheckConfig(oldConfig, newConfig, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "changing CompositeConveyorConfig.Enabled is not supported");
    }

    Y_UNIT_TEST(RejectsEnabling) {
        auto oldConfig = BuildValidConfig();
        oldConfig.MutableCompositeConveyorConfig()->SetEnabled(false);
        auto newConfig = oldConfig;
        newConfig.MutableCompositeConveyorConfig()->SetEnabled(true);
        TCompositeConveyorConfigValidator validator;
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(!validator.CheckConfig(oldConfig, newConfig, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "changing CompositeConveyorConfig.Enabled is not supported");
    }

    Y_UNIT_TEST(AcceptsUpdatesWithUnchangedEnabled) {
        TCompositeConveyorConfigValidator validator;
        for (const bool enabled : {true, false}) {
            auto oldConfig = BuildValidConfig();
            oldConfig.MutableCompositeConveyorConfig()->SetEnabled(enabled);
            auto newConfig = oldConfig;
            auto* pool = newConfig.MutableCompositeConveyorConfig()->MutableWorkerPools(0);
            pool->SetWorkersCount(2);
            pool->MutableLinks(0)->SetWeight(3);
            TVector<Ydb::Issue::IssueMessage> issues;
            UNIT_ASSERT(validator.CheckConfig(oldConfig, newConfig, issues));
            UNIT_ASSERT(issues.empty());
        }
    }

    Y_UNIT_TEST(EnabledUsesProtoDefault) {
        const auto implicitEnabled = BuildValidConfig();
        auto explicitEnabled = implicitEnabled;
        explicitEnabled.MutableCompositeConveyorConfig()->SetEnabled(true);
        auto disabled = implicitEnabled;
        disabled.MutableCompositeConveyorConfig()->SetEnabled(false);
        TCompositeConveyorConfigValidator validator;
        TVector<Ydb::Issue::IssueMessage> issues;

        UNIT_ASSERT(validator.CheckConfig(implicitEnabled, explicitEnabled, issues));
        UNIT_ASSERT(validator.CheckConfig(explicitEnabled, implicitEnabled, issues));
        UNIT_ASSERT(validator.CheckConfig({}, explicitEnabled, issues));
        UNIT_ASSERT(issues.empty());

        UNIT_ASSERT(!validator.CheckConfig(disabled, implicitEnabled, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "changing CompositeConveyorConfig.Enabled is not supported");

        issues.clear();
        UNIT_ASSERT(!validator.CheckConfig(implicitEnabled, disabled, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "changing CompositeConveyorConfig.Enabled is not supported");

        issues.clear();
        UNIT_ASSERT(!validator.CheckConfig({}, disabled, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "changing CompositeConveyorConfig.Enabled is not supported");
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

    Y_UNIT_TEST(WorkerCapacitySanityLimit) {
        auto config = BuildValidConfig();
        auto* pool = config.MutableCompositeConveyorConfig()->MutableWorkerPools(0);
        pool->SetWorkersCount(NActors::MaxWorkers);
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(Validate(config, issues));
        UNIT_ASSERT(issues.empty());

        pool->SetWorkersCount(NActors::MaxWorkers + 0.5);
        UNIT_ASSERT(!Validate(config, issues));
        UNIT_ASSERT_VALUES_EQUAL(issues.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(issues.front().message(), "exceeds limit " + ::ToString(NActors::MaxWorkers));
    }
}

}   // namespace NKikimr::NConsole
