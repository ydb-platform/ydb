#include "validator_bootstrap.h"
#include "validator_ut_common.h"

#include <ydb/library/actors/protos/interconnect.pb.h>

namespace NKikimr::NConsole {

using namespace NTests;

namespace {

void CheckConfig(const NKikimrConfig::TBootstrap &config,
                 bool result,
                 ui32 warnings = 0)
{
    NKikimrConfig::TAppConfig cfg;
    cfg.MutableBootstrapConfig()->CopyFrom(config);
    cfg.MutableNameserviceConfig()->CopyFrom(MakeDefaultNameserviceConfig());
    NTests::CheckConfig<TBootstrapConfigValidator>(cfg, result, warnings);
}

void CheckConfig(const NKikimrResourceBroker::TResourceBrokerConfig &config,
                 bool result,
                 ui32 warnings = 0)
{
    NKikimrConfig::TBootstrap cfg = MakeDefaultBootstrapConfig();
    cfg.MutableResourceBroker()->CopyFrom(config);
    CheckConfig(cfg, result, warnings);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(ResourceBrokerConfigValidatorTests) {
    Y_UNIT_TEST(TestEmptyConfig) {
        TBootstrapConfigValidator validator;
        TVector<Ydb::Issue::IssueMessage> issues;
        UNIT_ASSERT(validator.CheckConfig({}, {}, issues));
    }

    Y_UNIT_TEST(TestMinConfig) {
        CheckConfig(MakeDefaultResourceBrokerConfig(), true);
    }

    Y_UNIT_TEST(TestEmptyQueueName) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddQueue("", 100, {1}, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestZeroQueueWeight) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddQueue("queue", 0, {1}, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestRepeatedQueueName) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddQueue("queue", 100, {1}, config);
        AddQueue("queue", 100, {1}, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestEmptyTaskName) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddTask("", "queue_default", 10000000, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestRepeatedTaskName) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddTask("task", "queue_default", 10000000, config);
        AddTask("task", "queue_default", 10000000, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestZeroDefaultDuration) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddTask("task", "queue_default", 0, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestUnknownQueue) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddTask("task", "queue_unknown", 0, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestNoDefaultQueue) {
        NKikimrResourceBroker::TResourceBrokerConfig config;
        AddQueue("queue1", 100, {1}, config);
        AddTask(NLocalDb::UnknownTaskName, "queue1", 0, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestNoUnknownTask) {
        NKikimrResourceBroker::TResourceBrokerConfig config;
        AddQueue(NLocalDb::DefaultQueueName, 100, {1}, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestUnlimitedResource) {
        NKikimrResourceBroker::TResourceBrokerConfig config;
        AddQueue(NLocalDb::DefaultQueueName, 100, {}, config);
        AddTask(NLocalDb::UnknownTaskName, NLocalDb::DefaultQueueName, 10000000, config);
        config.MutableResourceLimit();
        CheckConfig(config, true, 2);
    }

    Y_UNIT_TEST(TestUnusedQueue) {
        auto config = MakeDefaultResourceBrokerConfig();
        AddQueue("queue1", 100, {1}, config);
        CheckConfig(config, true, 1);
    }
}

Y_UNIT_TEST_SUITE(BootstrapTabletsValidatorTests) {
    Y_UNIT_TEST(TestUnknownNodeForTablet) {
        auto config = MakeDefaultBootstrapConfig();
        config.MutableCompactionBroker();
        AddTablet(NKikimrConfig::TBootstrap::TX_MEDIATOR, {1, 5}, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestNoNodeForTablet) {
        auto config = MakeDefaultBootstrapConfig();
        config.MutableCompactionBroker();
        AddTablet(NKikimrConfig::TBootstrap::TX_MEDIATOR, {}, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestRequiredTablet) {
        auto config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::FLAT_BS_CONTROLLER, config);
        CheckConfig(config, false);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::FLAT_SCHEMESHARD, config);
        CheckConfig(config, false);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::FLAT_TX_COORDINATOR, config);
        CheckConfig(config, false);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::TX_MEDIATOR, config);
        CheckConfig(config, false);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::TX_ALLOCATOR, config);
        CheckConfig(config, false);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::CONSOLE, config);
        CheckConfig(config, false);
    }

    Y_UNIT_TEST(TestImportantTablet) {
        auto config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::CMS, config);
        CheckConfig(config, true, 1);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::NODE_BROKER, config);
        CheckConfig(config, true, 1);
        config = MakeDefaultBootstrapConfig();
        RemoveTablet(NKikimrConfig::TBootstrap::TENANT_SLOT_BROKER, config);
        CheckConfig(config, true, 1);
    }

    Y_UNIT_TEST(TestCompactionBroker) {
        auto config = MakeDefaultBootstrapConfig();
        config.MutableCompactionBroker();
        CheckConfig(config, false);
    }
}

} // namespace NKikimr::NConsole
