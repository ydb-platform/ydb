#include "console_dumper.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/protos/key.pb.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(ConsoleDumper) {

    void FillDomainItems(::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &items,
                         ui32 mergeStrategy,
                         const TVector<ui32> &orders) {
        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->SetMergeStrategy(mergeStrategy);
        configItem->MutableId()->SetId(1);
        configItem->SetOrder(orders[0]);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *logConfig = configItem->MutableConfig()->MutableLogConfig();
        logConfig->SetSysLog(true);
        auto *entry = logConfig->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->SetMergeStrategy(mergeStrategy);
        configItem->SetOrder(orders[1]);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        logConfig = configItem->MutableConfig()->MutableLogConfig();
        logConfig->SetSysLog(false);
        entry = logConfig->AddEntry();
        entry->SetComponent("AUDIT_LOG_WRITER");
        entry->SetLevel(4);

        configItem = items.Add();
        configItem->SetMergeStrategy(mergeStrategy);
        configItem->SetOrder(orders[2]);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::CmsConfigItem);
        configItem->SetCookie("test");
        auto *cmsConfig = configItem->MutableConfig()->MutableCmsConfig();
        cmsConfig->MutableSentinelConfig()->SetEnable(true);

        configItem = items.Add();
        configItem->SetMergeStrategy(mergeStrategy);
        configItem->SetOrder(orders[3]);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::CmsConfigItem);
        configItem->SetCookie("test");
        cmsConfig = configItem->MutableConfig()->MutableCmsConfig();
        cmsConfig->MutableSentinelConfig()->SetDryRun(true);
    }

    Y_UNIT_TEST(Basic) {
        // We have same result for all merge strategies
        for (int mergeStrategy = 1; mergeStrategy < 4; ++mergeStrategy) {
            ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
            NKikimrConsole::TConfigItem *configItem = items.Add();
            configItem->SetMergeStrategy(mergeStrategy);
            configItem->MutableId()->SetId(1);
            configItem->SetOrder(21);
            configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
            configItem->SetCookie("test");
            auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
            entry->SetComponent("BG_TASKS");
            entry->SetLevel(5);

            TString result = NYamlConfig::DumpConsoleConfigs(items);
            const TString expected = R"(config:
  log_config:
    entry:
    - component: BG_TASKS
      level: 5
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
            UNIT_ASSERT_VALUES_EQUAL(result, expected);
        }
    }

    Y_UNIT_TEST(CoupleMerge) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        FillDomainItems(items, 2, {21, 22, 23, 24});

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config:
  log_config:
    entry:
    - component: BG_TASKS
      level: 5
    - component: AUDIT_LOG_WRITER
      level: 4
    sys_log: false
  cms_config:
    sentinel_config:
      enable: true
      dry_run: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(CoupleOverwrite) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        FillDomainItems(items, 1, {21, 22, 23, 24});

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config:
  log_config:
    entry:
    - component: AUDIT_LOG_WRITER
      level: 4
    sys_log: false
  cms_config:
    sentinel_config:
      dry_run: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(CoupleMergeOverwriteRepeated) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        FillDomainItems(items, 3, {21, 22, 23, 24});

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config:
  log_config:
    entry:
    - component: AUDIT_LOG_WRITER
      level: 4
    sys_log: false
  cms_config:
    sentinel_config:
      enable: true
      dry_run: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(ReverseMerge) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        FillDomainItems(items, 2, {24, 23, 22, 21});

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config:
  log_config:
    entry:
    - component: AUDIT_LOG_WRITER
      level: 4
    - component: BG_TASKS
      level: 5
    sys_log: true
  cms_config:
    sentinel_config:
      enable: true
      dry_run: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(ReverseOverwrite) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        FillDomainItems(items, 1, {24, 23, 22, 21});

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config:
  log_config:
    entry:
    - component: BG_TASKS
      level: 5
    sys_log: true
  cms_config:
    sentinel_config:
      enable: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(ReverseMergeOverwriteRepeated) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        FillDomainItems(items, 3, {24, 23, 22, 21});

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config:
  log_config:
    entry:
    - component: BG_TASKS
      level: 5
    sys_log: true
  cms_config:
    sentinel_config:
      enable: true
      dry_run: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(Different) {
        // We have same result for all merge strategy combinations
        for (int mergeStrategyA = 1; mergeStrategyA < 4; ++mergeStrategyA) {
            for (int mergeStrategyB = 1; mergeStrategyB < 4; ++mergeStrategyB) {
                ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
                NKikimrConsole::TConfigItem *configItem = items.Add();
                configItem->SetMergeStrategy(mergeStrategyA);
                configItem->SetOrder(21);
                configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
                configItem->SetCookie("test");
                auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
                entry->SetComponent("BG_TASKS");
                entry->SetLevel(5);
                configItem = items.Add();
                configItem->SetMergeStrategy(mergeStrategyB);
                configItem->SetOrder(20);
                configItem->SetKind((ui32)NKikimrConsole::TConfigItem::CmsConfigItem);
                configItem->SetCookie("test");
                auto *cmsConfig = configItem->MutableConfig()->MutableCmsConfig();
                cmsConfig->MutableSentinelConfig()->SetEnable(true);

                TString result = NYamlConfig::DumpConsoleConfigs(items);
                const TString expected = R"(config:
  log_config:
    entry:
    - component: BG_TASKS
      level: 5
  cms_config:
    sentinel_config:
      enable: true
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
                UNIT_ASSERT_VALUES_EQUAL(result, expected);
            }
        }
    }

    Y_UNIT_TEST(SimpleNode) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    node_type: test_node_type
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(JoinSimilar) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_1");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_2");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.1,2.1
  selector:
    node_type:
      in:
      - test_node_type_1
      - test_node_type_2
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(DontJoinDifferent) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_1");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test_1");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_2");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test_2");
        entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected1 = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test_1 merge_strategy=OVERWRITE id=1.1
  selector:
    node_type: test_node_type_1
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=test_2 merge_strategy=OVERWRITE id=2.1
  selector:
    node_type: test_node_type_2
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected1);

        configItem->SetCookie("test_1");
        entry->SetLevel(6);

        result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected2 = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test_1 merge_strategy=OVERWRITE id=1.1
  selector:
    node_type: test_node_type_1
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=test_1 merge_strategy=OVERWRITE id=2.1
  selector:
    node_type: test_node_type_2
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 6
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected2);
    }

    Y_UNIT_TEST(SimpleTenant) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    tenant: test_tenant
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(SimpleNodeTenant) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant");
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    node_type: test_node_type
    tenant: test_tenant
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(SimpleHostId) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableHostFilter()->AddHosts("test_host_1");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected1 = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    host: test_host_1
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected1);

        configItem->MutableUsageScope()->MutableHostFilter()->AddHosts("test_host_2");

        result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected2 = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    host:
      in:
      - test_host_1
      - test_host_2
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected2);
    }

    Y_UNIT_TEST(SimpleNodeId) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(1);
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected1 = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    node_id: 1
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected1);

        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(2);

        result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected2 = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.0
  selector:
    node_id:
      in:
      - 1
      - 2
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected2);
    }

    Y_UNIT_TEST(DontJoinNodeTenant) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_1");
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_1");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_2");
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_2");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.1
  selector:
    node_type: test_node_type_1
    tenant: test_tenant_1
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=test merge_strategy=OVERWRITE id=2.1
  selector:
    node_type: test_node_type_2
    tenant: test_tenant_2
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(JoinMultipleSimple) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(1);
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(3);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(3);
        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(4);
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(5);
        configItem->SetMergeStrategy(1);
        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(5);
        configItem->MutableUsageScope()->MutableNodeFilter()->AddNodes(6);
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=OVERWRITE id=1.1,3.1,2.5
  selector:
    node_id:
      in:
      - 1
      - 3
      - 4
      - 5
      - 6
  config:
    log_config:
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(MergeNode) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(2);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(2);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type");
        configItem->SetOrder(22);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::CmsConfigItem);
        configItem->SetCookie("test");
        auto *cmsConfig = configItem->MutableConfig()->MutableCmsConfig();
        cmsConfig->MutableSentinelConfig()->SetEnable(true);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=MERGE id=1.1
  selector:
    node_type: test_node_type
  config:
    log_config: !inherit
      entry: !append
      - component: BG_TASKS
        level: 5
- description: cookie=test merge_strategy=MERGE id=2.1
  selector:
    node_type: test_node_type
  config:
    cms_config: !inherit
      sentinel_config: !inherit
        enable: true
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(MergeOverwriteRepeatedNode) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(3);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type");
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("test");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(3);
        configItem->MutableUsageScope()->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type");
        configItem->SetOrder(22);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::CmsConfigItem);
        configItem->SetCookie("test");
        auto *cmsConfig = configItem->MutableConfig()->MutableCmsConfig();
        cmsConfig->MutableSentinelConfig()->SetEnable(true);

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=test merge_strategy=MERGE_OVERWRITE_REPEATED id=1.1
  selector:
    node_type: test_node_type
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=test merge_strategy=MERGE_OVERWRITE_REPEATED id=2.1
  selector:
    node_type: test_node_type
  config:
    cms_config: !inherit
      sentinel_config: !inherit
        enable: true
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(Ordering) {
        // regardless of initial items order
        // output items should be ordered by category
        // NodeType -> Tenant -> Tenant && NodeType -> Hosts -> NodeIds
        // and by Order in every category
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        int configId = 1;

        auto addItem = [&](int order) {
            NKikimrConsole::TConfigItem *configItem = items.Add();
            configItem->MutableId()->SetId(configId++);
            configItem->MutableId()->SetGeneration(1);
            configItem->SetMergeStrategy(3);
            configItem->SetOrder(order);
            configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
            configItem->SetCookie(ToString(configId)); // to disable glueing of configs
            auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
            entry->SetComponent("BG_TASKS");
            entry->SetLevel(5);

            return configItem->MutableUsageScope();
        };

        addItem(14)->MutableNodeFilter()->AddNodes(1);
        addItem(15)->MutableHostFilter()->AddHosts("test_host_1");
        auto *us = addItem(13);
        us->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_1");
        us->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_1");
        addItem(11)->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_1_1");
        addItem(12)->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_1_1");;
        addItem(9)->MutableNodeFilter()->AddNodes(2);
        addItem(10)->MutableHostFilter()->AddHosts("test_host_2");
        us = addItem(8);
        us->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_2");
        us->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_2");
        addItem(7)->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_2_2");
        addItem(5)->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_2_2");;
        addItem(6)->MutableNodeFilter()->AddNodes(3);
        addItem(4)->MutableHostFilter()->AddHosts("test_host_3");
        us = addItem(2);
        us->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_3");
        us->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_3");
        addItem(3)->MutableTenantAndNodeTypeFilter()->SetTenant("test_tenant_3_3");
        addItem(1)->MutableTenantAndNodeTypeFilter()->SetNodeType("test_node_type_3_3");;

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config:
- description: cookie=16 merge_strategy=MERGE_OVERWRITE_REPEATED id=15.1
  selector:
    node_type: test_node_type_3_3
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=11 merge_strategy=MERGE_OVERWRITE_REPEATED id=10.1
  selector:
    node_type: test_node_type_2_2
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=6 merge_strategy=MERGE_OVERWRITE_REPEATED id=5.1
  selector:
    node_type: test_node_type_1_1
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=15 merge_strategy=MERGE_OVERWRITE_REPEATED id=14.1
  selector:
    tenant: test_tenant_3_3
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=10 merge_strategy=MERGE_OVERWRITE_REPEATED id=9.1
  selector:
    tenant: test_tenant_2_2
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=5 merge_strategy=MERGE_OVERWRITE_REPEATED id=4.1
  selector:
    tenant: test_tenant_1_1
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=14 merge_strategy=MERGE_OVERWRITE_REPEATED id=13.1
  selector:
    node_type: test_node_type_3
    tenant: test_tenant_3
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=9 merge_strategy=MERGE_OVERWRITE_REPEATED id=8.1
  selector:
    node_type: test_node_type_2
    tenant: test_tenant_2
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=4 merge_strategy=MERGE_OVERWRITE_REPEATED id=3.1
  selector:
    node_type: test_node_type_1
    tenant: test_tenant_1
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=13 merge_strategy=MERGE_OVERWRITE_REPEATED id=12.1
  selector:
    host: test_host_3
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=8 merge_strategy=MERGE_OVERWRITE_REPEATED id=7.1
  selector:
    host: test_host_2
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=3 merge_strategy=MERGE_OVERWRITE_REPEATED id=2.1
  selector:
    host: test_host_1
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=12 merge_strategy=MERGE_OVERWRITE_REPEATED id=11.1
  selector:
    node_id: 3
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=7 merge_strategy=MERGE_OVERWRITE_REPEATED id=6.1
  selector:
    node_id: 2
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
- description: cookie=2 merge_strategy=MERGE_OVERWRITE_REPEATED id=1.1
  selector:
    node_id: 1
  config:
    log_config: !inherit
      entry:
      - component: BG_TASKS
        level: 5
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }

    Y_UNIT_TEST(IgnoreUnmanagedItems) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;

        NKikimrConsole::TConfigItem *configItem = items.Add();
        configItem->MutableId()->SetId(1);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(3);
        configItem->SetOrder(21);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::LogConfigItem);
        configItem->SetCookie("ydbcp-cookie");
        auto *entry = configItem->MutableConfig()->MutableLogConfig()->AddEntry();
        entry->SetComponent("BG_TASKS");
        entry->SetLevel(5);

        configItem = items.Add();
        configItem->MutableId()->SetId(2);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(3);
        configItem->SetOrder(22);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::NameserviceConfigItem);
        configItem->MutableConfig()->MutableNameserviceConfig()->SetClusterUUID("test");

        configItem = items.Add();
        configItem->MutableId()->SetId(3);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(3);
        configItem->SetOrder(23);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem);
        configItem->MutableConfig()->MutableNetClassifierDistributableConfig()->SetLastUpdateDatetimeUTC("123");

        configItem = items.Add();
        configItem->MutableId()->SetId(4);
        configItem->MutableId()->SetGeneration(1);
        configItem->SetMergeStrategy(3);
        configItem->SetOrder(24);
        configItem->SetKind((ui32)NKikimrConsole::TConfigItem::NamedConfigsItem);
        configItem->MutableConfig()->AddNamedConfigs()->SetName("test");

        TString result = NYamlConfig::DumpConsoleConfigs(items);
        const TString expected = R"(config: {}
allowed_labels:
  node_id:
    type: string
  host:
    type: string
  tenant:
    type: string
selector_config: []
)";
        UNIT_ASSERT_VALUES_EQUAL(result, expected);
    }
}
