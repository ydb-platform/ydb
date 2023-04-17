#include "console_dumper.h"

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ConsoleDumper) {
    Y_UNIT_TEST(Basic) {
        ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> items;
        NKikimrConsole::TConfigItem &configItem = *items.Add();
        configItem.SetMergeStrategy(2);
        configItem.SetOrder(21);
        configItem.SetCookie("test");
        auto &entry = *configItem.MutableConfig()->MutableLogConfig()->AddEntry();
        entry.SetComponent("BG_TASKS");
        entry.SetLevel(5);

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
