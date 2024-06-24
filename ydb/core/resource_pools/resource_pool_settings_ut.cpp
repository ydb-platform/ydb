#include "resource_pool_settings.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NResourcePool;


Y_UNIT_TEST_SUITE(ResourcePoolTest) {
    Y_UNIT_TEST(SettingsValuesValidation) {
        TPoolSettings settings;
        auto propertiesMap = GetPropertiesMap(settings);

        // Uint64 and TDuration settings
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"string_value"}, propertiesMap["query_count_limit"]), TFromStringException, "Unexpected symbol \"s\" at pos 0 in string \"string_value\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"-1"}, propertiesMap["query_count_limit"]), TFromStringException, "Unexpected symbol \"-\" at pos 0 in string \"-1\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"99999999999999999999"}, propertiesMap["query_count_limit"]), TFromStringException, "Integer overflow in string \"99999999999999999999\".");

        // Ratio settings
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"-1.5"}, propertiesMap["query_memory_limit_ratio_per_node"]), yexception, "Invalid ratio value -1.5, it is should be between 0 and 100");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"101.5"}, propertiesMap["query_memory_limit_ratio_per_node"]), yexception, "Invalid ratio value 101.5, it is should be between 0 and 100");
    }
}

}  // namespace NKikimr
