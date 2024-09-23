#include "resource_pool_settings.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NResourcePool;


Y_UNIT_TEST_SUITE(ResourcePoolTest) {
    Y_UNIT_TEST(IntSettingsParsing) {
        TPoolSettings settings;
        auto propertiesMap = GetPropertiesMap(settings);

        std::visit(TSettingsParser{"-1"}, propertiesMap["queue_size"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueueSize, -1);

        std::visit(TSettingsParser{"10"}, propertiesMap["queue_size"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueueSize, 10);

        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"string_value"}, propertiesMap["queue_size"]), TFromStringException, "Unexpected symbol \"s\" at pos 0 in string \"string_value\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"2147483648"}, propertiesMap["queue_size"]), TFromStringException, "Integer overflow in string \"2147483648\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"-2"}, propertiesMap["queue_size"]), yexception, "Invalid integer value -2, it is should be greater or equal -1");
    }

    Y_UNIT_TEST(SecondsSettingsParsing) {
        TPoolSettings settings;
        auto propertiesMap = GetPropertiesMap(settings);

        std::visit(TSettingsParser{"0"}, propertiesMap["query_cancel_after_seconds"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueryCancelAfter, TDuration::Zero());

        std::visit(TSettingsParser{"10"}, propertiesMap["query_cancel_after_seconds"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueryCancelAfter, TDuration::Seconds(10));

        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"-1"}, propertiesMap["query_cancel_after_seconds"]), TFromStringException, "Unexpected symbol \"-\" at pos 0 in string \"-1\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"18446744073709552"}, propertiesMap["query_cancel_after_seconds"]), yexception, "Invalid seconds value 18446744073709552, it is should be less or equal than 18446744073709551");
    }

    Y_UNIT_TEST(PercentSettingsParsing) {
        TPoolSettings settings;
        auto propertiesMap = GetPropertiesMap(settings);

        std::visit(TSettingsParser{"-1"}, propertiesMap["query_memory_limit_percent_per_node"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueryMemoryLimitPercentPerNode, -1);

        std::visit(TSettingsParser{"0"}, propertiesMap["query_memory_limit_percent_per_node"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueryMemoryLimitPercentPerNode, 0);

        std::visit(TSettingsParser{"55.5"}, propertiesMap["query_memory_limit_percent_per_node"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueryMemoryLimitPercentPerNode, 55.5);

        std::visit(TSettingsParser{"100"}, propertiesMap["query_memory_limit_percent_per_node"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.QueryMemoryLimitPercentPerNode, 100);

        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"-1.5"}, propertiesMap["query_memory_limit_percent_per_node"]), yexception, "Invalid percent value -1.5, it is should be between 0 and 100 or -1");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"-0.5"}, propertiesMap["query_memory_limit_percent_per_node"]), yexception, "Invalid percent value -0.5, it is should be between 0 and 100 or -1");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TSettingsParser{"101.5"}, propertiesMap["query_memory_limit_percent_per_node"]), yexception, "Invalid percent value 101.5, it is should be between 0 and 100 or -1");
    }

    Y_UNIT_TEST(SettingsExtracting) {
        TPoolSettings settings;
        settings.ConcurrentQueryLimit = 10;
        settings.QueueSize = -1;
        settings.QueryCancelAfter = TDuration::Seconds(15);
        settings.QueryMemoryLimitPercentPerNode = 0.5;
        auto propertiesMap = GetPropertiesMap(settings);

        TSettingsExtractor extractor;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["concurrent_query_limit"]), "10");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["queue_size"]), "-1");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["query_cancel_after_seconds"]), "15");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["query_memory_limit_percent_per_node"]), "0.5");
    }
}

}  // namespace NKikimr
