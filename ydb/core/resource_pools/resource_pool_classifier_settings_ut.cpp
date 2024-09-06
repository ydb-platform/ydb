#include "resource_pool_classifier_settings.h"

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

using namespace NResourcePool;


Y_UNIT_TEST_SUITE(ResourcePoolClassifierTest) {
    Y_UNIT_TEST(IntSettingsParsing) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        std::visit(TClassifierSettings::TParser{"0"}, propertiesMap["rank"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.Rank, 0);

        std::visit(TClassifierSettings::TParser{"123"}, propertiesMap["rank"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.Rank, 123);

        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TClassifierSettings::TParser{"string_value"}, propertiesMap["rank"]), TFromStringException, "Unexpected symbol \"s\" at pos 0 in string \"string_value\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TClassifierSettings::TParser{"9223372036854775808"}, propertiesMap["rank"]), TFromStringException, "Integer overflow in string \"9223372036854775808\".");
        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TClassifierSettings::TParser{"-2"}, propertiesMap["rank"]), yexception, "Invalid integer value -2, it is should be greater or equal -1");
    }

    Y_UNIT_TEST(StringSettingsParsing) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        std::visit(TClassifierSettings::TParser{"test_pool"}, propertiesMap["resource_pool"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.ResourcePool, "test_pool");

        std::visit(TClassifierSettings::TParser{"test@user"}, propertiesMap["membername"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.Membername, "test@user");
    }

    Y_UNIT_TEST(SettingsExtracting) {
        TClassifierSettings settings;
        settings.Rank = 123;
        settings.ResourcePool = "test_pool";
        settings.Membername = "test@user";
        auto propertiesMap = settings.GetPropertiesMap();

        TClassifierSettings::TExtractor extractor;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["rank"]), "123");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["resource_pool"]), "test_pool");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["membername"]), "test@user");
    }
}

}  // namespace NKikimr
