#include "resource_pool_classifier_settings.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/aclib/aclib.h>


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

        std::visit(TClassifierSettings::TParser{"test@user"}, propertiesMap["member_name"]);
        UNIT_ASSERT_VALUES_EQUAL(settings.MemberName, "test@user");
    }

    Y_UNIT_TEST(RegexPredicateParsing) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        std::visit(TClassifierSettings::TParser{"ydb-ui|ydb-cli"}, propertiesMap["has_app_name"]);
        UNIT_ASSERT(settings.HasAppName.has_value());
        UNIT_ASSERT_VALUES_EQUAL(settings.HasAppName->Pattern, "ydb-ui|ydb-cli");

        std::visit(TClassifierSettings::TParser{"/Root/db/orders_.*"}, propertiesMap["has_full_scan"]);
        UNIT_ASSERT(settings.HasFullScan.has_value());
        UNIT_ASSERT_VALUES_EQUAL(settings.HasFullScan->Pattern, "/Root/db/orders_.*");

        std::visit(TClassifierSettings::TParser{"/Root/db/archive/.*"}, propertiesMap["has_path"]);
        UNIT_ASSERT(settings.HasPath.has_value());
        UNIT_ASSERT_VALUES_EQUAL(settings.HasPath->Pattern, "/Root/db/archive/.*");
    }

    Y_UNIT_TEST(RegexPredicateInvalidPattern) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        UNIT_ASSERT_EXCEPTION(std::visit(TClassifierSettings::TParser{"[invalid"}, propertiesMap["has_app_name"]), yexception);
        UNIT_ASSERT_EXCEPTION(std::visit(TClassifierSettings::TParser{"[invalid"}, propertiesMap["has_full_scan"]), yexception);
        UNIT_ASSERT_EXCEPTION(std::visit(TClassifierSettings::TParser{"[invalid"}, propertiesMap["has_path"]), yexception);
    }

    Y_UNIT_TEST(SettingsExtracting) {
        TClassifierSettings settings;
        settings.Rank = 123;
        settings.ResourcePool = "test_pool";
        settings.MemberName = "test@user";
        auto propertiesMap = settings.GetPropertiesMap();

        TClassifierSettings::TExtractor extractor;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["rank"]), "123");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["resource_pool"]), "test_pool");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["member_name"]), "test@user");
    }

    Y_UNIT_TEST(RegexPredicateExtracting) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        // Parse then extract — must round-trip the original pattern
        std::visit(TClassifierSettings::TParser{"ydb-.*"}, propertiesMap["has_app_name"]);
        std::visit(TClassifierSettings::TParser{"/Root/db/orders_.*"}, propertiesMap["has_full_scan"]);
        std::visit(TClassifierSettings::TParser{"/Root/db/archive/.*"}, propertiesMap["has_path"]);

        TClassifierSettings::TExtractor extractor;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["has_app_name"]), "ydb-.*");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["has_full_scan"]), "/Root/db/orders_.*");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["has_path"]), "/Root/db/archive/.*");
    }

    Y_UNIT_TEST(RegexPredicateExtractingEmpty) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        // Not set — should extract as empty string
        TClassifierSettings::TExtractor extractor;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["has_app_name"]), "");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["has_full_scan"]), "");
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["has_path"]), "");
    }

    Y_UNIT_TEST(ActionParsing) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        std::visit(TClassifierSettings::TParser{"reject"}, propertiesMap["action"]);
        UNIT_ASSERT(settings.Action.has_value());
        UNIT_ASSERT_EQUAL(*settings.Action, EClassifierAction::Reject);

        // Case-insensitive
        std::visit(TClassifierSettings::TParser{"REJECT"}, propertiesMap["action"]);
        UNIT_ASSERT_EQUAL(*settings.Action, EClassifierAction::Reject);
        std::visit(TClassifierSettings::TParser{"Reject"}, propertiesMap["action"]);
        UNIT_ASSERT_EQUAL(*settings.Action, EClassifierAction::Reject);

        // Empty resets to nullopt
        std::visit(TClassifierSettings::TParser{""}, propertiesMap["action"]);
        UNIT_ASSERT(!settings.Action.has_value());
    }

    Y_UNIT_TEST(ActionInvalid) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        UNIT_ASSERT_EXCEPTION_CONTAINS(std::visit(TClassifierSettings::TParser{"allow"}, propertiesMap["action"]), yexception, "Invalid action 'allow'");
    }

    Y_UNIT_TEST(ActionExtracting) {
        TClassifierSettings settings;
        auto propertiesMap = settings.GetPropertiesMap();

        TClassifierSettings::TExtractor extractor;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["action"]), "");

        settings.Action = EClassifierAction::Reject;
        UNIT_ASSERT_VALUES_EQUAL(std::visit(extractor, propertiesMap["action"]), "reject");
    }

    Y_UNIT_TEST(SettingsValidation) {
        TClassifierSettings settings;
        settings.MemberName = BUILTIN_ACL_METADATA;
        UNIT_ASSERT_STRING_CONTAINS(*settings.Validate(), TStringBuilder() << "Invalid resource pool classifier configuration, cannot create classifier for system user " << settings.MemberName);
    }
}

}  // namespace NKikimr
