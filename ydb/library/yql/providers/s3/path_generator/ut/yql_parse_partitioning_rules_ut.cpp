#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NPathGenerator {

Y_UNIT_TEST_SUITE(TParseTests) {
    Y_UNIT_TEST(SuccessParseEnum) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/"
            }
        )", {"city"});
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, true);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "/${city}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 1);
        const auto& rule = result.Rules.front();
        UNIT_ASSERT_VALUES_EQUAL(rule.Type, EType::ENUM);
        UNIT_ASSERT_VALUES_EQUAL(rule.Name, "city");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.front(), "MSK");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.back(), "SPB");
    }

    Y_UNIT_TEST(ParseTwoEnabled) {
        UNIT_ASSERT_NO_EXCEPTION(ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.enabled.type" : "enum",
                "projection.enabled.values" : "MSK,SPB",
                "storage.location.template" : "/${enabled}/"
            }
        )", {"enabled"}));
    }

    Y_UNIT_TEST(InvalidPartitioningTemplate) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/"
            }
        )", {"aba"}), yexception, "Template /${city}/ must include ${aba}");
    }

    Y_UNIT_TEST(InvalidProjectionTemplate) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${device_id}/"
            }
        )", {"city"}), yexception, "Template /${device_id}/ must include ${city}");
    }

    Y_UNIT_TEST(InvalidTemplate) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/${device_id}/"
            }
        )", {"city"}), yexception, "Colum named device_id does not exist for template /${city}/${device_id}");
    }

    Y_UNIT_TEST(StartSubstition) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/${device_id}/"
            }
        )", {"city", "device_id"});
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, true);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "/${city}/${device_id}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 1);
        const auto& rule = result.Rules.front();
        UNIT_ASSERT_VALUES_EQUAL(rule.Type, EType::ENUM);
        UNIT_ASSERT_VALUES_EQUAL(rule.Name, "city");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.front(), "MSK");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.back(), "SPB");
    }

    Y_UNIT_TEST(InvalidValuesType) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : 23,
                "storage.location.template" : "/${city}/"
            }
        )", {"city"}), yexception, "The values must be a string");
    }

}

}
