#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NPathGenerator {

Y_UNIT_TEST_SUITE(TParseTests) {
    Y_UNIT_TEST(SuccessParseEnum) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/"
            }
        )", {"city"});
        const auto& result = generator->GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, true);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "${city}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 1);
        const auto& rule = result.Rules.front();
        UNIT_ASSERT_VALUES_EQUAL(rule.Type, IPathGenerator::EType::ENUM);
        UNIT_ASSERT_VALUES_EQUAL(rule.Name, "city");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.front(), "MSK");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.back(), "SPB");
    }

    Y_UNIT_TEST(SuccessParseEnumWithStrip) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : " MSK ,  SPB   ",
                "storage.location.template" : "/${city}/"
            }
        )", {"city"});
        const auto& result = generator->GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, true);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "${city}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 1);
        const auto& rule = result.Rules.front();
        UNIT_ASSERT_VALUES_EQUAL(rule.Type, IPathGenerator::EType::ENUM);
        UNIT_ASSERT_VALUES_EQUAL(rule.Name, "city");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.front(), "MSK");
        UNIT_ASSERT_VALUES_EQUAL(rule.Values.back(), "SPB");
    }

    Y_UNIT_TEST(ParseTwoEnabled) {
        UNIT_ASSERT_NO_EXCEPTION(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.enabled.type" : "enum",
                "projection.enabled.values" : "MSK,SPB",
                "storage.location.template" : "/${enabled}/"
            }
        )", {"enabled"}));
    }

    Y_UNIT_TEST(InvalidPartitioningTemplate) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/"
            }
        )", {"aba"}), yexception, "Template ${city}/ must include ${aba}");
    }

    Y_UNIT_TEST(InvalidProjectionTemplate) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${device_id}/"
            }
        )", {"city"}), yexception, "Template ${device_id}/ must include ${city}");
    }

    Y_UNIT_TEST(InvalidTemplate) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "storage.location.template" : "/${city}/${device_id}/"
            }
        )", {"city"}), yexception, "Partitioned by column named device_id does not exist for template ${city}/${device_id}/");
    }

    Y_UNIT_TEST(StartSubstition) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.device_id.type" : "enum",
                "projection.device_id.values" : "3f75",
                "storage.location.template" : "/${city}/${device_id}/"
            }
        )", {"city", "device_id"});
        const auto& result = generator->GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, true);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "${city}/${device_id}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 2);
        {
            const auto& rule = result.Rules.front();
            UNIT_ASSERT_VALUES_EQUAL(rule.Type, IPathGenerator::EType::ENUM);
            UNIT_ASSERT_VALUES_EQUAL(rule.Name, "city");
            UNIT_ASSERT_VALUES_EQUAL(rule.Values.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(rule.Values.front(), "MSK");
            UNIT_ASSERT_VALUES_EQUAL(rule.Values.back(), "SPB");
        }
        {
            const auto& rule = result.Rules.back();
            UNIT_ASSERT_VALUES_EQUAL(rule.Type, IPathGenerator::EType::ENUM);
            UNIT_ASSERT_VALUES_EQUAL(rule.Name, "device_id");
            UNIT_ASSERT_VALUES_EQUAL(rule.Values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rule.Values.front(), "3f75");
        }
    }

    Y_UNIT_TEST(InvalidValuesType) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : null,
                "storage.location.template" : "/${city}/"
            }
        )", {"city"}), yexception, "The values must be a string");
    }

    Y_UNIT_TEST(SuccessParseInteger) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.id.type" : "integer",
                "projection.id.min" : "5",
                "projection.id.max" : "6",
                "storage.location.template" : "/${id}/"
            }
        )", {"id"});
        const auto& result = generator->GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, true);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "${id}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 1);
        const auto& rule = result.Rules.front();
        UNIT_ASSERT_VALUES_EQUAL(rule.Type, IPathGenerator::EType::INTEGER);
        UNIT_ASSERT_VALUES_EQUAL(rule.Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(rule.Min, 5);
        UNIT_ASSERT_VALUES_EQUAL(rule.Max, 6);
    }

    Y_UNIT_TEST(SuccessDisableProjection) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : false,
                "projection.id.type" : "integer",
                "projection.id.min" : "5",
                "projection.id.max" : "6",
                "storage.location.template" : "/${id}/"
            }
        )", {"id"});
        const auto& result = generator->GetConfig();
        UNIT_ASSERT_VALUES_EQUAL(result.Enabled, false);
        UNIT_ASSERT_VALUES_EQUAL(result.LocationTemplate, "${id}/");
        UNIT_ASSERT_VALUES_EQUAL(result.Rules.size(), 1);
        const auto& rule = result.Rules.front();
        UNIT_ASSERT_VALUES_EQUAL(rule.Type, IPathGenerator::EType::INTEGER);
        UNIT_ASSERT_VALUES_EQUAL(rule.Name, "id");
        UNIT_ASSERT_VALUES_EQUAL(rule.Min, 5);
        UNIT_ASSERT_VALUES_EQUAL(rule.Max, 6);
    }

    Y_UNIT_TEST(CheckErrorPrefix) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : null,
                "storage.location.template" : "/${city}/"
            }
        )", {"city"}), yexception, "projection error: ");
    }
}

}
