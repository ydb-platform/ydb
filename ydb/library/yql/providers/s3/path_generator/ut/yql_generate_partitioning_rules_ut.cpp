#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NPathGenerator {

Y_UNIT_TEST_SUITE(TGenerateTests) {
    Y_UNIT_TEST(SuccessGenerate) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "enum",
                "projection.code.values" : "0,1",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        auto rules = ExpandPartitioningRules(result, 100);
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "/MSK/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "/MSK/1/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "/SPB/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "/SPB/1/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateInteger) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "integer",
                "projection.code.min" : 0,
                "projection.code.max" : 35,
                "projection.code.interval" : 33,
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        auto rules = ExpandPartitioningRules(result, 100);
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "/MSK/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "/MSK/33/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "/SPB/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "/SPB/33/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateIntegerWithDigits) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "integer",
                "projection.code.min" : 0,
                "projection.code.max" : 35,
                "projection.code.interval" : 33,
                "projection.code.digits" : 5,
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        auto rules = ExpandPartitioningRules(result, 100);
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "/MSK/00000/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "/MSK/00033/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "/SPB/00000/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "/SPB/00033/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateIntegerWithDigitsOverflow) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "integer",
                "projection.code.min" : 0,
                "projection.code.max" : 35,
                "projection.code.interval" : 33,
                "projection.code.digits" : 10000,
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        UNIT_ASSERT_EXCEPTION_CONTAINS(ExpandPartitioningRules(result, 100), yexception, "Digits cannot exceed 64, but received 10000");
    }

    Y_UNIT_TEST(CheckLimit) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "enum",
                "projection.code.values" : "0,1",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        UNIT_ASSERT_EXCEPTION_CONTAINS(ExpandPartitioningRules(result, 2), yexception, "The limit on the number of paths has been reached: 2 of 2");
    }

    Y_UNIT_TEST(CheckHiveFormat) {
        auto result = ParsePartitioningRules({}, {"city", "code", "device_id"});
        auto rules = ExpandPartitioningRules(result, 1);
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "/city=${city}/code=${code}/device_id=${device_id}");
    }

    Y_UNIT_TEST(SuccessGenerateDateWith) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "date",
                "projection.code.min" : "2010-01-01",
                "projection.code.max" : "2010-01-02",
                "projection.code.format" : "%F",
                "projection.code.interval" : 1,
                "projection.code.unit" : "DAYS",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        auto rules = ExpandPartitioningRules(result, 100);
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "/MSK/2010-01-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "/MSK/2010-01-02/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "/SPB/2010-01-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "/SPB/2010-01-02/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateDateWithUnixtime) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "date",
                "projection.code.min" : "201701",
                "projection.code.max" : "201701",
                "projection.code.format" : "%F",
                "projection.code.interval" : 1,
                "projection.code.unit" : "DAYS",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        auto rules = ExpandPartitioningRules(result, 100);
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "/MSK/1970-01-03/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "/SPB/1970-01-03/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateDateWithNow) {
        auto result = ParsePartitioningRules(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "date",
                "projection.code.min" : "     NOW + 1 DAYS  ",
                "projection.code.max" : "NOW+1DAYS",
                "projection.code.format" : "%F",
                "projection.code.interval" : 1,
                "projection.code.unit" : "DAYS",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});
        auto nowBefore = TInstant::Now();
        auto rules = ExpandPartitioningRules(result, 100);
        auto nowAfter = TInstant::Now();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 2);
        UNIT_ASSERT_GE(rules[0].Path, "/MSK/" + (nowBefore + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_LE(rules[0].Path, "/MSK/" + (nowAfter + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_GE(rules[1].Path, "/SPB/" + (nowBefore + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_LE(rules[1].Path, "/SPB/" + (nowAfter + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
    }


}

}
