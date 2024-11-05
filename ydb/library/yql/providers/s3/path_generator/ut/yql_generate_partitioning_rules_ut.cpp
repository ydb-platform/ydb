#include <ydb/library/yql/providers/s3/path_generator/yql_s3_path_generator.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NYql::NPathGenerator {

Y_UNIT_TEST_SUITE(TGenerateTests) {
    Y_UNIT_TEST(SuccessGenerate) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "enum",
                "projection.code.values" : "0,1",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "MSK/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "MSK/1/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "SPB/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "SPB/1/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateInteger) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "integer",
                "projection.code.min" : 0,
                "projection.code.max" : 35,
                "projection.code.interval" : 33,
                "storage.location.template" : "${city}/${code}"
            }
        )", {"city", "code"});

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "MSK/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "MSK/33/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "SPB/0/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "SPB/33/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateIntegerWithDigits) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "integer",
                "projection.code.min" : 0,
                "projection.code.max" : 35,
                "projection.code.interval" : 33,
                "projection.code.digits" : 5,
                "storage.location.template" : "/${city}/${code}"
            }
        )", {"city", "code"});

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "MSK/00000/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "MSK/00033/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "SPB/00000/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "SPB/00033/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(SuccessGenerateIntegerWithDigitsOverflow) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
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
        )", {"city", "code"}), yexception, "Digits cannot exceed 64, but received 10000");
    }

    Y_UNIT_TEST(CheckLimit) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "enum",
                "projection.code.values" : "0,1",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"}, {}, 2), yexception, "The limit on the number of paths has been reached: 2 of 2");
    }

    Y_UNIT_TEST(CheckClash) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "00,0",
                "projection.code.type" : "enum",
                "projection.code.values" : "0,00",
                "storage.location.template" : "/${city}${code}/"
            }
        )", {"city", "code"}), yexception, "Location path 000/ is composed by different projection value sets { ${city} = 00, ${code} = 0 } and { ${city} = 0, ${code} = 00 }");
    }

    Y_UNIT_TEST(CheckHiveFormat) {
        auto generator = CreatePathGenerator({}, {"city", "code", "device_id"}, {}, 1);
        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "city=${city}/code=${code}/device_id=${device_id}/");
    }

    Y_UNIT_TEST(SuccessGenerateDateWith) {
        auto generator = CreatePathGenerator(R"(
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

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "MSK/2010-01-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "MSK/2010-01-02/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "SPB/2010-01-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(rules[3].Path, "SPB/2010-01-02/");
        UNIT_ASSERT_VALUES_EQUAL(rules[3].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(FailedGenerateDateWithUnixtime) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
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
                "storage.location.template" : "//${city}/${code}//"
            }
        )", {"city", "code"}), yexception, "error in datetime parsing. Input data: 201701");
    }

    Y_UNIT_TEST(SuccessGenerateDateWithNow) {
        auto generator = CreatePathGenerator(R"(
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
        auto rules = generator->GetRules();
        auto nowAfter = TInstant::Now();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 2);
        UNIT_ASSERT_GE(rules[0].Path, "MSK/" + (nowBefore + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_LE(rules[0].Path, "MSK/" + (nowAfter + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 2);
        UNIT_ASSERT_GE(rules[1].Path, "SPB/" + (nowBefore + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_LE(rules[1].Path, "SPB/" + (nowAfter + TDuration::Days(1)).FormatLocalTime("%F") + "/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 2);
    }

    Y_UNIT_TEST(ProjectionFormatWithEmptySubstitution) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.dt.type" : "date",
                "projection.dt.min" : "2012-01-01",
                "projection.dt.max" : "2012-02-01",
                "projection.dt.interval" : "1",
                "projection.dt.format" : "asdf asdf 444",
                "projection.dt.unit" : "YEARS",
                "storage.location.template" : "/yellow_tripdata_${dt}-01/"
            }
        )", {"dt"});

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "yellow_tripdata_asdf asdf 444-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 1);
    }

    Y_UNIT_TEST(ProjectionFormatWithStrangeSubstitution) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.dt.type" : "date",
                "projection.dt.min" : "2012-01-01",
                "projection.dt.max" : "2014-01-01",
                "projection.dt.interval" : "1",
                "projection.dt.format" : "asdf%0 asdf%Y%0 444",
                "projection.dt.unit" : "YEARS",
                "storage.location.template" : "yellow_tripdata_${dt}-01"
            }
        )", {"dt"});

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "yellow_tripdata_asdf%0 asdf2012%0 444-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[1].Path, "yellow_tripdata_asdf%0 asdf2013%0 444-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[1].ColumnValues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[2].Path, "yellow_tripdata_asdf%0 asdf2014%0 444-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[2].ColumnValues.size(), 1);
    }

    Y_UNIT_TEST(TimestampFormatCheck) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.dt.type" : "date",
                "projection.dt.min" : "2012-01-01",
                "projection.dt.max" : "2012-02-01",
                "projection.dt.interval" : "1",
                "projection.dt.format" : "asdf asdf 444",
                "projection.dt.unit" : "YEARS",
                "storage.location.template" : "yellow_tripdata_${dt}-01/"
            }
        )", {"dt"});

        auto rules = generator->GetRules();
        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(rules[0].Path, "yellow_tripdata_asdf asdf 444-01/");
        UNIT_ASSERT_VALUES_EQUAL(rules[0].ColumnValues.size(), 1);
        const auto& columnValue = rules[0].ColumnValues[0];
        auto result = NKikimr::NMiniKQL::ValueFromString(columnValue.Type, TStringBuf{columnValue.Value});
        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(result.Get<ui32>(), 15340);
    }

    Y_UNIT_TEST(InvalidDateFrom) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.dt.type" : "date",
                "projection.dt.min" : "1980-1-1",
                "projection.dt.max" : "NOW",
                "projection.dt.interval" : "1",
                "projection.dt.format" : "%Y-%m-%d",
                "projection.dt.unit" : "YEARS",
                "storage.location.template" : "yellow_tripdata_${dt}-01/"
            }
        )", {"dt"}), yexception, "error in datetime parsing. Input data: 1980-1-1");
    }

    Y_UNIT_TEST(EmptyOutput) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.dt.type" : "date",
                "projection.dt.min" : "2012-01-01",
                "projection.dt.max" : "2011-02-01",
                "projection.dt.interval" : "1",
                "projection.dt.format" : "asdf asdf 444",
                "projection.dt.unit" : "YEARS",
                "storage.location.template" : "/yellow_tripdata_${dt}-01/"
            }
        )", {"dt"}), yexception, "The projection contains an empty set of paths");
    }

    Y_UNIT_TEST(LocationPathCollision) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreatePathGenerator(R"(
            {
                "projection.enabled" : "true",
                "storage.location.template" : "${year}",
                "projection.year.type" : "date",
                "projection.year.min" : "1971-01-01",
                "projection.year.max" : "NOW",
                "projection.year.interval" : "1",
                "projection.year.format" : "%Y",
                "projection.year.unit" : "DAYS"
            }
        )", {"year"}), yexception, "Location path 1971/ is composed by different projection value sets { ${year} = 1971-01-01 } and { ${year} = 1971-01-02 }");;
    }

    Y_UNIT_TEST(SuccessGenerateDateWithNegativeNow) {
        auto generator = CreatePathGenerator(R"(
            {
                "projection.enabled" : true,
                "projection.city.type" : "enum",
                "projection.city.values" : "MSK,SPB",
                "projection.code.type" : "date",
                "projection.code.min" : "NOW - 3 DAYS",
                "projection.code.max" : "NOW",
                "projection.code.format" : "%F",
                "projection.code.interval" : 1,
                "projection.code.unit" : "DAYS",
                "storage.location.template" : "/${city}/${code}/"
            }
        )", {"city", "code"});
        auto nowBefore = TInstant::Now();
        auto rules = generator->GetRules();
        auto nowAfter = TInstant::Now();

        TSet<TString> items;
        for (auto from = std::min(nowBefore, nowAfter) - TDuration::Days(3); from <= std::max(nowBefore, nowAfter); from += TDuration::Days(1)) {
            items.insert("MSK/" + from.FormatLocalTime("%F") + "/");
            items.insert("SPB/" + from.FormatLocalTime("%F") + "/");
        }

        UNIT_ASSERT_VALUES_EQUAL(rules.size(), 8);
        for (const auto& rule: rules) {
            UNIT_ASSERT(items.contains(rule.Path));
            UNIT_ASSERT_VALUES_EQUAL(rule.ColumnValues.size(), 2);
        }
    }
}

}
