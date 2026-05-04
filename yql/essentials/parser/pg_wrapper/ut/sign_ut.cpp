#include <yql/essentials/parser/pg_wrapper/interface/sign.h>

#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

namespace {

ui32 PgTypeId(TStringBuf typeName) {
    return NPg::LookupType(TString(typeName)).TypeId;
}

constexpr TStringBuf SmallNumeric = "99999999999999999999999999999999999999.99";
constexpr TStringBuf NegativeNumeric = "-99999999999999999999999999999999999999.99";

} // namespace

Y_UNIT_TEST_SUITE(PgSignTests) {

Y_UNIT_TEST(TestInt4) {
    auto positive = PgSign("42", PgTypeId("int4"));
    UNIT_ASSERT(positive.has_value() && *positive > 0);

    auto zero = PgSign("0", PgTypeId("int4"));
    UNIT_ASSERT(zero.has_value() && *zero == 0);

    auto negative = PgSign("-42", PgTypeId("int4"));
    UNIT_ASSERT(negative.has_value() && *negative < 0);
}

Y_UNIT_TEST(TestNumeric) {
    auto positive = PgSign(SmallNumeric, PgTypeId("numeric"));
    UNIT_ASSERT(positive.has_value() && *positive > 0);

    auto zero = PgSign("0", PgTypeId("numeric"));
    UNIT_ASSERT(zero.has_value() && *zero == 0);

    auto negative = PgSign(NegativeNumeric, PgTypeId("numeric"));
    UNIT_ASSERT(negative.has_value() && *negative < 0);
}

Y_UNIT_TEST(TestInterval) {
    auto positive = PgSign("2 hours", PgTypeId("interval"));
    UNIT_ASSERT(positive.has_value() && *positive > 0);

    auto zero = PgSign("0", PgTypeId("interval"));
    UNIT_ASSERT(zero.has_value() && *zero == 0);

    auto negative = PgSign("-2 hours", PgTypeId("interval"));
    UNIT_ASSERT(negative.has_value() && *negative < 0);
}

Y_UNIT_TEST(TestStringFail) {
    auto result = PgSign("hello", PgTypeId("text"));
    UNIT_ASSERT(!result.has_value());
    UNIT_ASSERT_EQUAL_C(result.error(), TString("Unable to find an overload for proc sign with given argument types: (text)"), result.error());
}

} // Y_UNIT_TEST_SUITE(PgSignTests)

} // namespace NYql
