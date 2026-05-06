#include <yql/essentials/parser/pg_wrapper/interface/compare.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {

ui32 PgTypeId(TStringBuf typeName) {
    return NYql::NPg::LookupType(TString(typeName)).TypeId;
}

constexpr TStringBuf SmallNumeric = "99999999999999999999999999999999999999.99";
constexpr TStringBuf LargeNumeric = "100000000000000000000000000000000000000.01";

} // namespace

Y_UNIT_TEST_SUITE(PgCompareWithCastsTests) {

Y_UNIT_TEST(TestInt) {
    auto less = PgCompareWithCasts("1", PgTypeId("int4"), "2", PgTypeId("int4"), EPgCompareType::Less);
    UNIT_ASSERT(less.has_value() && *less);

    auto greater = PgCompareWithCasts("2", PgTypeId("int4"), "1", PgTypeId("int4"), EPgCompareType::Greater);
    UNIT_ASSERT(greater.has_value() && *greater);

    auto notLess = PgCompareWithCasts("5", PgTypeId("int4"), "5", PgTypeId("int4"), EPgCompareType::Less);
    UNIT_ASSERT(notLess.has_value() && !*notLess);

    auto notGreater = PgCompareWithCasts("1", PgTypeId("int4"), "2", PgTypeId("int4"), EPgCompareType::Greater);
    UNIT_ASSERT(notGreater.has_value() && !*notGreater);
}

Y_UNIT_TEST(TestInterval) {
    auto less = PgCompareWithCasts("1 hour", PgTypeId("interval"), "2 hours", PgTypeId("interval"), EPgCompareType::Less);
    UNIT_ASSERT(less.has_value() && *less);

    auto greater = PgCompareWithCasts("2 hours", PgTypeId("interval"), "1 hour", PgTypeId("interval"), EPgCompareType::Greater);
    UNIT_ASSERT(greater.has_value() && *greater);

    auto notLess = PgCompareWithCasts("1 day", PgTypeId("interval"), "1 day", PgTypeId("interval"), EPgCompareType::Less);
    UNIT_ASSERT(notLess.has_value() && !*notLess);

    auto notGreater = PgCompareWithCasts("1 hour", PgTypeId("interval"), "2 hours", PgTypeId("interval"), EPgCompareType::Greater);
    UNIT_ASSERT(notGreater.has_value() && !*notGreater);
}

Y_UNIT_TEST(TestNumeric) {
    auto less = PgCompareWithCasts(SmallNumeric, PgTypeId("numeric"), LargeNumeric, PgTypeId("numeric"), EPgCompareType::Less);
    UNIT_ASSERT(less.has_value() && *less);

    auto greater = PgCompareWithCasts(LargeNumeric, PgTypeId("numeric"), SmallNumeric, PgTypeId("numeric"), EPgCompareType::Greater);
    UNIT_ASSERT(greater.has_value() && *greater);

    auto notLess = PgCompareWithCasts(SmallNumeric, PgTypeId("numeric"), SmallNumeric, PgTypeId("numeric"), EPgCompareType::Less);
    UNIT_ASSERT(notLess.has_value() && !*notLess);

    auto notGreater = PgCompareWithCasts(SmallNumeric, PgTypeId("numeric"), LargeNumeric, PgTypeId("numeric"), EPgCompareType::Greater);
    UNIT_ASSERT(notGreater.has_value() && !*notGreater);
}

Y_UNIT_TEST(TestNumericWithInt) {
    auto less = PgCompareWithCasts("1", PgTypeId("int4"), "2.5", PgTypeId("numeric"), EPgCompareType::Less);
    UNIT_ASSERT(less.has_value() && *less);

    auto greater = PgCompareWithCasts("3.5", PgTypeId("numeric"), "2", PgTypeId("int4"), EPgCompareType::Greater);
    UNIT_ASSERT(greater.has_value() && *greater);

    auto notLess = PgCompareWithCasts("5", PgTypeId("int4"), "5", PgTypeId("numeric"), EPgCompareType::Less);
    UNIT_ASSERT(notLess.has_value() && !*notLess);

    auto notGreater = PgCompareWithCasts("1.5", PgTypeId("numeric"), "2", PgTypeId("int4"), EPgCompareType::Greater);
    UNIT_ASSERT(notGreater.has_value() && !*notGreater);
}

Y_UNIT_TEST(TestIncompatibleTypes) {
    auto result = PgCompareWithCasts("1", PgTypeId("int4"), "hello", PgTypeId("text"), EPgCompareType::Less);
    UNIT_ASSERT(!result.has_value());
    UNIT_ASSERT_EQUAL_C(result.error(), TString("Unable to find an overload for operator < with given argument type(s): (int4,text)"), result.error());
}

} // Y_UNIT_TEST_SUITE(TPgCompareWithCastsTests)

} // namespace NKikimr::NMiniKQL
