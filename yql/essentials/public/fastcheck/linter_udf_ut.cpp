#include "linter.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NFastCheck {

namespace {

void RunTypeCheck(TStringBuf sql, bool expectSuccess, TLangVersion langver = MinLangVersion) {
    TChecksRequest request;
    request.Program = sql;
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
    request.LangVer = langver;
    request.Filters.ConstructInPlace();
    request.Filters->push_back(TCheckFilter{.CheckNameGlob = "typecheck"});

    auto res = RunChecks(request);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(res.Checks[0].CheckName, "typecheck");
    if (expectSuccess) {
        UNIT_ASSERT_C(res.Checks[0].Success, res.Checks[0].Issues.ToString());
    } else {
        UNIT_ASSERT(!res.Checks[0].Success);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TLinterUdfTests) {

Y_UNIT_TEST(TypeCheckPireMultiMatchOk) {
    RunTypeCheck(R"sql(
        $fn = Pire::MultiMatch("a.*\nb.*");
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckPireMultiMatchWrongPattern) {
    RunTypeCheck(R"sql(
        $fn = Pire::MultiMatch(42u);
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckPireMultiMatchWrongArgType) {
    RunTypeCheck(R"sql(
        $fn = Pire::MultiMatch("a.*\nb.*");
        SELECT $fn(42u)
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckHyperscanMultiMatchOk) {
    RunTypeCheck(R"sql(
        $fn = Hyperscan::MultiMatch("a.*\nb.*");
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckHyperscanMultiMatchWrongPattern) {
    RunTypeCheck(R"sql(
        $fn = Hyperscan::MultiMatch(42u);
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckHyperscanMultiMatchWrongArgType) {
    RunTypeCheck(R"sql(
        $fn = Hyperscan::MultiMatch("a.*\nb.*");
        SELECT $fn(42u)
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckRe2CaptureOk) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture("(a).*");
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckRe2CaptureWrongPattern) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture(42u);
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckRe2CaptureWrongOptions) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture("(a).*", 42u);
        SELECT $fn(Nothing(String?))
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckRe2CaptureWrongArgType) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture("(a).*");
        SELECT $fn(42u)
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckYsonSerializeOk) {
    RunTypeCheck(R"sql(
        SELECT Yson::Serialize(Yson::Parse('foo'y))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckYsonParseResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Yson::Parse('foo'y) + 1
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckProtobufParseOk) {
    RunTypeCheck(R"sql(
        $p = Protobuf::Parse('foo');
        SELECT $p.a
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckProtobufParseWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Parse(1)
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckProtobufParseResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Parse('foo') + 1
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckProtobufSerializeOk) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Serialize(<|a:1|>) || 'suffix'
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckProtobufSerializeWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Serialize(1)
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckProtobufSerializeResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Serialize(<|a:1|>) + 1
    )sql", /*expectSuccess=*/false);
}

// Url::BuildQueryString
Y_UNIT_TEST(TypeCheckUrlBuildQueryStringDictOk) {
    RunTypeCheck(R"sql(
        SELECT Url::BuildQueryString(Nothing(Dict<String, List<String>>?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckUrlBuildQueryStringListOk) {
    RunTypeCheck(R"sql(
        SELECT Url::BuildQueryString(Nothing(List<Tuple<String, String>>?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckUrlBuildQueryStringFlattenDictOk) {
    RunTypeCheck(R"sql(
        SELECT Url::BuildQueryString(Nothing(Dict<String, String>?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckUrlBuildQueryStringWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Url::BuildQueryString(42)
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckUrlBuildQueryStringResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Url::BuildQueryString(Nothing(Dict<String, String>?)) + 42
    )sql", /*expectSuccess=*/false);
}

// Math::SwapBytes
Y_UNIT_TEST(TypeCheckMathSwapBytesUint32Ok) {
    RunTypeCheck(R"sql(
        SELECT Math::SwapBytes(1u)
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckMathSwapBytesWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Math::SwapBytes("hello")
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckMathSwapBytesResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Math::SwapBytes(1u) || "suffix"
    )sql", /*expectSuccess=*/false);
}

// Yson::From
Y_UNIT_TEST(TypeCheckYsonFromOk) {
    RunTypeCheck(R"sql(
        SELECT Yson::Serialize(Yson::From("hello"))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckYsonFromResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Yson::From("hello") + 1
    )sql", /*expectSuccess=*/false);
}

// Yson::ConvertTo
Y_UNIT_TEST(TypeCheckYsonConvertToOk) {
    RunTypeCheck(R"sql(
        SELECT Yson::ConvertTo(Yson::Parse("1"y))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckYsonConvertToWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Yson::ConvertTo("not_a_node", Int64)
    )sql", /*expectSuccess=*/false);
}

// DateTime::Split
Y_UNIT_TEST(TypeCheckDateTimeSplitDateOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(Date?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitDatetimeOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(Datetime?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitTimestampOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(Timestamp?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitTzDateOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(TzDate?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitDate32Ok) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(Date32?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitTimestamp64Ok) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(Timestamp64?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split("2023-01-01")
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeSplitResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Split(Nothing(Date?)) + 1
    )sql", /*expectSuccess=*/false);
}

// DateTime::StartOf/EndOf w/o Interval
Y_UNIT_TEST(TypeCheckDateTimeStartOfMonthSmallOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOfMonth(Nothing(Date?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeEndOfMonthBigTzOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::EndOfMonth(Nothing(TzDatetime64?))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeStartOfMonthSmallResOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOfMonth(DateTime::Split(Nothing(Date?)))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeEndOfMonthBigResOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::EndOfMonth(DateTime::Split(Nothing(Date32?)))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeStartOfMonthWrongArgument) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOfMonth("foo");
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeEndOfMonthWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT DateTime::EndOfMonth(Nothing(Date?)) + 1
    )sql", /*expectSuccess=*/false);
}

// DateTime::StartOf/EndOf with Interval
Y_UNIT_TEST(TypeCheckDateTimeStartOfSmallOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOf(Nothing(Date?), Interval("PT1H"))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeEndOfBigTzOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::EndOf(Nothing(TzDatetime64?), Interval("PT1H"))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeStartOfSmallResOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOf(DateTime::Split(Nothing(Date?)), Interval("PT1H"))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeEndOfBigResOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::EndOf(DateTime::Split(Nothing(Date32?)), Interval("PT1H"))
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeStartOfWrongArgument1) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOf("foo", Interval("PT1H"));
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeStartOfWrongArgument2) {
    RunTypeCheck(R"sql(
        SELECT DateTime::StartOf(Nothing(Date?), "foo");
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeEndOfWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT DateTime::EndOf(Nothing(Date?), Interval("PT1H")) + 1
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeFormatOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Format('%Y', true, true)(NULL)
    )sql", /*expectSuccess=*/true, MakeLangVersion(2025, 5));
}

Y_UNIT_TEST(TypeCheckDateTimeFormatOldOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Format('%Y', true)(NULL)
    )sql", /*expectSuccess=*/true, MakeLangVersion(2025, 4));
}

Y_UNIT_TEST(TypeCheckDateTimeFormatWrongArgument) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Format(1, true, true)(NULL)
    )sql", /*expectSuccess=*/false, MakeLangVersion(2025, 5));
}

Y_UNIT_TEST(TypeCheckDateTimeFormatWrongNumberOfArguments) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Format('%Y', true, true)(NULL)
    )sql", /*expectSuccess=*/false, MakeLangVersion(2025, 4));
}

Y_UNIT_TEST(TypeCheckDateTimeFormatWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT DateTime::Format('%Y', true, true) + 1
    )sql", /*expectSuccess=*/false, MakeLangVersion(2025, 5));
}

// DateTime::Shift
Y_UNIT_TEST(TypeCheckDateTimeShiftSmallOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftMonths(Nothing(Date?), 1)
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeShiftBigTzOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftYears(Nothing(TzDatetime64?), 1)
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeShiftSmallResOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftMonths(DateTime::Split(Nothing(Date?)), 1)
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeShiftBigResOk) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftYears(DateTime::Split(Nothing(Date32?)), 1)
    )sql", /*expectSuccess=*/true);
}

Y_UNIT_TEST(TypeCheckDateTimeShiftWrongArgument1) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftMonths("foo", 1);
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeShiftWrongArgument2) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftYears(Nothing(Date?), "foo");
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeShiftWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT DateTime::ShiftMonths(Nothing(Date?), 1) + 1
    )sql", /*expectSuccess=*/false);
}

Y_UNIT_TEST(TypeCheckDateTimeToSecondsFromIntervalOldOk) {
    RunTypeCheck(R"sql(
        SELECT EnsureType(DateTime::ToSeconds(Interval("P1D")), Int32)
    )sql", /*expectSuccess=*/true, MakeLangVersion(2025, 2));
}

Y_UNIT_TEST(TypeCheckDateTimeToSecondsFromIntervalOldFail) {
    RunTypeCheck(R"sql(
        SELECT EnsureType(DateTime::ToSeconds(Interval("P1D")), Int64)
    )sql", /*expectSuccess=*/false, MakeLangVersion(2025, 2));
}

Y_UNIT_TEST(TypeCheckDateTimeToSecondsFromIntervalNewOk) {
    RunTypeCheck(R"sql(
        SELECT EnsureType(DateTime::ToSeconds(Interval("P1D")), Int64)
    )sql", /*expectSuccess=*/true, MakeLangVersion(2025, 3));
}

Y_UNIT_TEST(TypeCheckDateTimeToSecondsFromIntervalNewFail) {
    RunTypeCheck(R"sql(
        SELECT EnsureType(DateTime::ToSeconds(Interval("P1D")), Int32)
    )sql", /*expectSuccess=*/false, MakeLangVersion(2025, 3));
}

Y_UNIT_TEST(AvoidMatchOverUniversalArgs) {
    RunTypeCheck(R"sql(
        select DateTime::MakeDate(DateTime::StartOfWeek(EvaluateExpr(null)))
    )sql", /*expectSuccess=*/true);
}

} // Y_UNIT_TEST_SUITE(TLinterUdfTests)

} // namespace NYql::NFastCheck
