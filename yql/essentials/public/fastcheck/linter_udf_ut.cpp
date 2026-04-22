#include "linter.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NFastCheck {

namespace {

void RunTypeCheck(TStringBuf sql, bool expectSuccess) {
    TChecksRequest request;
    request.Program = sql;
    request.ClusterMode = EClusterMode::Unknown;
    request.Syntax = ESyntax::YQL;
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
    )sql", true);
}

Y_UNIT_TEST(TypeCheckPireMultiMatchWrongPattern) {
    RunTypeCheck(R"sql(
        $fn = Pire::MultiMatch(42u);
        SELECT $fn(Nothing(String?))
    )sql", false);
}

Y_UNIT_TEST(TypeCheckPireMultiMatchWrongArgType) {
    RunTypeCheck(R"sql(
        $fn = Pire::MultiMatch("a.*\nb.*");
        SELECT $fn(42u)
    )sql", false);
}

Y_UNIT_TEST(TypeCheckHyperscanMultiMatchOk) {
    RunTypeCheck(R"sql(
        $fn = Hyperscan::MultiMatch("a.*\nb.*");
        SELECT $fn(Nothing(String?))
    )sql", true);
}

Y_UNIT_TEST(TypeCheckHyperscanMultiMatchWrongPattern) {
    RunTypeCheck(R"sql(
        $fn = Hyperscan::MultiMatch(42u);
        SELECT $fn(Nothing(String?))
    )sql", false);
}

Y_UNIT_TEST(TypeCheckHyperscanMultiMatchWrongArgType) {
    RunTypeCheck(R"sql(
        $fn = Hyperscan::MultiMatch("a.*\nb.*");
        SELECT $fn(42u)
    )sql", false);
}

Y_UNIT_TEST(TypeCheckRe2CaptureOk) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture("(a).*");
        SELECT $fn(Nothing(String?))
    )sql", true);
}

Y_UNIT_TEST(TypeCheckRe2CaptureWrongPattern) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture(42u);
        SELECT $fn(Nothing(String?))
    )sql", false);
}

Y_UNIT_TEST(TypeCheckRe2CaptureWrongOptions) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture("(a).*", 42u);
        SELECT $fn(Nothing(String?))
    )sql", false);
}

Y_UNIT_TEST(TypeCheckRe2CaptureWrongArgType) {
    RunTypeCheck(R"sql(
        $fn = Re2::Capture("(a).*");
        SELECT $fn(42u)
    )sql", false);
}

Y_UNIT_TEST(TypeCheckYsonSerializeOk) {
    RunTypeCheck(R"sql(
        SELECT Yson::Serialize(Yson::Parse('foo'y))
    )sql", true);
}

Y_UNIT_TEST(TypeCheckYsonParseResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Yson::Parse('foo'y) + 1
    )sql", false);
}

Y_UNIT_TEST(TypeCheckProtobufParseOk) {
    RunTypeCheck(R"sql(
        $p = Protobuf::Parse('foo');
        SELECT $p.a
    )sql", true);
}

Y_UNIT_TEST(TypeCheckProtobufParseWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Parse(1)
    )sql", false);
}

Y_UNIT_TEST(TypeCheckProtobufParseResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Parse('foo') + 1
    )sql", false);
}

Y_UNIT_TEST(TypeCheckProtobufSerializeOk) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Serialize(<|a:1|>) || 'suffix'
    )sql", true);
}

Y_UNIT_TEST(TypeCheckProtobufSerializeWrongArgType) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Serialize(1)
    )sql", false);
}

Y_UNIT_TEST(TypeCheckProtobufSerializeResultWrongUsage) {
    RunTypeCheck(R"sql(
        SELECT Protobuf::Serialize(<|a:1|>) + 1
    )sql", false);
}

} // Y_UNIT_TEST_SUITE(TLinterUdfTests)

} // namespace NYql::NFastCheck
