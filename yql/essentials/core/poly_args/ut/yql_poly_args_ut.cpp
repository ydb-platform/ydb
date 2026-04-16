#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

#include <yql/essentials/core/poly_args/yql_poly_args.h>

namespace NYql {

Y_UNIT_TEST_SUITE(PolyArgsParser) {

Y_UNIT_TEST(BadEmpty) {
    TString yson = "[]";
    UNIT_ASSERT_EXCEPTION(ParsePolyArgs(NYT::NodeFromYsonString(yson)), TConfigException);
}

Y_UNIT_TEST(BadNonPairs) {
    TString yson = R"([
        []
    ])";
    UNIT_ASSERT_EXCEPTION(ParsePolyArgs(NYT::NodeFromYsonString(yson)), TConfigException);
}

Y_UNIT_TEST(FinalPredicateMustBeEmptyList) {
    TString yson = R"([
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 1);
}

Y_UNIT_TEST(TypePredicate) {
    TString yson = R"([
        [{cmd=type;arg=T0;value=[DataType;Uint64]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 2);
}

Y_UNIT_TEST(KindPredicate) {
    TString yson = R"([
        [{cmd=kind;arg=T0;value=Data};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 2);
}

Y_UNIT_TEST(VerPredicate) {
    TString yson = R"([
        [{cmd=ver;value="2025.05"};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 2);
}

Y_UNIT_TEST(AndOverTypePredicate) {
    TString yson = R"([
        [[{cmd=type;arg=T0;value=[DataType;Uint64]};{cmd=type;arg=T1;value=[DataType;Uint32]}];{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 2);
}

Y_UNIT_TEST(OrOverTypePredicate) {
    TString yson = R"([
        [{cmd=or;value=[{cmd=type;arg=T0;value=[DataType;Uint64]};{cmd=type;arg=T1;value=[DataType;Uint32]}]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 2);
}

Y_UNIT_TEST(ActionVerAttribute) {
    TString yson = R"([
        [[];{ver="2025.05"}]
    ])";
    ParsePolyArgs(NYT::NodeFromYsonString(yson));
}

Y_UNIT_TEST(ActionArgsAttribute) {
    TString yson = R"([
        [[];{args=[[DataType;Yson];[DataType;Int64]]}]
    ])";
    ParsePolyArgs(NYT::NodeFromYsonString(yson));
}

Y_UNIT_TEST(ActionArgsType) {
    TString yson = R"([
        [[];{type=[CallableType;[];[[Universal]];[[DataType;Int64]]]}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT(!poly->GetUnresolvedInput(0).Defined());
}

} // Y_UNIT_TEST_SUITE(PolyArgsParser)

Y_UNIT_TEST_SUITE(PolyArgsUnresolved) {

Y_UNIT_TEST(UnresolvedEmpty) {
    TString yson = R"([
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 1);
    UNIT_ASSERT(poly->GetUnresolvedInput(0).Defined());
}

Y_UNIT_TEST(UnresolvedVer) {
    TString yson = R"([
        [[];{ver="2025.05"}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 1);
    auto unresolved = poly->GetUnresolvedInput(0);
    UNIT_ASSERT(unresolved.Defined());
    UNIT_ASSERT_VALUES_EQUAL(unresolved->LangVer, MakeLangVersion(2025, 5));
    UNIT_ASSERT(!unresolved->UserTypeArgs.Defined());
}

Y_UNIT_TEST(UnresolvedArgs) {
    TString yson = R"([
        [[];{args=[[DataType;Yson];[DataType;Int64]]}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->GetPredicatesCount(), 1);
    auto unresolved = poly->GetUnresolvedInput(0);
    UNIT_ASSERT(unresolved.Defined());
    UNIT_ASSERT(unresolved->UserTypeArgs.Defined());
    UNIT_ASSERT_VALUES_EQUAL(unresolved->UserTypeArgs->size(), 2);
}

} // Y_UNIT_TEST_SUITE(PolyArgsUnresolved)

Y_UNIT_TEST_SUITE(PolyArgsMatch) {

Y_UNIT_TEST(MatchEmpty) {
    TString yson = R"([
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->Match({}, MinLangVersion).Index, 0);
}

Y_UNIT_TEST(MatchVerOk) {
    TString yson = R"([
        [{cmd=ver;value="2025.05"};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->Match({}, MakeLangVersion(2025, 5)).Index, 0);
}

Y_UNIT_TEST(MatchVerFail) {
    TString yson = R"([
        [{cmd=ver;value="2025.05"};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    UNIT_ASSERT_VALUES_EQUAL(poly->Match({}, MakeLangVersion(2025, 4)).Index, 1);
}

Y_UNIT_TEST(MatchTypeOk) {
    TString yson = R"([
        [{cmd=type;arg=T0;value=[DataType;Yson]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;Yson]")},
    };
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 0);
}

Y_UNIT_TEST(MatchTypeFail) {
    TString yson = R"([
        [{cmd=type;arg=T0;value=[DataType;Yson]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;String]")},
    };
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 1);
}

Y_UNIT_TEST(MatchKindOk) {
    TString yson = R"([
        [{cmd=kind;arg=T0;value=Data};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;Yson]")},
    };
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 0);
}

Y_UNIT_TEST(MatchKindFail) {
    TString yson = R"([
        [{cmd=kind;arg=T0;value=Resource};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;Yson]")},
    };
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 1);
}

Y_UNIT_TEST(MatchAndOk) {
    TString yson = R"([
        [[{cmd=type;arg=T0;value=[DataType;Yson]};{cmd=type;arg=T1;value=[DataType;String]}];{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;Yson]")},
        {"T1", NYT::NodeFromYsonString("[DataType;String]")}};
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 0);
}

Y_UNIT_TEST(MatchAndFail1) {
    TString yson = R"([
        [[{cmd=type;arg=T0;value=[DataType;Yson]};{cmd=type;arg=T1;value=[DataType;String]}];{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;Yson]")},
        {"T2", NYT::NodeFromYsonString("[DataType;String]")}};
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 1);
}

Y_UNIT_TEST(MatchAndFail2) {
    TString yson = R"([
        [[{cmd=type;arg=T0;value=[DataType;Yson]};{cmd=type;arg=T1;value=[DataType;String]}];{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;String]")},
        {"T1", NYT::NodeFromYsonString("[DataType;String]")}};
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 1);
}

Y_UNIT_TEST(MatchOrOk1) {
    TString yson = R"([
        [{cmd=or;value=[{cmd=type;arg=T0;value=[DataType;Yson]};{cmd=type;arg=T1;value=[DataType;String]}]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;Yson]")},
        {"T2", NYT::NodeFromYsonString("[DataType;String]")}};
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 0);
}

Y_UNIT_TEST(MatchOrOk2) {
    TString yson = R"([
        [{cmd=or;value=[{cmd=type;arg=T0;value=[DataType;Yson]};{cmd=type;arg=T1;value=[DataType;String]}]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;String]")},
        {"T1", NYT::NodeFromYsonString("[DataType;String]")}};
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 0);
}

Y_UNIT_TEST(MatchOrFail) {
    TString yson = R"([
        [{cmd=or;value=[{cmd=type;arg=T0;value=[DataType;Yson]};{cmd=type;arg=T1;value=[DataType;String]}]};{}];
        [[];{}]
    ])";
    auto poly = ParsePolyArgs(NYT::NodeFromYsonString(yson));
    IPolyArgs::TArgs args = {
        {"T0", NYT::NodeFromYsonString("[DataType;String]")},
        {"T2", NYT::NodeFromYsonString("[DataType;String]")}};
    UNIT_ASSERT_VALUES_EQUAL(poly->Match(args, MinLangVersion).Index, 1);
}

} // Y_UNIT_TEST_SUITE(PolyArgsMatch)

} // namespace NYql
