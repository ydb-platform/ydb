#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(MaterializeStatement) {

Y_UNIT_TEST(TopLevelBasic) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(TopLevelUnused) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 0, prog);
}

Y_UNIT_TEST(TopLevelWithCluster) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            MATERIALIZE plato.Input INTO $result ON plato;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(WithInnerSelectFromTable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE (SELECT * FROM Input) INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(WithInnerSelectWithoutSource) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE (SELECT 1 as a) INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(WithInnerNamedExpr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $a = SELECT 1 as a;
            MATERIALIZE $a INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 1, prog);
}

Y_UNIT_TEST(CompileAsSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;
    settings.Mode = NSQLTranslation::ESqlMode::SUBQUERY;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CompileAsLimitedView) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;
    settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(UseInSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            DEFINE SUBQUERY $sub() AS
                MATERIALIZE (SELECT 1 as a) INTO $tmp;
                MATERIALIZE $tmp INTO $result;
                SELECT * FROM $result;
            END DEFINE;
            SELECT * FROM $sub();
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 2, prog);
}

Y_UNIT_TEST(UseInAction) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            DEFINE ACTION $sub() AS
                MATERIALIZE (SELECT 1 as a) INTO $tmp;
                MATERIALIZE $tmp INTO $result;
                SELECT * FROM $result;
            END DEFINE;
            DO $sub();
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Materialize!"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Materialize!"], 2, prog);
}

Y_UNIT_TEST(MissingCluster) {
    // No USE and no ON clause - should fail
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            MATERIALIZE plato.Input INTO $result;
        )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "USE statement is missing or cluster not specified in MATERIALIZE");
}

Y_UNIT_TEST(NotAllowedBeforeVer) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MinLangVersion;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "MATERIALIZE is not available before language version");
}

Y_UNIT_TEST(PreserveSortInRefSelect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $a = SELECT * FROM Input ORDER BY a;
            MATERIALIZE $a INTO $b;
            MATERIALIZE $b INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortInSubSelect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE (SELECT * FROM Input ORDER BY a) INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortInSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            DEFINE SUBQUERY $sub() AS
                SELECT * FROM Input ORDER BY key;
            END DEFINE;
            MATERIALIZE $sub() INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortForTopLevelOnly1) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $a = SELECT * FROM Input ORDER BY key;
            $b = SELECT * FROM $a ORDER BY key;
            MATERIALIZE $b INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "ORDER BY without LIMIT in subquery will be ignored");

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 1, prog);
}

Y_UNIT_TEST(PreserveSortForTopLevelOnly2) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $a = SELECT * FROM Input ORDER BY key;
            $b = SELECT * FROM $a;
            MATERIALIZE $b INTO $result;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "ORDER BY without LIMIT in subquery will be ignored");

    TWordCountHive elementStat = {{"Sort"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["Sort"], 0, prog);
}

Y_UNIT_TEST(SingleHint) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result WITH hint1;
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"hint1"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["hint1"], 1, prog);
}

Y_UNIT_TEST(MultiHints) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = ::NYql::NFeature::Materialize.MinLangVer;

    auto res = SqlToYqlWithSettings(R"sql(
            USE plato;
            MATERIALIZE Input INTO $result WITH (hint1, hint2);
            SELECT * FROM $result;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"hint1"}, {"hint2"}};
    auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["hint1"], 1, prog);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["hint2"], 1, prog);
}

} // Y_UNIT_TEST_SUITE(MaterializeStatement)
