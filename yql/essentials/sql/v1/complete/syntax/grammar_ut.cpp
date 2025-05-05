#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(RuleTests) {
    THolder<antlr4::Parser> GetDummyParser(bool ansi) {
        if (ansi) {
            return MakeHolder<NALAAnsiAntlr4::SQLv1Antlr4Parser>(nullptr);
        }
        return MakeHolder<NALADefaultAntlr4::SQLv1Antlr4Parser>(nullptr);
    }

    Y_UNIT_TEST(RuleIndexModeIndependent) {
        auto defaultRules = GetDummyParser(/* ansi = */ false)->getRuleIndexMap();
        auto ansiRules = GetDummyParser(/* ansi = */ true)->getRuleIndexMap();

        UNIT_ASSERT_EQUAL(defaultRules, ansiRules);
    }

    Y_UNIT_TEST(TokenTypeModeIndependent) {
        auto defaultVocab = GetDummyParser(/* ansi = */ false)->getVocabulary();
        auto ansiVocab = GetDummyParser(/* ansi = */ true)->getVocabulary();

        UNIT_ASSERT_VALUES_EQUAL(defaultVocab.getMaxTokenType(), ansiVocab.getMaxTokenType());

        for (size_t type = 0; type <= defaultVocab.getMaxTokenType(); ++type) {
            UNIT_ASSERT_VALUES_EQUAL(
                defaultVocab.getSymbolicName(type), ansiVocab.getSymbolicName(type));
            UNIT_ASSERT_VALUES_EQUAL(
                defaultVocab.getDisplayName(type), ansiVocab.getDisplayName(type));
            UNIT_ASSERT_VALUES_EQUAL(
                defaultVocab.getLiteralName(type), ansiVocab.getLiteralName(type));
        }
    }

} // Y_UNIT_TEST_SUITE(RuleTests)
