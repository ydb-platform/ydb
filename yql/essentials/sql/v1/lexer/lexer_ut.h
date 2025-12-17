#pragma once

#include "lexer.h"

#define LEXER_NAME_ANSI_false_ANTLR4_true_FLAVOR_Default "antlr4"
#define LEXER_NAME_ANSI_true_ANTLR4_true_FLAVOR_Default "antlr4_ansi"
#define LEXER_NAME_ANSI_false_ANTLR4_true_FLAVOR_Pure "antlr4_pure"
#define LEXER_NAME_ANSI_true_ANTLR4_true_FLAVOR_Pure "antlr4_pure_ansi"
#define LEXER_NAME_ANSI_false_ANTLR4_false_FLAVOR_Regex "regex"
#define LEXER_NAME_ANSI_true_ANTLR4_false_FLAVOR_Regex "regex_ansi"

#define Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, ANSI, ANTLR4, FLAVOR)                              \
    TCurrentTest::AddTest(                                                                       \
        #N "::" LEXER_NAME_ANSI_##ANSI##_ANTLR4_##ANTLR4##_FLAVOR_##FLAVOR,                      \
        static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ANSI, ANTLR4, ELexerFlavor::FLAVOR>), \
        /* forceFork = */ false)

#define Y_UNIT_TEST_ON_EACH_LEXER(N)                                     \
    template <bool ANSI, bool ANTLR4, ELexerFlavor FLAVOR>               \
    void N(NUnitTest::TTestContext&);                                    \
    struct TTestRegistration##N {                                        \
        TTestRegistration##N() {                                         \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, true, Default); \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, true, Default);  \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, true, Pure);    \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, true, Pure);     \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, false, Regex);  \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, false, Regex);   \
        }                                                                \
    };                                                                   \
    static TTestRegistration##N testRegistration##N;                     \
    template <bool ANSI, bool ANTLR4, ELexerFlavor FLAVOR>               \
    void N(NUnitTest::TTestContext&)
