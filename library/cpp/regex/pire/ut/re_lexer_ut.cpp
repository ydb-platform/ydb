#include "common.h"

#include <util/string/cast.h>

#include <limits>

namespace {
    void AssertRepetition(const TString& pattern, int lower, int upper) {
        // Inspect tokens without expanding potentially billions of FSM states.
        Pire::Lexer lexer(pattern);
        const auto term = lexer.Lex();
        UNIT_ASSERT(term.Value().IsA<Pire::Term::RepetitionCount>());
        const auto& count = term.Value().As<Pire::Term::RepetitionCount>();
        UNIT_ASSERT_VALUES_EQUAL(count.first, lower);
        UNIT_ASSERT_VALUES_EQUAL(count.second, upper);
        UNIT_ASSERT_VALUES_EQUAL(lexer.Lex().Type(), 0);
    }
}

Y_UNIT_TEST_SUITE(TRepetitionCount) {
    Y_UNIT_TEST(RepresentableBounds) {
        for (int bound : {0, 1, std::numeric_limits<int>::max() - 1, std::numeric_limits<int>::max()}) {
            const TString count = ToString(bound);
            AssertRepetition("{" + count + "}", bound, bound);
            AssertRepetition("{0," + count + "}", 0, bound);
            AssertRepetition("{" + count + "," + count + "}", bound, bound);
            AssertRepetition("{" + count + ",}", bound, Pire::Inf);
            AssertRepetition("{" + TString(1000, '0') + count + "}", bound, bound);
        }
        AssertRepetition("{2147483646,2147483647}", 2147483646, 2147483647);
        AssertRepetition("{0000,0001}", 0, 1);
    }

    Y_UNIT_TEST(Overflow) {
        const TString counts[] = {
            ToString(static_cast<ui64>(std::numeric_limits<int>::max()) + 1),
            "4294967296",
            "4294967297",
            "18446744073709551615",
            TString(10000, '9'),
        };
        for (const auto& count : counts) {
            for (const auto& pattern : {
                "{" + count + "}",
                "{" + count + ",}",
                "{" + count + ",0}",
                "{0," + count + "}",
                "{" + count + "," + count + "}",
                "{000" + count + "}",
            }) {
                UNIT_ASSERT_EXCEPTION_CONTAINS(
                    Pire::Lexer(pattern).Lex(), Pire::Error, "Repetition count exceeds INT_MAX");
            }
        }
    }

    Y_UNIT_TEST(ReversedBounds) {
        for (const auto* pattern : {"{20,10}", "{1,0}", "{2147483647,2147483646}"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                Pire::Lexer(pattern).Lex(), Pire::Error, "Repetition lower bound exceeds upper bound");
        }
    }

    Y_UNIT_TEST(MalformedCount) {
        for (const auto* pattern : {"{}", "{,1}", "{-1}", "{1,-2}", "{1", "{1,", "{1,2", "{1,2,3}", "{ 1}", R"({\1})"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                Pire::Lexer(pattern).Lex(), Pire::Error, "Wrong repetition count");
        }
    }

    Y_UNIT_TEST(ParserError) {
        // A count without an atom cannot expand an FSM if the lexer regresses.
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Pire::Lexer("{20,10}").Parse(), Pire::Error, "Repetition lower bound exceeds upper bound");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Pire::Lexer("{4294967297}").Parse(), Pire::Error, "Repetition count exceeds INT_MAX");
    }

    Y_UNIT_TEST(Matching) {
        const std::pair<const char*, const char*> cases[] = {
            {"a{2}", "aa"},
            {"a{2,2}", "aa"},
            {"a{2,3}", "aaa"},
            {"a{0002,0003}", "aa"},
            {"(a{2}){2}", "aaaa"},
            {R"(a\{4294967297\})", "a{4294967297}"},
            {R"([{}4294967297]{2})", "{}"},
            {R"([\]{}]{2})", "]{"},
            {R"(\x61{2})", "aa"},
            {R"(\x{0061}{2})", "aa"},
            {R"(\x{7b}4294967297})", "{4294967297}"},
            {R"(\\{2})", R"(\\)"},
            {R"(\\\{4294967297\})", R"(\{4294967297})"},
        };
        for (const auto& [pattern, accepted] : cases) {
            const TString anchored = TString("^(") + pattern + ")$";
            REGEXP(anchored.c_str()) {
                ACCEPTS(accepted);
                DENIES(TString(accepted) + "!");
            }
        }
        REGEXP2("^(a{0,1})$", "") {
            ACCEPTS("");
            ACCEPTS("a");
            DENIES("aa");
        }
        REGEXP2("^(a{0,})$", "") {
            ACCEPTS("");
            ACCEPTS("aaa");
            DENIES("b");
        }
        REGEXP2("^(a{2,})$", "") {
            ACCEPTS("aa");
            ACCEPTS("aaaa");
            DENIES("a");
        }
        REGEXP2("^(a{2})$", "i") {
            ACCEPTS("Aa");
            DENIES("a");
        }
        REGEXP2("^(a{1,3}&a{2,4})$", "a") {
            ACCEPTS("aa");
            DENIES("a");
        }
        REGEXP2("^(~(a{2}))$", "a") {
            ACCEPTS("a");
            DENIES("aa");
        }
    }
}
