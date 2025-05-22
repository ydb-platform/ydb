#include "regex.h"

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/re2/re2/re2.h>

using namespace NSQLTranslationV1;

namespace {
    auto grammar = NSQLReflect::LoadLexerGrammar();
    auto defaultRegexes = MakeRegexByOtherName(grammar, /* ansi = */ false);
    auto ansiRegexes = MakeRegexByOtherName(grammar, /* ansi = */ true);

    TString Get(const TVector<std::tuple<TString, TString>>& regexes, const TStringBuf name) {
        return std::get<1>(*FindIf(regexes, [&](const auto& pair) {
            return std::get<0>(pair) == name;
        }));
    }

    void CheckRegex(bool ansi, const TStringBuf name, const TStringBuf expected) {
        const auto& regexes = ansi ? ansiRegexes : defaultRegexes;
        const TString regex = Get(regexes, name);

        const RE2 re2(regex);
        Y_ENSURE(re2.ok(), re2.error());

        UNIT_ASSERT_VALUES_EQUAL(regex, expected);
    }

} // namespace

Y_UNIT_TEST_SUITE(SqlRegexTests) {
    Y_UNIT_TEST(StringValue) {
        CheckRegex(
            /* ansi = */ false,
            "STRING_VALUE",
            R"(((((\'([^'\\]|(\\(.|\n)))*\'))|((\"([^"\\]|(\\(.|\n)))*\"))|((\@\@(.|\n)*?\@\@)+\@?))([sS]|[uU]|[yY]|[jJ]|[pP]([tT]|[bB]|[vV])?)?))");
    }

    Y_UNIT_TEST(AnsiStringValue) {
        CheckRegex(
            /* ansi = */ true,
            "STRING_VALUE",
            R"(((((\'([^']|(\'\'))*\'))|((\"([^"]|(\"\"))*\"))|((\@\@(.|\n)*?\@\@)+\@?))([sS]|[uU]|[yY]|[jJ]|[pP]([tT]|[bB]|[vV])?)?))");
    }

    Y_UNIT_TEST(IdPlain) {
        CheckRegex(
            /* ansi = */ false,
            "ID_PLAIN",
            R"(([a-z]|[A-Z]|_)([a-z]|[A-Z]|_|[0-9])*)");
    }

    Y_UNIT_TEST(IdQuoted) {
        CheckRegex(
            /* ansi = */ false,
            "ID_QUOTED",
            R"(\`(\\(.|\n)|\`\`|[^`\\])*\`)");
    }

    Y_UNIT_TEST(Digits) {
        CheckRegex(
            /* ansi = */ false,
            "DIGITS",
            R"((0[xX]([0-9]|[a-f]|[A-F])+)|(0[oO][0-8]+)|(0[bB](0|1)+)|([0-9]+))");
    }

    Y_UNIT_TEST(IntegerValue) {
        CheckRegex(
            /* ansi = */ false,
            "INTEGER_VALUE",
            R"(((0[xX]([0-9]|[a-f]|[A-F])+)|(0[oO][0-8]+)|(0[bB](0|1)+)|([0-9]+))(([pP]|[uU])?([lL]|[sS]|[tT]|[iI]|[bB]|[nN])?))");
    }

    Y_UNIT_TEST(Real) {
        CheckRegex(
            /* ansi = */ false,
            "REAL",
            R"((([0-9]+)\.[0-9]*([eE](\+|\-)?([0-9]+))?|([0-9]+)([eE](\+|\-)?([0-9]+)))([fF]|[pP]([fF](4|8)|[nN])?)?)");
    }

    Y_UNIT_TEST(Ws) {
        CheckRegex(
            /* ansi = */ false,
            "WS",
            R"(( |\r|\t|\f|\n))");
    }

    Y_UNIT_TEST(Comment) {
        CheckRegex(
            /* ansi = */ false,
            "COMMENT",
            R"(((\/\*(.|\n)*?\*\/)|(\-\-[^\n\r]*(\r\n?|\n|$))))");
    }

    Y_UNIT_TEST(AnsiCommentSameAsDefault) {
        // Because of recursive definition
        UNIT_ASSERT_VALUES_EQUAL(
            Get(ansiRegexes, "COMMENT"),
            Get(defaultRegexes, "COMMENT"));
    }

} // Y_UNIT_TEST_SUITE(SqlRegexTests)
