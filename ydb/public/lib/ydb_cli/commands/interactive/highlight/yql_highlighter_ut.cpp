#include "yql_highlighter.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>
#include <util/system/compiler.h>
#include <util/charset/wide.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlHighlightTests) {
    auto Coloring = TColorSchema::Debug();

    std::unordered_map<char, TColor> colors = {
        {'k', Coloring.keyword},
        {'o', Coloring.operation},
        {'f', Coloring.identifier.function},
        {'t', Coloring.identifier.type},
        {'v', Coloring.identifier.variable},
        {'q', Coloring.identifier.quoted},
        {'s', Coloring.string},
        {'n', Coloring.number},
        {'u', Coloring.unknown},
        {'c', Coloring.comment},
        {' ', TColor::DEFAULT},
    };

    std::unordered_map<TColor, char> symbols = [] {
        std::unordered_map<TColor, char> symbols;
        for (const auto& [symbol, color] : colors) {
            symbols[color] = symbol;
        }
        return symbols;
    }();

    TVector<TColor> ColorsFromPattern(const TStringBuf& symbols) {
        TVector<TColor> result(symbols.size());
        for (std::size_t i = 0; i < symbols.size(); ++i) {
            result[i] = colors.at(symbols[i]);
        }
        return result;
    }

    TVector<TColor> Apply(IYQLHighlighter& highlight,
                          const TStringBuf& queryUtf8) {
        const auto queryUtf32 = UTF8ToUTF32</* robust = */ false>(queryUtf8);
        TVector<TColor> colors(queryUtf32.size(),
                               TColor::DEFAULT);
        highlight.Apply(queryUtf8, colors);
        return colors;
    }

    TString SymbolsFromColors(const TVector<TColor>& colors) {
        TString result(colors.size(), '-');
        for (std::size_t i = 0; i < colors.size(); ++i) {
            result[i] = symbols.at(colors[i]);
        }
        return result;
    }

    void Check(IYQLHighlighter::TPtr& highlight, const TStringBuf& query,
               const TStringBuf& pattern) {
        auto actual = Apply(*highlight, query);
        UNIT_ASSERT_EQUAL_C(Apply(*highlight, query), ColorsFromPattern(pattern),
                            SymbolsFromColors(actual));
    }

    Y_UNIT_TEST(Empty) {
        auto highlight = MakeYQLHighlighter(Coloring);
        Check(highlight, "", "");
    }

    Y_UNIT_TEST(Invalid) {
        auto highlight = MakeYQLHighlighter(Coloring);
        Check(highlight, "!", "u");
        Check(highlight, "й", "u");
        Check(highlight, "编", "u");
        Check(highlight, "\xF0\x9F\x98\x8A", "u");
        Check(highlight, "!select", "ukkkkkk");
        Check(highlight, "!sselect", "uvvvvvvv");
    }

    Y_UNIT_TEST(Emoji) {
        auto highlight = MakeYQLHighlighter(Coloring);
        Check(highlight, "☺", "u");
        Check(highlight, "\"☺\"", "sss");
        Check(highlight, "`☺`", "qqq");
        Check(highlight, "SELECT \"\xF0\x9F\x98\x8A\" FROM test", "kkkkkk sss kkkk vvvv");
        Check(highlight, "SELECT \"编码\" FROM test", "kkkkkk ssss kkkk vvvv");
        Check(highlight, "SELECT \"ай\" FROM test", "kkkkkk ssss kkkk vvvv");
        Check(highlight, "\xF0\x9F\x98\x8A\xF0\x9F\x98\x8A\xF0\x9F\x98\x8A\xF0\x9F\x98\x8A\xF0\x9F\x98\x8A\xF0\x9F\x98\x8A select", "uuuuuu kkkkkk");
    }

    Y_UNIT_TEST(Typing) {
        const TString query =
            "SELECT \n"
            "  123467, \"Hello, друг, {name} \xF0\x9F\x98\x8A!\", \n"
            "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
            "  Bool(field), Math::Sin(var) \n"
            "FROM `local/test/space/table` JOIN test;";

        auto highlight = MakeYQLHighlighter(Coloring);
        for (std::size_t size = 0; size <= query.size(); ++size) {
            const TStringBuf prefix(query, 0, size);

            auto colors = Apply(*highlight, prefix);
            Y_DO_NOT_OPTIMIZE_AWAY(colors);
        }
    }

} // Y_UNIT_TEST_SUITE(YqlHighlightTests)
