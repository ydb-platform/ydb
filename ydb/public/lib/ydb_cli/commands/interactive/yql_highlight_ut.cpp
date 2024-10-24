#include "yql_highlight.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/split.h>
#include <util/system/compiler.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlHighlightTests) {
    auto Coloring = YQLHighlight::ColorSchema::Debug();

    std::unordered_map<char, YQLHighlight::Color> colors = {
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
        {' ', YQLHighlight::Color::DEFAULT},
    };

    std::unordered_map<YQLHighlight::Color, char> symbols = [] {
        std::unordered_map<YQLHighlight::Color, char> symbols;
        for (const auto& [symbol, color] : colors) {
            symbols[color] = symbol;
        }
        return symbols;
    }();

    TVector<YQLHighlight::Color> ColorsFromPattern(const TStringBuf& symbols) {
        TVector<YQLHighlight::Color> result(symbols.size());
        for (std::size_t i = 0; i < symbols.size(); ++i) {
            result[i] = colors.at(symbols[i]);
        }
        return result;
    }

    TVector<YQLHighlight::Color> Apply(YQLHighlight& highlight,
                                       const TStringBuf& query) {
        TVector<YQLHighlight::Color> colors(query.size(),
                                            YQLHighlight::Color::DEFAULT);
        highlight.Apply(query, colors);
        return colors;
    }

    TString SymbolsFromColors(const TVector<YQLHighlight::Color>& colors) {
        TString result(colors.size(), '-');
        for (std::size_t i = 0; i < colors.size(); ++i) {
            result[i] = symbols.at(colors[i]);
        }
        return result;
    }

    void Check(YQLHighlight& highlight, const TStringBuf& query,
               const TStringBuf& pattern) {
        auto actual = Apply(highlight, query);
        UNIT_ASSERT_EQUAL_C(Apply(highlight, query), ColorsFromPattern(pattern),
                            SymbolsFromColors(actual));
    }

    Y_UNIT_TEST(Blank) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "", "");
        Check(highlight, " ", " ");
        Check(highlight, "   ", "   ");
    }

    Y_UNIT_TEST(Keyword) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "SELECT", "kkkkkk");
        Check(highlight, "select", "kkkkkk");
        Check(highlight, "ALTER", "kkkkk");
        Check(highlight, "GROUP BY", "kkkkk kk");
        Check(highlight, "INSERT", "kkkkkk");
    }

    Y_UNIT_TEST(Operation) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "(1 + 21 / 4)", "on o nn o no");
        Check(highlight, "(1+21/4)", "ononnono");
    }

    Y_UNIT_TEST(FunctionIdentifier) {
        YQLHighlight highlight(Coloring);

        Check(highlight, "MIN", "fff");
        Check(highlight, "min", "fff");
        Check(highlight, "MIN(123, 65)", "fffonnno nno");
        Check(highlight, "MIN", "fff");

        Check(highlight, "minimum", "vvvvvvv");
        Check(highlight, "MINimum", "vvvvvvv");

        Check(highlight, "Math::Sin", "ffffoofff");
        Check(highlight, "Math", "vvvv");
        Check(highlight, "Math::", "ffffoo");
        Check(highlight, "::Sin", "oovvv");
    }

    Y_UNIT_TEST(TypeIdentifier) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "Bool", "tttt");
        Check(highlight, "Bool(value)", "ttttovvvvvo");
    }

    Y_UNIT_TEST(VariableIdentifier) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "test", "vvvv");
    }

    Y_UNIT_TEST(QuotedIdentifier) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "`/cluster/database`", "qqqqqqqqqqqqqqqqqqq");
    }

    Y_UNIT_TEST(String) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "\"\"", "ss");
        Check(highlight, "\"test\"", "ssssss");
        Check(highlight, "\"", "o");
        Check(highlight, "\"\"\"", "sso");
        Check(highlight, "\"\\\"", "ooo");
        Check(highlight, "\"\\\"\"", "ssss");
    }

    Y_UNIT_TEST(Number) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "1234", "nnnn");
        Check(highlight, "-123", "onnn");
    }

    Y_UNIT_TEST(SQL) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "SELECT id, alias from users",
              "kkkkkk vvo vvvvv kkkk vvvvv");
        Check(highlight, "INSERT INTO users (id, alias) VALUES (12, \"tester\")",
              "kkkkkk kkkk vvvvv ovvo vvvvvo kkkkkk onno sssssssso");
        Check(
            highlight,
            "SELECT 123467, \"Hello, {name}!\", (1 + (5 * 1 / 0)), MIN(identifier)"
            "FROM `local/test/space/table` JOIN test;",
            "kkkkkk nnnnnno sssssssssssssssso on o on o n o nooo fffovvvvvvvvvvo"
            "kkkk qqqqqqqqqqqqqqqqqqqqqqqq kkkk vvvvo");
        Check(highlight, "SELECT Bool(phone) FROM customer",
              "kkkkkk ttttovvvvvo kkkk vvvvvvvv");
    }

    Y_UNIT_TEST(Emoji) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "☺", "uuu");
        Check(highlight, "\"☺\"", "sssuu");
        Check(highlight, "`☺`", "qqquu");
    }

    Y_UNIT_TEST(Typing) {
        const TString query =
            "SELECT "
            "  123467, \"Hello, {name}!\", "
            "  (1 + (5 * 1 / 0)), MIN(identifier), "
            "  Bool(field), Math::Sin(var) "
            "FROM `local/test/space/table` JOIN test;";

        const TString pattern =
            "kkkkkk "
            "  nnnnnno sssssssssssssssso "
            "  on o on o n o nooo fffovvvvvvvvvvoo "
            "  ttttovvvvvoo ffffoofffovvvo "
            "kkkk qqqqqqqqqqqqqqqqqqqqqqqq kkkk vvvvo";

        YQLHighlight highlight(Coloring);
        for (std::size_t size = 0; size <= query.size(); ++size) {
            const TStringBuf prefix(query, 0, size);

            auto colors = Apply(highlight, prefix);
            Y_DO_NOT_OPTIMIZE_AWAY(colors);

            if (size == query.size() || IsSpace(pattern[size])) {
                const TStringBuf pattern_prefix(pattern, 0, size);
                Check(highlight, prefix, pattern_prefix);
            }
        }
    }

    Y_UNIT_TEST(Comment) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "- select", "o kkkkkk");
        Check(highlight, "select -- select", "kkkkkk ccccccccc");
        Check(highlight, "-- select\nselect", "cccccccccckkkkkk");
        Check(highlight, "/* select */", "cccccccccccc");
        Check(highlight, "select /* select */ select", "kkkkkk cccccccccccc kkkkkk");
        Check(highlight, "/**/ --", "cccc cc");
        Check(highlight, "/*/**/*/", "ccccccoo");
    }
}
