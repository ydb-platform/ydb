#include "yql_highlight.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/compiler.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlHighlightTests) {
    auto Coloring = YQLHighlight::ColorSchema::Monaco();

    YQLHighlight::Color ColorOf(char symbol) {
        switch (symbol) {
            case 'k':
                return Coloring.keyword;
            case 'o':
                return Coloring.operation;
            case 'f':
                return Coloring.identifier.function;
            case 'v':
                return Coloring.identifier.variable;
            case 'q':
                return Coloring.identifier.quoted;
            case 's':
                return Coloring.string;
            case 'n':
                return Coloring.number;
            case 'u':
                return Coloring.unknown;
            case ' ':
                return YQLHighlight::Color::DEFAULT;
            default:
                Y_UNREACHABLE();
        }
    }

    TVector<YQLHighlight::Color> Pattern(const TString& symbols) {
        TVector<YQLHighlight::Color> colors(symbols.Size());
        for (std::size_t i = 0; i < symbols.Size(); ++i) {
            colors[i] = ColorOf(symbols[i]);
        }
        return colors;
    }

    TVector<YQLHighlight::Color> Apply(YQLHighlight& highlight,
                                       const TString& query) {
        TVector<YQLHighlight::Color> colors(query.Size());
        highlight.Apply(query, colors);
        return colors;
    }

    void Check(YQLHighlight& highlight, const TString& query,
               const TString& pattern) {
        UNIT_ASSERT_EQUAL(Apply(highlight, query), Pattern(pattern));
    }

    Y_UNIT_TEST(Blank) {
        YQLHighlight highlight(Coloring);
        Check(highlight, "", "");
        Check(highlight, " ", " ");
        Check(highlight, "   ", "   ");
    }

    Y_UNIT_TEST(Keyword) {
        YQLHighlight highlight(Coloring);
        Check(
            highlight,
            "SELECT id, alias from users",
            "kkkkkk vvo vvvvv kkkk vvvvv");
        Check(
            highlight,
            "INSERT INTO users (id, alias) VALUES (12, \"tester\")",
            "kkkkkk kkkk vvvvv ovvo vvvvvo kkkkkk onno sssssssso");
    }
}
