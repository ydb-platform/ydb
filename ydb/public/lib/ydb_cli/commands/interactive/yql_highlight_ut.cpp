#include "yql_highlight.h"

#include <util/system/compiler.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YqlHighlightTests) {
    auto Coloring = YQLHighlight::ColorSchema::Monaco();

    YQLHighlight::Color ColorOf(char symbol) {
        switch (symbol) {
            case 'k': return Coloring.keyword;
            case 'o': return Coloring.operation;
            case 'f': return Coloring.identifier.function;
            case 'v': return Coloring.identifier.variable;
            case 'q': return Coloring.identifier.quoted;
            case 's': return Coloring.string;
            case 'n': return Coloring.number;
            case 'u': return Coloring.unknown;
            case ' ': return YQLHighlight::Color::DEFAULT;
            default: Y_UNREACHABLE();
        }
    }

    TVector<YQLHighlight::Color> Pattern(const TString& symbols) {
        TVector<YQLHighlight::Color> colors(symbols.Size());
        for (std::size_t i = 0; i < symbols.Size(); ++i) {
            colors[i] = ColorOf(symbols[i]);
        }
        return colors;
    }

    TVector<YQLHighlight::Color> Apply(YQLHighlight& highlight, const TString& query) {
        TVector<YQLHighlight::Color> colors(query.Size());
        highlight.Apply(query, colors);
        return colors;
    }

    Y_UNIT_TEST(Blank) {
        YQLHighlight highlight(Coloring);
        UNIT_ASSERT_EQUAL(Apply(highlight, ""), Pattern(""));
        UNIT_ASSERT_EQUAL(Apply(highlight, " "), Pattern(" "));
        UNIT_ASSERT_EQUAL(Apply(highlight, "   "), Pattern("   "));
    }
}
