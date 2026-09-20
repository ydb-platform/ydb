#include <ydb/public/lib/ydb_cli/common/markdown.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/node.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/screen.hpp>

#include <util/generic/string.h>

#include <string>

namespace NYdb::NConsoleClient {

namespace {

// Render a Markdown string the way the CLI would, then flatten the FTXUI screen back to plain text
// (one row per line, trailing padding trimmed) so tests can assert on the visible characters.
// Styling such as bold/color is a per-cell attribute, not a glyph, so it does not show up here.
TString RenderMarkdown(TStringBuf markdown, int width = 100) {
    ftxui::Element element = MarkdownToElement(markdown);
    auto screen = ftxui::Screen::Create(
        ftxui::Dimension::Fixed(width),
        ftxui::Dimension::Fit(element, /* extend_beyond_screen */ true));
    ftxui::Render(screen, element);

    std::string result;
    for (int y = 0; y < screen.dimy(); ++y) {
        std::string row;
        for (int x = 0; x < screen.dimx(); ++x) {
            // Cells the renderer never wrote (e.g. flexbox inter-word gaps) keep the default empty
            // grapheme; the terminal shows them as spaces, so treat them as spaces here too.
            const std::string& cell = screen.at(x, y);
            row += cell.empty() ? " " : cell;
        }
        const size_t end = row.find_last_not_of(' ');
        result += (end == std::string::npos) ? std::string() : row.substr(0, end + 1);
        result += '\n';
    }
    return TString(result);
}

void AssertContains(TStringBuf haystack, TStringBuf needle) {
    UNIT_ASSERT_C(haystack.Contains(needle),
        "expected to find \"" << needle << "\" in rendered output:\n" << haystack);
}

void AssertNotContains(TStringBuf haystack, TStringBuf needle) {
    UNIT_ASSERT_C(!haystack.Contains(needle),
        "did not expect \"" << needle << "\" in rendered output:\n" << haystack);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(MarkdownToElementTests) {
    Y_UNIT_TEST(PlainProsePassesThrough) {
        AssertContains(RenderMarkdown("Hello world"), "Hello world");
    }

    Y_UNIT_TEST(BoldMarkersAreStripped) {
        const TString out = RenderMarkdown("This is **bold** text");
        AssertContains(out, "This is bold text");
        AssertNotContains(out, "*");
    }

    Y_UNIT_TEST(UnbalancedBoldRendersPlain) {
        const TString out = RenderMarkdown("This ** is broken");
        AssertContains(out, "is broken");
        AssertNotContains(out, "**");
    }

    Y_UNIT_TEST(ItalicMarkersAreStripped) {
        const TString out = RenderMarkdown("This is *italic* and _also_ here");
        AssertContains(out, "This is italic and also here");
        AssertNotContains(out, "*");
        AssertNotContains(out, "_also_");
    }

    Y_UNIT_TEST(SelectStarIsPreserved) {
        AssertContains(RenderMarkdown("Run SELECT * FROM table"), "SELECT * FROM table");
    }

    Y_UNIT_TEST(SnakeCaseIsPreserved) {
        AssertContains(RenderMarkdown("Use the my_table_name id"), "my_table_name");
    }

    Y_UNIT_TEST(MultipleEmphasisSpans) {
        const TString out = RenderMarkdown("*a* and *b* and *c*");
        AssertContains(out, "a and b and c");
        AssertNotContains(out, "*");
    }

    Y_UNIT_TEST(SingleBackticksArePreserved) {
        // YQL quotes scheme objects with backticks; they must survive verbatim, not be recolored.
        AssertContains(RenderMarkdown("Query `/Root/series` table now"), "`/Root/series`");
    }

    Y_UNIT_TEST(HeadingTextWithoutHashes) {
        const TString out = RenderMarkdown("# Section title");
        AssertContains(out, "Section title");
        AssertNotContains(out, "#");
    }

    Y_UNIT_TEST(BlockquoteMarkerStripped) {
        const TString out = RenderMarkdown("> quoted line");
        AssertContains(out, "quoted line");
        AssertNotContains(out, ">");
    }

    Y_UNIT_TEST(TabIndentedHeadingStripsMarker) {
        // A tab-indented heading is still detected (TrimSpaces tolerates tabs), so its '#' marker
        // must be stripped too, not rendered raw.
        const TString out = RenderMarkdown("\t# Section title");
        AssertContains(out, "Section title");
        AssertNotContains(out, "#");
    }

    Y_UNIT_TEST(TabIndentedBlockquoteStripsMarker) {
        const TString out = RenderMarkdown("\t> quoted line");
        AssertContains(out, "quoted line");
        AssertNotContains(out, ">");
    }

    Y_UNIT_TEST(BlockquoteTabAfterMarkerStripped) {
        // '>' followed by a tab must be fully consumed, like '>' followed by a space.
        const TString out = RenderMarkdown(">\tquoted line");
        AssertContains(out, "quoted line");
        AssertNotContains(out, ">");
    }

    Y_UNIT_TEST(TabInProseActsAsSeparator) {
        // A tab between prose words must become a word separator (like a space), not a raw '\t' that
        // renders inconsistently and blocks flexbox reflow.
        const TString out = RenderMarkdown("foo\tbar");
        AssertContains(out, "foo bar");
        AssertNotContains(out, "\t");
    }

    Y_UNIT_TEST(HorizontalRuleIsRendered) {
        // A standalone "---" must render as a full-width horizontal line, not the raw "---" text.
        const TString out = RenderMarkdown(
            "Before\n"
            "\n"
            "---\n"
            "\n"
            "After");
        AssertContains(out, "Before");
        AssertContains(out, "After");
        AssertContains(out, "──────────"); // a horizontal rule is drawn
        AssertNotContains(out, "---");      // not the literal Markdown marker
    }

    Y_UNIT_TEST(TableIsRenderedWithBorders) {
        const TString out = RenderMarkdown(
            "| Name | Age |\n"
            "| --- | --- |\n"
            "| Bob | 30 |\n");
        AssertContains(out, "Name");
        AssertContains(out, "Age");
        AssertContains(out, "Bob");
        AssertContains(out, "30");
        AssertContains(out, "\u2502");   // a vertical table border is drawn
        AssertNotContains(out, "---");   // the separator row is dropped
    }

    Y_UNIT_TEST(FencedCodeBlockKeepsContentAndLanguage) {
        const TString out = RenderMarkdown(
            "```yql\n"
            "SELECT 1;\n"
            "```\n");
        AssertContains(out, "SELECT 1;");
        AssertContains(out, "yql");      // language tag shown in the top bar
        AssertNotContains(out, "```");
    }

    Y_UNIT_TEST(FencedCodeBlockPreservesIndentation) {
        const TString out = RenderMarkdown(
            "```\n"
            "def f():\n"
            "    return 1\n"
            "```\n");
        AssertContains(out, "    return 1"); // leading indentation kept verbatim
    }

    Y_UNIT_TEST(Utf8ProseIsPreserved) {
        const TString out = RenderMarkdown("Привет **мир** и пока");
        AssertContains(out, "Привет мир и пока");
        AssertNotContains(out, "*");
    }

    Y_UNIT_TEST(MixedDocumentRenders) {
        const TString out = RenderMarkdown(
            "# Title\n"
            "Some **intro** text.\n"
            "\n"
            "| K | V |\n"
            "| - | - |\n"
            "| a | b |\n"
            "\n"
            "```sql\n"
            "SELECT 2;\n"
            "```\n");
        AssertContains(out, "Title");
        AssertContains(out, "Some intro text.");
        AssertContains(out, "SELECT 2;");
        AssertContains(out, "\u2502"); // table border present
    }

    Y_UNIT_TEST(EmptyInputDoesNotCrash) {
        Y_UNUSED(RenderMarkdown("")); // must simply not crash
    }

    Y_UNIT_TEST(LongLineDoesNotHang) {
        TString line;
        for (int i = 0; i < 400; ++i) {
            line += "word ";
        }
        AssertContains(RenderMarkdown(line, 60), "word");
    }

    Y_UNIT_TEST(LongUnclosedEmphasisDoesNotHang) {
        // Stresses RemovePairedEmphasis with many unclosed openers (must stay linear, not O(n^2)).
        TString line;
        for (int i = 0; i < 400; ++i) {
            line += "see *foo ";
        }
        AssertContains(RenderMarkdown(line, 60), "foo");
    }
}

} // namespace NYdb::NConsoleClient
