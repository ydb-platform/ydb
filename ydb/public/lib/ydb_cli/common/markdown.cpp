#include "markdown.h"

#include <contrib/libs/ftxui/include/ftxui/dom/flexbox_config.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/table.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>

#include <util/charset/utf8.h>
#include <util/generic/string.h>
#include <util/string/split.h>
#include <util/string/subst.h>

#include <algorithm>
#include <string>
#include <vector>

namespace NYdb::NConsoleClient {

namespace {

// Markdown treats spaces and tabs alike as horizontal whitespace; one predicate keeps marker
// detection (TrimSpaces/IsHeading) and marker stripping consistent.
bool IsSpaceOrTab(char c) {
    return c == ' ' || c == '\t';
}

TStringBuf TrimSpaces(TStringBuf s) {
    while (!s.empty() && IsSpaceOrTab(s.front())) {
        s.Skip(1);
    }
    while (!s.empty() && IsSpaceOrTab(s.back())) {
        s.Chop(1);
    }
    return s;
}

// ASCII alphanumerics, '_' and any UTF-8 lead/continuation byte (>= 0x80, e.g. Cyrillic) count
// as "word" bytes, so emphasis markers glued to a word are not mistaken for formatting.
bool IsWordByte(unsigned char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '_' || c >= 0x80;
}

// A line made only of '|', '-', ':' and spaces (with at least one '-') is a Markdown table
// separator row or a horizontal rule (the table separator is dropped inside a table because its
// border already conveys it; a standalone such line is rendered as a horizontal rule).
bool IsTableSeparatorLine(TStringBuf trimmed) {
    if (trimmed.empty()) {
        return false;
    }
    bool hasDash = false;
    for (const char c : trimmed) {
        if (c == '-') {
            hasDash = true;
        } else if (c != '|' && c != ':' && c != ' ') {
            return false;
        }
    }
    return hasDash;
}

// A line of 1-6 '#' followed by a space is a Markdown heading.
bool IsHeading(TStringBuf trimmed) {
    size_t hashes = 0;
    while (hashes < trimmed.size() && trimmed[hashes] == '#') {
        ++hashes;
    }
    return 1 <= hashes && hashes <= 6 && hashes < trimmed.size() && trimmed[hashes] == ' ';
}

// Remove paired single-char emphasis markers ('*' or '_') while keeping markers that are part of
// the text: "SELECT *" (marker followed by a space) or "snake_case" (marker glued to a word on
// both sides). Only a marker that opens right before non-space text and closes right after
// non-space text, on a word boundary, is treated as emphasis and dropped.
void RemovePairedEmphasis(TString& s, char marker) {
    const size_t n = s.size();

    // For every position, precompute the nearest valid closing marker at or after it. A marker can
    // close a span when it sticks to text on its left and to a boundary on its right. Computing this
    // once keeps the whole pass linear instead of re-scanning for a closer per opener (was O(n^2)).
    std::vector<size_t> nextCloser(n + 1, TString::npos);
    for (size_t j = n; j-- > 0;) {
        nextCloser[j] = nextCloser[j + 1];
        if (s[j] == marker) {
            const char cprev = (j > 0) ? s[j - 1] : '\0';
            const char cnext = (j + 1 < n) ? s[j + 1] : '\0';
            if (cprev != ' ' && cprev != '\t' && cprev != marker && !IsWordByte(cnext) && cnext != marker) {
                nextCloser[j] = j;
            }
        }
    }

    TString out;
    out.reserve(n);
    for (size_t i = 0; i < n;) {
        if (s[i] == marker) {
            const char prevOut = out.empty() ? '\0' : out.back();
            const char next = (i + 1 < n) ? s[i + 1] : '\0';
            const bool canOpen = !IsWordByte(prevOut) && prevOut != marker
                && next != '\0' && next != ' ' && next != '\t' && next != marker;
            if (canOpen) {
                const size_t close = nextCloser[i + 1];
                if (close != TString::npos) {
                    out.append(s.data() + i + 1, close - (i + 1)); // inner text, markers dropped
                    i = close + 1;
                    continue;
                }
            }
        }
        out.push_back(s[i++]);
    }
    s = std::move(out);
}

// Strip leading block markers (blockquotes, headings) and inline italic markers, keeping '**' bold
// markers for BuildStyledLine. Backticks are left untouched on purpose: the model often writes YQL
// inline, where backticks quote identifiers, so they must survive verbatim. Used for prose.
TString StripInlineMarkdownKeepStyles(TStringBuf line) {
    size_t start = 0;
    while (start < line.size() && IsSpaceOrTab(line[start])) {
        ++start;
    }

    bool strippedBlock = false;
    // Blockquote markers: '>' optionally followed by a space or tab, possibly repeated for nesting.
    while (start < line.size() && line[start] == '>') {
        ++start;
        if (start < line.size() && IsSpaceOrTab(line[start])) {
            ++start;
        }
        strippedBlock = true;
    }
    // Heading markers: 1-6 '#' followed by a space.
    size_t hashes = 0;
    while (start + hashes < line.size() && line[start + hashes] == '#') {
        ++hashes;
    }
    if (1 <= hashes && hashes <= 6 && start + hashes < line.size() && line[start + hashes] == ' ') {
        start += hashes + 1;
        strippedBlock = true;
    }

    // Keep the original leading whitespace unless a block marker was stripped.
    TString result(strippedBlock ? line.substr(start) : line);
    RemovePairedEmphasis(result, '*'); // italic; keeps "SELECT *" and '**' bold markers intact
    RemovePairedEmphasis(result, '_'); // italic; keeps snake_case identifiers intact
    return result;
}

// Same as above, but also drops '**' bold markers. Used where inline styling is impossible, such as
// table cells (which become plain strings). Backticks are kept verbatim (see above).
TString StripInlineMarkdown(TStringBuf line) {
    TString result = StripInlineMarkdownKeepStyles(line);
    SubstGlobal(result, "**", "");
    return result;
}

// Split a Markdown table row "| a | b |" into trimmed, Markdown-stripped cells.
std::vector<std::string> SplitTableRow(TStringBuf trimmed) {
    if (trimmed.StartsWith('|')) {
        trimmed.Skip(1);
    }
    if (trimmed.EndsWith('|')) {
        trimmed.Chop(1);
    }

    std::vector<std::string> cells;
    for (const TStringBuf cell : StringSplitter(trimmed).Split('|')) {
        cells.emplace_back(StripInlineMarkdown(TrimSpaces(cell)));
    }
    return cells;
}

// Render collected Markdown table rows (the first row is the header) as a bordered FTXUI table.
ftxui::Element BuildTable(std::vector<std::vector<std::string>> rows) {
    ftxui::Table table(std::move(rows));
    table.SelectAll().Border(ftxui::LIGHT);
    table.SelectAll().SeparatorVertical(ftxui::LIGHT);
    table.SelectAll().SeparatorHorizontal(ftxui::LIGHT);
    table.SelectRow(0).Decorate(ftxui::bold);
    return table.Render();
}

// A code-block bar: a full-width horizontal rule. With a language tag it shows it as
// "── yql ──────", mirroring the "── Agent response ──" header style; without one it is a plain rule.
ftxui::Element BuildCodeBlockBar(TStringBuf language) {
    if (language.empty()) {
        return ftxui::separator() | ftxui::color(ftxui::Color::Grey42);
    }

    // Display width of the tag in terminal cells. Fall back to the byte count if the model emitted
    // a broken (non-UTF-8) info string, so a stray byte can never spin the fill loop forever.
    size_t languageWidth = 0;
    if (!GetNumberOfUTF8Chars(language.data(), language.size(), languageWidth)) {
        languageWidth = language.size();
    }

    const int screenWidth = ftxui::Terminal::Size().dimx;
    const int fillWidth = std::max(0, screenWidth - static_cast<int>(languageWidth) - 4); // 4 = "──" + a space each side
    std::string fill;
    fill.reserve(fillWidth * 3); // "─" is 3 UTF-8 bytes
    for (int i = 0; i < fillWidth; ++i) {
        fill += "─";
    }

    return ftxui::hbox({
        ftxui::text("──") | ftxui::color(ftxui::Color::Grey42),
        ftxui::text(" " + std::string(language) + " ") | ftxui::bold | ftxui::color(ftxui::Color::Grey70),
        ftxui::text(fill) | ftxui::color(ftxui::Color::Grey42),
    });
}

// Render a fenced code block as its own panel: horizontal bars above and below (in their own color,
// the top one carrying the language tag) over a separate, slightly lighter background. Both
// background and foreground are pinned so the panel stays readable on any terminal theme. Lines are
// kept verbatim (indentation preserved, no reflow), unlike prose which paragraph() reflows.
ftxui::Element BuildCodeBlock(const std::vector<TStringBuf>& codeLines, TStringBuf language) {
    std::vector<ftxui::Element> content;
    content.reserve(codeLines.size());
    for (const TStringBuf line : codeLines) {
        content.push_back(ftxui::text(std::string(line)));
    }

    ftxui::Element body = ftxui::vbox(std::move(content))
        | ftxui::xflex                         // fill the whole width so the background spans it
        | ftxui::bgcolor(ftxui::Color::Grey19) // panel background, a touch lighter than Grey11
        | ftxui::color(ftxui::Color::Grey85);  // pinned text color, readable on the dark panel

    return ftxui::vbox({
        BuildCodeBlockBar(language),
        std::move(body),
        ftxui::separator() | ftxui::color(ftxui::Color::Grey42),
    });
}

// Render a single prose line as a flexbox of whitespace-separated tokens (mirroring how paragraph()
// reflows words), turning "**...**" spans into bold. Single backticks are passed through verbatim:
// the model uses them to quote YQL identifiers, so they must not be stripped or recolored. Inside a
// token the style can change between pieces without inserting a space; tokens are separated by a gap.
ftxui::Element BuildStyledLine(TStringBuf line) {
    // Unbalanced "**" would make the rest of the line bold; if so, drop the markers and render plain.
    int markers = 0;
    for (size_t i = 0; i + 1 < line.size();) {
        if (line[i] == '*' && line[i + 1] == '*') {
            ++markers;
            i += 2;
        } else {
            ++i;
        }
    }
    TString plain;
    if (markers % 2 != 0) {
        plain = line;
        SubstGlobal(plain, "**", "");
        line = plain;
    }

    std::vector<ftxui::Element> tokens;
    std::vector<ftxui::Element> pieces;
    std::string piece;
    bool bold = false;

    auto flushPiece = [&]() {
        if (!piece.empty()) {
            pieces.push_back(bold ? (ftxui::text(piece) | ftxui::bold) : ftxui::text(piece));
            piece.clear();
        }
    };
    auto flushToken = [&]() {
        flushPiece();
        if (pieces.empty()) {
            tokens.push_back(ftxui::text("")); // keep empty tokens so repeated whitespace is preserved
        } else if (pieces.size() == 1) {
            tokens.push_back(std::move(pieces.front()));
        } else {
            tokens.push_back(ftxui::hbox(std::move(pieces)));
        }
        pieces.clear();
    };

    for (size_t i = 0; i < line.size();) {
        if (line[i] == '*' && i + 1 < line.size() && line[i + 1] == '*') {
            flushPiece();
            bold = !bold;
            i += 2;
        } else if (IsSpaceOrTab(line[i])) {
            flushToken();
            ++i;
        } else {
            piece += line[i];
            ++i;
        }
    }
    flushToken();

    static const auto config = ftxui::FlexboxConfig().SetGap(1, 0);
    return ftxui::flexbox(std::move(tokens), config);
}

} // anonymous namespace

ftxui::Element MarkdownToElement(TStringBuf text) {
    std::vector<TStringBuf> lines;
    for (const TStringBuf line : StringSplitter(text).Split('\n')) {
        lines.push_back(line);
    }

    std::vector<ftxui::Element> blocks;
    std::vector<TString> proseLines;

    auto flushProse = [&]() {
        while (!proseLines.empty() && TrimSpaces(proseLines.back()).empty()) {
            proseLines.pop_back(); // drop trailing blank lines so blocks don't get extra padding
        }
        if (proseLines.empty()) {
            return;
        }
        std::vector<ftxui::Element> styled;
        styled.reserve(proseLines.size());
        for (const TString& proseLine : proseLines) {
            styled.push_back(BuildStyledLine(proseLine));
        }
        blocks.push_back(ftxui::vbox(std::move(styled)));
        proseLines.clear();
    };

    for (size_t i = 0; i < lines.size();) {
        const TStringBuf trimmed = TrimSpaces(lines[i]);

        // Fenced code block: collect everything up to the closing fence and render it as a panel.
        if (trimmed.StartsWith("```")) {
            flushProse();

            // The opening fence may carry an info string ("```yql"); its first token is the language.
            TStringBuf language = trimmed;
            while (language.StartsWith('`')) {
                language.Skip(1);
            }
            language = TrimSpaces(language);
            if (const size_t space = language.find(' '); space != TStringBuf::npos) {
                language = language.substr(0, space);
            }

            std::vector<TStringBuf> codeLines;
            for (++i; i < lines.size(); ++i) {
                if (TrimSpaces(lines[i]).StartsWith("```")) {
                    ++i; // consume the closing fence
                    break;
                }
                codeLines.push_back(lines[i]);
            }
            if (!codeLines.empty()) {
                blocks.push_back(BuildCodeBlock(codeLines, language));
            }
            continue;
        }

        // A run of consecutive "| ... |" lines is a Markdown table: render it as a real table.
        if (!IsTableSeparatorLine(trimmed) && trimmed.StartsWith('|')) {
            flushProse();
            std::vector<std::vector<std::string>> rows;
            for (; i < lines.size(); ++i) {
                const TStringBuf row = TrimSpaces(lines[i]);
                if (row.StartsWith("```") || (!IsTableSeparatorLine(row) && !row.StartsWith('|'))) {
                    break;
                }
                if (!IsTableSeparatorLine(row)) {
                    rows.push_back(SplitTableRow(row));
                }
            }
            if (!rows.empty()) {
                blocks.push_back(BuildTable(std::move(rows)));
            }
            continue;
        }

        // Heading line ("# ..."): render the whole line in bold.
        if (IsHeading(trimmed)) {
            flushProse();
            blocks.push_back(BuildStyledLine(StripInlineMarkdownKeepStyles(lines[i])) | ftxui::bold);
            ++i;
            continue;
        }

        // Standalone horizontal rule ("---", ":--:", "| --- |"): a table's separator is consumed in
        // the table branch above, so on its own such a line is a horizontal rule. Draw it full width.
        if (IsTableSeparatorLine(trimmed)) {
            flushProse();
            blocks.push_back(ftxui::separator() | ftxui::color(ftxui::Color::Grey42));
            ++i;
            continue;
        }

        proseLines.push_back(StripInlineMarkdownKeepStyles(lines[i]));
        ++i;
    }
    flushProse();

    if (blocks.empty()) {
        return ftxui::text("");
    }
    if (blocks.size() == 1) {
        return std::move(blocks.front());
    }
    return ftxui::vbox(std::move(blocks));
}

} // namespace NYdb::NConsoleClient
