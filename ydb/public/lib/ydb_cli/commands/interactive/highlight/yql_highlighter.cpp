#include "yql_highlighter.h"

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>
#include <yql/essentials/sql/v1/highlight/sql_highlighter.h>

#include <contrib/restricted/patched/replxx/src/util.hxx>

#include <util/charset/utf8.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {
    using NSQLHighlight::MakeHighlighter;
    using NSQLHighlight::MakeHighlighting;

namespace {

    static constexpr size_t InvalidPosition = Max<std::ptrdiff_t>();

    TVector<std::ptrdiff_t> BuildSymbolByByteIndex(TStringBuf text) {
        TVector<std::ptrdiff_t> index(text.size(), InvalidPosition);
        for (size_t i = 0, j = 0; i < text.size(); i += UTF8RuneLen(text[i]), j += 1) {
            index[i] = j;
        }
        return index;
    }

    ftxui::Color ToFtxuiColor(replxx::Replxx::Color rColor) {
        const int colorIndex = static_cast<int>(rColor);
        if (rColor == replxx::Replxx::Color::DEFAULT || colorIndex < 0) {
            return ftxui::Color::Default;
        }

        return ftxui::Color::Palette256(static_cast<uint8_t>(colorIndex));
    }

} // anonymous namespace

    class TYQLHighlighter: public IYQLHighlighter {
    public:
        explicit TYQLHighlighter(TColorSchema color)
            : Coloring(color)
            , Highlighter(MakeHighlighter(MakeHighlighting()))
        {
        }

        void Apply(TStringBuf query, TColors& colors) const override {
            const TVector<std::ptrdiff_t> symbolIndexByByte = BuildSymbolByByteIndex(query);

            Highlighter->Tokenize(query, [&](NSQLHighlight::TToken&& token) {
                if (token.Kind == NSQLHighlight::EUnitKind::Error) {
                    return;
                }

                TStringBuf content = query.SubString(token.Begin, token.Length);

                const std::ptrdiff_t start = symbolIndexByByte[token.Begin];
                const size_t length = GetNumberOfUTF8Chars(content);
                const std::ptrdiff_t stop = start + length;

                std::fill(std::next(std::begin(colors), start),
                          std::next(std::begin(colors), stop),
                          Color(token.Kind));
            });
        }

    private:
        TColor Color(const NSQLHighlight::EUnitKind& kind) const {
            switch (kind) {
                case NSQLHighlight::EUnitKind::Keyword:
                    return Coloring.keyword;
                case NSQLHighlight::EUnitKind::Punctuation:
                    return Coloring.operation;
                case NSQLHighlight::EUnitKind::QuotedIdentifier:
                    return Coloring.identifier.quoted;
                case NSQLHighlight::EUnitKind::BindParameterIdentifier:
                    return Coloring.identifier.variable;
                case NSQLHighlight::EUnitKind::TypeIdentifier:
                    return Coloring.identifier.type;
                case NSQLHighlight::EUnitKind::FunctionIdentifier:
                    return Coloring.identifier.function;
                case NSQLHighlight::EUnitKind::Identifier:
                    return Coloring.identifier.variable;
                case NSQLHighlight::EUnitKind::Literal:
                    return Coloring.number;
                case NSQLHighlight::EUnitKind::StringLiteral:
                    return Coloring.string;
                case NSQLHighlight::EUnitKind::Comment:
                    return Coloring.comment;
                case NSQLHighlight::EUnitKind::Whitespace:
                    return Coloring.unknown;
                case NSQLHighlight::EUnitKind::Error:
                    return Coloring.unknown;
                default:
                    return Coloring.unknown;
            }
        }

    private:
        TColorSchema Coloring;
        NSQLHighlight::IHighlighter::TPtr Highlighter;
    };

    TString PrintYqlHighlightAnsiColors(const TString& queryUtf8, const TColors& colors) {
        TStringBuilder result;

        replxx::Replxx::Color lastColor = replxx::Replxx::Color::DEFAULT;
        result << replxx::ansi_color(lastColor);

        const TVector<std::ptrdiff_t> symbolIndexByByte = BuildSymbolByByteIndex(queryUtf8);
        for (size_t i = 0; i < queryUtf8.length(); ++i) {
            if (symbolIndexByByte[i] != InvalidPosition && colors[symbolIndexByByte[i]] != lastColor) {
                result << replxx::ansi_color(colors[symbolIndexByByte[i]]);
                lastColor = colors[symbolIndexByByte[i]];
            }
            result << queryUtf8[i];
        }

        result << replxx::ansi_color(replxx::Replxx::Color::DEFAULT);
        return result;
    }

    ftxui::Element PrintYqlHighlightFtxuiColors(const TString& queryUtf8, const TColors& colors) {
        std::vector<ftxui::Element> rows;
        std::vector<ftxui::Element> currentRow;
        std::string currentSegment;

        replxx::Replxx::Color lastColor = replxx::Replxx::Color::DEFAULT;
        const TVector<std::ptrdiff_t> symbolIndexByByte = BuildSymbolByByteIndex(queryUtf8);

        auto flushSegment = [&]() {
            if (!currentSegment.empty()) {
                currentRow.push_back(
                    ftxui::text(currentSegment) | ftxui::color(ToFtxuiColor(lastColor))
                );
                currentSegment.clear();
            }
        };

        for (size_t i = 0; i < queryUtf8.length(); ++i) {
            const char c = queryUtf8[i];

            if (c == '\n') {
                flushSegment();
                rows.push_back(ftxui::hflow(std::move(currentRow)));
                currentRow = {};
                continue;
            }

            if (const auto symbolIndex = symbolIndexByByte[i]; symbolIndex != InvalidPosition) {
                if (const auto currentColor = colors[symbolIndex]; currentColor != lastColor) {
                    flushSegment();
                    lastColor = currentColor;
                }
            }

            if (c == ' ') {
                flushSegment();
                currentRow.push_back(ftxui::text(" ") | ftxui::color(ToFtxuiColor(lastColor)));
                continue;
            }

            currentSegment += c;
        }

        flushSegment();

        if (!currentRow.empty()) {
            rows.push_back(ftxui::hflow(std::move(currentRow)));
        }

        return ftxui::vbox(std::move(rows));
    }

    IYQLHighlighter::TPtr MakeYQLHighlighter(TColorSchema color) {
        return IYQLHighlighter::TPtr(new TYQLHighlighter(std::move(color)));
    }

} // namespace NYdb::NConsoleClient
