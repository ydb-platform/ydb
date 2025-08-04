#include "yql_highlighter.h"

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>
#include <yql/essentials/sql/v1/highlight/sql_highlighter.h>

#include <util/charset/utf8.h>
#include <util/string/strip.h>

#include <regex>

namespace NYdb::NConsoleClient {
    using NSQLHighlight::MakeHighlighter;
    using NSQLHighlight::MakeHighlighting;

    class TYQLHighlighter: public IYQLHighlighter {
    private:
        static constexpr size_t InvalidPosition = Max<std::ptrdiff_t>();

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

        static TVector<std::ptrdiff_t> BuildSymbolByByteIndex(TStringBuf text) {
            TVector<std::ptrdiff_t> index(text.size(), InvalidPosition);
            for (size_t i = 0, j = 0; i < text.size(); i += UTF8RuneLen(text[i]), j += 1) {
                index[i] = j;
            }
            return index;
        }

    private:
        TColorSchema Coloring;
        NSQLHighlight::IHighlighter::TPtr Highlighter;
    };

    IYQLHighlighter::TPtr MakeYQLHighlighter(TColorSchema color) {
        return IYQLHighlighter::TPtr(new TYQLHighlighter(std::move(color)));
    }

} // namespace NYdb::NConsoleClient
