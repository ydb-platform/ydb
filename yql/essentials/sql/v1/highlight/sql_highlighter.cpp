#include "sql_highlighter.h"

#include <yql/essentials/sql/v1/lexer/regex/lexer.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/deque.h>
#include <util/generic/maybe.h>

namespace NSQLHighlight {

    using NSQLTranslationV1::Compile;
    using NSQLTranslationV1::IGenericLexer;
    using NSQLTranslationV1::TGenericLexerGrammar;
    using NSQLTranslationV1::TGenericToken;

    THashMap<EUnitKind, TString> NamesByUnitKind = [] {
        THashMap<EUnitKind, TString> names;
        names[EUnitKind::Keyword] = "K";
        names[EUnitKind::Punctuation] = "P";
        names[EUnitKind::QuotedIdentifier] = "Q";
        names[EUnitKind::BindParameterIdentifier] = "B";
        names[EUnitKind::TypeIdentifier] = "T";
        names[EUnitKind::FunctionIdentifier] = "F";
        names[EUnitKind::Identifier] = "I";
        names[EUnitKind::Literal] = "L";
        names[EUnitKind::StringLiteral] = "S";
        names[EUnitKind::Comment] = "C";
        names[EUnitKind::Whitespace] = "W";
        names[EUnitKind::Error] = TGenericToken::Error;
        return names;
    }();

    THashMap<TString, EUnitKind> UnitKindsByName = [] {
        THashMap<TString, EUnitKind> kinds;
        for (const auto& [kind, name] : NamesByUnitKind) {
            Y_ENSURE(!kinds.contains(name));
            kinds[name] = kind;
        }
        return kinds;
    }();

    TGenericLexerGrammar ToGenericLexerGrammar(const THighlighting& highlighting, bool ansi) {
        using NSQLTranslationV1::ANSICommentMatcher;

        TGenericLexerGrammar grammar;
        for (const auto& unit : highlighting.Units) {
            const auto* patterns = &unit.Patterns;
            if (!unit.PatternsANSI.Empty() && ansi) {
                patterns = unit.PatternsANSI.Get();
            }

            const auto& name = NamesByUnitKind.at(unit.Kind);

            if (unit.Kind == EUnitKind::Comment && ansi) {
                Y_ENSURE(unit.Patterns.size() == 1);
                auto matcher = Compile(name, unit.Patterns[0]);
                grammar.emplace_back(ANSICommentMatcher(name, std::move(matcher)));
            }

            for (const auto& pattern : *patterns) {
                grammar.emplace_back(Compile(name, pattern));
            }
        }
        return grammar;
    }

    class THighlighter: public IHighlighter {
    public:
        explicit THighlighter(NSQLTranslationV1::IGenericLexer::TPtr lexer)
            : Lexer_(std::move(lexer))
        {
        }

        bool Tokenize(TStringBuf text, const TTokenCallback& onNext, size_t maxErrors) const override {
            const auto onNextToken = [&](NSQLTranslationV1::TGenericToken&& token) {
                if (token.Name == "EOF") {
                    return;
                }

                onNext({
                    .Kind = UnitKindsByName.at(token.Name),
                    .Begin = token.Begin,
                    .Length = token.Content.size(),
                });
            };

            return Lexer_->Tokenize(text, onNextToken, maxErrors);
        }

    private:
        NSQLTranslationV1::IGenericLexer::TPtr Lexer_;
    };

    class TCombinedHighlighter: public IHighlighter {
    public:
        explicit TCombinedHighlighter(const THighlighting& highlighting)
            : LexerDefault_(NSQLTranslationV1::MakeGenericLexer(
                  ToGenericLexerGrammar(highlighting, /* ansi = */ false)))
            , LexerANSI_(NSQLTranslationV1::MakeGenericLexer(
                  ToGenericLexerGrammar(highlighting, /* ansi = */ true)))
        {
        }

        bool Tokenize(TStringBuf text, const TTokenCallback& onNext, size_t maxErrors) const override {
            return Alt(text).Tokenize(text, onNext, maxErrors);
        }

    private:
        const IHighlighter& Alt(TStringBuf text) const {
            if (text.After('-').StartsWith("-!ansi_lexer")) {
                return LexerANSI_;
            }
            return LexerDefault_;
        }

        THighlighter LexerDefault_;
        THighlighter LexerANSI_;
    };

    TVector<TToken> Tokenize(IHighlighter& highlighter, TStringBuf text) {
        TVector<TToken> tokens;
        highlighter.Tokenize(text, [&](TToken&& token) {
            tokens.emplace_back(std::move(token));
        });
        return tokens;
    }

    IHighlighter::TPtr MakeHighlighter(const THighlighting& highlighting) {
        return IHighlighter::TPtr(new TCombinedHighlighter(highlighting));
    }

} // namespace NSQLHighlight
