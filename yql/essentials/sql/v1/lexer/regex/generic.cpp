#include "generic.h"

#include <contrib/libs/re2/re2/re2.h>

namespace NSQLTranslationV1 {

    namespace {

        TMaybe<TStringBuf> Match(TStringBuf prefix, const RE2& regex) {
            re2::StringPiece input(prefix.data(), prefix.size());
            if (RE2::Consume(&input, regex)) {
                return TStringBuf(prefix.data(), input.data());
            }
            return Nothing();
        }

    } // namespace

    class TGenericLexer: public IGenericLexer {
    private:
        static constexpr TStringBuf Utf8BOM = "\xEF\xBB\xBF";

    public:
        explicit TGenericLexer(TGenericLexerGrammar grammar)
            : Grammar_(std::move(grammar))
        {
        }

        virtual bool Tokenize(
            TStringBuf text,
            const TTokenCallback& onNext,
            size_t maxErrors) const override {
            Y_ENSURE(0 < maxErrors);
            size_t errors = 0;

            size_t pos = 0;
            if (text.StartsWith(Utf8BOM)) {
                pos += Utf8BOM.size();
            }

            while (pos < text.size() && errors < maxErrors) {
                TGenericToken matched = Match(TStringBuf(text, pos));
                matched.Begin = pos;

                pos += matched.Content.size();

                if (matched.Name == TGenericToken::Error) {
                    errors += 1;
                }

                onNext(std::move(matched));
            }

            if (errors == maxErrors) {
                return false;
            }

            onNext(TGenericToken{
                .Name = "EOF",
                .Content = "<EOF>",
                .Begin = pos,
            });

            return errors == 0;
        }

    private:
        TGenericToken Match(TStringBuf prefix) const {
            TMaybe<TGenericToken> max;
            Match(prefix, [&](TGenericToken&& token) {
                if (max.Empty() || max->Content.size() < token.Content.size()) {
                    max = std::move(token);
                }
            });

            if (max) {
                return *max;
            }

            return {
                .Name = TGenericToken::Error,
                .Content = prefix.substr(0, 1),
            };
        }

        void Match(TStringBuf prefix, auto onMatch) const {
            for (const auto& token : Grammar_) {
                if (auto content = token.Match(prefix)) {
                    onMatch(TGenericToken{
                        .Name = token.TokenName,
                        .Content = *content,
                    });
                }
            }
        }

        TGenericLexerGrammar Grammar_;
    };

    TTokenMatcher Compile(const TRegexPattern& regex) {
        RE2::Options options;
        options.set_case_sensitive(!regex.IsCaseInsensitive);

        return [bodyRe = MakeAtomicShared<RE2>(regex.Body, options),
                afterRe = MakeAtomicShared<RE2>(regex.After, options)](TStringBuf prefix) -> TMaybe<TStringBuf> {
            TMaybe<TStringBuf> body, after;
            if ((body = Match(prefix, *bodyRe)) &&
                (after = Match(prefix.Tail(body->size()), *afterRe))) {
                return body;
            }
            return Nothing();
        };
    }

    IGenericLexer::TPtr MakeGenericLexer(TGenericLexerGrammar grammar) {
        return IGenericLexer::TPtr(new TGenericLexer(std::move(grammar)));
    }

    TVector<TGenericToken> Tokenize(IGenericLexer::TPtr& lexer, TStringBuf text) {
        TVector<TGenericToken> tokens;
        lexer->Tokenize(text, [&](TGenericToken&& token) {
            tokens.emplace_back(std::move(token));
        });
        return tokens;
    }

} // namespace NSQLTranslationV1
