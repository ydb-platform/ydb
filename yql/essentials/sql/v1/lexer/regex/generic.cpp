#include "generic.h"

#include <contrib/libs/re2/re2/re2.h>

#include <util/string/builder.h>

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
            for (const auto& matcher : Grammar_) {
                if (auto token = matcher(prefix)) {
                    onMatch(std::move(*token));
                }
            }
        }

        TGenericLexerGrammar Grammar_;
    };

    TTokenMatcher Compile(TString name, const TRegexPattern& regex) {
        RE2::Options options;
        options.set_case_sensitive(!regex.IsCaseInsensitive);

        return [bodyRe = MakeAtomicShared<RE2>(regex.Body, options),
                afterRe = MakeAtomicShared<RE2>(regex.After, options),
                name = std::move(name)](TStringBuf prefix) -> TMaybe<TGenericToken> {
            TMaybe<TStringBuf> body, after;
            if ((body = Match(prefix, *bodyRe)) &&
                (after = Match(prefix.Tail(body->size()), *afterRe))) {
                return TGenericToken{
                    .Name = name,
                    .Content = *body,
                };
            }
            return Nothing();
        };
    }

    TRegexPattern Merged(TVector<TRegexPattern> patterns) {
        Y_ENSURE(!patterns.empty());

        const TRegexPattern& sample = patterns.back();
        Y_ENSURE(AllOf(patterns, [&](const TRegexPattern& pattern) {
            return std::tie(pattern.After, pattern.IsCaseInsensitive) ==
                   std::tie(sample.After, sample.IsCaseInsensitive);
        }));

        Sort(patterns, [](const TRegexPattern& lhs, const TRegexPattern& rhs) {
            return lhs.Body.length() > rhs.Body.length();
        });

        TStringBuilder body;
        for (const auto& pattern : patterns) {
            body << "(" << pattern.Body << ")|";
        }
        Y_ENSURE(body.back() == '|');
        body.pop_back();

        return TRegexPattern{
            .Body = std::move(body),
            .After = sample.After,
            .IsCaseInsensitive = sample.IsCaseInsensitive,
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
