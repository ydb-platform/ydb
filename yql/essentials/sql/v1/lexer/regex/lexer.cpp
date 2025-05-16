#include "lexer.h"

#include "generic.h"
#include "regex.h"

#include <contrib/libs/re2/re2/re2.h>

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/string/subst.h>
#include <util/string/ascii.h>

namespace NSQLTranslationV1 {

    using NSQLReflect::TLexerGrammar;
    using NSQLTranslation::TParsedToken;
    using NSQLTranslation::TParsedTokenList;

    size_t MatchANSIMultilineComment(TStringBuf remaining);

    TTokenMatcher ANSICommentMatcher(TTokenMatcher defaultComment) {
        return [defaultComment](TStringBuf prefix) -> TMaybe<TStringBuf> {
            const auto basic = defaultComment(prefix);
            if (basic.Empty()) {
                return Nothing();
            }

            if (!prefix.StartsWith("/*")) {
                return basic;
            }

            size_t ll1Length = MatchANSIMultilineComment(prefix);
            TStringBuf ll1Content = prefix.SubString(0, ll1Length);

            Y_ENSURE(ll1Content == 0 || basic <= ll1Content);
            if (ll1Content == 0) {
                return basic;
            }

            return ll1Content;
        };
    }

    size_t MatchANSIMultilineComment(TStringBuf prefix) {
        if (!prefix.StartsWith("/*")) {
            return 0;
        }

        size_t skipped = 0;

        prefix.Skip(2);
        skipped += 2;

        for (;;) {
            if (prefix.StartsWith("*/")) {
                prefix.Skip(2);
                skipped += 2;
                return skipped;
            }

            bool isSkipped = false;
            if (prefix.StartsWith("/*")) {
                size_t limit = prefix.rfind("*/");
                if (limit == std::string::npos) {
                    return 0;
                }

                size_t len = MatchANSIMultilineComment(prefix.Head(limit));
                prefix.Skip(len);
                skipped += len;

                isSkipped = len != 0;
            }

            if (isSkipped) {
                continue;
            }

            if (prefix.size() == 0) {
                return 0;
            }

            prefix.Skip(1);
            skipped += 1;
        }
    }

    TGenericLexerGrammar MakeGenericLexerGrammar(
        bool ansi,
        const TLexerGrammar& grammar,
        const TVector<std::tuple<TString, TString>>& regexByOtherName) {
        TGenericLexerGrammar generic;

        for (const auto& name : grammar.KeywordNames) {
            auto matcher = Compile({
                .Body = TString(TLexerGrammar::KeywordBlock(name)),
                .IsCaseInsensitive = true,
            });
            generic.emplace_back(name, std::move(matcher));
        }

        for (const auto& name : grammar.PunctuationNames) {
            generic.emplace_back(
                name, Compile({RE2::QuoteMeta(grammar.BlockByName.at(name))}));
        }

        for (const auto& [name, regex] : regexByOtherName) {
            auto matcher = Compile({
                .Body = regex,
            });
            generic.emplace_back(name, std::move(matcher));
        }

        if (ansi) {
            auto it = FindIf(generic, [](const auto& m) {
                return m.TokenName == "COMMENT";
            });
            Y_ENSURE(it != std::end(generic));
            it->Match = ANSICommentMatcher(it->Match);
        }

        return generic;
    }

    class TRegexLexer: public NSQLTranslation::ILexer {
    public:
        TRegexLexer(IGenericLexer::TPtr lexer)
            : Lexer_(std::move(lexer))
        {
        }

        bool Tokenize(
            const TString& query,
            const TString& queryName,
            const TTokenCallback& onNextToken,
            NYql::TIssues& issues,
            size_t maxErrors) override {
            bool isFailed = false;

            const auto onNext = [&](TGenericToken&& token) {
                if (token.Name == TGenericToken::Error) {
                    NYql::TPosition pos(token.Begin, 0, queryName);
                    TString message = TString("no candidates, skipping ") + token.Content;
                    issues.AddIssue(std::move(pos), std::move(message));
                    isFailed = true;
                    return;
                }

                onNextToken({
                    .Name = TString(token.Name),
                    .Content = TString(token.Content),
                });
            };

            Lexer_->Tokenize(query, onNext, maxErrors);
            return !isFailed;
        }

    private:
        IGenericLexer::TPtr Lexer_;
    };

    namespace {

        class TFactory final: public NSQLTranslation::ILexerFactory {
        public:
            explicit TFactory(bool ansi) {
                auto grammar = NSQLReflect::LoadLexerGrammar();
                auto regexes = MakeRegexByOtherName(grammar, ansi);
                Lexer_ = MakeGenericLexer(MakeGenericLexerGrammar(ansi, grammar, regexes));
            }

            NSQLTranslation::ILexer::TPtr MakeLexer() const override {
                return NSQLTranslation::ILexer::TPtr(
                    new TRegexLexer(Lexer_));
            }

        private:
            IGenericLexer::TPtr Lexer_;
        };

    } // namespace

    NSQLTranslation::TLexerFactoryPtr MakeRegexLexerFactory(bool ansi) {
        return NSQLTranslation::TLexerFactoryPtr(new TFactory(ansi));
    }

} // namespace NSQLTranslationV1
