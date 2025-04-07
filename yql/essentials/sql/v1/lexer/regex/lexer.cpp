#include "lexer.h"

#include "regex.h"

#include <contrib/libs/re2/re2/re2.h>

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/subst.h>
#include <util/string/ascii.h>

namespace NSQLTranslationV1 {

    using NSQLTranslation::TParsedToken;
    using NSQLTranslation::TParsedTokenList;

    class TRegexLexer: public NSQLTranslation::ILexer {
        static constexpr const char* CommentTokenName = "COMMENT";

    public:
        TRegexLexer(
            bool ansi,
            NSQLReflect::TLexerGrammar grammar,
            const TVector<std::tuple<TString, TString>>& RegexByOtherName)
            : Grammar_(std::move(grammar))
            , Ansi_(ansi)
        {
            for (const auto& [token, regex] : RegexByOtherName) {
                if (token == CommentTokenName) {
                    CommentRegex_.Reset(new RE2(regex));
                } else {
                    OtherRegexes_.emplace_back(token, new RE2(regex));
                }
            }
        }

        bool Tokenize(
            const TString& query,
            const TString& queryName,
            const TTokenCallback& onNextToken,
            NYql::TIssues& issues,
            size_t maxErrors) override {
            size_t errors = 0;
            for (size_t pos = 0; pos < query.size();) {
                TParsedToken matched = Match(TStringBuf(query, pos));

                if (matched.Name.empty() && maxErrors == errors) {
                    break;
                }

                if (matched.Name.empty()) {
                    pos += 1;
                    errors += 1;
                    issues.AddIssue(NYql::TPosition(pos, 0, queryName), "no candidates");
                    continue;
                }

                pos += matched.Content.length();
                onNextToken(std::move(matched));
            }

            onNextToken(TParsedToken{.Name = "EOF"});
            return errors == 0;
        }

    private:
        TParsedToken Match(const TStringBuf prefix) {
            TParsedTokenList matches;

            size_t keywordCount = MatchKeyword(prefix, matches);
            MatchPunctuation(prefix, matches);
            MatchRegex(prefix, matches);
            MatchComment(prefix, matches);

            if (matches.empty()) {
                return {};
            }

            auto maxLength = MaxElementBy(matches, [](const TParsedToken& m) {
                                 return m.Content.length();
                             })->Content.length();

            auto max = FindIf(matches, [&](const TParsedToken& m) {
                return m.Content.length() == maxLength;
            });

            auto isMatched = [&](const TStringBuf name) {
                return std::end(matches) != FindIf(matches, [&](const auto& m) {
                           return m.Name == name;
                       });
            };

            size_t conflicts = CountIf(matches, [&](const TParsedToken& m) {
                return m.Content.length() == max->Content.length();
            });
            conflicts -= 1;
            Y_ENSURE(
                conflicts == 0 ||
                (conflicts == 1 && keywordCount != 0 && isMatched("ID_PLAIN")) ||
                (conflicts == 1 && isMatched("DIGITS") && isMatched("INTEGER_VALUE")));

            Y_ENSURE(!max->Content.empty());
            return *max;
        }

        bool MatchKeyword(const TStringBuf prefix, TParsedTokenList& matches) {
            size_t count = 0;
            for (const auto& keyword : Grammar_.KeywordNames) {
                const TStringBuf content = prefix.substr(0, keyword.length());
                if (AsciiEqualsIgnoreCase(content, keyword)) {
                    matches.emplace_back(keyword, TString(content));
                    count += 1;
                }
            }
            return count;
        }

        size_t MatchPunctuation(const TStringBuf prefix, TParsedTokenList& matches) {
            size_t count = 0;
            for (const auto& name : Grammar_.PunctuationNames) {
                const auto& content = Grammar_.BlockByName.at(name);
                if (prefix.substr(0, content.length()) == content) {
                    matches.emplace_back(name, content);
                    count += 1;
                }
            }
            return count;
        }

        size_t MatchRegex(const TStringBuf prefix, TParsedTokenList& matches) {
            size_t count = 0;
            for (const auto& [token, regex] : OtherRegexes_) {
                if (const TStringBuf match = TryMatchRegex(prefix, *regex); !match.empty()) {
                    matches.emplace_back(token, TString(match));
                    count += 1;
                }
            }
            return count;
        }

        const TStringBuf TryMatchRegex(const TStringBuf prefix, const RE2& regex) {
            re2::StringPiece input(prefix.data(), prefix.size());
            if (RE2::Consume(&input, regex)) {
                return TStringBuf(prefix.data(), input.data());
            }
            return "";
        }

        size_t MatchComment(const TStringBuf prefix, TParsedTokenList& matches) {
            const TStringBuf reContent = TryMatchRegex(prefix, *CommentRegex_);
            if (reContent.empty()) {
                return 0;
            }

            if (!(Ansi_ && prefix.StartsWith("/*"))) {
                matches.emplace_back(CommentTokenName, TString(reContent));
                return 1;
            }

            size_t ll1Length = MatchANSIMultilineComment(prefix);
            const TStringBuf ll1Content = prefix.SubString(0, ll1Length);

            Y_ENSURE(ll1Content == 0 || reContent <= ll1Content);
            if (ll1Content == 0) {
                matches.emplace_back(CommentTokenName, TString(reContent));
                return 1;
            }

            matches.emplace_back(CommentTokenName, TString(ll1Content));
            return 1;
        }

        size_t MatchANSIMultilineComment(TStringBuf remaining) {
            if (!remaining.StartsWith("/*")) {
                return 0;
            }

            size_t skipped = 0;

            remaining.Skip(2);
            skipped += 2;

            for (;;) {
                if (remaining.StartsWith("*/")) {
                    remaining.Skip(2);
                    skipped += 2;
                    return skipped;
                }

                bool isSkipped = false;
                if (remaining.StartsWith("/*")) {
                    size_t limit = remaining.rfind("*/");
                    if (limit == std::string::npos) {
                        return 0;
                    }

                    size_t len = MatchANSIMultilineComment(remaining.Head(limit));
                    remaining.Skip(len);
                    skipped += len;

                    isSkipped = len != 0;
                }

                if (isSkipped) {
                    continue;
                }

                if (remaining.size() == 0) {
                    return 0;
                }

                remaining.Skip(1);
                skipped += 1;
            }
        }

        NSQLReflect::TLexerGrammar Grammar_;
        TVector<std::tuple<TString, THolder<RE2>>> OtherRegexes_;
        THolder<RE2> CommentRegex_;
        bool Ansi_;
    };

    namespace {

        class TFactory final: public NSQLTranslation::ILexerFactory {
        public:
            explicit TFactory(bool ansi)
                : Ansi_(ansi)
                , Grammar_(NSQLReflect::LoadLexerGrammar())
                , RegexByOtherName_(MakeRegexByOtherName(Grammar_, Ansi_))
            {
            }

            NSQLTranslation::ILexer::TPtr MakeLexer() const override {
                return NSQLTranslation::ILexer::TPtr(
                    new TRegexLexer(Ansi_, Grammar_, RegexByOtherName_));
            }

        private:
            bool Ansi_;
            NSQLReflect::TLexerGrammar Grammar_;
            TVector<std::tuple<TString, TString>> RegexByOtherName_;
        };

    } // namespace

    NSQLTranslation::TLexerFactoryPtr MakeRegexLexerFactory(bool ansi) {
        return NSQLTranslation::TLexerFactoryPtr(new TFactory(ansi));
    }

} // namespace NSQLTranslationV1
