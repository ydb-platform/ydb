#include "regex.h"

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/vector.h>

#define SUBSTITUTION(name, mode) \
    {#name, name##_##mode}

#define SUBSTITUTIONS(mode)                                         \
    {                                                               \
        #mode, {                                                    \
            SUBSTITUTION(GRAMMAR_STRING_CORE_SINGLE, mode),         \
                SUBSTITUTION(GRAMMAR_STRING_CORE_DOUBLE, mode),     \
                SUBSTITUTION(GRAMMAR_MULTILINE_COMMENT_CORE, mode), \
        }                                                           \
    }

namespace NSQLTranslationV1 {

    class TLexerGrammarToRegexTranslator {
    private:
        struct TRewriteRule {
            TString Repr;
            std::function<void(TString&)> Apply;
        };

        using TRewriteRules = TVector<TRewriteRule>;

    public:
        explicit TLexerGrammarToRegexTranslator(const NSQLReflect::TLexerGrammar& grammar, bool ansi)
            : Grammar_(&grammar)
            , Mode_(ansi ? "ANSI" : "DEFAULT")
        {
            AddExternalRules(Inliners_);
            AddFragmentRules(Inliners_);

            AddLetterRules(Transformations_);
            AddTransformationRules(Transformations_);

            UnwrapQuotes_ = UnwrapQuotesRule();
            AddSpaceCollapses(SpaceCollapses_);
            UnwrapQuotedSpace_ = UnwrapQuotedSpaceRule();
        }

        TString ToRegex(const TStringBuf name) {
            TString text = Grammar_->BlockByName.at(name);
            Preprocess(text);
            Inline(text);
            Transform(text);
            Finalize(text);
            return text;
        }

    private:
        void Preprocess(TString& text) {
            text = ChangedDigitsPrecendence(std::move(text));
        }

        void Inline(TString& text) {
            ApplyEachWhileChanging(text, Inliners_);
        }

        void AddExternalRules(TRewriteRules& rules) {
            THashMap<TString, THashMap<TString, TString>> Substitutions = {
                SUBSTITUTIONS(DEFAULT),
                SUBSTITUTIONS(ANSI),
            };

            // ANSI mode MULTILINE_COMMENT is recursive
            Substitutions["ANSI"]["GRAMMAR_MULTILINE_COMMENT_CORE"] =
                Substitutions["DEFAULT"]["GRAMMAR_MULTILINE_COMMENT_CORE"];

            for (const auto& [k, v] : Substitutions.at(Mode_)) {
                rules.emplace_back(RegexRewriteRule("@" + k + "@", v));
            }
        }

        void AddFragmentRules(TRewriteRules& rules) {
            const THashSet<TString> PunctuationFragments = {
                "BACKSLASH",
                "QUOTE_DOUBLE",
                "QUOTE_SINGLE",
                "BACKTICK",
                "DOUBLE_COMMAT",
            };

            for (const auto& [name, definition] : Grammar_->BlockByName) {
                TString def = definition;
                if (
                    Grammar_->PunctuationNames.contains(name) ||
                    PunctuationFragments.contains(name)) {
                    def = "'" + def + "'";
                } else if (name == "DIGITS") {
                    def = ChangedDigitsPrecendence(std::move(def));
                }
                def = QuoteAntlrRewrite(std::move(def));

                rules.emplace_back(RegexRewriteRule(
                    "(\\b" + name + "\\b)",
                    "(" + def + ")"));
            }
        }

        // Regex engine matches the first matched alternative,
        // even if it is not the longest one, while ANTLR is more gready.
        TString ChangedDigitsPrecendence(TString body) {
            if (SubstGlobal(body, "DECDIGITS | ", "") != 0) {
                SubstGlobal(body, "BINDIGITS", "BINDIGITS | DECDIGITS");
            }
            return body;
        }

        void Transform(TString& text) {
            ApplyEachWhileChanging(text, Transformations_);
        }

        void AddLetterRules(TRewriteRules& rules) {
            for (char letter = 'A'; letter <= 'Z'; ++letter) {
                TString lower(char(ToLower(letter)));
                TString upper(char(ToUpper(letter)));
                rules.emplace_back(RegexRewriteRule(
                    "([^'\\w\\[\\]]|^)" + upper + "([^'\\w\\[\\]]|$)",
                    "\\1[" + lower + upper + "]\\2"));
            }
        }

        void AddTransformationRules(TRewriteRules& rules) {
            rules.emplace_back(RegexRewriteRule(
                R"(~\('(..?)' \| '(..?)'\))", R"([^\1\2])"));

            rules.emplace_back(RegexRewriteRule(
                R"(~\('(..?)'\))", R"([^\1])"));

            rules.emplace_back(RegexRewriteRule(
                R"(('..?')\.\.('..?'))", R"([\1-\2])"));

            rules.emplace_back(RegexRewriteRule(
                R"(\((.)\))", R"(\1)"));

            rules.emplace_back(RegexRewriteRule(
                R"(\((\[.{1,8}\])\))", R"(\1)"));

            rules.emplace_back(RegexRewriteRule(
                R"(\(('..?')\))", R"(\1)"));

            rules.emplace_back(RegexRewriteRule(
                R"( \.)", R"( (.|\\n))"));

            rules.emplace_back(RegexRewriteRule(
                R"(\bEOF\b)", R"($)"));

            rules.emplace_back(RegexRewriteRule(
                R"('\\u000C' \|)", R"('\\f' |)"));
        }

        void Finalize(TString& text) {
            UnwrapQuotes_.Apply(text);
            ApplyEachWhileChanging(text, SpaceCollapses_);
            UnwrapQuotedSpace_.Apply(text);
        }

        void AddSpaceCollapses(TRewriteRules& rules) {
            rules.emplace_back(RegexRewriteRule(R"(([^']|^) )", R"(\1)"));
            rules.emplace_back(RegexRewriteRule(R"( ([^']|$))", R"(\1)"));
        }

        void ApplyEachOnce(TString& text, const TRewriteRules& rules) {
            for (const auto& rule : rules) {
                rule.Apply(text);
            }
        }

        void ApplyEachWhileChanging(TString& text, const TRewriteRules& rules) {
            constexpr size_t Limit = 16;

            TString prev;
            for (size_t i = 0; i < Limit + 1 && prev != text; ++i) {
                prev = text;
                ApplyEachOnce(text, rules);
                Y_ENSURE(i != Limit);
            }
        }

        TRewriteRule RegexRewriteRule(const TString& regex, TString rewrite) {
            auto re2 = std::make_shared<RE2>(regex, RE2::Quiet);
            Y_ENSURE(re2->ok(), re2->error() << " on regex '" << regex << "'");

            TString error;
            Y_ENSURE(
                re2->CheckRewriteString(rewrite, &error),
                error << " on rewrite '" << rewrite << "'");

            return {
                .Repr = regex + " -> " + rewrite,
                .Apply = [re2, rewrite = std::move(rewrite)](TString& text) {
                    RE2::GlobalReplace(&text, *re2, rewrite);
                },
            };
        }

        TRewriteRule UnwrapQuotesRule() {
            const TString regex = R"('([^ ][^ ]?)')";
            auto re2 = std::make_shared<RE2>(regex, RE2::Quiet);
            Y_ENSURE(re2->ok(), re2->error() << " on regex '" << regex << "'");

            return {
                .Repr = regex + " -> Quoted(\\1)",
                .Apply = [re2](TString& text) {
                    TString content;
                    std::size_t i = 256;
                    while (RE2::PartialMatch(text, *re2, &content) && --i != 0) {
                        TString quoted = RE2::QuoteMeta(content);
                        for (size_t i = 0; i < 2 && quoted.StartsWith(R"(\\)"); ++i) {
                            quoted.erase(std::begin(quoted));
                        }
                        SubstGlobal(text, "'" + content + "'", quoted);
                    }
                    Y_ENSURE(i != 0);
                },
            };
        }

        TRewriteRule UnwrapQuotedSpaceRule() {
            return RegexRewriteRule(R"(' ')", R"( )");
        }

        TString QuoteAntlrRewrite(TString rewrite) {
            SubstGlobal(rewrite, R"(\)", R"(\\)");
            SubstGlobal(rewrite, R"('\\')", R"('\\\\')");
            return rewrite;
        }

        const NSQLReflect::TLexerGrammar* Grammar_;
        const TStringBuf Mode_;

        TRewriteRules Inliners_;

        TRewriteRules Transformations_;

        TRewriteRule UnwrapQuotes_;
        TRewriteRules SpaceCollapses_;
        TRewriteRule UnwrapQuotedSpace_;
    };

    TVector<std::tuple<TString, TString>> MakeRegexByOtherName(const NSQLReflect::TLexerGrammar& grammar, bool ansi) {
        TLexerGrammarToRegexTranslator translator(grammar, ansi);

        TVector<std::tuple<TString, TString>> regexes;
        for (const auto& token : grammar.OtherNames) {
            regexes.emplace_back(token, translator.ToRegex(token));
        }
        return regexes;
    }

} // namespace NSQLTranslationV1
