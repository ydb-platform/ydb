#include "grammar.h"

#include <yql/essentials/sql/v1/lexer/regex/regex.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/algorithm.h>

namespace NSQLComplete {

    class TSqlGrammar: public ISqlGrammar {
    public:
        TSqlGrammar(const NSQLReflect::TLexerGrammar& grammar)
            : Parser_(MakeDummyParser())
            , AllTokens_(ComputeAllTokens())
            , KeywordTokens_(ComputeKeywordTokens(grammar))
            , PunctuationTokens_(ComputePunctuationTokens(grammar))
            , IdPlainRegex_(ComputeIdPlainRegex(grammar))
        {
        }

        const antlr4::dfa::Vocabulary& GetVocabulary() const override {
            return Parser_->getVocabulary();
        }

        const std::unordered_set<TTokenId>& GetAllTokens() const override {
            return AllTokens_;
        }

        const std::unordered_set<TTokenId>& GetKeywordTokens() const override {
            return KeywordTokens_;
        }

        const std::unordered_set<TTokenId>& GetPunctuationTokens() const override {
            return PunctuationTokens_;
        }

        const std::string& SymbolizedRule(TRuleId rule) const override {
            return Parser_->getRuleNames().at(rule);
        }

        TTokenId GetTokenId(std::string_view symbolized) const override {
            TTokenId type = Parser_->getTokenType(symbolized);
            Y_ENSURE(type != antlr4::Token::INVALID_TYPE, "Not found " << symbolized);
            return type;
        }

        TRuleId GetRuleId(std::string_view symbolized) const override {
            TRuleId index = Parser_->getRuleIndex(std::string(symbolized));
            if (index == INVALID_INDEX) {
                ythrow yexception() << "Rule \"" << symbolized << "\" not found";
            }
            return index;
        }

        const std::vector<std::string>& GetAllRules() const override {
            return Parser_->getRuleNames();
        }

        bool IsPlainIdentifier(TStringBuf content) const override {
            return RE2::FullMatch(content, IdPlainRegex_);
        }

    private:
        static THolder<antlr4::Parser> MakeDummyParser() {
            return MakeHolder<NALADefaultAntlr4::SQLv1Antlr4Parser>(nullptr);
        }

        std::unordered_set<TTokenId> ComputeAllTokens() {
            const auto& vocabulary = GetVocabulary();

            std::unordered_set<TTokenId> allTokens;

            for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
                allTokens.emplace(type);
            }

            return allTokens;
        }

        std::unordered_set<TTokenId> ComputeKeywordTokens(
            const NSQLReflect::TLexerGrammar& grammar) {
            const auto& vocabulary = GetVocabulary();

            auto keywordTokens = GetAllTokens();
            std::erase_if(keywordTokens, [&](TTokenId token) {
                return !grammar.KeywordNames.contains(vocabulary.getSymbolicName(token));
            });
            keywordTokens.erase(TOKEN_EOF);

            return keywordTokens;
        }

        std::unordered_set<TTokenId> ComputePunctuationTokens(
            const NSQLReflect::TLexerGrammar& grammar) {
            const auto& vocabulary = GetVocabulary();

            auto punctuationTokens = GetAllTokens();
            std::erase_if(punctuationTokens, [&](TTokenId token) {
                return !grammar.PunctuationNames.contains(vocabulary.getSymbolicName(token));
            });

            return punctuationTokens;
        }

        static TString ComputeIdPlainRegex(const NSQLReflect::TLexerGrammar& grammar) {
            TVector<std::tuple<TString, TString>> regexes =
                NSQLTranslationV1::MakeRegexByOtherName(grammar, /* ansi = */ false);

            std::tuple<TString, TString>* regex = FindIfPtr(regexes, [&](const auto& x) {
                return std::get<0>(x) == "ID_PLAIN";
            });

            Y_ENSURE(regex, "ID_PLAIN regex not found");
            return std::get<1>(*regex);
        }

        const THolder<antlr4::Parser> Parser_;
        const std::unordered_set<TTokenId> AllTokens_;
        const std::unordered_set<TTokenId> KeywordTokens_;
        const std::unordered_set<TTokenId> PunctuationTokens_;
        const RE2 IdPlainRegex_;
    };

    const ISqlGrammar& GetSqlGrammar() {
        const static TSqlGrammar DefaultSqlGrammar(NSQLReflect::LoadLexerGrammar());
        return DefaultSqlGrammar;
    }

} // namespace NSQLComplete
