#include "grammar.h"

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

namespace NSQLComplete {

    class TSqlGrammar: public ISqlGrammar {
    public:
        TSqlGrammar(const NSQLReflect::TLexerGrammar& grammar)
            : Parser_(MakeDummyParser())
            , AllTokens_(ComputeAllTokens())
            , KeywordTokens_(ComputeKeywordTokens(grammar))
            , PunctuationTokens_(ComputePunctuationTokens(grammar))
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

        const THolder<antlr4::Parser> Parser_;
        const std::unordered_set<TTokenId> AllTokens_;
        const std::unordered_set<TTokenId> KeywordTokens_;
        const std::unordered_set<TTokenId> PunctuationTokens_;
    };

    const ISqlGrammar& GetSqlGrammar() {
        const static TSqlGrammar DefaultSqlGrammar(NSQLReflect::LoadLexerGrammar());
        return DefaultSqlGrammar;
    }

} // namespace NSQLComplete
