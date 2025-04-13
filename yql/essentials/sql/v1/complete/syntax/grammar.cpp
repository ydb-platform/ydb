#include "grammar.h"

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

namespace NSQLComplete {

    class TSqlGrammar: public ISqlGrammar {
    public:
        TSqlGrammar(const NSQLReflect::TLexerGrammar& grammar)
            : Parser(MakeDummyParser())
            , AllTokens(ComputeAllTokens())
            , KeywordTokens(ComputeKeywordTokens(grammar))
            , PunctuationTokens(ComputePunctuationTokens(grammar))
        {
        }

        const antlr4::dfa::Vocabulary& GetVocabulary() const override {
            return Parser->getVocabulary();
        }

        const std::unordered_set<TTokenId>& GetAllTokens() const override {
            return AllTokens;
        }

        const std::unordered_set<TTokenId>& GetKeywordTokens() const override {
            return KeywordTokens;
        }

        const std::unordered_set<TTokenId>& GetPunctuationTokens() const override {
            return PunctuationTokens;
        }

        const std::string& SymbolizedRule(TRuleId rule) const override {
            return Parser->getRuleNames().at(rule);
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

        const THolder<antlr4::Parser> Parser;
        const std::unordered_set<TTokenId> AllTokens;
        const std::unordered_set<TTokenId> KeywordTokens;
        const std::unordered_set<TTokenId> PunctuationTokens;
    };

    const ISqlGrammar& GetSqlGrammar() {
        const static TSqlGrammar DefaultSqlGrammar(NSQLReflect::LoadLexerGrammar());
        return DefaultSqlGrammar;
    }

} // namespace NSQLComplete
