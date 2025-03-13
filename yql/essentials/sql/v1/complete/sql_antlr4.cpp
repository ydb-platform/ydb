#include "sql_antlr4.h"

#include <yql/essentials/sql/v1/format/sql_format.h>

#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#define RULE_(mode, name) NALA##mode##Antlr4::SQLv1Antlr4Parser::Rule##name

#define RULE(name) RULE_(Default, name)

#define STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(name) \
    static_assert(RULE_(Default, name) == RULE_(Ansi, name))

namespace NSQLComplete {

    class TSqlGrammar: public ISqlGrammar {
    public:
        TSqlGrammar(bool isAnsiLexer)
            : Vocabulary(GetVocabulary(isAnsiLexer))
            , AllTokens(ComputeAllTokens())
            , KeywordTokens(ComputeKeywordTokens())
        {
        }

        const antlr4::dfa::Vocabulary& GetVocabulary() const override {
            return *Vocabulary;
        }

        const std::unordered_set<TTokenId>& GetAllTokens() const override {
            return AllTokens;
        }

        const std::unordered_set<TTokenId>& GetKeywordTokens() const override {
            return KeywordTokens;
        }

        const TVector<TRuleId>& GetKeywordRules() const override {
            static const TVector<TRuleId> KeywordRules = {
                RULE(Keyword),
                RULE(Keyword_expr_uncompat),
                RULE(Keyword_table_uncompat),
                RULE(Keyword_select_uncompat),
                RULE(Keyword_alter_uncompat),
                RULE(Keyword_in_uncompat),
                RULE(Keyword_window_uncompat),
                RULE(Keyword_hint_uncompat),
                RULE(Keyword_as_compat),
                RULE(Keyword_compat),
            };

            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_expr_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_table_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_select_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_alter_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_in_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_window_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_hint_uncompat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_as_compat);
            STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_compat);

            return KeywordRules;
        }

    private:
        static const antlr4::dfa::Vocabulary* GetVocabulary(bool isAnsiLexer) {
            if (isAnsiLexer) { // Taking a reference is okay as vocabulary storage is static
                return &NALAAnsiAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
            }
            return &NALADefaultAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
        }

        std::unordered_set<TTokenId> ComputeAllTokens() {
            const auto& vocabulary = GetVocabulary();

            std::unordered_set<TTokenId> allTokens;

            for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
                allTokens.emplace(type);
            }

            return allTokens;
        }

        std::unordered_set<TTokenId> ComputeKeywordTokens() {
            const auto& vocabulary = GetVocabulary();
            const auto keywords = NSQLFormat::GetKeywords();

            auto keywordTokens = GetAllTokens();
            std::erase_if(keywordTokens, [&](TTokenId token) {
                return !keywords.contains(vocabulary.getSymbolicName(token));
            });
            keywordTokens.erase(TOKEN_EOF);

            return keywordTokens;
        }

        const antlr4::dfa::Vocabulary* Vocabulary;
        const std::unordered_set<TTokenId> AllTokens;
        const std::unordered_set<TTokenId> KeywordTokens;
    };

    const ISqlGrammar& GetSqlGrammar(bool isAnsiLexer) {
        const static TSqlGrammar DefaultSqlGrammar(/* isAnsiLexer = */ false);
        const static TSqlGrammar AnsiSqlGrammar(/* isAnsiLexer = */ true);

        if (isAnsiLexer) {
            return AnsiSqlGrammar;
        }
        return DefaultSqlGrammar;
    }

} // namespace NSQLComplete
