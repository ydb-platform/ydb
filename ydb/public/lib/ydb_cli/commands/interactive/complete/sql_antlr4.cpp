#include "sql_antlr4.h"

#include <yql/essentials/sql/v1/format/sql_format.h>

#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#define RULE_(mode, name) NALP##mode##Antlr4::SQLv1Antlr4Parser::Rule##name

#define RULE(name) RULE_(Default, name)

#define STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(name) \
    static_assert(RULE_(Default, name) == RULE_(Ansi, name))

namespace NSQLComplete {

    // Returning a reference is okay as vocabulary storage is static
    const antlr4::dfa::Vocabulary& GetVocabulary(ESqlSyntaxMode mode) {
        switch (mode) {
            case ESqlSyntaxMode::Default:
                return NALPDefaultAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
            case ESqlSyntaxMode::ANSI:
                return NALPAnsiAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
        }
    }

    std::unordered_set<TTokenId> GetAllTokens(ESqlSyntaxMode mode) {
        const auto& vocabulary = GetVocabulary(mode);

        std::unordered_set<TTokenId> allTokens;

        for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
            allTokens.emplace(type);
        }

        return allTokens;
    }

    std::unordered_set<TTokenId> GetKeywordTokens(ESqlSyntaxMode mode) {
        const auto& vocabulary = GetVocabulary(mode);
        const auto keywords = NSQLFormat::GetKeywords();

        auto keywordTokens = GetAllTokens(mode);
        std::erase_if(keywordTokens, [&](TTokenId token) {
            return !keywords.contains(vocabulary.getSymbolicName(token));
        });
        keywordTokens.erase(TOKEN_EOF);

        return keywordTokens;
    }

    const TVector<TRuleId>& GetKeywordRules(ESqlSyntaxMode mode) {
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

        Y_UNUSED(mode);

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

} // namespace NSQLComplete
