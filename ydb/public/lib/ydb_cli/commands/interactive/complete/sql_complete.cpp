#include "sql_complete.h"

#include "c3_engine.h"
#include "sql_syntax.h"

#include <util/generic/algorithm.h>
#include <util/stream/output.h>

#define RULE_(mode, name) NALP##mode##Antlr4::SQLv1Antlr4Parser::Rule##name

#define RULE(name) RULE_(Default, name)

#define STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(name) \
    static_assert(RULE_(Default, name) == RULE_(Ansi, name))

namespace NSQLComplete {

    const TVector<TRuleId>& GetKeywordRules(ESqlSyntaxMode mode);

    IC3Engine::TConfig GetC3Config(ESqlSyntaxMode mode);

    void FilterIdKeywords(TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode);

    TSqlCompletionEngine::TSqlCompletionEngine()
        : DefaultEngine(GetC3Config(ESqlSyntaxMode::Default))
        , AnsiEngine(GetC3Config(ESqlSyntaxMode::ANSI))
        , DefaultKeywordTokens(NSQLComplete::GetKeywordTokens(ESqlSyntaxMode::Default))
        , AnsiKeywordTokens(NSQLComplete::GetKeywordTokens(ESqlSyntaxMode::ANSI))
    {
    }

    TCompletionContext TSqlCompletionEngine::Complete(TCompletionInput input) {
        auto prefix = input.Text.Head(input.CursorPosition);
        auto mode = QuerySyntaxMode(TString(prefix));

        auto& c3 = GetEngine(mode);

        auto tokens = c3.Complete(prefix);
        FilterIdKeywords(tokens, mode);

        return {
            .Keywords = SiftedKeywords(tokens, mode),
        };
    }

    IC3Engine& TSqlCompletionEngine::GetEngine(ESqlSyntaxMode mode) {
        switch (mode) {
            case ESqlSyntaxMode::Default:
                return DefaultEngine;
            case ESqlSyntaxMode::ANSI:
                return AnsiEngine;
        }
    }

    TVector<std::string> TSqlCompletionEngine::SiftedKeywords(const TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode) {
        const auto& vocabulary = GetVocabulary(mode);
        const auto& keywordTokens = GetKeywordTokens(mode);

        TVector<std::string> keywords;
        for (const auto& token : tokens) {
            if (keywordTokens.contains(token.Number)) {
                keywords.emplace_back(vocabulary.getDisplayName(token.Number));
            }
        }
        return keywords;
    }

    const std::unordered_set<TTokenId>& TSqlCompletionEngine::GetKeywordTokens(ESqlSyntaxMode mode) {
        switch (mode) {
            case ESqlSyntaxMode::Default:
                return DefaultKeywordTokens;
            case ESqlSyntaxMode::ANSI:
                return AnsiKeywordTokens;
        }
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

    std::unordered_set<TTokenId> GetIgnoredTokens(ESqlSyntaxMode mode) {
        auto ignoredTokens = GetAllTokens(mode);
        for (auto keywordToken : GetKeywordTokens(mode)) {
            ignoredTokens.erase(keywordToken);
        }
        return ignoredTokens;
    }

    std::unordered_set<TRuleId> GetPreferredRules(ESqlSyntaxMode mode) {
        const auto& keywordRules = GetKeywordRules(mode);

        std::unordered_set<TRuleId> preferredRules;
        preferredRules.insert(std::begin(keywordRules), std::end(keywordRules));
        return preferredRules;
    }

    IC3Engine::TConfig GetC3Config(ESqlSyntaxMode mode) {
        return {
            .IgnoredTokens = GetIgnoredTokens(mode),
            .PreferredRules = GetPreferredRules(mode),
        };
    }

    void FilterIdKeywords(TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode) {
        const auto& keywordRules = GetKeywordRules(mode);
        std::ranges::remove_if(tokens, [&](const TSuggestedToken& token) {
            return AnyOf(token.ParserCallStack, [&](TRuleId rule) {
                return Find(keywordRules, rule) != std::end(keywordRules);
            });
        });
    }

} // namespace NSQLComplete
