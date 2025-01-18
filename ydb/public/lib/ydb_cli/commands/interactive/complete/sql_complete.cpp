#include "sql_complete.h"

#include "c3_engine.h"
#include "string_util.h"
#include "sql_syntax.h"

#include <yql/essentials/sql/v1/format/sql_format.h>

#include <util/stream/output.h>

#define RULE_(mode, name) NALP##mode##Antlr4::SQLv1Antlr4Parser::Rule##name

#define RULE(name) RULE_(Default, name)

#define STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(name) \
    static_assert(RULE_(Default, name) == RULE_(Ansi, name))

namespace NSQLComplete {

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

    const antlr4::dfa::Vocabulary& GetVocabulary(ESqlSyntaxMode mode);
    std::unordered_set<TTokenId> GetAllTokens(ESqlSyntaxMode mode);
    std::unordered_set<TTokenId> GetKeywordTokens(ESqlSyntaxMode mode);
    std::unordered_set<TTokenId> GetIgnoredTokens(ESqlSyntaxMode mode);
    std::unordered_set<TRuleId> GetPreferredRules(ESqlSyntaxMode mode);
    IC3Engine::TConfig GetEngineConfig(ESqlSyntaxMode mode);

    size_t LastWordIndex(TStringBuf text);
    TCompletedToken GetCompletedToken(TStringBuf prefix);

    bool IsIdKeyword(const TSuggestedToken& token);

    TVector<TString> SiftedKeywords(
        const TVector<TSuggestedToken>& tokens,
        const antlr4::dfa::Vocabulary& vocabulary);

    void EnrichWithKeywords(TVector<TCandidate>& candidates, TVector<TString> keywords);

    void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix);

    void RankingSort(TVector<TCandidate>& candidates);

    TSqlCompletionEngine::TSqlCompletionEngine()
        : DefaultEngine(GetEngineConfig(ESqlSyntaxMode::Default))
        , AnsiEngine(GetEngineConfig(ESqlSyntaxMode::ANSI))
        , DefaultKeywordTokens(NSQLComplete::GetKeywordTokens(ESqlSyntaxMode::Default))
        , AnsiKeywordTokens(NSQLComplete::GetKeywordTokens(ESqlSyntaxMode::ANSI))
    {
    }

    TCompletion TSqlCompletionEngine::Complete(TCompletionInput input) {
        auto prefix = input.Text.Head(input.CursorPosition);

        auto completedToken = GetCompletedToken(prefix);

        auto mode = QuerySyntaxMode(TString(prefix));
        auto& c3 = GetEngine(mode);

        auto tokens = c3.Complete(prefix);
        std::ranges::remove_if(tokens, IsIdKeyword);

        TVector<TCandidate> candidates;
        EnrichWithKeywords(candidates, SiftedKeywords(tokens, mode));

        FilterByContent(candidates, completedToken.Content);

        RankingSort(candidates);

        return {
            .CompletedToken = std::move(completedToken),
            .Candidates = std::move(candidates),
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

    TVector<TString> TSqlCompletionEngine::SiftedKeywords(const TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode) {
        const auto& vocabulary = GetVocabulary(mode);
        const auto& keywordTokens = GetKeywordTokens(mode);

        TVector<TString> keywords;
        for (const auto& token : tokens) {
            if (keywordTokens.contains(token.Number)) {
                keywords.emplace_back(vocabulary.getDisplayName(token.Number));
            }
        }
        return keywords;
    }

    const std::unordered_set<TTokenId>& TSqlCompletionEngine::GetKeywordTokens(
        ESqlSyntaxMode mode) {
        switch (mode) {
            case ESqlSyntaxMode::Default:
                return DefaultKeywordTokens;
            case ESqlSyntaxMode::ANSI:
                return AnsiKeywordTokens;
        }
    }

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

    std::unordered_set<TTokenId> GetIgnoredTokens(ESqlSyntaxMode mode) {
        auto ignoredTokens = GetAllTokens(mode);

        for (auto keywordToken : GetKeywordTokens(mode)) {
            ignoredTokens.erase(keywordToken);
        }

        return ignoredTokens;
    }

    std::unordered_set<TRuleId> GetPreferredRules(ESqlSyntaxMode mode) {
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

        std::unordered_set<TRuleId> preferredRules;
        preferredRules.insert(std::begin(KeywordRules), std::end(KeywordRules));
        return preferredRules;
    }

    IC3Engine::TConfig GetEngineConfig(ESqlSyntaxMode mode) {
        return {
            .IgnoredTokens = GetIgnoredTokens(mode),
            .PreferredRules = GetPreferredRules(mode),
        };
    }

    TCompletedToken GetCompletedToken(TStringBuf prefix) {
        return {
            .Content = LastWord(prefix),
            .SourcePosition = LastWordIndex(prefix),
        };
    }

    bool IsIdKeyword(const TSuggestedToken& token) {
        return AnyOf(token.ParserCallStack, [&](TRuleId rule) {
            return Find(KeywordRules, rule) != std::end(KeywordRules);
        });
    }

    void EnrichWithKeywords(
        TVector<TCandidate>& candidates,
        TVector<TString> keywords) {
        for (auto keyword : keywords) {
            candidates.push_back({
                .Kind = ECandidateKind::Keyword,
                .Content = std::move(keyword),
            });
        }
    }

    void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix) {
        auto removed = std::ranges::remove_if(candidates, [&](const auto& candidate) {
            return !ToLowerUTF8(candidate.Content).StartsWith(prefix);
        });
        candidates.erase(std::begin(removed), std::end(removed));
    }

    void RankingSort(TVector<TCandidate>& candidates) {
        Sort(candidates, [](const TCandidate& lhs, const TCandidate& rhs) {
            return std::tie(lhs.Content, lhs.Kind) < std::tie(rhs.Content, rhs.Kind);
        });
    }

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::ECandidateKind>(IOutputStream& out, NSQLComplete::ECandidateKind kind) {
    switch (kind) {
        case NSQLComplete::ECandidateKind::Keyword:
            out << "Keyword";
            break;
    }
}

template <>
void Out<NSQLComplete::TCandidate>(IOutputStream& out, const NSQLComplete::TCandidate& candidate) {
    out << "(" << candidate.Kind << ": " << candidate.Content << ")";
}
