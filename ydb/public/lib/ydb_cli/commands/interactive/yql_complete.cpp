#include "yql_complete.h"

#include "c3_engine.h"
#include "string_util.h"
#include "yql_name_source.h"
#include "yql_syntax.h"

#include <yql/essentials/sql/v1/format/sql_format.h>

#include <util/stream/output.h>

#define RULE_(mode, name) NALP##mode##Antlr4::SQLv1Antlr4Parser::Rule##name

#define RULE(name) RULE_(Default, name)

#define STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(name) \
    static_assert(RULE_(Default, name) == RULE_(Ansi, name))

namespace NYdb {
    namespace NConsoleClient {

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

        const antlr4::dfa::Vocabulary& GetVocabulary(EYQLSyntaxMode mode);
        std::unordered_set<TTokenId> GetIgnoredTokens(EYQLSyntaxMode mode);
        std::unordered_set<TRuleId> GetPreferredRules(EYQLSyntaxMode mode);
        IC3Engine::TConfig GetEngineConfig(EYQLSyntaxMode mode);

        size_t LastWordIndex(TStringBuf text);
        TCompletedToken GetCompletedToken(TStringBuf prefix);

        bool IsIdKeyword(const TSuggestedToken& token);

        TYQLCompletionEngine::TYQLCompletionEngine()
            : DefaultEngine(GetEngineConfig(EYQLSyntaxMode::Default))
            , AnsiEngine(GetEngineConfig(EYQLSyntaxMode::ANSI))
        {
        }

        TCompletion TYQLCompletionEngine::Complete(TStringBuf prefix) {
            auto completedToken = GetCompletedToken(prefix);

            auto& c3 = GetEngine(QuerySyntaxMode(TString(prefix)));

            auto tokens = c3.Complete(prefix);

            std::ranges::remove_if(tokens, IsIdKeyword);

            TYQLKeywordSource keywordSource([&] {
                TYQLNamesList names;
                for (const auto& token : tokens) {
                    auto content = c3.GetVocabulary().getDisplayName(token.Number);
                    names.emplace_back(std::move(content));
                }
                return names;
            }());

            TMap<ECandidateKind, TFuture<TYQLNamesList>> requests;
            requests.emplace(
                ECandidateKind::Keyword,
                keywordSource.GetNamesStartingWith(completedToken.Content));

            TVector<TCandidate> candidates;

            for (auto [kind, names] : requests) {
                for (auto& name : names.ExtractValueSync()) {
                    candidates.push_back({
                        .Kind = kind,
                        .Content = std::move(name),
                    });
                }
            }

            Sort(candidates, [](const TCandidate& lhs, const TCandidate& rhs) {
                return std::tie(lhs.Content, lhs.Kind) < std::tie(rhs.Content, rhs.Kind);
            });

            return {
                .CompletedToken = std::move(completedToken),
                .Candidates = std::move(candidates),
            };
        }

        IC3Engine& TYQLCompletionEngine::GetEngine(EYQLSyntaxMode mode) {
            switch (mode) {
                case EYQLSyntaxMode::Default:
                    return DefaultEngine;
                case EYQLSyntaxMode::ANSI:
                    return AnsiEngine;
            }
        }

        // Returning a reference is okay as vocabulary storage is static
        const antlr4::dfa::Vocabulary& GetVocabulary(EYQLSyntaxMode mode) {
            switch (mode) {
                case EYQLSyntaxMode::Default:
                    return NALPDefaultAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
                case EYQLSyntaxMode::ANSI:
                    return NALPAnsiAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
            }
        }

        std::unordered_set<TTokenId> GetIgnoredTokens(EYQLSyntaxMode mode) {
            std::unordered_set<TTokenId> ignoredTokens;

            const auto keywords = NSQLFormat::GetKeywords();
            const auto& vocabulary = GetVocabulary(mode);

            for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
                if (!keywords.contains(vocabulary.getSymbolicName(type))) {
                    ignoredTokens.emplace(type);
                }
            }

            ignoredTokens.emplace(TOKEN_EOF);
            return ignoredTokens;
        }

        std::unordered_set<TRuleId> GetPreferredRules(EYQLSyntaxMode mode) {
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

        IC3Engine::TConfig GetEngineConfig(EYQLSyntaxMode mode) {
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
            return AnyOf(token.ParseStack, [&](TRuleId rule) {
                return Find(KeywordRules, rule) != std::end(KeywordRules);
            });
        }

    } // namespace NConsoleClient
} // namespace NYdb

template <>
void Out<NYdb::NConsoleClient::ECandidateKind>(IOutputStream& out, NYdb::NConsoleClient::ECandidateKind kind) {
    switch (kind) {
        case NYdb::NConsoleClient::ECandidateKind::Keyword:
            out << "Keyword";
            break;
    }
}

template <>
void Out<NYdb::NConsoleClient::TCandidate>(IOutputStream& out, const NYdb::NConsoleClient::TCandidate& candidate) {
    out << "(" << candidate.Kind << ": " << candidate.Content << ")";
}
