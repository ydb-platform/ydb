#include "sql_complete.h"

#include "c3_engine.h"
#include "sql_syntax.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <util/generic/algorithm.h>
#include <util/stream/output.h>

namespace NSQLComplete {

    template <ESqlSyntaxMode M>
    class TSpecializedSqlCompletionEngine: public ISqlCompletionEngine {
    private:
        using TDefaultYQLGrammar = TAntlrGrammar<
            NALPDefaultAntlr4::SQLv1Antlr4Lexer,
            NALPDefaultAntlr4::SQLv1Antlr4Parser>;

        using TAnsiYQLGrammar = TAntlrGrammar<
            NALPAnsiAntlr4::SQLv1Antlr4Lexer,
            NALPAnsiAntlr4::SQLv1Antlr4Parser>;

        using G = std::conditional_t<
            M == ESqlSyntaxMode::Default,
            TDefaultYQLGrammar,
            TAnsiYQLGrammar>;

    public:
        TSpecializedSqlCompletionEngine()
            : Grammar(MakeSqlGrammar(M))
            , C3(ComputeC3Config())
        {
        }

        TCompletionContext Complete(TCompletionInput input) override {
            auto prefix = input.Text.Head(input.CursorPosition);
            auto tokens = C3.Complete(prefix);
            FilterIdKeywords(tokens);
            return {
                .Keywords = SiftedKeywords(tokens),
            };
        }

    private:
        IC3Engine::TConfig ComputeC3Config() {
            return {
                .IgnoredTokens = ComputeIgnoredTokens(),
                .PreferredRules = ComputePreferredRules(),
            };
        }

        std::unordered_set<TTokenId> ComputeIgnoredTokens() {
            auto ignoredTokens = Grammar->GetAllTokens();
            for (auto keywordToken : Grammar->GetKeywordTokens()) {
                ignoredTokens.erase(keywordToken);
            }
            return ignoredTokens;
        }

        std::unordered_set<TRuleId> ComputePreferredRules() {
            const auto& keywordRules = Grammar->GetKeywordRules();

            std::unordered_set<TRuleId> preferredRules;
            preferredRules.insert(std::begin(keywordRules), std::end(keywordRules));
            return preferredRules;
        }

        void FilterIdKeywords(TVector<TSuggestedToken>& tokens) {
            const auto& keywordRules = Grammar->GetKeywordRules();
            std::ranges::remove_if(tokens, [&](const TSuggestedToken& token) {
                return AnyOf(token.ParserCallStack, [&](TRuleId rule) {
                    return Find(keywordRules, rule) != std::end(keywordRules);
                });
            });
        }

        TVector<std::string> SiftedKeywords(const TVector<TSuggestedToken>& tokens) {
            const auto& vocabulary = Grammar->GetVocabulary();
            const auto& keywordTokens = Grammar->GetKeywordTokens();

            TVector<std::string> keywords;
            for (const auto& token : tokens) {
                if (keywordTokens.contains(token.Number)) {
                    keywords.emplace_back(vocabulary.getDisplayName(token.Number));
                }
            }
            return keywords;
        }

        ISqlGrammar::TPtr Grammar;
        TC3Engine<G> C3;
    };

    class TSqlCompletionEngine: public ISqlCompletionEngine {
    public:
        TCompletionContext Complete(TCompletionInput input) override {
            auto mode = QuerySyntaxMode(TString(input.Text));
            auto& engine = GetSpecializedEngine(mode);
            return engine.Complete(std::move(input));
        }

    private:
        ISqlCompletionEngine& GetSpecializedEngine(ESqlSyntaxMode mode) {
            switch (mode) {
                case ESqlSyntaxMode::Default:
                    return DefaultEngine;
                case ESqlSyntaxMode::ANSI:
                    return AnsiEngine;
            }
        }

        TSpecializedSqlCompletionEngine<ESqlSyntaxMode::Default> DefaultEngine;
        TSpecializedSqlCompletionEngine<ESqlSyntaxMode::ANSI> AnsiEngine;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine() {
        return ISqlCompletionEngine::TPtr(new TSqlCompletionEngine());
    }

} // namespace NSQLComplete
