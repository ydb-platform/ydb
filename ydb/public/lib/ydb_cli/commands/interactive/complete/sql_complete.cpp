#include "sql_complete.h"

#include "c3_engine.h"
#include "sql_syntax.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr4_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <util/generic/algorithm.h>
#include <util/stream/output.h>

namespace NSQLComplete {

    class TSqlCompletionEngine : public ISqlCompletionEngine {
    private:
        using TDefaultYQLGrammar = TAntlrGrammar<
            NALPDefaultAntlr4::SQLv1Antlr4Lexer,
            NALPDefaultAntlr4::SQLv1Antlr4Parser>;

        using TAnsiYQLGrammar = TAntlrGrammar<
            NALPAnsiAntlr4::SQLv1Antlr4Lexer,
            NALPAnsiAntlr4::SQLv1Antlr4Parser>;

    public:
        TSqlCompletionEngine()
            : DefaultEngine(GetC3Config(ESqlSyntaxMode::Default))
            , AnsiEngine(GetC3Config(ESqlSyntaxMode::ANSI))
            , DefaultKeywordTokens(NSQLComplete::GetKeywordTokens(ESqlSyntaxMode::Default))
            , AnsiKeywordTokens(NSQLComplete::GetKeywordTokens(ESqlSyntaxMode::ANSI))
        {
        }

        TCompletionContext Complete(TCompletionInput input) override {
            auto prefix = input.Text.Head(input.CursorPosition);
            auto mode = QuerySyntaxMode(TString(prefix));

            auto& c3 = GetEngine(mode);

            auto tokens = c3.Complete(prefix);
            FilterIdKeywords(tokens, mode);

            return {
                .Keywords = SiftedKeywords(tokens, mode),
            };
        }

    private:
        static IC3Engine::TConfig GetC3Config(ESqlSyntaxMode mode) {
            return {
                .IgnoredTokens = GetIgnoredTokens(mode),
                .PreferredRules = GetPreferredRules(mode),
            };
        }

        static std::unordered_set<TTokenId> GetIgnoredTokens(ESqlSyntaxMode mode) {
            auto ignoredTokens = GetAllTokens(mode);
            for (auto keywordToken : NSQLComplete::GetKeywordTokens(mode)) {
                ignoredTokens.erase(keywordToken);
            }
            return ignoredTokens;
        }

        static std::unordered_set<TRuleId> GetPreferredRules(ESqlSyntaxMode mode) {
            const auto& keywordRules = NSQLComplete::GetKeywordRules(mode);

            std::unordered_set<TRuleId> preferredRules;
            preferredRules.insert(std::begin(keywordRules), std::end(keywordRules));
            return preferredRules;
        }

        static void FilterIdKeywords(TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode) {
            const auto& keywordRules = GetKeywordRules(mode);
            std::ranges::remove_if(tokens, [&](const TSuggestedToken& token) {
                return AnyOf(token.ParserCallStack, [&](TRuleId rule) {
                    return Find(keywordRules, rule) != std::end(keywordRules);
                });
            });
        }

        IC3Engine& GetEngine(ESqlSyntaxMode mode) {
            switch (mode) {
            case ESqlSyntaxMode::Default:
                return DefaultEngine;
            case ESqlSyntaxMode::ANSI:
                return AnsiEngine;
            }
        }

        TVector<std::string> SiftedKeywords(const TVector<TSuggestedToken>& tokens, ESqlSyntaxMode mode) {
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
        
        const std::unordered_set<TTokenId>& GetKeywordTokens(ESqlSyntaxMode mode) {
            switch (mode) {
                case ESqlSyntaxMode::Default:
                    return DefaultKeywordTokens;
                case ESqlSyntaxMode::ANSI:
                    return AnsiKeywordTokens;
            }
        }

        TC3Engine<TDefaultYQLGrammar> DefaultEngine;
        TC3Engine<TAnsiYQLGrammar> AnsiEngine;

        std::unordered_set<TTokenId> DefaultKeywordTokens;
        std::unordered_set<TTokenId> AnsiKeywordTokens;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine() {
        return ISqlCompletionEngine::TPtr(new TSqlCompletionEngine());
    }

} // namespace NSQLComplete
