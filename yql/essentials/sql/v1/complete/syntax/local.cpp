#include "local.h"

#include "ansi.h"
#include "parser_call_stack.h"
#include "grammar.h"

#include <yql/essentials/sql/v1/complete/antlr4/c3i.h>
#include <yql/essentials/sql/v1/complete/antlr4/c3t.h>

#include <yql/essentials/core/issue/yql_issue.h>

#include <util/generic/algorithm.h>
#include <util/stream/output.h>

#ifdef TOKEN_QUERY // Conflict with the winnt.h
    #undef TOKEN_QUERY
#endif
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

namespace NSQLComplete {

    template <bool IsAnsiLexer>
    class TSpecializedLocalSyntaxAnalysis: public ILocalSyntaxAnalysis {
    private:
        using TDefaultYQLGrammar = TAntlrGrammar<
            NALADefaultAntlr4::SQLv1Antlr4Lexer,
            NALADefaultAntlr4::SQLv1Antlr4Parser>;

        using TAnsiYQLGrammar = TAntlrGrammar<
            NALAAnsiAntlr4::SQLv1Antlr4Lexer,
            NALAAnsiAntlr4::SQLv1Antlr4Parser>;

        using G = std::conditional_t<
            IsAnsiLexer,
            TAnsiYQLGrammar,
            TDefaultYQLGrammar>;

    public:
        explicit TSpecializedLocalSyntaxAnalysis(TLexerSupplier lexer)
            : Grammar(&GetSqlGrammar())
            , Lexer_(lexer(/* ansi = */ IsAnsiLexer))
            , C3(ComputeC3Config())
        {
        }

        TLocalSyntaxContext Analyze(TCompletionInput input) override {
            TStringBuf prefix;
            if (!GetC3Prefix(input, &prefix)) {
                return {};
            }

            auto candidates = C3.Complete(prefix);
            return {
                .Keywords = SiftedKeywords(candidates),
                .IsTypeName = IsTypeNameMatched(candidates),
                .IsFunctionName = IsFunctionNameMatched(candidates),
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
            return GetC3PreferredRules();
        }

        bool GetC3Prefix(TCompletionInput input, TStringBuf* prefix) {
            *prefix = input.Text.Head(input.CursorPosition);

            TVector<TString> statements;
            NYql::TIssues issues;
            if (!NSQLTranslationV1::SplitQueryToStatements(
                    TString(*prefix) + (prefix->EndsWith(';') ? ";" : ""), Lexer_,
                    statements, issues, /* file = */ "",
                    /* areBlankSkipped = */ false)) {
                return false;
            }

            if (statements.empty()) {
                return true;
            }

            *prefix = prefix->Last(statements.back().size());
            return true;
        }

        TVector<TString> SiftedKeywords(const TC3Candidates& candidates) {
            const auto& vocabulary = Grammar->GetVocabulary();
            const auto& keywordTokens = Grammar->GetKeywordTokens();

            TVector<TString> keywords;
            for (const auto& token : candidates.Tokens) {
                if (keywordTokens.contains(token.Number)) {
                    keywords.emplace_back(vocabulary.getDisplayName(token.Number));
                }
            }
            return keywords;
        }

        bool IsTypeNameMatched(const TC3Candidates& candidates) {
            return AnyOf(candidates.Rules, [&](const TMatchedRule& rule) {
                return IsLikelyTypeStack(rule.ParserCallStack);
            });
        }

        bool IsFunctionNameMatched(const TC3Candidates& candidates) {
            return AnyOf(candidates.Rules, [&](const TMatchedRule& rule) {
                return IsLikelyFunctionStack(rule.ParserCallStack);
            });
        }

        const ISqlGrammar* Grammar;
        NSQLTranslation::ILexer::TPtr Lexer_;
        TC3Engine<G> C3;
    };

    class TLocalSyntaxAnalysis: public ILocalSyntaxAnalysis {
    public:
        explicit TLocalSyntaxAnalysis(TLexerSupplier lexer)
            : DefaultEngine(lexer)
            , AnsiEngine(lexer)
        {
        }

        TLocalSyntaxContext Analyze(TCompletionInput input) override {
            auto isAnsiLexer = IsAnsiQuery(TString(input.Text));
            auto& engine = GetSpecializedEngine(isAnsiLexer);
            return engine.Analyze(std::move(input));
        }

    private:
        ILocalSyntaxAnalysis& GetSpecializedEngine(bool isAnsiLexer) {
            if (isAnsiLexer) {
                return AnsiEngine;
            }
            return DefaultEngine;
        }

        TSpecializedLocalSyntaxAnalysis</* IsAnsiLexer = */ false> DefaultEngine;
        TSpecializedLocalSyntaxAnalysis</* IsAnsiLexer = */ true> AnsiEngine;
    };

    ILocalSyntaxAnalysis::TPtr MakeLocalSyntaxAnalysis(TLexerSupplier lexer) {
        return TLocalSyntaxAnalysis::TPtr(new TLocalSyntaxAnalysis(lexer));
    }

} // namespace NSQLComplete
