#include "local.h"

#include "ansi.h"
#include "grammar.h"
#include "parser_call_stack.h"
#include "token.h"

#include <yql/essentials/sql/v1/complete/antlr4/c3i.h>
#include <yql/essentials/sql/v1/complete/antlr4/c3t.h>
#include <yql/essentials/sql/v1/complete/antlr4/vocabulary.h>

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

    template <std::regular_invocable<TParserCallStack> StackPredicate>
    std::regular_invocable<TMatchedRule> auto RuleAdapted(StackPredicate predicate) {
        return [=](const TMatchedRule& rule) {
            return predicate(rule.ParserCallStack);
        };
    }

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
            TCompletionInput statement;
            if (!GetStatement(Lexer_, input, statement)) {
                return {};
            }

            auto candidates = C3.Complete(statement);

            TParsedTokenList tokens;
            TCaretTokenPosition caret;
            if (!TokenizePrefix(statement, tokens, caret)) {
                return {};
            }

            if (IsCaretEnslosed(tokens, caret)) {
                return {};
            }

            return {
                .Keywords = SiftedKeywords(candidates),
                .Pragma = PragmaMatch(tokens, candidates),
                .IsTypeName = IsTypeNameMatched(candidates),
                .Function = FunctionMatch(tokens, candidates),
                .Hint = HintMatch(candidates),
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
            for (auto punctuationToken : Grammar->GetPunctuationTokens()) {
                ignoredTokens.erase(punctuationToken);
            }
            return ignoredTokens;
        }

        std::unordered_set<TRuleId> ComputePreferredRules() {
            return GetC3PreferredRules();
        }

        TLocalSyntaxContext::TKeywords SiftedKeywords(const TC3Candidates& candidates) {
            const auto& vocabulary = Grammar->GetVocabulary();
            const auto& keywordTokens = Grammar->GetKeywordTokens();

            TLocalSyntaxContext::TKeywords keywords;
            for (const auto& token : candidates.Tokens) {
                if (keywordTokens.contains(token.Number)) {
                    auto& following = keywords[Display(vocabulary, token.Number)];
                    for (auto next : token.Following) {
                        following.emplace_back(Display(vocabulary, next));
                    }
                }
            }
            return keywords;
        }

        std::optional<TLocalSyntaxContext::TPragma> PragmaMatch(
            const TParsedTokenList& tokens, const TC3Candidates& candidates) {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyPragmaStack))) {
                return std::nullopt;
            }

            TLocalSyntaxContext::TPragma pragma;
            if (EndsWith(tokens, {"ID_PLAIN", "DOT"})) {
                pragma.Namespace = tokens[tokens.size() - 2].Content;
            } else if (EndsWith(tokens, {"ID_PLAIN", "DOT", ""})) {
                pragma.Namespace = tokens[tokens.size() - 3].Content;
            }
            return pragma;
        }

        bool IsTypeNameMatched(const TC3Candidates& candidates) {
            return AnyOf(candidates.Rules, RuleAdapted(IsLikelyTypeStack));
        }

        std::optional<TLocalSyntaxContext::TFunction> FunctionMatch(
            const TParsedTokenList& tokens, const TC3Candidates& candidates) {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyFunctionStack))) {
                return std::nullopt;
            }

            TLocalSyntaxContext::TFunction function;
            if (EndsWith(tokens, {"ID_PLAIN", "NAMESPACE"})) {
                function.Namespace = tokens[tokens.size() - 2].Content;
            } else if (EndsWith(tokens, {"ID_PLAIN", "NAMESPACE", ""})) {
                function.Namespace = tokens[tokens.size() - 3].Content;
            }
            return function;
        }

        std::optional<TLocalSyntaxContext::THint> HintMatch(const TC3Candidates& candidates) {
            // TODO(YQL-19747): detect local contexts with a single iteration through the candidates.Rules
            auto rule = FindIf(candidates.Rules, RuleAdapted(IsLikelyHintStack));
            if (rule == std::end(candidates.Rules)) {
                return std::nullopt;
            }

            auto stmt = StatementKindOf(rule->ParserCallStack);
            if (stmt == std::nullopt) {
                return std::nullopt;
            }

            return TLocalSyntaxContext::THint{
                .StatementKind = *stmt,
            };
        }

        bool TokenizePrefix(TCompletionInput input, TParsedTokenList& tokens, TCaretTokenPosition& caret) {
            NYql::TIssues issues;
            if (!NSQLTranslation::Tokenize(
                    *Lexer_, TString(input.Text), /* queryName = */ "",
                    tokens, issues, /* maxErrors = */ 1)) {
                return false;
            }

            Y_ENSURE(!tokens.empty() && tokens.back().Name == "EOF");
            tokens.pop_back();

            caret = CaretTokenPosition(tokens, input.CursorPosition);
            tokens.crop(caret.NextTokenIndex + 1);
            return true;
        }

        bool IsCaretEnslosed(const TParsedTokenList& tokens, TCaretTokenPosition caret) {
            if (tokens.empty() || caret.PrevTokenIndex != caret.NextTokenIndex) {
                return false;
            }

            const auto& token = tokens.back();
            return token.Name == "STRING_VALUE" ||
                   token.Name == "ID_QUOTED" ||
                   token.Name == "DIGIGTS" ||
                   token.Name == "INTEGER_VALUE" ||
                   token.Name == "REAL";
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
        return MakeHolder<TLocalSyntaxAnalysis>(lexer);
    }

} // namespace NSQLComplete
