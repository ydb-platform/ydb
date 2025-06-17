#include "local.h"

#include "cursor_token_context.h"
#include "parser_call_stack.h"

#include <yql/essentials/sql/v1/complete/antlr4/c3i.h>
#include <yql/essentials/sql/v1/complete/antlr4/c3t.h>
#include <yql/essentials/sql/v1/complete/antlr4/vocabulary.h>

#include <yql/essentials/sql/v1/complete/syntax/ansi.h>
#include <yql/essentials/sql/v1/complete/syntax/format.h>
#include <yql/essentials/sql/v1/complete/syntax/grammar.h>

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
        TSpecializedLocalSyntaxAnalysis(
            TLexerSupplier lexer, const THashSet<TString>& IgnoredRules)
            : Grammar_(&GetSqlGrammar())
            , Lexer_(lexer(/* ansi = */ IsAnsiLexer))
            , C3_(ComputeC3Config(IgnoredRules))
        {
        }

        TLocalSyntaxContext Analyze(TCompletionInput input) override {
            TMaterializedInput materialized = {
                .Text = TString(input.Text),
                .CursorPosition = input.CursorPosition,
            };

            // - ";" is for a correct stetement split
            // - "-- `" is for a ilformed ID_QUOTED recovery
            materialized.Text += "; -- `";

            TCompletionInput statement;
            size_t statement_position;
            if (!GetStatement(Lexer_, materialized, statement, statement_position)) {
                return {};
            }

            TCursorTokenContext context;
            if (!GetCursorTokenContext(Lexer_, statement, context)) {
                return {};
            }

            TC3Candidates candidates = C3Complete(statement, context);

            TLocalSyntaxContext result;

            result.EditRange = EditRange(context);
            result.EditRange.Begin += statement_position;

            if (auto enclosing = context.Enclosing()) {
                if (enclosing->IsLiteral()) {
                    return result;
                }

                if (enclosing->Base->Name == "ID_QUOTED") {
                    result.Object = ObjectMatch(context, candidates);
                    return result;
                }
            }

            result.Keywords = SiftedKeywords(candidates);
            result.Pragma = PragmaMatch(context, candidates);
            result.Type = TypeMatch(candidates);
            result.Function = FunctionMatch(context, candidates);
            result.Hint = HintMatch(candidates);
            result.Object = ObjectMatch(context, candidates);
            result.Cluster = ClusterMatch(context, candidates);
            result.Column = ColumnMatch(candidates);
            result.Binding = BindingMatch(candidates);

            return result;
        }

    private:
        IC3Engine::TConfig ComputeC3Config(const THashSet<TString>& IgnoredRules) const {
            return {
                .IgnoredTokens = ComputeIgnoredTokens(),
                .PreferredRules = ComputePreferredRules(),
                .IgnoredRules = ComputeIgnoredRules(IgnoredRules),
            };
        }

        std::unordered_set<TTokenId> ComputeIgnoredTokens() const {
            auto ignoredTokens = Grammar_->GetAllTokens();
            for (auto keywordToken : Grammar_->GetKeywordTokens()) {
                ignoredTokens.erase(keywordToken);
            }
            for (auto punctuationToken : Grammar_->GetPunctuationTokens()) {
                ignoredTokens.erase(punctuationToken);
            }
            return ignoredTokens;
        }

        std::unordered_set<TRuleId> ComputePreferredRules() const {
            return GetC3PreferredRules();
        }

        std::unordered_set<TRuleId> ComputeIgnoredRules(const THashSet<TString>& IgnoredRules) const {
            std::unordered_set<TRuleId> ignored;
            ignored.reserve(IgnoredRules.size());
            for (const auto& ruleName : IgnoredRules) {
                ignored.emplace(Grammar_->GetRuleId(ruleName));
            }
            return ignored;
        }

        TC3Candidates C3Complete(TCompletionInput statement, const TCursorTokenContext& context) {
            auto enclosing = context.Enclosing();

            size_t caretTokenIndex = context.Cursor.NextTokenIndex;
            if (enclosing.Defined()) {
                caretTokenIndex = enclosing->Index;
            }

            TStringBuf text = statement.Text;
            if (enclosing.Defined() && enclosing->Base->Name == "NOT_EQUALS2") {
                text = statement.Text.Head(statement.CursorPosition);
                caretTokenIndex += 1;
            }

            return C3_.Complete(text, caretTokenIndex);
        }

        TLocalSyntaxContext::TKeywords SiftedKeywords(const TC3Candidates& candidates) const {
            const auto& vocabulary = Grammar_->GetVocabulary();
            const auto& keywordTokens = Grammar_->GetKeywordTokens();

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

        TMaybe<TLocalSyntaxContext::TPragma> PragmaMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyPragmaStack))) {
                return Nothing();
            }

            TLocalSyntaxContext::TPragma pragma;

            if (TMaybe<TRichParsedToken> begin;
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "DOT"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "DOT", ""}))) {
                pragma.Namespace = begin->Base->Content;
            }
            return pragma;
        }

        bool TypeMatch(const TC3Candidates& candidates) const {
            return AnyOf(candidates.Rules, RuleAdapted(IsLikelyTypeStack));
        }

        TMaybe<TLocalSyntaxContext::TFunction> FunctionMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyFunctionStack))) {
                return Nothing();
            }

            TLocalSyntaxContext::TFunction function;
            if (TMaybe<TRichParsedToken> begin;
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "NAMESPACE"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "NAMESPACE", ""}))) {
                function.Namespace = begin->Base->Content;
            }
            return function;
        }

        TMaybe<TLocalSyntaxContext::THint> HintMatch(const TC3Candidates& candidates) const {
            // TODO(YQL-19747): detect local contexts with a single iteration through the candidates.Rules
            auto rule = FindIf(candidates.Rules, RuleAdapted(IsLikelyHintStack));
            if (rule == std::end(candidates.Rules)) {
                return Nothing();
            }

            auto stmt = StatementKindOf(rule->ParserCallStack);
            if (stmt.Empty()) {
                return Nothing();
            }

            return TLocalSyntaxContext::THint{
                .StatementKind = *stmt,
            };
        }

        TMaybe<TLocalSyntaxContext::TObject> ObjectMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            TLocalSyntaxContext::TObject object;

            if (AnyOf(candidates.Rules, RuleAdapted(IsLikelyObjectRefStack))) {
                object.Kinds.emplace(EObjectKind::Folder);
            }

            if (AnyOf(candidates.Rules, RuleAdapted(IsLikelyExistingTableStack))) {
                object.Kinds.emplace(EObjectKind::Folder);
                object.Kinds.emplace(EObjectKind::Table);
            }

            if (object.Kinds.empty()) {
                return Nothing();
            }

            if (TMaybe<TRichParsedToken> begin;
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "DOT"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "DOT", ""}))) {
                object.Cluster = begin->Base->Content;
            }

            if (TMaybe<TRichParsedToken> begin;
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "COLON", "ID_PLAIN", "DOT"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "COLON", "ID_PLAIN", "DOT", ""}))) {
                object.Provider = begin->Base->Content;
            }

            if (auto path = ObjectPath(context)) {
                object.Path = *path;
            }

            if (auto enclosing = context.Enclosing();
                enclosing.Defined() && enclosing->Base->Name == "ID_QUOTED") {
                object.IsQuoted = true;
            }

            return object;
        }

        TMaybe<TString> ObjectPath(const TCursorTokenContext& context) const {
            if (auto enclosing = context.Enclosing()) {
                TString path = enclosing->Base->Content;
                if (enclosing->Base->Name == "ID_QUOTED") {
                    path = Unquoted(std::move(path));
                    enclosing->Position += 1;
                }
                path.resize(context.Cursor.Position - enclosing->Position);
                return path;
            }
            return Nothing();
        }

        TMaybe<TLocalSyntaxContext::TCluster> ClusterMatch(
            const TCursorTokenContext& context, const TC3Candidates& candidates) const {
            if (!AnyOf(candidates.Rules, RuleAdapted(IsLikelyClusterStack))) {
                return Nothing();
            }

            TLocalSyntaxContext::TCluster cluster;
            if (TMaybe<TRichParsedToken> begin;
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "COLON"})) ||
                (begin = context.MatchCursorPrefix({"ID_PLAIN", "COLON", ""}))) {
                cluster.Provider = begin->Base->Content;
            }
            return cluster;
        }

        bool ColumnMatch(const TC3Candidates& candidates) const {
            return AnyOf(candidates.Rules, RuleAdapted(IsLikelyColumnStack));
        }

        bool BindingMatch(const TC3Candidates& candidates) const {
            return AnyOf(candidates.Rules, RuleAdapted(IsLikelyBindingStack));
        }

        TEditRange EditRange(const TCursorTokenContext& context) const {
            if (auto enclosing = context.Enclosing()) {
                return EditRange(*enclosing, context.Cursor);
            }

            return {
                .Begin = context.Cursor.Position,
                .Length = 0,
            };
        }

        TEditRange EditRange(const TRichParsedToken& token, const TCursor& cursor) const {
            size_t begin = token.Position;
            if (token.Base->Name == "NOT_EQUALS2") {
                begin += 1;
            }

            return {
                .Begin = begin,
                .Length = cursor.Position - begin,
            };
        }

        const ISqlGrammar* Grammar_;
        NSQLTranslation::ILexer::TPtr Lexer_;
        TC3Engine<G> C3_;
    };

    class TLocalSyntaxAnalysis: public ILocalSyntaxAnalysis {
    public:
        TLocalSyntaxAnalysis(
            TLexerSupplier lexer, const THashSet<TString>& IgnoredRules)
            : DefaultEngine_(lexer, IgnoredRules)
            , AnsiEngine_(lexer, IgnoredRules)
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
                return AnsiEngine_;
            }
            return DefaultEngine_;
        }

        TSpecializedLocalSyntaxAnalysis</* IsAnsiLexer = */ false> DefaultEngine_;
        TSpecializedLocalSyntaxAnalysis</* IsAnsiLexer = */ true> AnsiEngine_;
    };

    ILocalSyntaxAnalysis::TPtr MakeLocalSyntaxAnalysis(
        TLexerSupplier lexer, const THashSet<TString>& IgnoredRules) {
        return MakeHolder<TLocalSyntaxAnalysis>(lexer, IgnoredRules);
    }

} // namespace NSQLComplete
