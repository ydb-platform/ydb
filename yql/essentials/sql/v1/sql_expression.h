#pragma once

#include "result.h"

#include "sql_translation.h"

#include <yql/essentials/core/sql_types/yql_atom_enums.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

class TSqlExpression: public TSqlTranslation {
public:
    enum class ESmartParenthesis {
        Default,
        GroupBy,
        InStatement,
        SqlLambdaParams,
    };

    TSqlExpression(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodeResult BuildSourceOrNode(const TRule_expr& node);
    TNodeResult Build(const TRule_expr& node);
    TNodeResult Build(const TRule_lambda_or_parameter& node);
    TSourcePtr BuildSource(const TRule_select_or_expr& node);
    TNodePtr BuildSourceOrNode(const TRule_smart_parenthesis& node);

    void SetSmartParenthesisMode(ESmartParenthesis mode) {
        SmartParenthesisMode_ = mode;
    }

    void MarkAsNamed() {
        MaybeUnnamedSmartParenOnTop_ = false;
    }

    void ProduceYqlColumnRef() {
        IsYqlColumnRefProduced_ = true;
    }

    TMaybe<TExprOrIdent> LiteralExpr(const TRule_literal_value& node);

private:
    struct TTrailingQuestions {
        size_t Count = 0;
        TPosition Pos;
    };

    TNodePtr BindParameterRule(const TRule_bind_parameter& rule, const TTrailingQuestions& tail);
    TNodeResult LambdaRule(const TRule_lambda& rule);
    TNodePtr CastRule(const TRule_cast_expr& rule);
    TNodePtr BitCastRule(const TRule_bitcast_expr& rule);
    TNodePtr ExistsRule(const TRule_exists_expr& rule);
    TNodePtr CaseRule(const TRule_case_expr& rule);

    TSQLResult<TExprOrIdent> AtomExpr(const TRule_atom_expr& node, const TTrailingQuestions& tail);
    TSQLResult<TExprOrIdent> InAtomExpr(const TRule_in_atom_expr& node, const TTrailingQuestions& tail);

    TNodeResult JsonInputArg(const TRule_json_common_args& node);
    TNodePtr JsonPathSpecification(const TRule_jsonpath_spec& node);
    TNodePtr JsonReturningTypeRule(const TRule_type_name_simple& node);
    TNodePtr JsonValueCaseHandler(const TRule_json_case_handler& node, EJsonValueHandlerMode& mode);
    void AddJsonValueCaseHandlers(const TRule_json_value& node, TVector<TNodePtr>& children);
    [[nodiscard]] TSQLStatus AddJsonVariable(const TRule_json_variable& node, TVector<TNodePtr>& children);
    [[nodiscard]] TSQLStatus AddJsonVariables(const TRule_json_variables& node, TVector<TNodePtr>& children);
    TNodeResult JsonVariables(const TRule_json_common_args& node);
    [[nodiscard]] TSQLStatus AddJsonCommonArgs(const TRule_json_common_args& node, TVector<TNodePtr>& children);
    TNodeResult JsonValueExpr(const TRule_json_value& node);
    void AddJsonExistsHandler(const TRule_json_exists& node, TVector<TNodePtr>& children);
    TNodeResult JsonExistsExpr(const TRule_json_exists& node);
    EJsonQueryWrap JsonQueryWrapper(const TRule_json_query& node);
    EJsonQueryHandler JsonQueryHandler(const TRule_json_query_handler& node);
    TNodeResult JsonQueryExpr(const TRule_json_query& node);
    TNodeResult JsonApiExpr(const TRule_json_api_expr& node);

    template <typename TUnaryCasualExprRule>
    TNodeResult UnaryCasualExpr(const TUnaryCasualExprRule& node, const TTrailingQuestions& tail);

    template <typename TUnarySubExprRule>
    TNodeResult UnaryExpr(const TUnarySubExprRule& node, const TTrailingQuestions& tail);

    bool SqlLambdaParams(const TNodePtr& node, TVector<TSymbolNameWithPos>& args, ui32& optionalArgumentsCount);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_expr& node, TVector<TNodePtr>& exprSeq);

    TNodePtr KeyExpr(const TRule_key_expr& node) {
        TSqlExpression expr(Ctx_, Mode_);
        return Unwrap(expr.Build(node.GetRule_expr2()));
    }

    TNodeResult SubExpr(const TRule_con_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_xor_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_mul_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_add_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_bit_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_neq_subexpr& node, const TTrailingQuestions& tailExternal);

    TNodeResult SubExpr(const TRule_eq_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_or_subexpr& node, const TTrailingQuestions& tail);

    TNodeResult SubExpr(const TRule_and_subexpr& node, const TTrailingQuestions& tail);

    template <typename TNode, typename TGetNode, typename TIter>
    TNodeResult BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    template <typename TGetNode, typename TIter>
    TNodeResult BinOpList(const TRule_bit_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    template <typename TGetNode, typename TIter>
    TNodeResult BinOpList(const TRule_eq_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    TNodePtr BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const;

    TNodePtr RowPatternVarAccess(TString var, const TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2 block);

    struct TCaseBranch {
        TNodePtr Pred;
        TNodePtr Value;
    };
    TCaseBranch ReduceCaseBranches(TVector<TCaseBranch>::const_iterator begin, TVector<TCaseBranch>::const_iterator end) const;

    template <typename TNode, typename TGetNode, typename TIter>
    TNodeResult BinOper(const TString& operName, const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    TNodePtr SqlInExpr(const TRule_in_expr& node, const TTrailingQuestions& tail);

    void UnexpectedQuestionToken(const TTrailingQuestions& tail) {
        YQL_ENSURE(tail.Count > 0);
        Ctx_.Error(tail.Pos) << "Unexpected token '?' at the end of expression";
    }

    bool IsTopLevelGroupBy() const;
    TSourcePtr LangVersionedSubSelect(TSourcePtr source);
    TNodeResult SelectSubExpr(const TRule_select_subexpr& node);
    TNodeResult SelectOrExpr(const TRule_select_or_expr& node);
    TNodeResult TupleOrExpr(const TRule_tuple_or_expr& node);
    TNodePtr EmptyTuple();
    TNodeResult SmartParenthesis(const TRule_smart_parenthesis& node);

    ESmartParenthesis SmartParenthesisMode_ = ESmartParenthesis::Default;
    bool MaybeUnnamedSmartParenOnTop_ = true;
    bool IsSourceAllowed_ = true;
    bool IsYqlColumnRefProduced_ = false;

    THashMap<TString, TNodePtr> ExprShortcuts_;
};

bool ChangefeedSettingsEntry(const TRule_changefeed_settings_entry& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter);

bool ChangefeedSettings(const TRule_changefeed_settings& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter);

bool CreateChangefeed(const TRule_changefeed& node, TSqlExpression& ctx, TVector<TChangefeedDescription>& changefeeds);

TSQLStatus Expr(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr& node);

TSQLStatus ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node);

} // namespace NSQLTranslationV1
