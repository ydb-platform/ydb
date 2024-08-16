#pragma once

#include "sql_translation.h"
#include <ydb/library/yql/core/yql_atom_enums.h>

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

    TNodePtr Build(const TRule_expr& node);

    void SetSmartParenthesisMode(ESmartParenthesis mode) {
        SmartParenthesisMode = mode;
    }

    void MarkAsNamed() {
        MaybeUnnamedSmartParenOnTop = false;
    }

    TMaybe<TExprOrIdent> LiteralExpr(const TRule_literal_value& node);
private:
    struct TTrailingQuestions {
        size_t Count = 0;
        TPosition Pos;
    };

    TNodePtr BindParameterRule(const TRule_bind_parameter& rule, const TTrailingQuestions& tail);
    TNodePtr LambdaRule(const TRule_lambda& rule);
    TNodePtr CastRule(const TRule_cast_expr& rule);
    TNodePtr BitCastRule(const TRule_bitcast_expr& rule);
    TNodePtr ExistsRule(const TRule_exists_expr& rule);
    TNodePtr CaseRule(const TRule_case_expr& rule);

    TMaybe<TExprOrIdent> AtomExpr(const TRule_atom_expr& node, const TTrailingQuestions& tail);
    TMaybe<TExprOrIdent> InAtomExpr(const TRule_in_atom_expr& node, const TTrailingQuestions& tail);

    TNodePtr JsonInputArg(const TRule_json_common_args& node);
    TNodePtr JsonPathSpecification(const TRule_jsonpath_spec& node);
    TNodePtr JsonReturningTypeRule(const TRule_type_name_simple& node);
    TNodePtr JsonValueCaseHandler(const TRule_json_case_handler& node, EJsonValueHandlerMode& mode);
    void AddJsonValueCaseHandlers(const TRule_json_value& node, TVector<TNodePtr>& children);
    void AddJsonVariable(const TRule_json_variable& node, TVector<TNodePtr>& children);
    void AddJsonVariables(const TRule_json_variables& node, TVector<TNodePtr>& children);
    TNodePtr JsonVariables(const TRule_json_common_args& node);
    void AddJsonCommonArgs(const TRule_json_common_args& node, TVector<TNodePtr>& children);
    TNodePtr JsonValueExpr(const TRule_json_value& node);
    void AddJsonExistsHandler(const TRule_json_exists& node, TVector<TNodePtr>& children);
    TNodePtr JsonExistsExpr(const TRule_json_exists& node);
    EJsonQueryWrap JsonQueryWrapper(const TRule_json_query& node);
    EJsonQueryHandler JsonQueryHandler(const TRule_json_query_handler& node);
    TNodePtr JsonQueryExpr(const TRule_json_query& node);
    TNodePtr JsonApiExpr(const TRule_json_api_expr& node);

    template<typename TUnaryCasualExprRule>
    TNodePtr UnaryCasualExpr(const TUnaryCasualExprRule& node, const TTrailingQuestions& tail);

    template<typename TUnarySubExprRule>
    TNodePtr UnaryExpr(const TUnarySubExprRule& node, const TTrailingQuestions& tail);

    bool SqlLambdaParams(const TNodePtr& node, TVector<TSymbolNameWithPos>& args, ui32& optionalArgumentsCount);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_expr& node, TVector<TNodePtr>& exprSeq);

    TNodePtr KeyExpr(const TRule_key_expr& node) {
        TSqlExpression expr(Ctx, Mode);
        return expr.Build(node.GetRule_expr2());
    }

    TNodePtr SubExpr(const TRule_con_subexpr& node, const TTrailingQuestions& tail);
    TNodePtr SubExpr(const TRule_xor_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_mul_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_add_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_bit_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_neq_subexpr& node, const TTrailingQuestions& tailExternal);

    TNodePtr SubExpr(const TRule_eq_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_or_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_and_subexpr& node, const TTrailingQuestions& tail);

    template <typename TNode, typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    template <typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TRule_bit_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    template <typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TRule_eq_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    TNodePtr BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const;

    TNodePtr RowPatternVarAccess(const TString& alias, const TRule_unary_subexpr_suffix_TBlock1_TBlock1_TAlt3_TBlock2 block);

    struct TCaseBranch {
        TNodePtr Pred;
        TNodePtr Value;
    };
    TCaseBranch ReduceCaseBranches(TVector<TCaseBranch>::const_iterator begin, TVector<TCaseBranch>::const_iterator end) const;

    template <typename TNode, typename TGetNode, typename TIter>
    TNodePtr BinOper(const TString& operName, const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    TNodePtr SqlInExpr(const TRule_in_expr& node, const TTrailingQuestions& tail);

    void UnexpectedQuestionToken(const TTrailingQuestions& tail) {
        YQL_ENSURE(tail.Count > 0);
        Ctx.Error(tail.Pos) << "Unexpected token '?' at the end of expression";
    }

    TNodePtr SmartParenthesis(const TRule_smart_parenthesis& node);

    ESmartParenthesis SmartParenthesisMode = ESmartParenthesis::Default;
    bool MaybeUnnamedSmartParenOnTop = true;

    THashMap<TString, TNodePtr> ExprShortcuts;
};

bool ChangefeedSettingsEntry(const TRule_changefeed_settings_entry& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter);

bool ChangefeedSettings(const TRule_changefeed_settings& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter);

bool CreateChangefeed(const TRule_changefeed& node, TSqlExpression& ctx, TVector<TChangefeedDescription>& changefeeds);

bool Expr(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr& node);

bool ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node);

} // namespace NSQLTranslationV1
