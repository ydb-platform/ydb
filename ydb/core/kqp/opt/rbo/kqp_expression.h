#pragma once

#include "kqp_info_unit.h"
#include "kqp_rbo_context.h"
#include "kqp_plan_props.h"
#include <ydb/core/kqp/common/kqp_yql.h>


namespace NKikimr {
namespace NKqp {

using namespace NYql;

/**
 * This is a wrapper class with convenient methods to work with expressions in YQL
 * Ideally it should be the single point of entry for all operations with expressions
 */
class TExpression {
  public:

    // Constructs an expression from ExprNode, also save expression context and plan
    // properties. Plan properties are needed to access subplan IUs
    // The expression can be constructed from a full lambda as well as any yql expression.
    // In the former case a new lambda is built automatically

    TExpression(TExprNode::TPtr node, TExprContext* ctx, TPlanProps* props = nullptr); 

    TExpression() = default;
    ~TExpression() = default;

    // Split a conjunct into a vector of expressions. If the is no conjunction at the top level,
    // just return a vector with this node. Handles pg conversions as well
    TVector<TExpression> SplitConjunct() const;

    // Check if the expression is just getting a single column from a tuple
    bool IsColumnAccess() const;

    // Check if the expression is a just a single callable on top of a column expression
    bool IsSingleCallable(const THashSet<TString>& allowedCallables) const;

    // Check if the expression is a cast
    bool IsCast() const;

    // Check is the expression can be folded
    bool IsConstantExpr() const;

    // Check if this is a potential join condition
    bool MaybeJoinCondition(bool includeExpressions = false) const;

    // Return the full lambda ExprNode of this expression
    TExprNode::TPtr GetLambda() const;

    // Return just the body part of the lambda
    TExprNode::TPtr GetExpressionBody() const;

    // Return all column references used in this expression
    // Optionally include columns that bind to subplan results and external columns inside correlated subqueries
    // If the result list of column references is not empty and plan properties are not set in the expression,
    // an exception will be thrown
    TVector<TInfoUnit> GetInputIUs(bool includeSubplanVars = false, bool includeCorrelatedDeps = false) const;

    // Rename column references in the expression
    TExpression ApplyRenames(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> &renameMap) const;

    // Apply a generic replace map to the lambda of the expression
    TExpression ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) const;

    // Remove a cast from the expression
    TExpression PruneCast() const;

    // Produce a pretty string for this expression
    TString ToString() const;

    TExprNode::TPtr Node;
    TExprContext* Ctx;
    TPlanProps* PlanProps;
};

/**
 * Model a generic potential join condition
 */
class TJoinCondition {
  public:

    TJoinCondition(const TExpression& expr);

    // In case this is a simple predicate that contains a single column reference on each side, return left column
    TInfoUnit GetLeftIU() const;

    // In case this is a simple predicate that contains a single column reference on each side, return right column
    TInfoUnit GetRightIU() const;

    // Find all non-column reference expression in this condition and insert them into a map
    bool ExtractExpressions(TNodeOnNodeOwnedMap& map, TVector<std::pair<TInfoUnit, TExprNode::TPtr>>& exprMap);

    const TExpression& Expr;
    TVector<TInfoUnit> LeftIUs;
    TVector<TInfoUnit> RightIUs;

    bool IncludesExpressions = true;
    bool EquiJoin = false;
};

// Create an expression that accesses a single column
TExpression MakeColumnAccess(const TInfoUnit& column, TPositionHandle pos, TExprContext* ctx, TPlanProps* props = nullptr);

// Create a constant expression. Constant expressions don't need plan properties
TExpression MakeConstant(const TString& type, const TString& value, TPositionHandle pos, TExprContext* ctx);

// Create. a null expression of a specific type, also doesn't need plan properties
TExpression MakeNothing(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext* ctx);

// Make a conjunction from a list of conjuncts. Expression context and plan properies will be extracted
// from one of the conjuncts.
TExpression MakeConjunction(const TVector<TExpression>& vec, bool pgSyntax = false);

// Make a binary predicate with an arbitrary callable, extract context and properties from one of the arguments
TExpression MakeBinaryPredicate(const TString& callable, const TExpression& left, const TExpression& right);

// Get all members from a expression node
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit>& IUs);

// Get all members from an expression node, but also mark subplan context separately and optionally include 
// dependencies in correlated subqueries
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit>& IUs, const TPlanProps& props, bool withSubplanContext, bool withDependencies);

TString PrintRBOExpression(TExprNode::TPtr expr, TExprContext& ctx);

}
}