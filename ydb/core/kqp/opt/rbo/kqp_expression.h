#pragma once

#include "kqp_info_unit.h"
#include "kqp_rbo_context.h"
#include "kqp_plan_props.h"

#include <optional>

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
    // Conversion-only source spellings. Never reconstruct this scope from
    // mutable registry labels after conversion.
    using TBindings = THashMap<TString, TInfoUnitId>;

    // Bind only Members of this lambda's row argument. Whole-row values are
    // reconstructed from those members, preserving their source struct shape.
    // Explicit replacements are already ID-bound (e.g. extracted sublinks).
    static TExpression FromExpr(TExprNode::TPtr lambda, const TBindings& bindings,
        TExprContext& ctx, TPlanProps& props, TNodeOnNodeOwnedMap replacements = {});

    // Internal construction: references already use decimal IDs. A bare body
    // is wrapped in a lambda, rebinding free row arguments, not inner locals.
    TExpression(TExprNode::TPtr node, TExprContext* ctx, TPlanProps* props = nullptr); 

    TExpression() = default;
    TExpression(const TExpression&) = default;
    TExpression(TExpression&&) noexcept = default;
    TExpression& operator=(const TExpression&) = default;
    TExpression& operator=(TExpression&&) noexcept = default;
    ~TExpression() = default;

    // Split a conjunct into a vector of expressions. If the is no conjunction at the top level,
    // just return a vector with this node.
    TVector<TExpression> SplitConjunct() const;

    // Split a disjunct into a vector of expressions. If the is no disjunction at the top level,
    // just return a vector with this node.
    TVector<TExpression> SplitDisjunct() const;

    // Check if the expression is just getting a single column from a tuple
    bool IsColumnAccess() const;

    // Check if the expression is a just a single callable on top of a column expression
    bool IsSingleCallable(const THashSet<TString>& allowedCallables) const;

    // Check if this is a potential equi-join condition
    bool MaybeEquiJoinCondition() const;

    // Check if this is a potential equi-join condition over simple expressions
    bool MaybeExprEquiJoinCondition() const;

    // Check if this is a potential comparison of a column with a constant
    bool MaybeConstantCondition() const;

    // Return the full lambda ExprNode of this expression
    TExprNode::TPtr GetLambda() const;

    // Return just the body part of the lambda
    TExprNode::TPtr GetExpressionBody() const;

    // Return all column references used in this expression
    // Optionally include columns that bind to subplan results and external columns inside correlated subqueries
    // Nonempty expressions need plan properties for subplan classification.
    // A subsequent GetInputIUs() call on the same
    // TExpression may refresh the returned buffer; do not retain references,
    // pointers, or iterators into it across calls.
    const TUnorderedIUs& GetInputIUs(bool includeSubplanVars = false, bool includeCorrelatedDeps = false) const Y_LIFETIME_BOUND;

    // Return direct row-reference IDs without subplan classification.
    // The result depends only on Node and is cached after the first AST traversal.
    const TUnorderedIUs& GetRawInputIUs() const Y_LIFETIME_BOUND;

    void BindPlanProps(TPlanProps* props) const {
        PlanProps = props;
    }

    // Simultaneous ID substitution, not a change to registry display labels.
    TExpression ApplyRenames(const TSubstitutions& substitutions) const;

    // Apply a generic replace map to the lambda of the expression
    TExpression ApplyReplaceMap(const TNodeOnNodeOwnedMap& map, TRBOContext& ctx) const;

    // Extract common conjuncts from OR branches.
    std::optional<TExpression> TryExtractCommonConjuncts() const;

    // Produce a pretty string for this expression
    TString ToString() const;

    // Produce a compact string suitable for explain output. Complex expressions are summarized by dependencies.
    TString ToExplainString(const TInfoUnitRegistry& registry) const;

    TExprNode::TPtr Node;
    TExprContext* Ctx = nullptr;
    mutable TPlanProps* PlanProps = nullptr;

  private:
    bool MaybeEquiJoinConditionInternal(bool includeExpressions) const;

    mutable TExprNode::TPtr RawInputIUsCacheKey;
    mutable TUnorderedIUs RawInputIUs;
    // Reusable scratch buffer. Every resolving call refreshes it against the
    // current subplan registry, so registry mutations cannot stale the result.
    mutable TUnorderedIUs ResolvedInputIUs;

};

/**
 * Model a generic potential join condition
 */
class TEquiJoinCondition {
  public:

    TEquiJoinCondition(const TExpression& expr);

    // In case this is a simple predicate that contains a single column reference on each side, return left column
    TInfoUnitId GetLeftIU() const;

    // In case this is a simple predicate that contains a single column reference on each side, return right column
    TInfoUnitId GetRightIU() const;

    // Find all non-column reference expression in this condition and insert them into a map
    bool ExtractExpressions(TNodeOnNodeOwnedMap& map, TMappedIUs<TExprNode::TPtr>& expressions);

    const TExpression& Expr;
    TUnorderedIUs LeftIUs;
    TUnorderedIUs RightIUs;

    bool IncludesExpressions = true;
};

// Create an expression that accesses a single column
TExpression MakeColumnAccess(TInfoUnitId column, TPositionHandle pos, TExprContext* ctx, TPlanProps* props = nullptr);

// Create a constant expression. Constant expressions don't need plan properties
TExpression MakeConstant(const TString& type, const TString& value, TPositionHandle pos, TExprContext* ctx);

// Create. a null expression of a specific type, also doesn't need plan properties
TExpression MakeNothing(TPositionHandle pos, const TTypeAnnotationNode* type, TExprContext* ctx);

// Make a conjunction from a list of conjuncts. Expression context and plan properies will be extracted
// from one of the conjuncts.
TExpression MakeConjunction(const TVector<TExpression>& vec, bool pgSyntax = false);

// Negate a predicate
TExpression MakeNegation(const TExpression& expr);

// Make a binary predicate with an arbitrary callable, extract context and properties from one of the arguments
TExpression MakeBinaryPredicate(const TString& callable, const TExpression& left, const TExpression& right);

// Make an unary callable
TExpression MakeUnaryCallable(const TString& callable, const TExpression& arg);

// Make ensure.
TExpression MakeEnsure(const TExpression& value, const TExpression& predicate, const TString& message);

// The ID a row argument's `Member` refers to.
TInfoUnitId GetMemberId(const TExprNode& member);

// Get all members from a expression node
void GetAllMembers(TExprNode::TPtr node, TVector<TInfoUnit>& IUs);

TString PrintRBOExpression(TExprNode::TPtr expr, TExprContext& ctx, const TInfoUnitRegistry* registry = nullptr);

}
}
