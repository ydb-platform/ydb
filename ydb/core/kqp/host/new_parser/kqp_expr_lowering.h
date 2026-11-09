#pragma once

#include "kqp_name_resolver.h"

#include <yql/essentials/ast/yql_expr.h>

#include <functional>

namespace NKikimr::NKqp::NNewYql {

// The caller translates logical bindings into values in its own expression
// context. Lowering substitutes these nodes by identity, so self-join columns
// with equal names still remain different values.
struct TExpressionBindings {
    // Required for AST lambdas and named definitions: spelling alone cannot
    // distinguish a captured binding from a later redefinition of its name.
    const TResolvedNames* Names = nullptr;
    // Bindings belong to the destination TExprContext. The relational caller
    // supplies the actual input expressions, including outer-join nullability.
    THashMap<TColumnId, NYql::TExprNode::TPtr> Columns;
    // Relational expressions (aggregate results, grouped keys and sublinks)
    // become ordinary typed arguments at the scalar annotation boundary.
    THashMap<const TExpression*, NYql::TExprNode::TPtr> Expressions;
    // Optional lazy bindings. Called only for references actually reached by
    // lowering, after checking the corresponding explicit binding map. A null
    // result leaves the reference unresolved. Callbacks must return nodes owned
    // by the destination context and live only for this LowerExpression call.
    std::function<NYql::TExprNode::TPtr(TColumnId)> ResolveColumn;
    std::function<NYql::TExprNode::TPtr(const TExpression&)> ResolveExpression;
    THashMap<TString, NYql::TExprNode::TPtr> Parameters; // Includes '$'.
    // Already resolved callable values (e.g. Udf nodes or lambdas). Module
    // names use the decoded SQL spelling "Module::Function".
    THashMap<TString, NYql::TExprNode::TPtr> Callables;
};

// Supply effective SQL settings, not parser spelling defaults: these switches
// change callable selection and literal types. AnsiIn is tri-state because
// unspecified mode preserves the legacy warning behavior.
struct TExpressionLoweringSettings {
    bool ClassicDivision = true;
    bool CheckedOps = false;
    bool UnicodeLiterals = false;
    bool FlexibleTypes = false;
    std::optional<bool> AnsiIn;
};

// Produces ordinary YQL expressions for the existing type annotation pipeline
// (e.g. InstantAnnotateTypes with wholeProgram=false). Does not assign types or
// mutate the AST. Explicit binding maps are read directly; lazy callbacks may
// populate them. Arguments supplied by the caller must be enclosed in
// their owning lambda before annotation. Keep the rewritten root returned by
// the annotator, which may insert coercions and replace callables.
// Returns null and adds a positioned diagnostic to ctx for invalid input.
// Unsupported SQL semantics throw TNewYqlParserFallback, as in name resolution.
NYql::TExprNode::TPtr LowerExpression(const TExpression& expression, const TScript& script,
    const TExpressionBindings& bindings, NYql::TExprContext& ctx,
    const TExpressionLoweringSettings& settings = {});

} // namespace NKikimr::NKqp::NNewYql
