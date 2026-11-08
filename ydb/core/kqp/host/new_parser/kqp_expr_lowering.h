#pragma once

#include "kqp_name_resolver.h"

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NKqp::NNewYql {

// The caller translates logical bindings into values in its own expression
// context. Lowering substitutes these nodes by identity, so self-join columns
// with equal names still remain different values.
struct TExpressionBindings {
    const TResolvedNames* Names = nullptr;
    // Bindings belong to the destination TExprContext. The relational caller
    // supplies the actual input expressions, including outer-join nullability.
    THashMap<TColumnId, NYql::TExprNode::TPtr> Columns;
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
// mutate the AST/bindings. Arguments supplied by the caller must be enclosed in
// their owning lambda before annotation. Keep the rewritten root returned by
// the annotator, which may insert coercions and replace callables.
// Returns null and adds a positioned diagnostic to ctx for invalid input.
// Unsupported SQL semantics throw TNewYqlParserFallback, as in name resolution.
NYql::TExprNode::TPtr LowerExpression(const TExpression& expression, const TScript& script,
    const TExpressionBindings& bindings, NYql::TExprContext& ctx,
    const TExpressionLoweringSettings& settings = {});

} // namespace NKikimr::NKqp::NNewYql
