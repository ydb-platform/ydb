#pragma once

#include "kqp_expr_lowering.h"

namespace NYql {
struct TTypeAnnotationContext;
}

namespace NKikimr::NKqp::NNewYql {

using TColumnTypes = THashMap<TColumnId, const NYql::TTypeAnnotationNode*>;

// The lambda is the bridge between scalar typing and relational lowering.
// Its arguments are private to this expression and Inputs maps them back to
// resolved IDs; consumers substitute arguments, never re-resolve SQL names.
struct TAnnotatedExpression {
    // A typed lambda whose arguments correspond to Inputs. Its body is the
    // rewritten YQL expression, including coercions inserted by annotation.
    NYql::TExprNode::TPtr Lambda;
    TVector<TColumnId> Inputs;

    const NYql::TTypeAnnotationNode* GetType() const { return Lambda->Tail().GetTypeAnn(); }
};

// Relational values use IDs beyond the resolver's column vector. They are
// private to this annotation result and never modify the immutable AST.
struct TAnnotatedAggregate {
    TColumnId Output;
    const TExpression* Argument; // Null for COUNT(*).
    TString Function;
    bool Distinct = false;
};
struct TAnnotatedGrouping {
    TVector<std::pair<TColumnId, const TExpression*>> Keys;
    TVector<TAnnotatedAggregate> Aggregates;
    TVector<TVector<TColumnId>> Sets;
    THashMap<TColumnId, TColumnId> Indicators; // GROUPING result -> key.
};
struct TAnnotatedWindow {
    TColumnId Output;
    const TExpression* Argument;
    const TWindowSpecification* Specification;
    TString Function;
    bool Native = false;
};
enum class EAstSubquery { Scalar, Exists, In };
struct TAnnotatedSubquery {
    const TQuery* Query;
    EAstSubquery Kind;
    const TExpression* Comparison = nullptr;
};

// AST pointers are borrowed keys and type pointers belong to TExprContext.
// Keep both owners alive. Annotation stores rewritten expressions separately
// so the same immutable syntax and name bindings can be lowered again.
struct TAnnotatedAst {
    // Scalar roots of relational clauses: projections, predicates, sort keys,
    // LIMIT/OFFSET. A star projection has a tuple body in catalog column order.
    THashMap<const TExpression*, TAnnotatedExpression> Expressions;
    // Expression substitutions are shared with operator lowering. Scalar
    // lambdas refer to their IDs exactly as they refer to stored columns.
    THashMap<const TExpression*, TColumnId> Values;
    THashMap<const TQuery*, TAnnotatedGrouping> Groupings;
    THashMap<const TQuery*, TVector<TAnnotatedWindow>> Windows;
    THashMap<TColumnId, TAnnotatedSubquery> Subqueries;
    THashMap<const TQuery*, TColumnTypes> Captures;
    // Per-column operand conversions are the actual rewritten YQL lambdas,
    // including Just/optional mapping and decimal/numeric coercions.
    THashMap<const TQuery*, TVector<std::pair<TAnnotatedExpression, TAnnotatedExpression>>> SetConversions;
    TVector<const NYql::TTypeAnnotationNode*> Columns; // Indexed by resolved column ID.
    // Source-local output types preserve the distinction between an input's
    // schema and the nullable output of an enclosing outer join.
    THashMap<const TSource*, TColumnTypes> Sources;
    THashMap<const TQuery*, TVector<const NYql::TTypeAnnotationNode*>> QueryOutputs;
};

struct TAstTypeAnnotationSettings {
    TExpressionLoweringSettings Expressions;
    // Types belong to the destination expression context; names include '$'.
    THashMap<TString, const NYql::TTypeAnnotationNode*> Parameters;
};

// Annotates the supported scalar-query subset using the existing YQL callable
// type rules. The AST, resolved names and destination context must outlive the
// result. Neither syntax nor name bindings are changed. The caller supplies
// effective SQL expression settings; TablePathPrefix and the legacy translator's
// YqlSelect control pragma are accepted.
// Returns null on a type error, with diagnostics in ctx. Unsupported constructs
// throw TNewYqlParserFallback. No partial annotation result escapes either case.
// Does not run provider IO stages or reset ctx.Step.
std::unique_ptr<TAnnotatedAst> AnnotateAst(const TScript& script, const TResolvedNames& names,
    NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& types,
    const TAstTypeAnnotationSettings& settings = {});

} // namespace NKikimr::NKqp::NNewYql
