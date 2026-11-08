#pragma once

#include "kqp_expr_lowering.h"

namespace NYql {
struct TTypeAnnotationContext;
}

namespace NKikimr::NKqp::NNewYql {

using TColumnTypes = THashMap<TColumnId, const NYql::TTypeAnnotationNode*>;

struct TAnnotatedExpression {
    // A typed lambda whose arguments correspond to Inputs. Its body is the
    // rewritten YQL expression, including coercions inserted by annotation.
    NYql::TExprNode::TPtr Lambda;
    TVector<TColumnId> Inputs;

    const NYql::TTypeAnnotationNode* GetType() const { return Lambda->Tail().GetTypeAnn(); }
};

struct TAnnotatedAst {
    // Scalar roots of relational clauses: projections, predicates, sort keys,
    // LIMIT/OFFSET. A star projection has a tuple body in catalog column order.
    THashMap<const TExpression*, TAnnotatedExpression> Expressions;
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
