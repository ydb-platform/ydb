#pragma once

#include <ydb/core/protos/index_builder.pb.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NYql {
struct TKikimrTableMetadata;
class TKikimrSessionContext;

bool CompileGeneratedColumnBackfill(const TExprNode::TPtr& typedLambda, TExprContext& ctx,
    TTypeAnnotationContext& typeCtx, const NKikimr::NMiniKQL::IFunctionRegistry& registry,
    NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor);

// Binds only existing ordinary physical columns and applies the CREATE conversion rules.
TExprNode::TPtr PrepareGeneratedColumnBackfill(const TString& expression, const TString& columnName,
    const TTypeAnnotationNode& outputType, const TKikimrTableMetadata& table,
    TKikimrSessionContext& sessionCtx, TExprContext& ctx, TTypeAnnotationContext& typeCtx,
    NKikimrIndexBuilder::TGeneratedColumnBuild* descriptor = nullptr);
} // namespace NYql
