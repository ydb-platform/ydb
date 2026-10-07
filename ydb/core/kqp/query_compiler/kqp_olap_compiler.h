#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <yql/essentials/core/yql_type_annotation.h>

namespace NKikimr {
namespace NKqp {

void CompileOlapProgram(const NYql::NNodes::TCoLambda& lambda, const NYql::TKikimrTableMetadata& tableMeta,
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto, const std::vector<std::string>& resultColNames, NYql::TExprContext &exprCtx,
    NYql::TTypeAnnotationContext& typesCtx);

} // namespace NKqp
} // namespace NKikimr
