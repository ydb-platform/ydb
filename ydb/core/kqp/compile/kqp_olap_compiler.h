#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/protos/ssa.pb.h>

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

namespace NKikimr {
namespace NKqp {

void CompileOlapProgram(const NYql::NNodes::TCoLambda& lambda, const NYql::TKikimrTableMetadata& tableMeta,
    NKqpProto::TKqpPhyOpReadOlapRanges& readProto);

} // namespace NKqp
} // namespace NKikimr
