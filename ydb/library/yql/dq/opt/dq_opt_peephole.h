#pragma once

#include "dq_opt.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql::NDq {

NNodes::TExprBase DqPeepholeRewriteCrossJoin(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteJoinDict(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteMapJoinWithGraceCore(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteMapJoinWithMapCore(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteReplicate(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewritePureJoin(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeDropUnusedInputs(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteLength(const NNodes::TExprBase& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);

} // namespace NYql::NDq
