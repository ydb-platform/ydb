#pragma once 
 
#include "dq_opt.h" 
 
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
 
namespace NYql::NDq { 
 
NNodes::TExprBase DqPeepholeRewriteCrossJoin(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteJoinDict(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewriteMapJoin(const NNodes::TExprBase& node, TExprContext& ctx); 
NNodes::TExprBase DqPeepholeRewriteReplicate(const NNodes::TExprBase& node, TExprContext& ctx);
NNodes::TExprBase DqPeepholeRewritePureJoin(const NNodes::TExprBase& node, TExprContext& ctx); 
 
} // namespace NYql::NDq 
