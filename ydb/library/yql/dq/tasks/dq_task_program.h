#pragma once

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/tasks/dq_tasks_graph.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>

namespace NYql::NDq {

const TStructExprType* CollectParameters(NNodes::TCoLambda program, TExprContext& ctx);

TString BuildProgram(NNodes::TCoLambda program, const TStructExprType& paramsType,
                     const NCommon::IMkqlCallableCompiler& compiler, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
                     const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry, TExprContext& exprCtx,
                     const TVector<NNodes::TExprBase>& reads, const TSpillingSettings& spillingSettings);

} // namespace NYql::NDq
