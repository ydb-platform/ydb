#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql::NDq::NDphyp {

using TProviderCollectFunction = 
    std::function<void(TVector<std::shared_ptr<TRelOptimizerNode>>&, TStringBuf, const TExprNode::TPtr, const std::shared_ptr<TOptimizerStatistics>&)>;

NYql::NNodes::TExprBase DqOptimizeEquiJoinWithCosts(
    const NYql::NNodes::TExprBase& node,
    TExprContext& ctx,
    TTypeAnnotationContext& typesCtx,
    ui32 optLevel,
    IOptimizerNew& opt,
    const TProviderCollectFunction& providerCollect
);

} // namespace NYql::NDq
