#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/guid.h>

namespace NYql::NDq {

NNodes::TCoAtom BuildAtom(TStringBuf value, TPositionHandle pos, TExprContext& ctx);
NNodes::TCoAtomList BuildAtomList(TStringBuf value, TPositionHandle pos, TExprContext& ctx);
NNodes::TCoLambda BuildIdentityLambda(TPositionHandle pos, TExprContext& ctx);

bool EnsureDqUnion(const NNodes::TExprBase& node, TExprContext& ctx);

const TNodeSet& GetConsumers(const NNodes::TExprBase& node, const TParentsMap& parentsMap);
const TNodeMultiSet& GetConsumers(const NNodes::TExprBase& node, const TParentsMultiMap& parentsMap);

ui32 GetConsumersCount(const NNodes::TExprBase& node, const TParentsMap& parentsMap);
bool IsSingleConsumer(const NNodes::TExprBase& node, const TParentsMap& parentsMap);

bool IsSingleConsumerConnection(const NNodes::TDqConnection& node, const TParentsMap& parentsMap, bool allowStageMultiUsage = true);

ui32 GetStageOutputsCount(const NNodes::TDqStageBase& stage);

void FindDqConnections(const NNodes::TExprBase& node, TVector<NNodes::TDqConnection>& connections, bool& isPure);
bool IsDqPureExpr(const NNodes::TExprBase& node, bool isPrecomputePure = true);
bool IsDqSelfContainedExpr(const NNodes::TExprBase& node);
bool IsDqDependsOnStage(const NNodes::TExprBase& node, const NNodes::TDqStageBase& stage);

bool CanPushDqExpr(const NNodes::TExprBase& expr, const NNodes::TDqStageBase& stage);
bool CanPushDqExpr(const NNodes::TExprBase& expr, const NNodes::TDqConnection& connection);

} // namespace NYql::NDq
