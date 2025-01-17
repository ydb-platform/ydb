#pragma once

#include "yql_pq_provider_impl.h"
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_pos_handle.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NYql {

void Add(TVector<NNodes::TCoNameValueTuple>& settings, TStringBuf name, TStringBuf value, TPositionHandle pos, TExprContext& ctx);

NNodes::TCoNameValueTupleList BuildTopicPropsList(const TPqState::TTopicMeta& meta, TPositionHandle pos, TExprContext& ctx);

void FindYdsDbIdsForResolving(
    const TPqState::TPtr& state,
    TExprNode::TPtr input,
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth>& ids);

void FillSettingsWithResolvedYdsIds(
    const TPqState::TPtr& state,
    const TDatabaseResolverResponse::TDatabaseDescriptionMap& fullResolvedIds);

NNodes::TMaybeNode<NNodes::TExprBase> FindSetting(TExprNode::TPtr settings, TStringBuf name);

} // namespace NYql
