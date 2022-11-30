#pragma once

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/strbuf.h>


namespace NYql {

TExprNode::TPtr ApplyExtractMembersToTake(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToSkip(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToExtend(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToSkipNullMembers(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToFilterNullMembers(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToSort(const TExprNode::TPtr& node, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToAssumeUnique(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToTop(const TExprNode::TPtr& node, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToEquiJoin(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToFlatMap(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToPartitionByKey(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToCalcOverWindow(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToAggregate(const TExprNode::TPtr& node, const TExprNode::TPtr& members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToChopper(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToCollect(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToMapJoinCore(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToMapNext(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToChain1Map(const TExprNode::TPtr& node, TExprNode::TPtr members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToCondense1(const TExprNode::TPtr& node, TExprNode::TPtr members, const TParentsMap& parentsMap, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToCombineCore(const TExprNode::TPtr& node, const TExprNode::TPtr& members, TExprContext& ctx, TStringBuf logSuffix);
TExprNode::TPtr ApplyExtractMembersToNarrowMap(const TExprNode::TPtr& node, const TExprNode::TPtr& members, bool isFlat, TExprContext& ctx, TStringBuf logSuffix);

} // NYql
