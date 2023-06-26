#pragma once

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

namespace NKikimr::NKqp::NOpt {

using TSecondaryIndexes = TVector<std::pair<
    NYql::TExprNode::TPtr,
    const NYql::TIndexDescription*>>;

TSecondaryIndexes BuildSecondaryIndexVector(const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx, const THashSet<TStringBuf>* filter = nullptr);

NYql::TExprNode::TPtr MakeMessage(TStringBuf message, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

struct TCondenseInputResult {
    NYql::NNodes::TExprBase Stream;
    TVector<NYql::NNodes::TExprBase> StageInputs;
    TVector<NYql::NNodes::TCoArgument> StageArgs;
};

TMaybe<TCondenseInputResult> CondenseInput(const NYql::NNodes::TExprBase& input, NYql::TExprContext& ctx);

TMaybe<TCondenseInputResult> CondenseAndDeduplicateInput(const NYql::NNodes::TExprBase& input,
    const NYql::TKikimrTableDescription& table, NYql::TExprContext& ctx);

TMaybe<TCondenseInputResult> CondenseInputToDictByPk(const NYql::NNodes::TExprBase& input,
    const NYql::TKikimrTableDescription& table, const NYql::NNodes::TCoLambda& payloadSelector,
    NYql::TExprContext& ctx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TDqPhyPrecompute> PrecomputeTableLookupDict(
    const NYql::NNodes::TDqPhyPrecompute& lookupKeys, const NYql::TKikimrTableDescription& table,
    const THashSet<TString>& dataColumns, const THashSet<TString>& keyColumns, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TCoLambda MakeTableKeySelector(const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TCoLambda MakeRowsPayloadSelector(const NYql::NNodes::TCoAtomList& columns,
    const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase MakeRowsFromDict(const NYql::NNodes::TDqPhyPrecompute& dict, const TVector<TString>& dictKeys,
    const THashSet<TStringBuf>& columns, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TDqCnUnionAll> MakeConditionalInsertRows(const NYql::NNodes::TExprBase& input,
    const NYql::TKikimrTableDescription& table, bool abortOnError, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

enum class TKqpPhyUpsertIndexMode {
    Upsert,
    UpdateOn
};

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprList> KqpPhyUpsertIndexEffectsImpl(TKqpPhyUpsertIndexMode mode,
    const NYql::NNodes::TExprBase& inputRows, const NYql::NNodes::TCoAtomList& inputColumns,
    const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

} // NKikimr::NKqp::NOpt



