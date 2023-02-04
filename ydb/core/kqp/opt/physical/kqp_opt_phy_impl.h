#pragma once

#include <ydb/core/kqp/opt/kqp_opt_impl.h>

#include <ydb/library/yql/dq/opt/dq_opt_phy.h>

namespace NKikimr::NKqp::NOpt {

NYql::NNodes::TExprBase BuildReadNode(NYql::TPositionHandle pos, NYql::TExprContext& ctx,
    NYql::NNodes::TExprBase input, NYql::TKqpReadTableSettings& settings);

NYql::NNodes::TCoAtom GetReadTablePath(NYql::NNodes::TExprBase input, bool isReadRanges);

NYql::TKqpReadTableSettings GetReadTableSettings(NYql::NNodes::TExprBase input, bool isReadRanges);

NYql::NNodes::TMaybeNode<NYql::NNodes::TDqPhyPrecompute> BuildLookupKeysPrecompute(
    const NYql::NNodes::TExprBase& input, NYql::TExprContext& ctx);

NYql::NNodes::TCoAtomList BuildColumnsList(const THashSet<TStringBuf>& columns, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TCoAtomList BuildColumnsList(const TVector<TString>& columns, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TDqStage ReplaceStageArg(NYql::NNodes::TDqStage stage, size_t inputIndex,
    NYql::NNodes::TCoArgument replaceArg, NYql::NNodes::TExprBase bodyExpression, NYql::TExprContext& ctx);

NYql::NNodes::TDqStage ReplaceTableSourceSettings(NYql::NNodes::TDqStage stage, size_t inputIndex,
    NYql::NNodes::TKqpReadRangesSourceSettings settings, NYql::TExprContext& ctx);

} // NKikimr::NKqp::NOpt



