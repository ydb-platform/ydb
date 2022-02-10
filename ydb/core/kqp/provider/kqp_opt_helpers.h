#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

// Common opt helpers for "Old" and "New" engine.
// Should be in core/kqp/opt directory, but also use by Old engine from core/kqp/provider.

namespace NYql {
    class TKikimrTableDescription;
}

namespace NKikimr {
namespace NKqp {

NYql::NNodes::TExprBase ExtractKeys(NYql::NNodes::TCoArgument itemArg, const NYql::TKikimrTableDescription& tableDesc,
    NYql::TExprContext& ctx);

TVector<std::pair<NYql::TExprNode::TPtr, const NYql::TIndexDescription*>> BuildSecondaryIndexVector(
    const NYql::TKikimrTableDescription& table,
    NYql::TPositionHandle pos,
    NYql::TExprContext& ctx,
    const THashSet<TStringBuf>* filter,
    const std::function<NYql::NNodes::TExprBase (const NYql::TKikimrTableMetadata&, NYql::TPositionHandle, NYql::TExprContext&)>& tableBuilder);

TVector<NYql::NNodes::TExprBase> CreateColumnsToSelectToUpdateIndex(
    const TVector<std::pair<NYql::TExprNode::TPtr, const NYql::TIndexDescription*>> indexes,
    const TVector<TString>& pk,
    const THashSet<TString>& dataColumns,
    NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

THashSet<TString> CreateDataColumnSetToRead(
    const TVector<std::pair<NYql::TExprNode::TPtr, const NYql::TIndexDescription*>>& indexes,
    const THashSet<TStringBuf>& inputColumns);

NYql::NNodes::TExprBase RemoveDuplicateKeyFromInput(
    const NYql::NNodes::TExprBase& input, const NYql::TKikimrTableDescription& tableDesc,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

std::pair<NYql::NNodes::TExprBase, NYql::NNodes::TCoAtomList> CreateRowsToReplace(const NYql::NNodes::TExprBase& input,
    const NYql::NNodes::TCoAtomList& inputColumns, const NYql::TKikimrTableDescription& tableDesc,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

} // namespace NKqp
} // namespace NKikimr
