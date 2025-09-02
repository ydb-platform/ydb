#pragma once

#include "kqp_opt.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

namespace NKikimr::NKqp::NOpt {

void DumpAppliedRule(const TString& name, const NYql::TExprNode::TPtr& input,
    const NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx);

bool IsKqpPureLambda(const NYql::NNodes::TCoLambda& lambda);
bool IsKqpPureInputs(const NYql::NNodes::TExprList& inputs);

const NYql::TKikimrTableDescription& GetTableData(const NYql::TKikimrTablesData& tablesData,
    TStringBuf cluster, TStringBuf table);

NYql::NNodes::TExprBase ProjectColumns(const NYql::NNodes::TExprBase& input, const TVector<TString>& columnNames,
    NYql::TExprContext& ctx);
NYql::NNodes::TExprBase ProjectColumns(const NYql::NNodes::TExprBase& input, const TVector<TStringBuf>& columnNames,
    NYql::TExprContext& ctx);
NYql::NNodes::TExprBase ProjectColumns(const NYql::NNodes::TExprBase& input, const THashSet<TStringBuf>& columnNames,
    NYql::TExprContext& ctx);

NYql::NNodes::TKqpTable BuildTableMeta(const NYql::TKikimrTableDescription& tableDesc,
    const NYql::TPositionHandle& pos, NYql::TExprContext& ctx);
NYql::NNodes::TKqpTable BuildTableMeta(const NYql::TKikimrTableMetadata& tableMeta,
    const NYql::TPositionHandle& pos, NYql::TExprContext& ctx);

TIntrusivePtr<NYql::TKikimrTableMetadata> GetIndexMetadata(const NYql::NNodes::TKqlReadTableIndex& index,
    const NYql::TKikimrTablesData& tables, TStringBuf cluster);

TVector<std::pair<NYql::TExprNode::TPtr, const NYql::TIndexDescription*>> BuildSecondaryIndexVector(
    const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos, NYql::TExprContext& ctx,
    const THashSet<TStringBuf>* filter,
    const std::function<NYql::NNodes::TExprBase (const NYql::TKikimrTableMetadata&,
        NYql::TPositionHandle, NYql::TExprContext&)>& tableBuilder);

bool IsBuiltEffect(const NYql::NNodes::TExprBase& effect);

bool IsSortKeyPrimary(const NYql::NNodes::TCoLambda& keySelector, const NYql::TKikimrTableDescription& tableDesc,
    const TMaybe<THashSet<TStringBuf>>& passthroughFields = {});

enum ESortDirection : ui32 {
    None = 0,
    Forward = 1,
    Reverse = 2,
    Unknown = 4,
};

using ESortDirectionRaw = std::underlying_type<ESortDirection>::type;

inline ESortDirection operator|(ESortDirection a, ESortDirection b) {
    return ESortDirection(static_cast<ESortDirectionRaw>(a) | static_cast<ESortDirectionRaw>(b));
}

inline ESortDirection operator|=(ESortDirection& a, ESortDirection b) { return (a = a | b); }

ESortDirection GetSortDirection(const NYql::NNodes::TExprBase& sortDirections);

} // namespace NKikimr::NKqp::NOpt
