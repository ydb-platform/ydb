#pragma once

#include "kqp_opt.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

namespace NKikimr::NKqp::NOpt {

static inline void DumpAppliedRule(const TString& name, const NYql::TExprNode::TPtr& input,
    const NYql::TExprNode::TPtr& output, NYql::TExprContext& ctx)
{
// #define KQP_ENABLE_DUMP_APPLIED_RULE
#ifdef KQP_ENABLE_DUMP_APPLIED_RULE
    if (input != output) {
        auto builder = TStringBuilder() << "Rule applied: " << name << Endl;
        builder << "Expression before rule application: " << Endl;
        builder << KqpExprToPrettyString(*input, ctx) << Endl;
        builder << "Expression after rule application: " << Endl;
        builder << KqpExprToPrettyString(*output, ctx);
        YQL_CLOG(TRACE, ProviderKqp) << builder;
    }
#else
    Y_UNUSED(ctx);
    if (input != output) {
        YQL_CLOG(TRACE, ProviderKqp) << name;
    }
#endif
}

bool IsKqpPureLambda(const NYql::NNodes::TCoLambda& lambda);
bool IsKqpPureInputs(const NYql::NNodes::TExprList& inputs);

const NYql::TKikimrTableDescription& GetTableData(const NYql::TKikimrTablesData& tablesData,
    TStringBuf cluster, TStringBuf table);

NYql::NNodes::TExprBase ProjectColumns(const NYql::NNodes::TExprBase& input, const TVector<TString>& columnNames,
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

} // namespace NKikimr::NKqp::NOpt
