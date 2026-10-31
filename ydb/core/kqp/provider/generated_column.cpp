#include "generated_column.h"
#include "yql_kikimr_provider.h"

#include <ydb/core/kqp/host/kqp_translate.h>

namespace NYql {

bool IsGeneratedColumn(const TKikimrColumnMetadata* column) {
    return column && column->IsDefaultFromExpression();
}

bool IsStoredGeneratedColumn(const TKikimrColumnMetadata* column) {
    return IsGeneratedColumn(column) && column->DefaultExpression && column->DefaultExpression->Stored;
}

bool IsVirtualGeneratedColumn(const TKikimrColumnMetadata* column) {
    return IsGeneratedColumn(column) && column->DefaultExpression && !column->DefaultExpression->Stored;
}

bool BuildGeneratedColumnGraph(const TKikimrTableMetadata& metadata,
    NKikimr::NGeneratedColumns::TGeneratedColumnDependencyGraph& graph, TString& error)
{
    TVector<TString> columns;
    TMap<TString, TVector<TString>> dependencies;
    for (const auto& [name, column] : metadata.Columns) {
        columns.push_back(name);
        if (IsGeneratedColumn(&column) != column.DefaultExpression.Defined()) {
            error = TStringBuilder() << "Column " << name << " has inconsistent generated metadata";
            return false;
        }
        if (IsGeneratedColumn(&column)) {
            dependencies[name] = column.DefaultExpression->Dependencies;
        }
    }
    return NKikimr::NGeneratedColumns::BuildGeneratedColumnDependencyGraph(columns, dependencies, graph, error);
}

const NKikimr::NGeneratedColumns::TGeneratedColumnDependencyGraph* TKikimrTableDescription::GetGeneratedColumnGraph(
    TPositionHandle pos, TExprContext& ctx) const
{
    if (!Metadata) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), "Missing table metadata for generated columns"));
        return nullptr;
    }
    if (!GeneratedColumnGraph) {
        NKikimr::NGeneratedColumns::TGeneratedColumnDependencyGraph graph;
        TString error;
        if (!BuildGeneratedColumnGraph(*Metadata, graph, error)) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), error));
            return nullptr;
        }
        GeneratedColumnGraph = std::move(graph);
    }
    return GeneratedColumnGraph.Get();
}


TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver)
{
    return CompileGeneratedExpr(sqlText, columnName, ctx, settingsBuilder.Build(ctx), moduleResolver,
        settingsBuilder.GetIsAmbiguityError());
}

} // namespace NYql
