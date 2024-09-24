#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TExprBase MakeInsertIndexRows(const TDqPhyPrecompute& inputRows, const TKikimrTableDescription& table,
    const THashSet<TStringBuf>& inputColumns, const TVector<TStringBuf>& indexColumns,
    TPositionHandle pos, TExprContext& ctx)
{
    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));

    TVector<TExprBase> rowTuples;
    for (const auto& column : indexColumns) {
        auto columnAtom = ctx.NewAtom(pos, column);

        if (inputColumns.contains(column)) {
            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoMember>()
                    .Struct(inputRowArg)
                    .Name(columnAtom)
                    .Build()
                .Done();

            rowTuples.emplace_back(tuple);
        } else {
            auto columnType = table.GetColumnType(TString(column));

            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name(columnAtom)
                .Value<TCoNothing>()
                    .OptionalType(NCommon::BuildTypeExpr(pos, *columnType, ctx))
                    .Build()
                .Done();

            rowTuples.emplace_back(tuple);
        }
    }

    auto computeRowsStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoIterator>()
                .List<TCoMap>()
                    .Input("rows")
                    .Lambda()
                        .Args(inputRowArg)
                        .Body<TCoAsStruct>()
                            .Add(rowTuples)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(computeRowsStage)
            .Index().Build("0")
            .Build()
        .Done();
}

} // namespace

TExprBase KqpBuildInsertIndexStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlInsertRowsIndex>()) {
        return node;
    }

    auto insert = node.Cast<TKqlInsertRowsIndex>();
    bool abortOnError = insert.OnConflict().Value() == "abort"sv;
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, insert.Table().Path());

    THashSet<TStringBuf> inputColumnsSet;
    for (const auto& column : insert.Columns()) {
        inputColumnsSet.emplace(column.Value());
    }

    auto insertRows = MakeConditionalInsertRows(insert.Input(), table, inputColumnsSet, abortOnError, insert.Pos(), ctx);
    if (!insertRows) {
        return node;
    }

    auto insertRowsPrecompute = Build<TDqPhyPrecompute>(ctx, node.Pos())
        .Connection(insertRows.Cast())
        .Done();

    auto indexes = BuildSecondaryIndexVector(table, insert.Pos(), ctx);
    YQL_ENSURE(indexes);

    auto upsertTable = Build<TKqlUpsertRows>(ctx, insert.Pos())
        .Table(insert.Table())
        .Input(insertRowsPrecompute)
        .Columns(insert.Columns())
        .ReturningColumns(insert.ReturningColumns())
        .Done();

    TVector<TExprBase> effects;
    effects.emplace_back(upsertTable);

    for (const auto& [tableNode, indexDesc] : indexes) {
        THashSet<TStringBuf> indexTableColumnsSet;
        TVector<TStringBuf> indexTableColumns;

        for (const auto& column : indexDesc->KeyColumns) {
            YQL_ENSURE(indexTableColumnsSet.emplace(column).second);
            indexTableColumns.emplace_back(column);
        }

        for (const auto& column : table.Metadata->KeyColumnNames) {
            if (indexTableColumnsSet.insert(column).second) {
                indexTableColumns.emplace_back(column);
            }
        }

        for (const auto& column : indexDesc->DataColumns) {
            if (inputColumnsSet.contains(column)) {
                YQL_ENSURE(indexTableColumnsSet.emplace(column).second);
                indexTableColumns.emplace_back(column);
            }
        }

        auto upsertIndexRows = MakeInsertIndexRows(insertRowsPrecompute, table, inputColumnsSet, indexTableColumns,
            insert.Pos(), ctx);

        auto upsertIndex = Build<TKqlUpsertRows>(ctx, insert.Pos())
            .Table(tableNode)
            .Input(upsertIndexRows)
            .Columns(BuildColumnsList(indexTableColumns, insert.Pos(), ctx))
            .ReturningColumns<TCoAtomList>().Build()
            .Done();

        effects.emplace_back(upsertIndex);
    }

    return Build<TExprList>(ctx, insert.Pos())
        .Add(effects)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
