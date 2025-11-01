#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TDqStageBase ReadTableToStage(const TExprBase& expr, TExprContext& ctx) {
    if (expr.Maybe<TDqStageBase>()) {
        return expr.Cast<TDqStageBase>();
    }
    if (expr.Maybe<TDqCnUnionAll>()) {
        return expr.Cast<TDqCnUnionAll>().Output().Stage();
    }
    auto pos = expr.Pos();
    TVector<TExprNode::TPtr> inputs;
    TVector<TExprNode::TPtr> args;
    TNodeOnNodeOwnedMap replaces;
    int i = 1;
    VisitExpr(expr.Ptr(), [&](const TExprNode::TPtr& node) {
        TExprBase expr(node);
        if (auto cast = expr.Maybe<TDqCnUnionAll>()) {
            auto newArg = ctx.NewArgument(pos, TStringBuilder() << "rows" << i);
            inputs.emplace_back(node);
            args.emplace_back(newArg);
            replaces.emplace(expr.Raw(), newArg);
            return false;
        }
        return true;
    });
    return Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputs)
            .Build()
        .Program()
            .Args(args)
            .Body(ctx.ReplaceNodes(expr.Ptr(), replaces))
            .Build()
        .Settings()
            .Build()
        .Done();
}

TExprBase BuildVectorIndexPostingRows(const TKikimrTableDescription& table,
    const TKqpTable& tableNode,
    const TString& indexName,
    const TVector<TStringBuf>& indexTableColumns,
    const TExprBase& inputRows,
    bool withData,
    TPositionHandle pos, TExprContext& ctx) {
    // Generate input type for vector resolve
    TVector<const TItemExprType*> rowItems;
    for (const auto& column : indexTableColumns) {
        const TTypeAnnotationNode* type;
        if (column == NTableIndex::NKMeans::ParentColumn) {
            // Parent cluster ID for the prefixed index
            type = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64);
        } else {
            type = table.GetColumnType(TString(column));
            YQL_ENSURE(type, "No key column: " << column);
        }
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        YQL_ENSURE(itemType->Validate(pos, ctx));
        rowItems.push_back(itemType);
    }
    auto rowType = ctx.MakeType<TStructExprType>(rowItems);
    YQL_ENSURE(rowType->Validate(pos, ctx));
    const TTypeAnnotationNode* resolveInputType = ctx.MakeType<TListExprType>(rowType);

    auto resolveInput = (inputRows.Maybe<TDqCnUnionAll>()
        ? inputRows.Maybe<TDqCnUnionAll>().Cast().Output()
        : Build<TDqOutput>(ctx, pos)
            .Stage(ReadTableToStage(inputRows, ctx))
            .Index().Build(0)
            .Done());

    auto resolveOutput = Build<TKqpCnVectorResolve>(ctx, pos)
        .Output(resolveInput)
        .Table(tableNode)
        .InputType(ExpandType(pos, *resolveInputType, ctx))
        .Index(ctx.NewAtom(pos, indexName))
        .WithData(ctx.NewAtom(pos, withData ? "true" : "false"))
        .Done();

    auto resolveStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(resolveOutput)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoToStream>()
                .Input("rows")
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(resolveStage)
            .Index().Build(0)
            .Build()
        .Done();
}

TVector<TStringBuf> BuildVectorIndexPostingColumns(const TKikimrTableDescription& table,
    const TIndexDescription* indexDesc) {
    TVector<TStringBuf> indexTableColumns;
    THashSet<TStringBuf> indexTableColumnSet;

    indexTableColumns.emplace_back(NTableIndex::NKMeans::ParentColumn);
    indexTableColumnSet.insert(NTableIndex::NKMeans::ParentColumn);

    for (const auto& column : table.Metadata->KeyColumnNames) {
        if (indexTableColumnSet.insert(column).second) {
            indexTableColumns.emplace_back(column);
        }
    }

    for (const auto& column : indexDesc->DataColumns) {
        if (indexTableColumnSet.insert(column).second) {
            indexTableColumns.emplace_back(column);
        }
    }

    return indexTableColumns;
}

TVector<TExprBase> MakeColumnGetters(const TExprBase& rowArgument, const TVector<TStringBuf>& columnNames, TPositionHandle pos, TExprContext& ctx) {
    TVector<TExprBase> columnGetters;
    columnGetters.reserve(columnNames.size());
    for (const auto& column : columnNames) {
        const auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(column)
            .template Value<TCoMember>()
                .Struct(rowArgument)
                .Name().Build(column)
                .Build()
            .Done();
        columnGetters.emplace_back(std::move(tuple));
    }
    return columnGetters;
}

TExprBase BuildVectorIndexPrefixRows(const TKikimrTableDescription& table, const TKikimrTableDescription& prefixTable,
    bool withData, const TIndexDescription* indexDesc, const TExprBase& inputRows,
    TVector<TStringBuf>& indexTableColumns, TPositionHandle pos, TExprContext& ctx)
{
    // This whole method does a very simple thing:
    // SELECT i.<indexColumns>, p.__ydb_id AS __ydb_parent FROM <inputRows> i INNER JOIN prefixTable p ON p.<prefixColumns>=i.<prefixColumns>
    // To implement it, we use a StreamLookup in Join mode.
    // It takes <lookup key, left row> tuples as input and returns <left row, right row, cookie> tuples as output ("cookie" contains read stats).

    TVector<TStringBuf> prefixColumns(indexDesc->KeyColumns.begin(), indexDesc->KeyColumns.end()-1);
    THashSet<TStringBuf> postingColumnsSet;
    TVector<TStringBuf> postingColumns;
    auto embeddingColumn = indexDesc->KeyColumns.back();
    YQL_ENSURE(postingColumnsSet.emplace(embeddingColumn).second);
    postingColumns.emplace_back(embeddingColumn);
    for (const auto& column : table.Metadata->KeyColumnNames) {
        if (postingColumnsSet.insert(column).second) {
            postingColumns.emplace_back(column);
        }
    }
    if (withData) {
        for (const auto& column : indexDesc->DataColumns) {
            if (postingColumnsSet.emplace(column).second) {
                postingColumns.emplace_back(column);
            }
        }
    }

    TKqpStreamLookupSettings streamLookupSettings;
    streamLookupSettings.Strategy = EStreamLookupStrategyType::LookupJoinRows;

    TVector<const TItemExprType*> prefixItems;
    for (auto& column : prefixColumns) {
        auto type = prefixTable.GetColumnType(TString(column));
        YQL_ENSURE(type, "No prefix column: " << column);
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        prefixItems.push_back(itemType);
    }
    auto prefixType = ctx.MakeType<TStructExprType>(prefixItems);

    TVector<const TItemExprType*> postingItems;
    for (auto& column : postingColumns) {
        auto type = table.GetColumnType(TString(column));
        YQL_ENSURE(type, "No index column: " << column);
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        postingItems.push_back(itemType);
    }
    auto postingType = ctx.MakeType<TStructExprType>(postingItems);

    TVector<const TTypeAnnotationNode*> joinItemItems;
    joinItemItems.push_back(postingType);
    joinItemItems.push_back(ctx.MakeType<TOptionalExprType>(prefixType));
    auto joinItemType = ctx.MakeType<TTupleExprType>(joinItemItems);
    auto joinInputType = ctx.MakeType<TListExprType>(joinItemType);

    auto stagedInput = (inputRows.Maybe<TDqCnUnionAll>()
        ? inputRows
        : Build<TDqCnUnionAll>(ctx, pos)
            .Output<TDqOutput>()
                .Stage(ReadTableToStage(inputRows, ctx))
                .Index().Build(0)
                .Build()
            .Done());

    const auto rowArg = Build<TCoArgument>(ctx, pos).Name("inputRow").Done();
    const auto rowsArg = Build<TCoArgument>(ctx, pos).Name("inputRows").Done();
    auto prefixTableNode = BuildTableMeta(prefixTable, pos, ctx);
    auto lookup = Build<TKqpCnStreamLookup>(ctx, pos)
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(stagedInput)
                    .Build()
                .Program()
                    .Args({rowsArg})
                    .Body<TCoToStream>()
                        .Input<TCoMap>()
                            .Input(rowsArg)
                            .Lambda()
                                .Args({rowArg})
                                // Join StreamLookup takes <left row, key> tuples as input - build them
                                .Body<TExprList>()
                                    .Add<TCoAsStruct>()
                                        .Add(MakeColumnGetters(rowArg, postingColumns, pos, ctx))
                                        .Build()
                                    .Add<TCoJust>()
                                        .Input<TCoAsStruct>()
                                            .Add(MakeColumnGetters(rowArg, prefixColumns, pos, ctx))
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Settings().Build()
                .Build()
            .Index().Build(0)
            .Build()
        .Table(prefixTableNode)
        .Columns(BuildColumnsList(prefixTable.Metadata->KeyColumnNames, pos, ctx))
        .InputType(ExpandType(pos, *joinInputType, ctx))
        .Settings(streamLookupSettings.BuildNode(ctx, pos))
        .Done();

    // Join StreamLookup returns <left row, right row, cookie> tuples as output
    // But we need left row + 1 field of the right row - build it using TCoMap

    const auto lookupArg = Build<TCoArgument>(ctx, pos).Name("lookupRow").Done();
    const auto leftRow = Build<TCoNth>(ctx, pos)
        .Tuple(lookupArg)
        .Index().Value("0").Build()
        .Done();
    const auto rightRow = Build<TCoNth>(ctx, pos)
        .Tuple(lookupArg)
        .Index().Value("1").Build()
        .Done();

    auto mapLambda = Build<TCoLambda>(ctx, pos)
        .Args({lookupArg})
        .Body<TCoFlatOptionalIf>()
            .Predicate<TCoExists>()
                .Optional(rightRow)
                .Build()
            .Value<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(MakeColumnGetters(leftRow, postingColumns, pos, ctx))
                    .Add<TCoNameValueTuple>()
                        .Name().Build(NTableIndex::NKMeans::ParentColumn)
                        .template Value<TCoMember>()
                            .Struct<TCoUnwrap>().Optional(rightRow).Build()
                            .Name().Build(NTableIndex::NKMeans::IdColumn)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    auto mapStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(lookup)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoToStream>()
                .Input<TCoFlatMap>()
                    .Input("rows")
                    .Lambda(mapLambda)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    postingColumns.push_back(NTableIndex::NKMeans::ParentColumn);
    indexTableColumns = std::move(postingColumns);

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(mapStage)
            .Index().Build(0)
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
