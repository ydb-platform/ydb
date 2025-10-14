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

TExprBase BuildNth(const TExprBase& lookupArg, const char *n, TPositionHandle pos, TExprContext& ctx) {
    return Build<TCoNth>(ctx, pos)
        .Tuple(lookupArg)
        .Index().Value(n).Build()
        .Done();
};

struct TVectorIndexPrefixLookup {
    TExprBase Node;
    TKqpTable PrefixTableNode;
    TVector<TStringBuf> PrefixColumns;
    const TStructExprType* PrefixType;
    TVector<TStringBuf> PostingColumns;

    TVectorIndexPrefixLookup(TExprBase&& node, TKqpTable&& prefixTableNode):
        Node(std::move(node)),
        PrefixTableNode(std::move(prefixTableNode)) {
    }
};

TVectorIndexPrefixLookup BuildVectorIndexPrefixLookup(
    const TKikimrTableDescription& table, const TKikimrTableDescription& prefixTable,
    bool withData, bool withNulls, const TIndexDescription* indexDesc, const TExprBase& inputRows,
    TPositionHandle pos, TExprContext& ctx)
{
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
    streamLookupSettings.Strategy = EStreamLookupStrategyType::LookupSemiJoinRows;
    TVector<TStringBuf> leftColumns = postingColumns;
    if (withNulls) {
        for (const auto& column : prefixColumns) {
            if (postingColumnsSet.emplace(column).second) {
                leftColumns.emplace_back(column);
            }
        }
    }

    TVector<const TItemExprType*> prefixItems;
    for (auto& column : prefixColumns) {
        auto type = prefixTable.GetColumnType(TString(column));
        YQL_ENSURE(type, "No prefix column: " << column);
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        prefixItems.push_back(itemType);
    }
    auto prefixType = ctx.MakeType<TStructExprType>(prefixItems);

    TVector<const TItemExprType*> leftItems;
    for (auto& column : leftColumns) {
        auto type = table.GetColumnType(TString(column));
        YQL_ENSURE(type, "No index column: " << column);
        auto itemType = ctx.MakeType<TItemExprType>(column, type);
        leftItems.push_back(itemType);
    }
    auto leftType = ctx.MakeType<TStructExprType>(leftItems);

    TVector<const TTypeAnnotationNode*> joinItemItems;
    joinItemItems.push_back(leftType);
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
                                        .Add(MakeColumnGetters(rowArg, leftColumns, pos, ctx))
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

    TVectorIndexPrefixLookup res(std::move(lookup), std::move(prefixTableNode));
    res.PrefixColumns = std::move(prefixColumns);
    res.PrefixType = prefixType;
    res.PostingColumns = std::move(postingColumns);
    return res;
}

TExprBase BuildVectorIndexPrefixRows(const TKikimrTableDescription& table, const TKikimrTableDescription& prefixTable,
    bool withData, const TIndexDescription* indexDesc, const TExprBase& inputRows,
    TVector<TStringBuf>& indexTableColumns, TPositionHandle pos, TExprContext& ctx)
{
    // This whole method does a very simple thing:
    // SELECT i.<indexColumns>, p.__ydb_id AS __ydb_parent FROM <inputRows> i INNER JOIN prefixTable p ON p.<prefixColumns>=i.<prefixColumns>
    // To implement it, we use a StreamLookup in Join mode.
    // It takes <lookup key, left row> tuples as input and returns <left row, right row, cookie> tuples as output ("cookie" contains read stats).

    TVectorIndexPrefixLookup lookup = BuildVectorIndexPrefixLookup(table, prefixTable, withData, false, indexDesc, inputRows, pos, ctx);

    // Join StreamLookup returns <left row, right row, cookie> tuples as output
    // But we need left row + 1 field of the right row - build it using TCoMap

    const auto lookupArg = Build<TCoArgument>(ctx, pos).Name("lookupRow").Done();
    const auto leftRow = BuildNth(lookupArg, "0", pos, ctx);
    const auto rightRow = BuildNth(lookupArg, "1", pos, ctx);

    // Filter rows where rightRow is null

    auto mapLambda = Build<TCoLambda>(ctx, pos)
        .Args({lookupArg})
        .Body<TCoFlatOptionalIf>()
            .Predicate<TCoExists>()
                .Optional(rightRow)
                .Build()
            .Value<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add(MakeColumnGetters(leftRow, lookup.PostingColumns, pos, ctx))
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
            .Add(lookup.Node)
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

    indexTableColumns = std::move(lookup.PostingColumns);
    indexTableColumns.push_back(NTableIndex::NKMeans::ParentColumn);

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(mapStage)
            .Index().Build(0)
            .Build()
        .Done();
}

std::pair<TExprBase, TExprBase> BuildVectorIndexPrefixRowsWithNew(
    const TKikimrTableDescription& table, const TKikimrTableDescription& prefixTable,
    const TIndexDescription* indexDesc, const TExprBase& inputRows,
    TVector<TStringBuf>& indexTableColumns, TPositionHandle pos, TExprContext& ctx)
{
    // This method is similar to the previous one, but also handles unknown prefixes.
    // StreamLookupJoin is executed in SemiJoin mode in this case, so <right row> may
    // be null for prefixes missing from the prefix table.
    // We feed such rows to KqpCnSequencer, replicate their IDs to the output stream,
    // and also separately insert them into the prefix table.

    TVectorIndexPrefixLookup lookup = BuildVectorIndexPrefixLookup(table, prefixTable, true, true, indexDesc, inputRows, pos, ctx);

    const auto lookupArg = Build<TCoArgument>(ctx, pos).Name("lookupRow").Done();
    const auto leftRow = BuildNth(lookupArg, "0", pos, ctx);
    const auto rightRow = BuildNth(lookupArg, "1", pos, ctx);

    const auto newRowsArg = Build<TCoArgument>(ctx, pos).Name("rows").Done();
    auto newPrefixStream = Build<TCoFlatMap>(ctx, pos)
        .Input(newRowsArg)
        .Lambda()
            .Args({lookupArg})
            .Body<TCoOptionalIf>()
                .Predicate<TCoNot>()
                    .Value<TCoExists>()
                        .Optional(rightRow)
                        .Build()
                    .Build()
                .Value(leftRow)
                .Build()
            .Build()
        .Done();

    const auto keyArg = Build<TCoArgument>(ctx, pos).Name("keyArg").Done();
    const auto payloadArg = Build<TCoArgument>(ctx, pos).Name("payloadArg").Done();
    auto newPrefixDict = Build<TCoSqueezeToDict>(ctx, pos)
        .Stream(newPrefixStream)
        .KeySelector<TCoLambda>()
            .Args(keyArg)
            .Body<TCoAsStruct>()
                .Add(MakeColumnGetters(keyArg, lookup.PrefixColumns, pos, ctx))
                .Build()
            .Build()
        .PayloadSelector<TCoLambda>()
            .Args(payloadArg)
            .Body<TCoVoid>()
                .Build()
            .Build()
        .Settings()
            .Add().Build("One")
            .Add().Build("Hashed")
            .Build()
        .Done();

    auto newPrefixDictPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(lookup.Node)
                        .Build()
                    .Program()
                        .Args({newRowsArg})
                        .Body(newPrefixDict)
                        .Build()
                    .Settings().Build()
                    .Build()
                .Index().Build(0)
                .Build()
            .Build()
        .Done();

    // Feed newPrefixes from dict to KqpCnSequencer

    const auto dictArg = Build<TCoArgument>(ctx, pos).Name("dict").Done();
    auto newPrefixesStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(newPrefixDictPrecompute)
            .Build()
        .Program()
            .Args({dictArg})
            .Body<TCoToStream>()
                .Input<TCoDictKeys>()
                    .Dict(dictArg)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto listPrefixType = ctx.MakeType<TListExprType>(lookup.PrefixType);
    auto fullPrefixColumns = lookup.PrefixColumns;
    fullPrefixColumns.push_back(NTableIndex::NKMeans::IdColumn);
    auto fullPrefixColumnList = BuildColumnsList(fullPrefixColumns, pos, ctx);

    auto cnSequencer = Build<TKqpCnSequencer>(ctx, pos)
        .Output()
            .Stage(newPrefixesStage)
            .Index().Build(0)
            .Build()
        .Table(lookup.PrefixTableNode)
        .Columns(fullPrefixColumnList)
        .DefaultConstraintColumns<TCoAtomList>()
            .Add(ctx.NewAtom(pos, NTableIndex::NKMeans::IdColumn))
            .Build()
        .InputItemType(ExpandType(pos, *listPrefixType, ctx))
        .Done();

    auto sequencerPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnUnionAll>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(cnSequencer)
                        .Build()
                    .Program()
                        .Args({"rows"})
                        .Body<TCoToStream>()
                            .Input("rows")
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Build()
                .Index().Build(0)
                .Build()
            .Build()
        .Done();

    const auto sequencerArg = Build<TCoArgument>(ctx, pos).Name("seqRows").Done();
    const auto seqKeyArg = Build<TCoArgument>(ctx, pos).Name("seqKey").Done();
    const auto seqValueArg = Build<TCoArgument>(ctx, pos).Name("seqValue").Done();
    auto sequencerDict = Build<TCoToDict>(ctx, pos)
        .List(sequencerArg)
        .KeySelector<TCoLambda>()
            .Args({seqKeyArg})
            .Body<TCoAsStruct>()
                .Add(MakeColumnGetters(seqKeyArg, lookup.PrefixColumns, pos, ctx))
                .Build()
            .Build()
        .PayloadSelector<TCoLambda>()
            .Args({seqValueArg})
            .Body<TCoMember>()
                .Struct(seqValueArg)
                .Name().Build(NTableIndex::NKMeans::IdColumn)
                .Build()
            .Build()
        .Settings()
            .Add().Build("One")
            .Add().Build("Hashed")
            .Build()
        .Done();

    auto sequencerDictPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(sequencerPrecompute)
                        .Build()
                    .Program()
                        .Args({sequencerArg})
                        .Body<TCoToStream>()
                            .Input<TCoAsList>()
                                .Add(sequencerDict)
                                .Build()
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Build()
                .Index().Build(0)
                .Build()
            .Build()
        .Done();

    // Take output and remap it to the input using a dictionary

    const auto sequencerDictArg = Build<TCoArgument>(ctx, pos).Name("dict").Done();
    const auto origArg = Build<TCoArgument>(ctx, pos).Name("origRows").Done();
    const auto fillRowArg = Build<TCoArgument>(ctx, pos).Name("fillRow").Done();
    const auto leftFillRow = BuildNth(fillRowArg, "0", pos, ctx);
    const auto rightFillRow = BuildNth(fillRowArg, "1", pos, ctx);
    auto mapLambda = Build<TCoLambda>(ctx, pos)
        .Args({fillRowArg})
        .Body<TCoAsStruct>()
            .Add(MakeColumnGetters(leftFillRow, lookup.PostingColumns, pos, ctx))
            .Add<TCoNameValueTuple>()
                .Name().Build(NTableIndex::NKMeans::ParentColumn)
                .template Value<TCoIf>()
                    .Predicate<TCoExists>()
                        .Optional(rightFillRow)
                        .Build()
                    .ThenValue<TCoMember>()
                        .Struct<TCoUnwrap>().Optional(rightFillRow).Build()
                        .Name().Build(NTableIndex::NKMeans::IdColumn)
                        .Build()
                    .ElseValue<TCoUnwrap>()
                        .Optional<TCoLookup>()
                            .Collection(sequencerDictArg)
                            .Lookup<TCoAsStruct>()
                                .Add(MakeColumnGetters(leftFillRow, lookup.PrefixColumns, pos, ctx))
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    auto mapStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(lookup.Node)
            .Add(sequencerDictPrecompute)
            .Build()
        .Program()
            .Args({origArg, sequencerDictArg})
            .Body<TCoToStream>()
                .Input<TCoMap>()
                    .Input(origArg)
                    .Lambda(mapLambda)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    indexTableColumns = std::move(lookup.PostingColumns);
    indexTableColumns.push_back(NTableIndex::NKMeans::ParentColumn);

    auto mappedRows = Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(mapStage)
            .Index().Build(0)
            .Build()
        .Done();

    auto upsertPrefixStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(sequencerPrecompute)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoToStream>()
                .Input("rows")
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto upsertPrefix = Build<TKqlUpsertRows>(ctx, pos)
        .Table(lookup.PrefixTableNode)
        .Input<TDqCnUnionAll>()
            .Output()
                .Stage(upsertPrefixStage)
                .Index().Build(0)
                .Build()
            .Build()
        .Columns(fullPrefixColumnList)
        .ReturningColumns<TCoAtomList>().Build()
        .IsBatch(ctx.NewAtom(pos, "false"))
        .Done();

    return std::make_pair((TExprBase)mappedRows, (TExprBase)upsertPrefix);
}

} // namespace NKikimr::NKqp::NOpt
