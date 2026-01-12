#include "kqp_opt_phy_effects_impl.h"
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TExprBase BuildFulltextAnalyze(const TExprBase& inputRow, const TIndexDescription* indexDesc, TPositionHandle pos, NYql::TExprContext& ctx)
{
    // Extract fulltext index settings
    const auto* fulltextDesc = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&indexDesc->SpecializedIndexDescription);
    YQL_ENSURE(fulltextDesc, "Expected fulltext index description");

    const auto& settings = fulltextDesc->GetSettings();
    YQL_ENSURE(settings.columns().size() == 1, "Expected single text column in fulltext index");

    const TString textColumn = settings.columns().at(0).column();
    const auto& analyzers = settings.columns().at(0).analyzers();

    // Get text member from input row
    auto textMember = Build<TCoMember>(ctx, pos)
        .Struct(inputRow)
        .Name().Build(textColumn)
        .Done();

    // Serialize analyzer settings for FulltextAnalyze
    TString settingsProto;
    YQL_ENSURE(analyzers.SerializeToString(&settingsProto));
    auto settingsLiteral = Build<TCoString>(ctx, pos)
        .Literal().Build(settingsProto)
        .Done();

    // Create callable for fulltext tokenization
    // Format: FulltextAnalyze(text: String, settings: String) -> List<Struct<__ydb_token, __ydb_freq>>
    auto analyzeCallable = ctx.Builder(pos)
        .Callable("FulltextAnalyze")
            .Add(0, textMember.Ptr())
            .Add(1, settingsLiteral.Ptr())
        .Seal()
        .Build();

    return TExprBase(analyzeCallable);
}

TExprBase BuildFulltextIndexRows(const TKikimrTableDescription& table, const TIndexDescription* indexDesc,
    const NNodes::TExprBase& inputRows, const THashSet<TStringBuf>& inputColumns, TVector<TStringBuf>& indexTableColumns,
    bool forDelete, TPositionHandle pos, NYql::TExprContext& ctx)
{
    // Extract fulltext index settings
    const auto* fulltextDesc = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&indexDesc->SpecializedIndexDescription);
    YQL_ENSURE(fulltextDesc, "Expected fulltext index description");

    const auto& settings = fulltextDesc->GetSettings();
    const bool withRelevance = settings.layout() == Ydb::Table::FulltextIndexSettings::FLAT_RELEVANCE;

    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));
    auto tokenArg = TCoArgument(ctx.NewArgument(pos, "token"));

    // Build output row structure for each token
    TVector<TExprBase> tokenRowTuples;

    indexTableColumns.clear();
    auto addIndexColumn = [&](const TStringBuf& column) {
        indexTableColumns.emplace_back(column);
        auto columnAtom = ctx.NewAtom(pos, column);
        YQL_ENSURE(inputColumns.contains(column));
        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name(columnAtom)
            .Value<TCoMember>()
                .Struct(inputRowArg)
                .Name(columnAtom)
                .Build()
            .Done();
        tokenRowTuples.emplace_back(tuple);
    };

    if (withRelevance) {
        // Add frequency column
        indexTableColumns.emplace_back(NTableIndex::NFulltext::FreqColumn);
        TExprBase value = Build<TCoMember>(ctx, pos)
            .Struct(tokenArg)
            .Name().Build(NTableIndex::NFulltext::FreqColumn)
            .Done();
        if (forDelete) {
            value = Build<TCoMinus>(ctx, pos)
                .Arg(value)
                .Done();
        }
        auto freqTuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(NTableIndex::NFulltext::FreqColumn)
            .Value(value)
            .Done();
        tokenRowTuples.emplace_back(freqTuple);
    }

    // Add token column
    indexTableColumns.emplace_back(NTableIndex::NFulltext::TokenColumn);
    auto tokenTuple = Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(NTableIndex::NFulltext::TokenColumn)
        .Value<TCoMember>()
            .Struct(tokenArg)
            .Name().Build(NTableIndex::NFulltext::TokenColumn)
            .Build()
        .Done();
    tokenRowTuples.emplace_back(tokenTuple);

    // Add primary key columns
    for (const auto& column : table.Metadata->KeyColumnNames) {
        addIndexColumn(column);
    }

    // Add data columns (covered columns)
    if (!forDelete && !withRelevance) {
        for (const auto& column : indexDesc->DataColumns) {
            addIndexColumn(column);
        }
    }

    // Create lambda that builds output row for each token
    // FlatMap expects lambda to return list/stream/optional, so wrap struct in Just
    auto tokenRowsLambda = Build<TCoLambda>(ctx, pos)
        .Args({tokenArg})
        .Body<TCoJust>()
            .Input<TCoAsStruct>()
                .Add(tokenRowTuples)
                .Build()
            .Build()
        .Done();

    auto analyzeCallable = BuildFulltextAnalyze(inputRowArg, indexDesc, pos, ctx);

    auto analyzeStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoIterator>()
                .List<TCoFlatMap>()
                    .Input("rows")
                    .Lambda()
                        .Args({inputRowArg})
                        .Body<TCoFlatMap>()
                            .Input(analyzeCallable)
                            .Lambda(tokenRowsLambda)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(analyzeStage)
            .Index().Build("0")
            .Build()
        .Done();
}

// Takes input rows, returns document table rows
// Runs FulltextAnalyze one more time -- it's probably more efficient than precomputing
// and grouping the previous FulltextAnalyze from BuildFulltextIndexRows()
TExprBase BuildFulltextDocsRows(const TKikimrTableDescription& table, const TIndexDescription* indexDesc,
    const NNodes::TExprBase& inputRows, const THashSet<TStringBuf>& inputColumns, TVector<TStringBuf>& docsColumns,
    bool forDelete, TPositionHandle pos, NYql::TExprContext& ctx)
{
    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));

    // Build output row structure for each token
    TVector<TExprBase> docRowTuples;

    docsColumns.clear();
    auto addIndexColumn = [&](const TStringBuf& column) {
        docsColumns.emplace_back(column);
        auto columnAtom = ctx.NewAtom(pos, column);
        YQL_ENSURE(inputColumns.contains(column));
        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name(columnAtom)
            .Value<TCoMember>()
                .Struct(inputRowArg)
                .Name(columnAtom)
                .Build()
            .Done();
        docRowTuples.emplace_back(tuple);
    };

    // Add primary key columns
    for (const auto& column : table.Metadata->KeyColumnNames) {
        addIndexColumn(column);
    }

    // Add data columns (covered columns)
    if (!forDelete) {
        for (const auto& column : indexDesc->DataColumns) {
            addIndexColumn(column);
        }
    }

    auto analyzeCallable = BuildFulltextAnalyze(inputRowArg, indexDesc, pos, ctx);

    auto tokenArg = TCoArgument(ctx.NewArgument(pos, "token"));
    auto stateArg = TCoArgument(ctx.NewArgument(pos, "state"));
    auto lengthTuple = Build<TCoNameValueTuple>(ctx, pos)
        .Name().Build(NTableIndex::NFulltext::DocLengthColumn)
        .Value<TCoFold>()
            .Input(analyzeCallable)
            .State<TCoUint64>()
                .Literal().Build("0")
                .Build()
            .UpdateHandler()
                .Args({tokenArg, stateArg})
                .Body<TCoAggrAdd>()
                    .Left(stateArg)
                    .Right<TCoConvert>()
                        .Input<TCoMember>()
                            .Struct(tokenArg)
                            .Name().Build(NTableIndex::NFulltext::FreqColumn)
                            .Build()
                        .Type().Build(NTableIndex::NFulltext::DocCountTypeName) // from ui32 to ui64
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
    docRowTuples.emplace_back(lengthTuple);
    docsColumns.emplace_back(NTableIndex::NFulltext::DocLengthColumn);

    auto docRowStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Build()
        .Program()
            .Args({"rows"})
            .Body<TCoToStream>()
                .Input<TCoMap>()
                    .Input("rows")
                    .Lambda()
                        .Args({inputRowArg})
                        .Body<TCoAsStruct>()
                            .Add(docRowTuples)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(docRowStage)
            .Index().Build("0")
            .Build()
        .Done();
}

// This is just...
// SELECT token, CAST(SUM(CASE WHEN freq > 0 THEN 1 ELSE -1 END) AS Uint64) AS freq FROM <tokenRows> GROUP BY token
// or when useSum = true:
// SELECT token, CAST(SUM(freq) AS Uint64) AS freq FROM <tokenRows> GROUP BY token
TExprBase BuildFulltextDictRows(const NNodes::TExprBase& tokenRows, bool useSum, bool useStage, TPositionHandle pos, NYql::TExprContext& ctx)
{
    std::optional<TCoArgument> rowsArg;
    if (useStage) {
        rowsArg = TCoArgument(ctx.NewArgument(pos, "rows"));
    }

    auto sortArg = TCoArgument(ctx.NewArgument(pos, "srt"));
    auto preMapArg = TCoArgument(ctx.NewArgument(pos, "row"));
    auto keySelArg = TCoArgument(ctx.NewArgument(pos, "row"));
    auto initKeyArg = TCoArgument(ctx.NewArgument(pos, "key"));
    auto initRowArg = TCoArgument(ctx.NewArgument(pos, "row"));
    auto updateUnusedArg = TCoArgument(ctx.NewArgument(pos, "key"));
    auto updateRowArg = TCoArgument(ctx.NewArgument(pos, "row"));
    auto updateAggregateArg = TCoArgument(ctx.NewArgument(pos, "aggr"));
    auto finishKeyArg = TCoArgument(ctx.NewArgument(pos, "key"));
    auto finishAggregateArg = TCoArgument(ctx.NewArgument(pos, "aggr"));

    auto sumItem = [&](TExprBase in) -> TExprBase {
        if (useSum) {
            return Build<TCoConvert>(ctx, pos)
                .Input<TCoMember>()
                    .Struct(in)
                    .Name().Build(NTableIndex::NFulltext::FreqColumn)
                    .Build()
                .Type().Build(NTableIndex::NFulltext::DocCountTypeName) // from ui32 to ui64
                .Done();
        }
        return Build<TCoIfStrict>(ctx, pos)
            .Predicate<TCoCmpLess>()
                .Left<TCoMember>()
                    .Struct(in)
                    .Name().Build(NTableIndex::NFulltext::FreqColumn) // remember it's uint32!
                    .Build()
                .Right<TCoUint32>()
                    .Literal().Build((ui32)1 << 31)
                    .Build()
                .Build()
            .ThenValue<TCoUint64>()
                .Literal().Build("1")
                .Build()
            .ElseValue<TCoUint64>()
                .Literal().Build(std::to_string(UINT64_MAX)) // -1
                .Build()
            .Done();
    };
    auto combinedRows = Build<TCoCombineByKey>(ctx, pos)
        .Input(useStage ? TExprBase(*rowsArg) : tokenRows)
        .PreMapLambda<TCoLambda>()
            .Args({preMapArg})
            .Body<TCoJust>()
                .Input(preMapArg)
                .Build()
            .Build()
        .KeySelectorLambda<TCoLambda>()
            .Args({keySelArg})
            .Body<TCoMember>()
                .Struct(keySelArg)
                .Name().Build(NTableIndex::NFulltext::TokenColumn)
                .Build()
            .Build()
        .InitHandlerLambda<TCoLambda>()
            .Args({initKeyArg, initRowArg})
            .Body(sumItem(initRowArg))
            .Build()
        .UpdateHandlerLambda<TCoLambda>()
            .Args({updateUnusedArg, updateRowArg, updateAggregateArg})
            .Body<TCoAggrAdd>()
                .Left(updateAggregateArg)
                .Right(sumItem(updateRowArg))
                .Build()
            .Build()
        .FinishHandlerLambda<TCoLambda>()
            .Args({finishKeyArg, finishAggregateArg})
            .Body<TCoJust>()
                .Input<TCoAsStruct>()
                    .Add<TCoNameValueTuple>()
                        .Name().Build(NTableIndex::NFulltext::TokenColumn)
                        .Value(finishKeyArg)
                        .Build()
                    .Add<TCoNameValueTuple>()
                        .Name().Build(NTableIndex::NFulltext::FreqColumn)
                        .Value(finishAggregateArg)
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();

    if (!useStage) {
        return combinedRows;
    }

    auto combineStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(tokenRows)
            .Build()
        .Program()
            .Args({*rowsArg})
            .Body<TCoToStream>()
                .Input(combinedRows)
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(combineStage)
            .Index().Build("0")
            .Build()
        .Done();
}

// Same as BuildFulltextDictRows, but merges multiple lists/streams from <deltas> into one
TExprBase CombineFulltextDictRows(const TVector<TExprBase>& deltas, TPositionHandle pos, NYql::TExprContext& ctx)
{
    TVector<TCoArgument> args;
    TVector<TExprBase> streams;
    for (size_t i = 0; i < deltas.size(); i++) {
        auto arg = TCoArgument(ctx.NewArgument(pos, "arg"+std::to_string(i)));
        args.push_back(arg);
        streams.push_back(arg);
    }
    auto mergedDeltas = Build<TCoExtend>(ctx, pos)
        .Add(streams)
        .Done();
    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(deltas)
                    .Build()
                .Program()
                    .Args(args)
                    .Body<TCoToStream>()
                        .Input(BuildFulltextDictRows(mergedDeltas, false /*useSum*/, false /*useStage*/, pos, ctx))
                        .Build()
                    .Build()
                .Settings().Build()
                .Build()
            .Index().Build("0")
            .Build()
        .Done();
}

// This is...
// SELECT <pk columns>, token FROM <tokenRows> - to delete this set of keys during deletion
TExprBase BuildFulltextPostingKeys(const TKikimrTableDescription& table, const NNodes::TExprBase& tokenRows,
    TPositionHandle pos, NYql::TExprContext& ctx)
{
    TVector<TExprBase> keyColumns;
    keyColumns.push_back(Build<TCoAtom>(ctx, pos).Value(NTableIndex::NFulltext::TokenColumn).Done());
    for (const auto& column : table.Metadata->KeyColumnNames) {
        keyColumns.push_back(Build<TCoAtom>(ctx, pos).Value(column).Done());
    }

    auto rowsArg = TCoArgument(ctx.NewArgument(pos, "rows"));
    auto keyStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(tokenRows)
            .Build()
        .Program()
            .Args({rowsArg})
            .Body<TCoExtractMembers>()
                .Input(rowsArg)
                .Members<TCoAtomList>()
                    .Add(keyColumns)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(keyStage)
            .Index().Build("0")
            .Build()
        .Done();
}

// Until we have INSERT-or-INCREMENT (!) support at data shards, we have to do it like this:
//
// UPSERT INTO <dictTable>
// SELECT token, SUM(freq) AS freq FROM (
//     SELECT * FROM <dictTable> WHERE token IN (SELECT token FROM <tokenRows>)
//     UNION ALL
//     SELECT token, SUM(freq) AS freq FROM <tokenRows>
// ) GROUP BY token
TExprBase BuildFulltextDictUpsert(const TKikimrTableDescription& dictTable, const NNodes::TExprBase& dictRows,
    TPositionHandle pos, NYql::TExprContext& ctx)
{
    auto dictRowsPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnUnionAll>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(dictRows)
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

    TKqpStreamLookupSettings streamLookupSettings;
    streamLookupSettings.Strategy = EStreamLookupStrategyType::LookupRows;

    TVector<const TItemExprType*> inputItems;
    {
        auto type = dictTable.GetColumnType(NTableIndex::NFulltext::TokenColumn);
        YQL_ENSURE(type, "No token column in dict table: " << NTableIndex::NFulltext::TokenColumn);
        auto itemType = ctx.MakeType<TItemExprType>(NTableIndex::NFulltext::TokenColumn, type);
        inputItems.push_back(itemType);
    }
    auto inputType = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(inputItems));

    const auto dictRowsArg = Build<TCoArgument>(ctx, pos).Name("dictRows").Done();
    const auto rowArg = Build<TCoArgument>(ctx, pos).Name("row").Done();
    const auto keyArg = Build<TCoArgument>(ctx, pos).Name("key").Done();
    auto dictTableNode = BuildTableMeta(dictTable, pos, ctx);
    auto dictColumns = BuildColumnsList(TVector<TStringBuf>({
        NTableIndex::NFulltext::TokenColumn,
        NTableIndex::NFulltext::FreqColumn
    }), pos, ctx);

    auto distinctTokens = Build<TCoMap>(ctx, pos)
        .Input<TCoDictKeys>()
            .Dict<TCoToDict>()
                .List(dictRowsArg)
                .KeySelector()
                    .Args({rowArg})
                    .Body<TCoMember>()
                        .Struct(rowArg)
                        .Name().Build(NTableIndex::NFulltext::TokenColumn)
                        .Build()
                    .Build()
                .PayloadSelector()
                    .Args({"stub"})
                    .Body<TCoVoid>()
                        .Build()
                    .Build()
                .Settings()
                    .Add().Build("One")
                    .Add().Build("Hashed")
                    .Build()
                .Build()
            .Build()
        .Lambda()
            .Args({keyArg})
            .Body<TCoAsStruct>()
                .Add<TCoNameValueTuple>()
                    .Name().Build(NTableIndex::NFulltext::TokenColumn)
                    .Value(keyArg)
                    .Build()
                .Build()
            .Build()
        .Done();

    auto dictLookup = Build<TKqpCnStreamLookup>(ctx, pos)
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(dictRowsPrecompute)
                    .Build()
                .Program()
                    .Args({dictRowsArg})
                    .Body<TCoToStream>()
                        .Input(distinctTokens)
                        .Build()
                    .Build()
                .Settings().Build()
                .Build()
            .Index().Build(0)
            .Build()
        .Table(dictTableNode)
        .Columns(dictColumns)
        .InputType(ExpandType(pos, *inputType, ctx))
        .Settings(streamLookupSettings.BuildNode(ctx, pos))
        .Done();
    auto dictLookupPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnUnionAll>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(dictLookup)
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

    const auto lookupRowsArg = Build<TCoArgument>(ctx, pos).Name("rows").Done();
    const auto incRowsArg = Build<TCoArgument>(ctx, pos).Name("inc").Done();
    const auto listArg = Build<TCoArgument>(ctx, pos).Name("list").Done();
    // FIXME: Use TCoExtend
    auto mergeStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(dictLookupPrecompute)
            .Add(dictRowsPrecompute)
            .Build()
        .Program()
            .Args({lookupRowsArg, incRowsArg})
            .Body<TCoToStream>()
                .Input<TCoFlatMap>()
                    .Input<TCoAsList>()
                        .Add(lookupRowsArg)
                        .Add(incRowsArg)
                        .Build()
                    .Lambda()
                        .Args({listArg})
                        .Body(listArg)
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto mergeUnion = Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(mergeStage)
            .Index().Build("0")
            .Build()
        .Done();

    auto incrementStage = BuildFulltextDictRows(mergeUnion, true /*useSum*/, true /*useStage*/, pos, ctx);

    return Build<TKqlUpsertRows>(ctx, pos)
        .Table(dictTableNode)
        .Input(incrementStage)
        .Columns(dictColumns)
        .ReturningColumns<TCoAtomList>().Build()
        .IsBatch(ctx.NewAtom(pos, "false"))
        .Done();
}

}
