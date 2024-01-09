#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"
#include "kqp_opt_phy_uniq_helper.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

struct TRowsAndKeysResult {
    TDqPhyPrecompute RowsPrecompute;
    TDqPhyPrecompute KeysPrecompute;
};

TRowsAndKeysResult PrecomputeRowsAndKeys(const TCondenseInputResult& condenseResult,
    const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
{
    TCoArgument rowsListArg(ctx.NewArgument(pos, "rows_list"));

    auto extractKeys = Build<TCoMap>(ctx, pos)
        .Input(rowsListArg)
        .Lambda(MakeTableKeySelector(table.Metadata, pos, ctx))
        .Done();

    auto variantType = Build<TCoVariantType>(ctx, pos)
        .UnderlyingType<TCoTupleType>()
            .Add<TCoTypeOf>()
                .Value(rowsListArg)
                .Build()
            .Add<TCoTypeOf>()
                .Value(extractKeys)
                .Build()
            .Build()
        .Done();

    auto computeStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult.StageInputs)
            .Build()
        .Program()
            .Args(condenseResult.StageArgs)
            .Body<TCoFlatMap>()
                .Input(condenseResult.Stream)
                .Lambda()
                    .Args({rowsListArg})
                    .Body<TCoAsList>()
                        .Add<TCoVariant>()
                            .Item(rowsListArg)
                            .Index().Build("0")
                            .VarType(variantType)
                            .Build()
                        .Add<TCoVariant>()
                            .Item(extractKeys)
                            .Index().Build("1")
                            .VarType(variantType)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto rowsPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeStage)
               .Index().Build("0")
               .Build()
           .Build()
        .Done();

    auto keysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeStage)
               .Index().Build("1")
               .Build()
           .Build()
        .Done();

    return TRowsAndKeysResult {
        .RowsPrecompute = rowsPrecompute,
        .KeysPrecompute = keysPrecompute
    };
}

// Return set of data columns need to be save during index update
THashSet<TString> CreateDataColumnSetToRead(
    const TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>>& indexes)
{
    THashSet<TString> res;

    for (const auto& index : indexes) {
        for (const auto& col : index.second->DataColumns) {
            res.emplace(col);
        }
    }

    return res;
}

THashSet<TString> CreateKeyColumnSetToRead(
    const TVector<std::pair<TExprNode::TPtr, const TIndexDescription*>>& indexes)
{
    THashSet<TString> res;

    for (const auto& index : indexes) {
        for (const auto& col : index.second->KeyColumns) {
            res.emplace(col);
        }
    }

    return res;
}

TExprBase MakeNonexistingRowsFilter(const TDqPhyPrecompute& inputRows, const TDqPhyPrecompute& lookupDict,
    const TVector<TString>& dictKeys, TPositionHandle pos, TExprContext& ctx)
{
    auto inputRowsArg = TCoArgument(ctx.NewArgument(pos, "input_rows"));
    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));
    auto lookupDictArg = TCoArgument(ctx.NewArgument(pos, "lookup_dict"));

    TVector<TExprBase> dictLookupKeyTuples;
    for (const auto& key : dictKeys) {
        dictLookupKeyTuples.emplace_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(key)
                .Value<TCoMember>()
                    .Struct(inputRowArg)
                    .Name().Build(key)
                    .Build()
                .Done());
    }

    auto filterStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Add(lookupDict)
            .Build()
        .Program()
            .Args({inputRowsArg, lookupDictArg})
            .Body<TCoIterator>()
                .List<TCoFlatMap>()
                    .Input(inputRowsArg)
                    .Lambda()
                        .Args(inputRowArg)
                        .Body<TCoOptionalIf>()
                            .Predicate<TCoContains>()
                                .Collection(lookupDictArg)
                                .Lookup<TCoAsStruct>()
                                    .Add(dictLookupKeyTuples)
                                    .Build()
                                .Build()
                            .Value(inputRowArg)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(filterStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase MakeUpsertIndexRows(TKqpPhyUpsertIndexMode mode, const TDqPhyPrecompute& inputRows,
    const TDqPhyPrecompute& lookupDict, const THashSet<TStringBuf>& inputColumns,
    const THashSet<TStringBuf>& indexColumns, const TKikimrTableDescription& table, TPositionHandle pos,
    TExprContext& ctx, bool opt)
{
    // Check if we can update index table from just input data
    bool allColumnFromInput = true; // - indicate all data from input
    for (const auto& column : indexColumns) {
        allColumnFromInput = allColumnFromInput && inputColumns.contains(column);
    }

    if (allColumnFromInput) {
        return mode == TKqpPhyUpsertIndexMode::UpdateOn
            ? MakeNonexistingRowsFilter(inputRows, lookupDict, table.Metadata->KeyColumnNames, pos, ctx)
            : TExprBase(inputRows);
    }

    auto inputRowsArg = TCoArgument(ctx.NewArgument(pos, "input_rows"));
    auto inputRowArg = TCoArgument(ctx.NewArgument(pos, "input_row"));
    auto lookupDictArg = TCoArgument(ctx.NewArgument(pos, "lookup_dict"));

    TVector<TExprBase> dictLookupKeyTuples;
    for (const auto& key : table.Metadata->KeyColumnNames) {
        YQL_ENSURE(inputColumns.contains(key));

        dictLookupKeyTuples.emplace_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(key)
                .Value<TCoMember>()
                    .Struct(inputRowArg)
                    .Name().Build(key)
                    .Build()
                .Done());
    }

    auto lookup = Build<TCoLookup>(ctx, pos)
        .Collection(lookupDictArg)
        .Lookup<TCoAsStruct>()
            .Add(dictLookupKeyTuples)
            .Build()
        .Done();

    // rows to be added into the index table in case if the given key hasn't been found in the main table
    TVector<TExprBase> absentKeyRow;
    absentKeyRow.reserve(indexColumns.size());

    // rows to be updated in the index table in case if the given key has been found in the main table
    TVector<TExprBase> presentKeyRow;
    presentKeyRow.reserve(indexColumns.size());

    auto payload = TCoArgument(ctx.NewArgument(pos, "payload"));

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

            absentKeyRow.emplace_back(tuple);
            presentKeyRow.emplace_back(tuple);
        } else {
            auto columnType = table.GetColumnType(TString(column));
            absentKeyRow.emplace_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name(columnAtom)
                    .Value<TCoNothing>()
                        .OptionalType(NCommon::BuildTypeExpr(pos, *columnType, ctx))
                        .Build()
                    .Done()
            );
            TExprNode::TPtr member = payload.Ptr();
            if (opt) {
                member = Build<TCoNth>(ctx, pos)
                    .Tuple(member)
                    .Index().Build(0)
                    .Done().Ptr();
            }
            presentKeyRow.emplace_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name(columnAtom)
                    .Value<TCoMember>()
                        .Struct(member)
                        .Name(columnAtom)
                        .Build()
                    .Done()
            );
        }
    }

    auto presentKeyRowStruct = Build<TCoAsStruct>(ctx, pos)
        .Add(presentKeyRow)
        .Done();

    TExprNode::TPtr b;

    if (opt) {
        b = Build<TCoFlatOptionalIf>(ctx, pos)
            .Predicate<TCoNth>()
                .Tuple(payload)
                .Index().Build(1)
                .Build()
            .Value<TCoJust>()
                .Input(presentKeyRowStruct)
                .Build()
            .Done().Ptr();
    } else {
        b = Build<TCoJust>(ctx, pos)
            .Input(presentKeyRowStruct)
            .Done().Ptr();
    }

    TExprBase flatmapBody = Build<TCoIfPresent>(ctx, pos)
        .Optional(lookup)
        .PresentHandler<TCoLambda>()
            .Args(payload)
            .Body(b)
            .Build()
        .MissingValue<TCoJust>()
            .Input<TCoAsStruct>()
                .Add(absentKeyRow)
                .Build()
            .Build()
        .Done();

    if (mode == TKqpPhyUpsertIndexMode::UpdateOn) {
        // Filter non-existing rows
        flatmapBody = Build<TCoFlatOptionalIf>(ctx, pos)
            .Predicate<TCoExists>()
                .Optional(lookup)
                .Build()
            .Value(flatmapBody)
            .Done();
    }

    auto computeRowsStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputRows)
            .Add(lookupDict)
            .Build()
        .Program()
            .Args({inputRowsArg, lookupDictArg})
            .Body<TCoIterator>()
                .List<TCoFlatMap>()
                    .Input(inputRowsArg)
                    .Lambda()
                        .Args(inputRowArg)
                        .Body(flatmapBody)
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

TMaybe<TCondenseInputResult> CheckUniqueConstraint(const TExprBase& inputRows, const THashSet<TStringBuf> inputColumns, bool checkOnlyGivenColumns,
    const TKikimrTableDescription& table, const TSecondaryIndexes& indexes, TPositionHandle pos, TExprContext& ctx)
{
    auto condenseResult = CondenseInput(inputRows, ctx);
    if (!condenseResult) {
        return {};
    }

    // Check uniq constraint for indexes which will be updated by input data.
    // but skip main table pk columns - handle case where we have a complex index is a tuple contains pk
    const auto& mainPk = table.Metadata->KeyColumnNames;
    THashSet<TString> usedIndexes;
    for (const auto& [_, indexDesc] : indexes) {
        for (const auto& indexKeyCol : indexDesc->KeyColumns) {
            if (inputColumns.contains(indexKeyCol) &&
                (std::find(mainPk.begin(), mainPk.end(), indexKeyCol) == mainPk.end()))
            {
                usedIndexes.insert(indexDesc->Name);
                break;
            }
        }
    }

    auto helper = CreateUpsertUniqBuildHelper(table, checkOnlyGivenColumns ? &inputColumns : nullptr, usedIndexes, pos, ctx);
    if (helper->GetChecksNum() == 0) {
        return condenseResult;
    }

    auto computeKeysStage = helper->CreateComputeKeysStage(condenseResult.GetRef(), pos, ctx);
    auto inputPrecompute = helper->CreateInputPrecompute(computeKeysStage, pos, ctx);
    auto uniquePrecomputes = helper->CreateUniquePrecompute(computeKeysStage, pos, ctx);

    auto _true = MakeBool(pos, true, ctx);

    auto aggrStage = helper->CreateLookupExistStage(computeKeysStage, table, _true, pos, ctx);

    // Returns <bool>: <true> - no existing keys, <false> - at least one key exists
    auto noExistingKeysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(aggrStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    TCoArgument inputRowList(ctx.NewArgument(pos, "rows_list"));
    TCoArgument noExistingKeysArg(ctx.NewArgument(pos, "no_existing_keys"));

    struct TUniqueCheckNodes {
        TUniqueCheckNodes(size_t sz) {
            Bodies.reserve(sz);
            Args.reserve(sz);
        }
        TVector<TExprNode::TPtr> Bodies;
        TVector<TCoArgument> Args;
    } uniqueCheckNodes(helper->GetChecksNum());

    for (size_t i = 0; i < helper->GetChecksNum(); i++) {
        uniqueCheckNodes.Args.emplace_back(ctx.NewArgument(pos, "are_keys_unique"));
        uniqueCheckNodes.Bodies.emplace_back(Build<TKqpEnsure>(ctx, pos)
            .Value(_true)
            .Predicate(uniqueCheckNodes.Args.back())
            .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
            .Message(MakeMessage("Duplicated keys found.", pos, ctx))
            .Done().Ptr()
        );
    }

    auto noExistingKeysCheck = Build<TKqpEnsure>(ctx, pos)
        .Value(_true)
        .Predicate(noExistingKeysArg)
        .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
        .Message(MakeMessage("Conflict with existing key.", pos, ctx))
        .Done();

    auto body  = Build<TCoToStream>(ctx, pos)
        .Input<TCoJust>()
            .Input<TCoIfStrict>()
                .Predicate<TCoAnd>()
                    .Add(uniqueCheckNodes.Bodies)
                    .Add(noExistingKeysCheck)
                    .Build()
                .ThenValue(inputRowList)
                .ElseValue<TCoList>()
                    .ListType(ExpandType(pos, *inputRows.Ref().GetTypeAnn(), ctx))
                    .Build()
                .Build()
            .Build()
        .Done();

    TVector<NYql::NNodes::TCoArgument> stageArgs;
    stageArgs.reserve(uniqueCheckNodes.Args.size() + 2);
    stageArgs.emplace_back(inputRowList);
    stageArgs.insert(stageArgs.end(), uniqueCheckNodes.Args.begin(), uniqueCheckNodes.Args.end());
    stageArgs.emplace_back(noExistingKeysArg);

    TVector<TExprBase> stageInputs;
    stageInputs.reserve(uniquePrecomputes.size() + 2);
    stageInputs.emplace_back(inputPrecompute);
    stageInputs.insert(stageInputs.end(), uniquePrecomputes.begin(), uniquePrecomputes.end());
    stageInputs.emplace_back(noExistingKeysPrecompute);

    return TCondenseInputResult {
        .Stream = body,
        .StageInputs = std::move(stageInputs),
        .StageArgs = std::move(stageArgs)
    };
}

} // namespace

TMaybeNode<TExprList> KqpPhyUpsertIndexEffectsImpl(TKqpPhyUpsertIndexMode mode, const TExprBase& inputRows,
    const TCoAtomList& inputColumns, const TCoAtomList& returningColumns, const TKikimrTableDescription& table,
    const TMaybeNode<NYql::NNodes::TCoNameValueTupleList>& settings, TPositionHandle pos, TExprContext& ctx)
{
    switch (mode) {
        case TKqpPhyUpsertIndexMode::Upsert:
        case TKqpPhyUpsertIndexMode::UpdateOn:
            break;

        default:
            YQL_ENSURE(false, "Unexpected phy index upsert mode: " << (ui32) mode);
    }

    const auto& pk = table.Metadata->KeyColumnNames;

    THashSet<TStringBuf> inputColumnsSet;
    for (const auto& column : inputColumns) {
        inputColumnsSet.emplace(column.Value());
    }

    bool checkOnlyGivenColumns = false;
    if (settings) {
        for (const auto& setting : settings.Cast()) {
            if (setting.Name().Value() == "IsUpdate") {
                checkOnlyGivenColumns = true;
                break;
            }
        }
    }

    auto filter =  (mode == TKqpPhyUpsertIndexMode::UpdateOn) ? &inputColumnsSet : nullptr;
    const auto indexes = BuildSecondaryIndexVector(table, pos, ctx, filter);

    auto checkedInput = CheckUniqueConstraint(inputRows, inputColumnsSet, checkOnlyGivenColumns, table, indexes, pos, ctx);

    if (!checkedInput) {
        return {};
    }

    auto condenseInputResult = DeduplicateInput(checkedInput.GetRef(), table, ctx);

    auto inputRowsAndKeys = PrecomputeRowsAndKeys(condenseInputResult, table, pos, ctx);

    // For UPSERT check that indexes is not empty
    YQL_ENSURE(mode == TKqpPhyUpsertIndexMode::UpdateOn || indexes);

    THashSet<TString> indexDataColumns = CreateDataColumnSetToRead(indexes);
    THashSet<TString> indexKeyColumns = CreateKeyColumnSetToRead(indexes);

    auto lookupDict = PrecomputeTableLookupDict(inputRowsAndKeys.KeysPrecompute, table, indexDataColumns, indexKeyColumns, pos, ctx);
    if (!lookupDict) {
        return {};
    }

    TExprBase tableUpsertRows = (mode == TKqpPhyUpsertIndexMode::UpdateOn)
        ? MakeNonexistingRowsFilter(inputRowsAndKeys.RowsPrecompute, lookupDict.Cast(), pk, pos, ctx)
        : inputRowsAndKeys.RowsPrecompute;

    auto tableUpsert = Build<TKqlUpsertRows>(ctx, pos)
        .Table(BuildTableMeta(table, pos, ctx))
        .Input(tableUpsertRows)
        .Columns(inputColumns)
        .ReturningColumns(returningColumns)
        .Settings(settings)
        .Done();

    TVector<TExprBase> effects;
    effects.emplace_back(tableUpsert);

    for (const auto& [tableNode, indexDesc] : indexes) {
        bool indexKeyColumnsUpdated = false;
        THashSet<TStringBuf> indexTableColumns;
        for (const auto& column : indexDesc->KeyColumns) {
            YQL_ENSURE(indexTableColumns.emplace(column).second);

            if (mode == TKqpPhyUpsertIndexMode::UpdateOn && table.GetKeyColumnIndex(column)) {
                // Table PK cannot be updated, so don't consider PK columns update as index update
                continue;
            }

            if (inputColumnsSet.contains(column)) {
                indexKeyColumnsUpdated = true;
            }
        }

        for (const auto& column : pk) {
            indexTableColumns.insert(column);
        }

        auto indexTableColumnsWithoutData = indexTableColumns;

        bool indexDataColumnsUpdated = false;
        bool optUpsert = true;
        for (const auto& column : indexDesc->DataColumns) {
            // TODO: Conder not fetching/updating data columns without input value.
            YQL_ENSURE(indexTableColumns.emplace(column).second);

            if (inputColumnsSet.contains(column)) {
                indexDataColumnsUpdated = true;
                // 'skip index update' optimization checks given value equal to saved one
                // so the type must be equatable to perform it
                auto t = table.GetColumnType(column);
                YQL_ENSURE(t);
                optUpsert &= t->IsEquatable();
            }
        }

        // Need to calc is the updated row in index same
        TVector<TExprBase> payloadTuples;
        TVector<TExprBase> keyTuples;
        auto payloadSelectorArg = TCoArgument(ctx.NewArgument(pos, "payload_selector_row_for_index"));
        auto payload_table_row = TCoArgument(ctx.NewArgument(pos, "payload_table_row"));

        TVector<TExprBase> inputRowsForIndex;
        inputRowsForIndex.reserve(indexTableColumns.size());

        TVector<TExprBase> lookupRow;
        lookupRow.reserve(indexTableColumns.size());

        auto inputItem = TCoArgument(ctx.NewArgument(pos, "input_item_" + indexDesc->Name));
        for (const auto& column : indexTableColumns) {
            if (table.GetKeyColumnIndex(TString(column))) {
                continue;
            }
            auto columnAtom = ctx.NewAtom(pos, column);

            if (inputColumnsSet.contains(column)) {
                inputRowsForIndex.emplace_back(
                   Build<TCoNameValueTuple>(ctx, pos)
                       .Name(columnAtom)
                       .Value<TCoMember>()
                           .Struct(inputItem)
                           .Name(columnAtom)
                           .Build()
                   .Done());

                 lookupRow.emplace_back(
                     Build<TCoNameValueTuple>(ctx, pos)
                         .Name(columnAtom)
                         .Value<TCoMember>()
                             .Struct(payload_table_row)
                             .Name(columnAtom)
                             .Build()
                     .Done());
            }

            payloadTuples.emplace_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name(columnAtom)
                    .Value<TCoMember>()
                        .Struct<TCoNth>()
                            .Tuple(payloadSelectorArg)
                            .Index().Build(1)
                            .Build()
                        .Name(columnAtom)
                        .Build()
                    .Done());
        }

        auto inputArg = TCoArgument(ctx.NewArgument(pos, "recalc_input_arg_" + indexDesc->Name));

        auto cmp = ctx.Builder(pos)
            .Callable("AggrNotEquals")
                .Add(0, Build<TCoAsStruct>(ctx, pos)
                    .Add(inputRowsForIndex)
                    .Done().Ptr())
                .Add(1, Build<TCoAsStruct>(ctx, pos)
                    .Add(lookupRow)
                    .Done().Ptr())
            .Seal().Build();

        for (const auto& key : pk) {
            auto tuple = Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(key)
                .Value<TCoMember>()
                    .Struct(inputItem)
                    .Name().Build(key)
                    .Build()
                .Done();

            keyTuples.emplace_back(tuple);
        }

        auto lookupDictArg = TCoArgument(ctx.NewArgument(pos, "recalc_dict_arg_" + indexDesc->Name));
        auto reComputeDictStage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(inputRowsAndKeys.RowsPrecompute) // input rows
                .Add(lookupDict.Cast()) // dict contains loockuped from table rows
                .Build()
            .Program()
                .Args({inputArg, lookupDictArg})
                .Body<TCoIterator>()
                    .List<TCoMap>()
                    .Input<TCoToList>()
                        .Optional<TCoJust>()
                            .Input(lookupDictArg)
                            .Build()
                        .Build()
                    .Lambda()
                        .Args({"collection"})
                        .Body<TCoToDict>()
                            .List<TCoFlatMap>()
                                .Input(inputArg)
                                .Lambda()
                                    .Args({inputItem})
                                    .Body<TCoMap>()
                                        .Input<TCoLookup>()
                                            .Collection("collection")
                                            .Lookup<TCoAsStruct>()
                                                .Add(keyTuples)
                                                .Build()
                                            .Build()
                                        .Lambda<TCoLambda>()
                                            .Args({payload_table_row})
                                            .Body<TExprList>() // Key of tuple - key columns of index
                                                .Add<TCoAsStruct>()
                                                    .Add(keyTuples)
                                                    .Build()
                                                .Add(payload_table_row) // rows read from main table
                                                .Add(cmp) // comparation on rows from input and from index. true if not equal
                                                .Build()
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .KeySelector(MakeTableKeySelector(table.Metadata, pos, ctx, 0))
                            .PayloadSelector<TCoLambda>()
                                .Args({payloadSelectorArg})
                                .Body<TExprList>()
                                    .Add<TCoNth>()
                                        .Tuple(payloadSelectorArg)
                                        .Index().Build(1)
                                        .Build()
                                    .Add<TCoNth>()
                                        .Tuple(payloadSelectorArg)
                                        .Index().Build(2)
                                        .Build()
                                    .Build()
                                .Build()
                            .Settings()
                                .Add().Build("One")
                                .Add().Build("Hashed")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        auto lookupDictRecomputed = Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(reComputeDictStage)
                    .Index().Build("0")
                    .Build()
                .Build()
            .Done();

        if (indexKeyColumnsUpdated) {
            // Have to delete old index value from index table in case when index key columns were updated
            auto deleteIndexKeys = optUpsert
                ? MakeRowsFromTupleDict(lookupDictRecomputed, pk, indexTableColumnsWithoutData, pos, ctx)
                : MakeRowsFromDict(lookupDict.Cast(), pk, indexTableColumnsWithoutData, pos, ctx);

            auto indexDelete = Build<TKqlDeleteRows>(ctx, pos)
                .Table(tableNode)
                .Input(deleteIndexKeys)
                .Done();

            effects.emplace_back(indexDelete);
        }

        // Index update always required for UPSERT operations as they can introduce new table rows
        bool needIndexTableUpdate = mode != TKqpPhyUpsertIndexMode::UpdateOn;
        // Index table update required in case when index key or data columns were updated
        needIndexTableUpdate = needIndexTableUpdate || indexKeyColumnsUpdated || indexDataColumnsUpdated;

        if (needIndexTableUpdate) {
            auto upsertIndexRows = optUpsert
                ? MakeUpsertIndexRows(mode, inputRowsAndKeys.RowsPrecompute, lookupDictRecomputed,
                      inputColumnsSet, indexTableColumns, table, pos, ctx, true)
                : MakeUpsertIndexRows(mode, inputRowsAndKeys.RowsPrecompute, lookupDict.Cast(),
                      inputColumnsSet, indexTableColumns, table, pos, ctx, false);

            auto indexUpsert = Build<TKqlUpsertRows>(ctx, pos)
                .Table(tableNode)
                .Input(upsertIndexRows)
                .Columns(BuildColumnsList(indexTableColumns, pos, ctx))
                .ReturningColumns<TCoAtomList>().Build()
                .Done();

            effects.emplace_back(indexUpsert);
        }
    }

    return Build<TExprList>(ctx, pos)
        .Add(effects)
        .Done();
}

TExprBase KqpBuildUpsertIndexStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlUpsertRowsIndex>()) {
        return node;
    }

    auto upsert = node.Cast<TKqlUpsertRowsIndex>();
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, upsert.Table().Path());

    auto effects = KqpPhyUpsertIndexEffectsImpl(TKqpPhyUpsertIndexMode::Upsert, upsert.Input(), upsert.Columns(),
        upsert.ReturningColumns(), table, upsert.Settings(), upsert.Pos(), ctx);

    if (!effects) {
        return node;
    }

    return effects.Cast();
}

} // namespace NKikimr::NKqp::NOpt
