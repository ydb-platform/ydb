#include "kqp_opt_phy_uniq_helper.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp::NOpt;

namespace {

struct TLookupNodes {
    TLookupNodes(size_t sz) {
        Stages.reserve(sz);
        Args.reserve(sz);
    }
    TVector<TExprBase> Stages;
    TVector<TCoArgument> Args;
};


NYql::TExprNode::TPtr MakeUniqCheckDict(const TCoLambda& selector,
    const TExprBase& rowsListArg, TPositionHandle pos, TExprContext& ctx)
{
    return Build<TCoToDict>(ctx, pos)
        .List(rowsListArg)
        .KeySelector(selector)
        .PayloadSelector()
            .Args({"stub"})
            .Body<TCoVoid>()
                .Build()
            .Build()
        .Settings()
            .Add().Build("One")
            .Add().Build("Hashed")
            .Build()
        .Done().Ptr();
}

class TInsertUniqBuildHelper : public TUniqBuildHelper {
public:
    TInsertUniqBuildHelper(const NYql::TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumns,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx)
    : TUniqBuildHelper(table, inputColumns, nullptr, pos, ctx, true)
    {}

private:
    const NYql::TExprNode::TPtr GetPkDict() const override {
        return {};
    }

    TDqCnUnionAll CreateLookupStageWithConnection(const TDqStage& computeKeysStage, size_t stageOut,
        const NYql::TKikimrTableMetadata& mainTableMeta, TUniqCheckNodes::TIndexId indexId,
        TPositionHandle pos, TExprContext& ctx) const override
    {
        const NYql::TKikimrTableMetadata* meta;
        if (indexId == -1) {
            meta = &mainTableMeta;
        } else {
            YQL_ENSURE((size_t)indexId < mainTableMeta.SecondaryGlobalIndexMetadata.size());
            meta = mainTableMeta.SecondaryGlobalIndexMetadata[indexId].Get();
        }

        auto inputs = Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeKeysStage)
                    .Index().Build(IntToString<10>(stageOut))
                    .Build()
                .Build()
            .Done();

        auto args = Build<TCoArgument>(ctx, pos)
            .Name(TString("arg0"))
            .Done();

        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoJust>()
                .Input(False)
                .Build()
            .Done()
            .Ptr();

        auto stage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(inputs)
                .Build()
            .Program()
                .Args(args)
                .Body<TCoFlatMap>()
                    .Input<TCoTake>()
                        .Input<TKqpLookupTable>()
                            .Table(BuildTableMeta(*meta, pos, ctx))
                            .LookupKeys<TCoIterator>()
                                .List(args)
                                .Build()
                            .Columns()
                                .Build()
                            .Build()
                        .Count<TCoUint64>()
                            .Literal().Build("1")
                            .Build()
                        .Build()
                    .Lambda(lambda)
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        return Build<TDqCnUnionAll>(ctx, pos)
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .Done();
    }
};

class TUpsertUniqBuildHelper : public TUniqBuildHelper {
public:
    TUpsertUniqBuildHelper(const NYql::TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumns, const THashSet<TString>& usedIndexes,
        NYql::TPositionHandle pos, NYql::TExprContext& ctx)
    : TUniqBuildHelper(table, inputColumns, &usedIndexes, pos, ctx, false)
    , PkDict(MakeUniqCheckDict(MakeTableKeySelector(table.Metadata, pos, ctx), RowsListArg, pos, ctx))
    {}

private:
    const NYql::TExprNode::TPtr GetPkDict() const override {
        return PkDict;
    }

    TDqCnUnionAll CreateLookupStageWithConnection(const TDqStage& computeKeysStage, size_t stageOut,
        const NYql::TKikimrTableMetadata& mainTableMeta, TUniqCheckNodes::TIndexId indexId,
        TPositionHandle pos, TExprContext& ctx) const override
    {
        const NYql::TKikimrTableMetadata* meta;
        if (indexId == -1) {
            meta = &mainTableMeta;
        } else {
            YQL_ENSURE((size_t)indexId < mainTableMeta.SecondaryGlobalIndexMetadata.size());
            meta = mainTableMeta.SecondaryGlobalIndexMetadata[indexId].Get();
        }

        TVector<TExprBase> inputs;
        TVector<TCoArgument> args;

        inputs.emplace_back(Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeKeysStage)
                    .Index().Build(IntToString<10>(stageOut))
                    .Build()
                .Build()
            .Done()
        );

        args.emplace_back(
            Build<TCoArgument>(ctx, pos)
                .Name(TString("arg0"))
                .Done()
        );

        NYql::TExprNode::TPtr lambda;
        TVector<TExprBase> columnsToSelect;

        columnsToSelect.reserve(mainTableMeta.KeyColumnNames.size());

        inputs.emplace_back(Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeKeysStage)
                    .Index().Build(IntToString<10>(CalcComputeKeysStageOutputNum()))
                    .Build()
                .Build()
            .Done()
        );

        args.emplace_back(Build<TCoArgument>(ctx, pos)
            .Name(TString("arg1"))
            .Done()
        );

        for (const auto& key : mainTableMeta.KeyColumnNames) {
            columnsToSelect.emplace_back(Build<TCoAtom>(ctx, pos)
                .Value(key)
                .Done()
            );
        }

        lambda = Build<TCoLambda>(ctx, pos)
            .Args({"row_from_index"})
            .Body<TCoOptionalIf>()
                .Predicate<TCoNot>()
                    .Value<TCoContains>()
                        .Collection(args[1])
                        .Lookup("row_from_index")
                        .Build()
                    .Build()
                .Value(False)
                .Build()
            .Done()
            .Ptr();

        auto stage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(inputs)
                .Build()
            .Program()
                .Args(args)
                .Body<TCoFlatMap>()
                    .Input<TCoTake>()
                        .Input<TKqpLookupTable>()
                            .Table(BuildTableMeta(*meta, pos, ctx))
                            .LookupKeys<TCoIterator>()
                                .List(args[0])
                                .Build()
                            .Columns<TCoAtomList>()
                                .Add(columnsToSelect)
                                .Build()
                            .Build()
                        .Count<TCoUint64>()
                            .Literal().Build("1")
                            .Build()
                        .Build()
                    .Lambda(lambda)
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        return Build<TDqCnUnionAll>(ctx, pos)
            .Output()
                .Stage(stage)
                .Index().Build("0")
                .Build()
            .Done();
    }
private:
    const NYql::TExprNode::TPtr PkDict;
};

}

TVector<TUniqBuildHelper::TUniqCheckNodes> TUniqBuildHelper::Prepare(const TCoArgument& rowsListArg,
    const TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumns,
    const THashSet<TString>* usedIndexes, TPositionHandle pos, TExprContext& ctx, bool insertMode)
{
    TVector<TUniqCheckNodes> checks;

    if (insertMode) {
        checks.emplace_back(MakeUniqCheckNodes(MakeTableKeySelector(table.Metadata, pos, ctx), rowsListArg, pos, ctx));
    }

    // make uniq check for each uniq constraint
    for (size_t i = 0; i < table.Metadata->Indexes.size(); i++) {
        if (table.Metadata->Indexes[i].State != TIndexDescription::EIndexState::Ready)
            continue;
        if (table.Metadata->Indexes[i].Type != TIndexDescription::EType::GlobalSyncUnique)
            continue;
        if (usedIndexes && !usedIndexes->contains(table.Metadata->Indexes[i].Name))
            continue;

        // Compatibility with PG semantic - allow multiple null in columns with unique constaint
        // NOTE: to change this it's important to consider insert and replace in case of partial column set
        TVector<TCoAtom> skipNullColumns;
        skipNullColumns.reserve(table.Metadata->Indexes[i].KeyColumns.size());

        bool used = false;
        bool skip = false;

        YQL_ENSURE(inputColumns, "Attempt to check uniq constraint without given columns");

        for (const auto& column : table.Metadata->Indexes[i].KeyColumns) {
            if (inputColumns->contains(column)) {
                // Skip check if no input for index update
                used = true;
            } else if (insertMode) {
                // In case of insert, 'column' will contain NULL for the new PK (or query will fail in case of NOT NULL)
                // NULL != NULL and NULL != "any other value" so we can just skip uniq check.
                skip = true;
                continue;
            }
            TCoAtom atom(ctx.NewAtom(pos, column));
            skipNullColumns.emplace_back(atom);
        }

        if (!used)
            continue;

        if (skip)
            continue;

        auto skipNull = Build<TCoSkipNullMembers>(ctx, pos)
            .Input(rowsListArg)
            .Members().Add(skipNullColumns).Build()
            .Done();

        checks.emplace_back(
            MakeUniqCheckNodes(
                MakeIndexPrefixKeySelector(table.Metadata->Indexes[i], pos, ctx), skipNull, pos, ctx));

        YQL_ENSURE(i < Max<TUniqCheckNodes::TIndexId>());
        checks.back().IndexId = i;
    }

    return checks;
}

static TExprNode::TPtr CreateRowsToPass(const TCoArgument& rowsListArg, const TMaybe<THashSet<TStringBuf>>& inputColumns,
    TPositionHandle pos, TExprContext& ctx)
{
    if (!inputColumns) {
        return rowsListArg.Ptr();
    }

    auto arg = TCoArgument(ctx.NewArgument(pos, "arg"));

    TVector<TExprBase> columns;
    columns.reserve(inputColumns->size());

    for (const auto x : *inputColumns) {
        columns.emplace_back(
            Build<TCoNameValueTuple>(ctx, pos)
                .Name().Build(x)
                .Value<TCoMember>()
                    .Struct(arg)
                    .Name().Build(x)
                    .Build()
                .Done());
    }

    return Build<TCoMap>(ctx, pos)
        .Input(rowsListArg)
        .Lambda()
            .Args({arg})
            .Body<TCoAsStruct>()
                .Add(columns)
                .Build()
            .Build()
        .Done().Ptr();
}

TUniqBuildHelper::TUniqBuildHelper(const TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumns, const THashSet<TString>* usedIndexes,
    TPositionHandle pos, TExprContext& ctx, bool insertMode)
    : RowsListArg(ctx.NewArgument(pos, "rows_list"))
    , False(MakeBool(pos, false, ctx))
    , Checks(Prepare(RowsListArg, table, inputColumns, usedIndexes, pos, ctx, insertMode))
    , RowsToPass(CreateRowsToPass(RowsListArg, inputColumns, pos, ctx))
{}

TUniqBuildHelper::TUniqCheckNodes TUniqBuildHelper::MakeUniqCheckNodes(const TCoLambda& selector,
    const TExprBase& rowsListArg, TPositionHandle pos, TExprContext& ctx)
{
    TUniqCheckNodes result;

    auto dict = MakeUniqCheckDict(selector, rowsListArg, pos, ctx);

    result.DictKeys = Build<TCoDictKeys>(ctx, pos)
        .Dict(dict)
        .Done().Ptr();

    result.UniqCmp = Build<TCoCmpEqual>(ctx, pos)
        .Left<TCoLength>()
            .List(rowsListArg)
            .Build()
        .Right<TCoLength>()
            .List(dict)
            .Build()
        .Done().Ptr();

    return result;
}

size_t TUniqBuildHelper::GetChecksNum() const {
    return Checks.Size();
}

size_t TUniqBuildHelper::CalcComputeKeysStageOutputNum() const {
    return Checks.Size() * 2 + 1;
}

TDqStage TUniqBuildHelper::CreateComputeKeysStage(const TCondenseInputResult& condenseResult,
    TPositionHandle pos, TExprContext& ctx) const
{
    // Number of items for output list 2 for each table + 1 for params itself
    const size_t nItems = CalcComputeKeysStageOutputNum();
    TVector<TExprBase> types;
    types.reserve(nItems);

    types.emplace_back(
        Build<TCoTypeOf>(ctx, pos)
            .Value(RowsToPass)
            .Done()
    );

    for (size_t i = 0; i < Checks.Size(); i++) {
        types.emplace_back(
            Build<TCoTypeOf>(ctx, pos)
                .Value(Checks[i].DictKeys)
                .Done()
        );
        types.emplace_back(
            Build<TCoTypeOf>(ctx, pos)
                .Value(Checks[i].UniqCmp)
                .Done()
        );
    }

    if (auto dict = GetPkDict()) {
        types.emplace_back(
            Build<TCoTypeOf>(ctx, pos)
                .Value(dict)
                .Done()
        );
    }

    auto variantType = Build<TCoVariantType>(ctx, pos)
        .UnderlyingType<TCoTupleType>()
            .Add(types)
            .Build()
        .Done();

    TVector<TExprBase> variants;
    variants.reserve(nItems);

    variants.emplace_back(
        Build<TCoVariant>(ctx, pos)
            .Item(RowsToPass)
            .Index().Build("0")
            .VarType(variantType)
            .Done()
    );

    size_t ch = 1;
    for (size_t i = 0; i < Checks.Size(); i++) {
        variants.emplace_back(
            Build<TCoVariant>(ctx, pos)
                .Item(Checks[i].DictKeys)
                .Index().Build(IntToString<10>(ch++))
                .VarType(variantType)
                .Done()
        );
        variants.emplace_back(
            Build<TCoVariant>(ctx, pos)
                .Item(Checks[i].UniqCmp)
                .Index().Build(IntToString<10>(ch++))
                .VarType(variantType)
                .Done()
        );
    }

    if (auto dict = GetPkDict()) {
        variants.emplace_back(
            Build<TCoVariant>(ctx, pos)
                .Item(dict)
                .Index().Build(IntToString<10>(ch++))
                .VarType(variantType)
                .Done()
        );
    }

    return Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult.StageInputs)
            .Build()
        .Program()
            .Args(condenseResult.StageArgs)
            .Body<TCoFlatMap>()
                .Input(condenseResult.Stream)
                .Lambda()
                    .Args({RowsListArg})
                    .Body<TCoAsList>()
                        .Add(variants)
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
}

TDqPhyPrecompute TUniqBuildHelper::CreateInputPrecompute(const TDqStage& computeKeysStage,
    TPositionHandle pos, TExprContext& ctx) const
{
    return Build<TDqPhyPrecompute>(ctx, pos)
    .Connection<TDqCnValue>()
       .Output()
           .Stage(computeKeysStage)
           .Index().Build("0")
           .Build()
       .Build()
    .Done();
}

TVector<TExprBase> TUniqBuildHelper::CreateUniquePrecompute(const TDqStage& computeKeysStage,
    TPositionHandle pos, TExprContext& ctx) const
{
    TVector<TExprBase> uniquePrecomputes;
    uniquePrecomputes.reserve(Checks.Size());
    for (size_t i = 0, output_index = 2; i < Checks.Size(); i++, output_index += 2) {
        uniquePrecomputes.emplace_back(Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeKeysStage)
                    .Index().Build(IntToString<10>(output_index))
                    .Build()
               .Build()
            .Done()
        );
    }
    return uniquePrecomputes;
}

TDqStage TUniqBuildHelper::CreateLookupExistStage(const TDqStage& computeKeysStage,
    const TKikimrTableDescription& table, TExprNode::TPtr _true, TPositionHandle pos, TExprContext& ctx) const
{
    TLookupNodes lookupNodes(Checks.Size());

    // 0 output - input stream itself so start with output 1
    // Each check produces 2 outputs
    for (size_t i = 0, stage_out = 1; i < Checks.Size(); i++, stage_out += 2) {
        const auto indexId = Checks[i].IndexId;
        lookupNodes.Stages.emplace_back(
            CreateLookupStageWithConnection(computeKeysStage, stage_out, *table.Metadata, indexId,
                pos, ctx)
        );

        lookupNodes.Args.emplace_back(
            Build<TCoArgument>(ctx, pos)
                .Name(TString("arg") + IntToString<10>(i))
                .Done()
        );
    }

    return Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(lookupNodes.Stages)
            .Build()
        .Program()
            .Args(lookupNodes.Args)
            .Body<TCoCondense>()
                .Input<TCoToStream>()
                    .Input<TCoExtend>()
                        .Add(TVector<TExprBase>(lookupNodes.Args.begin(), lookupNodes.Args.end()))
                        .Build()
                    .Build()
                .State(_true)
                .SwitchHandler()
                    .Args({"item", "state"})
                    .Body(False)
                    .Build()
                .UpdateHandler()
                    .Args({"item", "state"})
                    .Body(False)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
}

namespace NKikimr::NKqp::NOpt {


TUniqBuildHelper::TPtr CreateInsertUniqBuildHelper(const NYql::TKikimrTableDescription& table,
    const TMaybe<THashSet<TStringBuf>>& inputColumns, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx)
{
    return std::make_unique<TInsertUniqBuildHelper>(table, inputColumns, pos, ctx);
}

TUniqBuildHelper::TPtr CreateUpsertUniqBuildHelper(const NYql::TKikimrTableDescription& table,
    const TMaybe<THashSet<TStringBuf>>& inputColumns,
    const THashSet<TString>& usedIndexes, NYql::TPositionHandle pos, NYql::TExprContext& ctx)
{
    return std::make_unique<TUpsertUniqBuildHelper>(table, inputColumns, usedIndexes, pos, ctx);
}

}
