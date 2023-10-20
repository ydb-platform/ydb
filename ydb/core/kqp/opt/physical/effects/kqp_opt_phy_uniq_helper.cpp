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

TDqCnUnionAll CreateLookupStageWithConnection(const TDqStage& computeKeysStage, size_t stageOut,
    const NYql::TKikimrTableMetadata& mainTableMeta, int indexId, TExprNode::TPtr _false,
    std::pair<TExprNode::TPtr, size_t> pkChecks, TPositionHandle pos, TExprContext& ctx)
{
    const NYql::TKikimrTableMetadata* meta;
    if (indexId == -1) {
        pkChecks.first.Reset();
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

    if (pkChecks.first) {
        columnsToSelect.reserve(mainTableMeta.KeyColumnNames.size());

        inputs.emplace_back(Build<TDqPhyPrecompute>(ctx, pos)
            .Connection<TDqCnValue>()
                .Output()
                    .Stage(computeKeysStage)
                    .Index().Build(IntToString<10>(pkChecks.second))
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
                .Value(_false)
                .Build()
            .Done()
            .Ptr();
    } else {
        lambda = Build<TCoLambda>(ctx, pos)
            .Args({"row"})
            .Body<TCoJust>()
                .Input(_false)
                .Build()
            .Done()
            .Ptr();
    }

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

}

std::pair<TVector<TUniqBuildHelper::TUniqCheckNodes>, NYql::TExprNode::TPtr> TUniqBuildHelper::Prepare(const TCoArgument& rowsListArg,
    const TKikimrTableDescription& table,
    TPositionHandle pos, TExprContext& ctx, bool skipPkCheck)
{
    TVector<TUniqCheckNodes> checks;
    NYql::TExprNode::TPtr pkDict;

    if (!skipPkCheck) {
        checks.emplace_back(MakeUniqCheckNodes(MakeTableKeySelector(table.Metadata, pos, ctx), rowsListArg, pos, ctx));
    } else {
        // In case of update we must make additional filtering to exclude duplicate checks
        // non pk constraint for rows where pk was given
        pkDict = MakeUniqCheckDict(MakeTableKeySelector(table.Metadata, pos, ctx), rowsListArg, pos, ctx);
    }

    // make uniq check for each uniq constraint
    for (size_t i = 0; i < table.Metadata->Indexes.size(); i++) {
        if (table.Metadata->Indexes[i].State != TIndexDescription::EIndexState::Ready)
            continue;
        if (table.Metadata->Indexes[i].Type != TIndexDescription::EType::GlobalSyncUnique)
            continue;

        // Compatibility with PG semantic - allow multiple null in columns with unique constaint
        TVector<TCoAtom> skipNullColumns;
        skipNullColumns.reserve(table.Metadata->Indexes[i].KeyColumns.size());
        for (const auto& column : table.Metadata->Indexes[i].KeyColumns) {
            TCoAtom atom(ctx.NewAtom(pos, column));
            skipNullColumns.emplace_back(atom);
        }

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

    return {checks, pkDict};
}

TUniqBuildHelper::TUniqBuildHelper(const TKikimrTableDescription& table,
    TPositionHandle pos, TExprContext& ctx, bool skipPkCheck)
    : RowsListArg(ctx.NewArgument(pos, "rows_list"))
    , Checks(Prepare(RowsListArg, table, pos, ctx, skipPkCheck))
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
            .Value(RowsListArg)
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

    if (auto dict = Checks.GetPkDict()) {
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
            .Item(RowsListArg)
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

    if (auto dict = Checks.GetPkDict()) {
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

    auto _false = MakeBool(pos, false, ctx);

    // last stage output is pk dict for update mode
    int pkDictOutputId = Checks.GetPkDict() ? CalcComputeKeysStageOutputNum() : -1;

    // 0 output - input stream itself so start with output 1
    // Each check produces 2 outputs
    for (size_t i = 0, stage_out = 1; i < Checks.Size(); i++, stage_out += 2) {
        const auto indexId = Checks[i].IndexId;
        lookupNodes.Stages.emplace_back(
            CreateLookupStageWithConnection(computeKeysStage, stage_out, *table.Metadata, indexId,
                _false, {Checks.GetPkDict(), pkDictOutputId}, pos, ctx)
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
                    .Body(_false)
                    .Build()
                .UpdateHandler()
                    .Args({"item", "state"})
                    .Body(_false)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
}
