#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

struct TUniqCheckNodes {
    using TIndexId = int;
    static constexpr TIndexId INVALID_INDEX_ID = -1;
    TExprNode::TPtr DictKeys;
    TExprNode::TPtr UniqCmp;
    TIndexId IndexId = INVALID_INDEX_ID;
};

TUniqCheckNodes MakeUniqCheckNodes(const TCoLambda& selector,
    const TExprBase& rowsListArg, TPositionHandle pos, TExprContext& ctx)
{
    TUniqCheckNodes result;
    auto dict = Build<TCoToDict>(ctx, pos)
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

TDqCnUnionAll CreateLookupStageWithConnection(const TDqStage& computeKeysStage, size_t index, const NYql::TKikimrTableMetadata& meta,
    TExprNode::TPtr _false, TPositionHandle pos, TExprContext& ctx)
{
    auto lookupKeysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeKeysStage)
               .Index().Build(IntToString<10>(index))
               .Build()
           .Build()
        .Done();


    auto stage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(lookupKeysPrecompute)
            .Build()
        .Program()
            .Args({"keys_list"})
            .Body<TCoMap>()
                .Input<TCoTake>()
                    .Input<TKqpLookupTable>()
                        .Table(BuildTableMeta(meta, pos, ctx))
                        .LookupKeys<TCoIterator>()
                            .List("keys_list")
                            .Build()
                        .Columns()
                            .Build()
                        .Build()
                    .Count<TCoUint64>()
                        .Literal().Build("1")
                        .Build()
                    .Build()
                .Lambda()
                    .Args({"row"})
                    .Body<TCoJust>()
                        .Input(_false)
                        .Build()
                    .Build()
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

class TUniqBuildHelper {
    static TVector<TUniqCheckNodes> Prepare(const TCoArgument& rowsListArg, const TKikimrTableDescription& table,
        TPositionHandle pos, TExprContext& ctx)
    {

        TVector<TUniqCheckNodes> checks;
        checks.emplace_back(MakeUniqCheckNodes(MakeTableKeySelector(table.Metadata, pos, ctx), rowsListArg, pos, ctx));

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

            checks.emplace_back(MakeUniqCheckNodes(MakeIndexPrefixKeySelector(table.Metadata->Indexes[i], pos, ctx), skipNull, pos, ctx));
            YQL_ENSURE(i < Max<TUniqCheckNodes::TIndexId>());
            checks.back().IndexId = i;
        }
        return checks;
    }

public:
    TUniqBuildHelper(const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
        : RowsListArg(ctx.NewArgument(pos, "rows_list"))
        , Checks(Prepare(RowsListArg, table, pos, ctx))
    {}

    size_t GetChecksNum() const {
        return Checks.size();
    }

    TDqStage CreateComputeKeysStage(const TCondenseInputResult& condenseResult, TPositionHandle pos, TExprContext& ctx) const
    {
        // Number of items for output list 2 for each table + 1 for params itself
        const size_t nItems = Checks.size() * 2 + 1;
        TVector<TExprBase> types;
        types.reserve(nItems);

        types.emplace_back(
            Build<TCoTypeOf>(ctx, pos)
                .Value(RowsListArg)
                .Done()
        );

        for (size_t i = 0; i < Checks.size(); i++) {
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

        for (size_t i = 0, ch = 1; i < Checks.size(); i++) {
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

    TDqPhyPrecompute CreateInputPrecompute(const TDqStage& computeKeysStage, TPositionHandle pos, TExprContext& ctx) {
        return Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
           .Output()
               .Stage(computeKeysStage)
               .Index().Build("0")
               .Build()
           .Build()
        .Done();
    }

    TVector<TExprBase> CreateUniquePrecompute(const TDqStage& computeKeysStage, TPositionHandle pos, TExprContext& ctx) {
        TVector<TExprBase> uniquePrecomputes;
        uniquePrecomputes.reserve(Checks.size());
        for (size_t i = 0, output_index = 2; i < Checks.size(); i++, output_index += 2) {
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

    struct TLookupNodes {
        TLookupNodes(size_t sz) {
            Stages.reserve(sz);
            Args.reserve(sz);
        }
        TVector<TExprBase> Stages;
        TVector<TCoArgument> Args;
    };

    TDqStage CreateLookupExistStage(const TDqStage& computeKeysStage, const TKikimrTableDescription& table,
        TExprNode::TPtr _true, TPositionHandle pos, TExprContext& ctx)
    {
        TLookupNodes lookupNodes(Checks.size());

        auto _false = MakeBool(pos, false, ctx);

        lookupNodes.Stages.emplace_back(
            // 1 is id of precompute key stage output for primary key
            CreateLookupStageWithConnection(computeKeysStage, 1, *table.Metadata, _false, pos, ctx)
        );
        lookupNodes.Args.emplace_back(
            Build<TCoArgument>(ctx, pos)
                .Name("arg0")
                .Done()
        );

        // For each index create lookup stage using computeKeysStage.
        // 3 is id of of precompute key stage output for index
        for (size_t i = 1, stage_out = 3; i < Checks.size(); i++, stage_out += 2) {
            const auto indexId = Checks[i].IndexId;
            YQL_ENSURE(indexId >=0);
            YQL_ENSURE((size_t)indexId < table.Metadata->SecondaryGlobalIndexMetadata.size());
            lookupNodes.Stages.emplace_back(
                CreateLookupStageWithConnection(computeKeysStage, stage_out,
                    *(table.Metadata->SecondaryGlobalIndexMetadata[indexId]), _false, pos, ctx)
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

private:
    TCoArgument RowsListArg;
    TVector<TUniqCheckNodes> Checks;
};

}

TMaybeNode<TDqCnUnionAll> MakeConditionalInsertRows(const TExprBase& input, const TKikimrTableDescription& table,
    bool abortOnError, TPositionHandle pos, TExprContext& ctx)
{
    auto condenseResult = CondenseInput(input, ctx);
    if (!condenseResult) {
        return {};
    }

    TUniqBuildHelper helper(table, pos, ctx);
    auto computeKeysStage = helper.CreateComputeKeysStage(condenseResult.GetRef(), pos, ctx);

    auto inputPrecompute = helper.CreateInputPrecompute(computeKeysStage, pos, ctx);
    auto uniquePrecomputes = helper.CreateUniquePrecompute(computeKeysStage, pos, ctx);

    auto _true = MakeBool(pos, true, ctx);

    auto aggrStage = helper.CreateLookupExistStage(computeKeysStage, table, _true, pos, ctx);

    // Returns <bool>: <true> - no existing keys, <false> - at least one key exists
    auto noExistingKeysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(aggrStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    struct TUniqueCheckNodes {
        TUniqueCheckNodes(size_t sz) {
            Bodies.reserve(sz);
            Args.reserve(sz);
        }
        TVector<TExprNode::TPtr> Bodies;
        TVector<TCoArgument> Args;
    } uniqueCheckNodes(helper.GetChecksNum());

    TCoArgument noExistingKeysArg(ctx.NewArgument(pos, "no_existing_keys"));
    TExprNode::TPtr noExistingKeysCheck;

    // Build condition checks depending on INSERT kind
    if (abortOnError) {
        for (size_t i = 0; i < helper.GetChecksNum(); i++) {
            uniqueCheckNodes.Args.emplace_back(ctx.NewArgument(pos, "are_keys_unique"));
            uniqueCheckNodes.Bodies.emplace_back(Build<TKqpEnsure>(ctx, pos)
                .Value(_true)
                .Predicate(uniqueCheckNodes.Args.back())
                .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
                .Message(MakeMessage("Duplicated keys found.", pos, ctx))
                .Done().Ptr()
            );
        }

        noExistingKeysCheck = Build<TKqpEnsure>(ctx, pos)
            .Value(_true)
            .Predicate(noExistingKeysArg)
            .IssueCode().Build(ToString((ui32) TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION))
            .Message(MakeMessage("Conflict with existing key.", pos, ctx))
            .Done().Ptr();
    } else {
        for (size_t i = 0; i < helper.GetChecksNum(); i++) {
            uniqueCheckNodes.Args.emplace_back(ctx.NewArgument(pos, "are_keys_unique"));
            uniqueCheckNodes.Bodies.emplace_back(uniqueCheckNodes.Args.back().Ptr());
        }

        noExistingKeysCheck = noExistingKeysArg.Ptr();
    }


    TCoArgument inputRowsArg(ctx.NewArgument(pos, "input_rows"));
    // Final pure compute stage to compute rows to insert and check for both conditions:
    // 1. No rows were found in table PK from input
    // 2. No duplicate PKs were found in input
    TVector<TCoArgument> args;
    args.reserve(uniqueCheckNodes.Args.size() + 2);
    args.emplace_back(inputRowsArg);
    args.insert(args.end(), uniqueCheckNodes.Args.begin(), uniqueCheckNodes.Args.end());
    args.emplace_back(noExistingKeysArg);
    auto conditionStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(inputPrecompute)
            .Add(uniquePrecomputes)
            .Add(noExistingKeysPrecompute)
            .Build()
        .Program()
            .Args(args)
            .Body<TCoToStream>()
                .Input<TCoIfStrict>()
                    .Predicate<TCoAnd>()
                        .Add(uniqueCheckNodes.Bodies)
                        .Add(noExistingKeysCheck)
                        .Build()
                    .ThenValue(inputRowsArg)
                    .ElseValue<TCoList>()
                        .ListType(ExpandType(pos, *input.Ref().GetTypeAnn(), ctx))
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    return Build<TDqCnUnionAll>(ctx, pos)
        .Output()
            .Stage(conditionStage)
            .Index().Build("0")
            .Build()
        .Done();
}

TExprBase KqpBuildInsertStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlInsertRows>()) {
        return node;
    }

    auto insert = node.Cast<TKqlInsertRows>();
    bool abortOnError = insert.OnConflict().Value() == "abort"sv;
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, insert.Table().Path());

    auto insertRows = MakeConditionalInsertRows(insert.Input(), table, abortOnError, insert.Pos(), ctx);
    if (!insertRows) {
        return node;
    }

    return Build<TKqlUpsertRows>(ctx, insert.Pos())
        .Table(insert.Table())
        .Input(insertRows.Cast())
        .Columns(insert.Columns())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
