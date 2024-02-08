#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

using namespace NYql;
using namespace NYql::NNodes;

namespace NKikimr::NKqp::NOpt {

template<typename Container>
TCoAtomList MakeColumnsList(Container rows, TExprContext& ctx, TPositionHandle pos) {
    TVector<TExprBase> columnsVector;
    for (auto&& column : rows) {
        columnsVector.push_back(Build<TCoAtom>(ctx, pos).Value(column).Done());
    }
    return Build<TCoAtomList>(ctx, pos).Add(columnsVector).Done();
}

TExprBase KqpBuildReturning(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    auto maybeReturning = node.Maybe<TKqlReturningList>();
    if (!maybeReturning) {
        return node;
    }

    auto returning = maybeReturning.Cast();
    const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, returning.Table().Path());

    auto buildFromUpsert = [&](TMaybeNode<TKqlUpsertRows> upsert) -> TExprBase {
        auto rows = upsert.Cast().Input();
        auto pos = upsert.Input().Cast().Pos();

        TSet<TString> inputColumns;
        TSet<TString> columnsToReadSet;

        for (auto&& column : upsert.Columns().Cast()) {
            inputColumns.insert(TString(column.Value()));
        }
        for (auto&& column : upsert.ReturningColumns().Cast()) {
            if (!inputColumns.contains(column) && !tableDesc.GetKeyColumnIndex(TString(column))) {
                columnsToReadSet.insert(TString(column));
            }
        }

        TMaybeNode<TExprBase> input = upsert.Input();

        if (!columnsToReadSet.empty()) {
            TString upsertInputName = "upsertInput";
            TString tableInputName = "table";

            auto payloadSelector = MakeRowsPayloadSelector(upsert.Columns().Cast(), tableDesc, pos, ctx);
            auto condenseResult = CondenseInputToDictByPk(input.Cast(), tableDesc, payloadSelector, ctx);
            if (!condenseResult) {
                return node;
            }

            auto inputDictAndKeys = PrecomputeDictAndKeys(*condenseResult, pos, ctx);

            TSet<TString> columnsToLookup = columnsToReadSet;
            for (auto&& column : tableDesc.Metadata->KeyColumnNames) {
                columnsToReadSet.insert(column);
            }

            for (auto&& column : tableDesc.Metadata->KeyColumnNames) {
                columnsToReadSet.erase(column);
            }
            TCoAtomList additionalColumnsToRead = MakeColumnsList(columnsToReadSet, ctx, pos);

            auto prepareUpdateStage = Build<TDqStage>(ctx, pos)
                .Inputs()
                    .Add(inputDictAndKeys.KeysPrecompute)
                    .Add(inputDictAndKeys.DictPrecompute)
                    .Build()
                .Program()
                    .Args({"keys_list", "dict"})
                    .Body<TCoFlatMap>()
                        .Input<TKqpLookupTable>()
                            .Table(returning.Table())
                            .LookupKeys<TCoIterator>()
                                .List("keys_list")
                                .Build()
                            .Columns(MakeColumnsList(columnsToLookup, ctx, pos))
                            .Build()
                        .Lambda()
                            .Args({"existingRow"})
                            .Body<TCoJust>()
                                .Input<TCoFlattenMembers>()
                                    .Add()
                                        .Name().Build("")
                                        .Value<TCoUnwrap>() // Key should always exist in the dict
                                            .Optional<TCoLookup>()
                                                .Collection("dict")
                                                .Lookup<TCoExtractMembers>()
                                                    .Input("existingRow")
                                                    .Members(MakeColumnsList(tableDesc.Metadata->KeyColumnNames, ctx, pos))
                                                    .Build()
                                                .Build()
                                            .Build()
                                        .Build()
                                    .Add()
                                        .Name().Build("")
                                        .Value<TCoExtractMembers>()
                                            .Input("existingRow")
                                            .Members(additionalColumnsToRead)
                                            .Build()
                                        .Build()
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Settings().Build()
                .Done();

            input = Build<TDqCnUnionAll>(ctx, pos)
                .Output()
                    .Stage(prepareUpdateStage)
                    .Index().Build("0")
                    .Build()
                .Done();
        }

        return Build<TCoExtractMembers>(ctx, pos)
            .Input(input.Cast())
            .Members(returning.Columns())
            .Done();
    };

    if (auto maybeList = returning.Update().Maybe<TExprList>()) {
        for (auto item : maybeList.Cast()) {
            if (auto upsert = item.Maybe<TKqlUpsertRows>()) {
                if (upsert.Cast().Table().Raw() == returning.Table().Raw()) {
                    return buildFromUpsert(upsert);
                }
            }
        }
    }

    if (auto upsert = returning.Update().Maybe<TKqlUpsertRows>()) {
        return buildFromUpsert(upsert);
    }

    return node;
}


TExprBase KqpRewriteReturningUpsert(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext&) {
    auto upsert = node.Cast<TKqlUpsertRows>();
    if (upsert.ReturningColumns().Empty()) {
        return node;
    }

    if (!upsert.Input().Maybe<TDqPrecompute>() && !upsert.Input().Maybe<TDqPhyPrecompute>()) {
        return node;
    }

    return
        Build<TKqlUpsertRows>(ctx, upsert.Pos())
            .Input<TDqPrecompute>()
                .Input(upsert.Input())
                .Build()
            .Table(upsert.Table())
            .Columns(upsert.Columns())
            .Settings(upsert.Settings())
            .ReturningColumns<TCoAtomList>().Build()
            .Done();
}

} // namespace NKikimr::NKqp::NOpt
