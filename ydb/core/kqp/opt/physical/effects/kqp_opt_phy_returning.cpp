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

template<typename Container>
TExprBase SelectFields(TExprBase node, Container fields, TExprContext& ctx, TPositionHandle pos) {
    TVector<TExprBase> items;
    for (auto&& field : fields) {
        TString name;

        if constexpr (std::is_same_v<NYql::NNodes::TCoAtom&&, decltype(field)>) {
            name = field.Value();
        } else {
            name = field;
        }

        auto tuple = Build<TCoNameValueTuple>(ctx, pos)
            .Name().Build(field)
            .template Value<TCoMember>()
                .Struct(node)
                .Name().Build(name)
                .Build()
            .Done();

        items.emplace_back(tuple);
    }
    return Build<TCoAsStruct>(ctx, pos)
        .Add(items)
        .Done();
}

TExprBase KqpBuildReturning(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    auto maybeReturning = node.Maybe<TKqlReturningList>();
    if (!maybeReturning) {
        return node;
    }

    auto returning = maybeReturning.Cast();
    const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, returning.Table().Path());

    auto buildReturningRows = [&](TExprBase rows, TCoAtomList columns, TCoAtomList returningColumns) -> TExprBase {
        auto pos = rows.Pos();

        TSet<TString> inputColumns;
        TSet<TString> columnsToReadSet;
        for (auto&& column : columns) {
            inputColumns.insert(TString(column.Value()));
        }
        for (auto&& column : returningColumns) {
            if (!inputColumns.contains(column) && !tableDesc.GetKeyColumnIndex(TString(column))) {
                columnsToReadSet.insert(TString(column));
            }
        }
        TMaybeNode<TExprBase> input = rows;

        if (!columnsToReadSet.empty()) {
            auto payloadSelectorArg = TCoArgument(ctx.NewArgument(pos, "payload_selector_row"));
            TVector<TExprBase> payloadTuples;
            for (const auto& column : columns) {
                payloadTuples.emplace_back(
                    Build<TCoNameValueTuple>(ctx, pos)
                        .Name(column)
                        .Value<TCoMember>()
                            .Struct(payloadSelectorArg)
                            .Name(column)
                            .Build()
                        .Done());
            }

            auto payloadSelector = Build<TCoLambda>(ctx, pos)
                .Args({payloadSelectorArg})
                .Body<TCoAsStruct>()
                    .Add(payloadTuples)
                    .Build()
                .Done();

            auto condenseResult = CondenseInputToDictByPk(input.Cast(), tableDesc, payloadSelector, ctx);
            if (!condenseResult) {
                return node;
            }

            auto inputDictAndKeys = PrecomputeDictAndKeys(*condenseResult, pos, ctx);
            for (auto&& column : tableDesc.Metadata->KeyColumnNames) {
                columnsToReadSet.insert(column);
            }
            TSet<TString> columnsToLookup = columnsToReadSet;
            for (auto&& column : tableDesc.Metadata->KeyColumnNames) {
                columnsToReadSet.erase(column);
            }
            TCoAtomList additionalColumnsToRead = MakeColumnsList(columnsToReadSet, ctx, pos);

            TCoArgument existingRow = Build<TCoArgument>(ctx, node.Pos())
                .Name("existing_row")
                .Done();
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
                            .Args({existingRow})
                            .Body<TCoJust>()
                                .Input<TCoFlattenMembers>()
                                    .Add()
                                        .Name().Build("")
                                        .Value<TCoUnwrap>() // Key should always exist in the dict
                                            .Optional<TCoLookup>()
                                                .Collection("dict")
                                                .Lookup(SelectFields(existingRow, tableDesc.Metadata->KeyColumnNames, ctx, pos))
                                                .Build()
                                            .Build()
                                        .Build()
                                    .Add()
                                        .Name().Build("")
                                        .Value(SelectFields(existingRow, additionalColumnsToRead, ctx, pos))
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

        auto inputExpr = Build<TCoExtractMembers>(ctx, pos)
            .Input(input.Cast())
            .Members(returning.Columns())
            .Done().Ptr();

        return TExprBase(ctx.ChangeChild(*returning.Raw(), TKqlReturningList::idx_Update, std::move(inputExpr)));
    };

    if (auto maybeList = returning.Update().Maybe<TExprList>()) {
        for (auto item : maybeList.Cast()) {
            if (auto upsert = item.Maybe<TKqlUpsertRows>()) {
                if (upsert.Cast().Table().Raw() == returning.Table().Raw()) {
                    return buildReturningRows(upsert.Input().Cast(), upsert.Columns().Cast(), returning.Columns());
                }
            }
            if (auto del = item.Maybe<TKqlDeleteRows>()) {
                if (del.Cast().Table().Raw() == returning.Table().Raw()) {
                    return buildReturningRows(del.Input().Cast(), MakeColumnsList(tableDesc.Metadata->KeyColumnNames, ctx, node.Pos()), returning.Columns());
                }
            }
        }
    }

    if (auto upsert = returning.Update().Maybe<TKqlUpsertRows>()) {
        return buildReturningRows(upsert.Input().Cast(), upsert.Columns().Cast(), returning.Columns());
    }
    if (auto del = returning.Update().Maybe<TKqlDeleteRows>()) {
        return buildReturningRows(del.Input().Cast(), MakeColumnsList(tableDesc.Metadata->KeyColumnNames, ctx, node.Pos()), returning.Columns());
    }

    TExprNode::TPtr result = returning.Update().Ptr();
    auto status = TryConvertTo(result, *result->GetTypeAnn(), *returning.Raw()->GetTypeAnn(), ctx);
    YQL_ENSURE(status.Level != IGraphTransformer::TStatus::Error, "wrong returning expr type");

    if (status.Level == IGraphTransformer::TStatus::Repeat) {
        return TExprBase(ctx.ChangeChild(*returning.Raw(), TKqlReturningList::idx_Update, std::move(result)));
    }

    if (status.Level == IGraphTransformer::TStatus::Ok) {
        return TExprBase(result);
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

TExprBase KqpRewriteReturningDelete(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext&) {
    auto del = node.Cast<TKqlDeleteRows>();
    if (del.ReturningColumns().Empty()) {
        return node;
    }

    if (!del.Input().Maybe<TDqPrecompute>() && !del.Input().Maybe<TDqPhyPrecompute>()) {
        return node;
    }

    return
        Build<TKqlDeleteRows>(ctx, del.Pos())
            .Input<TDqPrecompute>()
                .Input(del.Input())
                .Build()
            .Table(del.Table())
            .ReturningColumns<TCoAtomList>().Build()
            .Done();
}

} // namespace NKikimr::NKqp::NOpt
