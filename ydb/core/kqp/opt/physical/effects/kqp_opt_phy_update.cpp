#include "kqp_opt_phy_effects_rules.h"
#include "kqp_opt_phy_effects_impl.h"

#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TDictAndKeysResult PrecomputeDictAndKeys(const TCondenseInputResult& condenseResult, TPositionHandle pos,
    TExprContext& ctx)
{
    auto dictType = Build<TCoListItemType>(ctx, pos)
        .ListType<TCoTypeOf>()
            .Value<TCoCollect>()
                .Input(condenseResult.Stream)
                .Build()
            .Build()
        .Done();

    auto variantType = Build<TCoVariantType>(ctx, pos)
        .UnderlyingType<TCoTupleType>()
            .Add(dictType)
            .Add<TCoListType>()
                .ItemType<TCoDictKeyType>()
                    .DictType(dictType)
                    .Build()
                .Build()
            .Build()
        .Done();

    auto computeKeysStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(condenseResult.StageInputs)
            .Build()
        .Program()
            .Args(condenseResult.StageArgs)
            .Body<TCoFlatMap>()
                .Input(condenseResult.Stream)
                .Lambda()
                    .Args({"dict"})
                    .Body<TCoAsList>()
                        .Add<TCoVariant>()
                            .Item("dict")
                            .Index().Build("0")
                            .VarType(variantType)
                            .Build()
                        .Add<TCoVariant>()
                            .Item<TCoDictKeys>()
                                .Dict("dict")
                                .Build()
                            .Index().Build("1")
                            .VarType(variantType)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto dictPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(computeKeysStage)
                .Index().Build("0")
                .Build()
            .Build()
        .Done();

    auto keysPrecompute = Build<TDqPhyPrecompute>(ctx, pos)
        .Connection<TDqCnValue>()
            .Output()
                .Stage(computeKeysStage)
                .Index().Build("1")
                .Build()
            .Build()
        .Done();

    return TDictAndKeysResult {
        .DictPrecompute = dictPrecompute,
        .KeysPrecompute = keysPrecompute
    };
}

TKqpCnStreamLookup BuildStreamLookupOverPrecompute(const TKikimrTableDescription & table,  NYql::NNodes::TDqPhyPrecompute& keysPrecompute,
    NYql::NNodes::TExprBase originalInput, const TKqpTable& kqpTableNode, const TPositionHandle& pos, TExprContext& ctx, const TVector<TString>& extraColumnsToRead)
{
    TKqpStreamLookupSettings streamLookupSettings;
    streamLookupSettings.Strategy = EStreamLookupStrategyType::LookupRows;

    TVector<const TItemExprType*> expectedRowTypeItems;
    const TTypeAnnotationNode* originalAnnotation = originalInput.Ptr()->GetTypeAnn();
    YQL_ENSURE(originalAnnotation, "stream lookup received input which isn't properly annontated");

    TExprNode::TPtr input = originalInput.Ptr();
    const TTypeAnnotationNode* itemType = nullptr;

    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        itemType = input->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
    } else {
        YQL_ENSURE(EnsureListType(*input, ctx), "stream or list is allowed as input of the stream lookup");
        itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    }

    YQL_ENSURE(itemType->GetKind() == ETypeAnnotationKind::Struct);
    auto* columns = itemType->Cast<TStructExprType>();

    for (auto& column : table.Metadata->KeyColumnNames) {
        auto columnType = columns->FindItemType(column);
        YQL_ENSURE(columnType, "stream lookup input doesn't contain required column " << column);
        expectedRowTypeItems.push_back(ctx.MakeType<TItemExprType>(column, columnType));
    }

    const TTypeAnnotationNode* expectedRowType = ctx.MakeType<TStructExprType>(expectedRowTypeItems);
    const TTypeAnnotationNode* streamLookupInputType = ctx.MakeType<TListExprType>(expectedRowType);

    TSet<TString> columnsToReadSet(table.Metadata->KeyColumnNames.begin(), table.Metadata->KeyColumnNames.end());
    for(const auto& col: extraColumnsToRead) {
        columnsToReadSet.insert(col);
    }

    TVector<TString> columnsToRead(columnsToReadSet.begin(), columnsToReadSet.end());

    return Build<TKqpCnStreamLookup>(ctx, pos)
        .Output()
            .Stage<TDqStage>()
                .Inputs()
                    .Add(keysPrecompute)
                    .Build()
                .Program()
                    .Args({"stream_lookup_keys"})
                    .Body<TCoToStream>()
                        .Input("stream_lookup_keys")
                        .Build()
                    .Build()
                .Settings().Build()
                .Build()
            .Index().Build(0)
            .Build()
        .Table(kqpTableNode)
        .Columns(BuildColumnsList(columnsToRead, pos, ctx))
        .InputType(ExpandType(pos, *streamLookupInputType, ctx))
        .Settings(streamLookupSettings.BuildNode(ctx, pos))
        .Done();
}

TExprBase KqpBuildUpdateStages(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlUpdateRows>()) {
        return node;
    }
    auto update = node.Cast<TKqlUpdateRows>();

    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, update.Table().Path());

    const bool isSink = NeedSinks(table, kqpCtx);
    const bool needPrecompute = !isSink;

    if (needPrecompute) {
        auto payloadSelector = MakeRowsPayloadSelector(update.Columns(), table, update.Pos(), ctx);
        auto condenseResult = CondenseInputToDictByPk(update.Input(), table, payloadSelector, ctx);
        if (!condenseResult) {
            return node;
        }

        auto inputDictAndKeys = PrecomputeDictAndKeys(*condenseResult, update.Pos(), ctx);

        auto prepareUpdateStage = Build<TDqStage>(ctx, update.Pos())
            .Inputs()
                .Add(BuildStreamLookupOverPrecompute(table, inputDictAndKeys.KeysPrecompute, update.Input(), update.Table(), update.Pos(), ctx))
                .Add(inputDictAndKeys.DictPrecompute)
                .Build()
            .Program()
                .Args({"keys_list", "dict"})
                .Body<TCoFlatMap>()
                    .Input("keys_list")
                    .Lambda()
                        .Args({"existingKey"})
                        .Body<TCoJust>()
                            .Input<TCoFlattenMembers>()
                                .Add()
                                    .Name().Build("")
                                    .Value<TCoUnwrap>() // Key should always exist in the dict
                                        .Optional<TCoLookup>()
                                            .Collection("dict")
                                            .Lookup("existingKey")
                                            .Build()
                                        .Build()
                                    .Build()
                                .Add()
                                    .Name().Build("")
                                    .Value("existingKey")
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        auto prepareUpdate = Build<TDqCnUnionAll>(ctx, update.Pos())
            .Output()
                .Stage(prepareUpdateStage)
                .Index().Build("0")
                .Build()
            .Done();

        return Build<TKqlUpsertRows>(ctx, node.Pos())
            .Table(update.Table())
            .Input(prepareUpdate)
            .Columns(update.Columns())
            .ReturningColumns(update.ReturningColumns())
            .IsBatch(ctx.NewAtom(update.Pos(), "false"))
            .Settings()
                .Add()
                    .Name().Build("IsUpdate")
                .Build()
            .Build()
            .Done();
    } else {
        return Build<TKqlUpsertRows>(ctx, update.Pos())
            .Table(update.Table())
            .Input(update.Input())
            .Columns(update.Columns())
            .ReturningColumns(update.ReturningColumns())
            .IsBatch(ctx.NewAtom(update.Pos(), "false"))
            .Settings()
                .Add()
                    .Name().Build("Mode")
                    .Value<TCoAtom>().Build("update")
                .Build()
                .Add()
                    .Name().Build("IsUpdate")
                .Build()
            .Build()
            .Done();
    }
}

} // namespace NKikimr::NKqp::NOpt
