#include "kqp_opt_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

bool InplaceUpdateEnabled(const TKikimrConfiguration& config) {
    if (!config.HasAllowKqpUnsafeCommit()) {
        return false;
    }

    if (!config.HasOptEnableInplaceUpdate()) {
        return false;
    }

    return true;
}

bool InplaceUpdateEnabled(
    const TKikimrConfiguration& config,
    const TKikimrTableDescription& tableDesc,
    const TCoAtomList& columns)
{
    for (const auto& column : columns) {
        if (tableDesc.GetColumnType(column.StringValue())->GetKind() == ETypeAnnotationKind::Pg) {
            return false;
        }
    }

    return InplaceUpdateEnabled(config);
}

bool IsSingleKeyStream(const TExprBase& stream, TExprContext&) {
    auto asList = stream.Maybe<TCoIterator>().List().Maybe<TCoAsList>();
    if (!asList) {
        return false;
    }

    if (asList.Cast().ArgCount() > 1) {
        return false;
    }

    auto asStruct = asList.Cast().Arg(0).Maybe<TCoAsStruct>();
    if (!asStruct) {
        return false;
    }

    return true;
}

const THashSet<TStringBuf> SafeCallables {
    TCoJust::CallableName(),
    TCoCoalesce::CallableName(),
    TCoToOptional::CallableName(),
    TCoHead::CallableName(),
    TCoLast::CallableName(),
    TCoNth::CallableName(),
    TCoToList::CallableName(),
    TCoAsList::CallableName(),

    TCoMember::CallableName(),
    TCoAsStruct::CallableName(),

    TCoNothing::CallableName(),
    TCoNull::CallableName(),
    TCoDefault::CallableName(),
    TCoExists::CallableName(),

    TCoIf::CallableName(),

    TCoDataType::CallableName(),
    TCoOptionalType::CallableName(),

    TCoParameter::CallableName(),

    "Concat",
    "Substring",
};

bool IsStructOrOptionalStruct(const NYql::TTypeAnnotationNode* type) {
    if (type->GetKind() == ETypeAnnotationKind::Struct) {
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        return type->Cast<TOptionalExprType>()->GetItemType()->GetKind() == ETypeAnnotationKind::Struct;
    }

    return false;
}

bool IsMapWrite(const TKikimrTableDescription& table, TExprBase input, TExprContext& ctx) {
// #define DBG YQL_CLOG(ERROR, ProviderKqp)
#define DBG TStringBuilder()

    DBG << "--> " << KqpExprToPrettyString(input, ctx);

    auto maybeFlatMap = input.Maybe<TCoFlatMap>();
    if (!maybeFlatMap) {
        return false;
    }
    auto flatmap = maybeFlatMap.Cast();

    if (!IsStructOrOptionalStruct(flatmap.Lambda().Ref().GetTypeAnn())) {
        DBG << " --> FlatMap with expanding lambda: " << *flatmap.Lambda().Ref().GetTypeAnn();
        return false;
    }

    auto maybeLookupTable = flatmap.Input().Maybe<TKqpLookupTable>();
    if (!maybeLookupTable) {
        maybeLookupTable = flatmap.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqpLookupTable>();
    }

    if (!maybeLookupTable) {
        DBG << " --> not FlatMap over KqpLookupTable";
        return false;
    }

    auto read = maybeLookupTable.Cast();

    // check same table
    if (table.Metadata->PathId.ToString() != read.Table().PathId().Value()) {
        DBG << " --> not same table";
        return false;
    }

    // check keys count
    if (!IsSingleKeyStream(read.LookupKeys(), ctx)) {
        DBG << " --> not single key stream";
        return false;
    }

    // full key (not prefix)
    const auto& lookupKeyType = GetSeqItemType(*read.LookupKeys().Ref().GetTypeAnn());
    if (table.Metadata->KeyColumnNames.size() != lookupKeyType.Cast<TStructExprType>()->GetSize()) {
        DBG << " --> not full key";
        return false;
    }

    TMaybe<THashSet<TStringBuf>> passthroughFields;
    if (!IsPassthroughFlatMap(flatmap, &passthroughFields)) {
        return false;
    }

    if (passthroughFields) {
        for (auto& keyColumn : table.Metadata->KeyColumnNames) {
            if (!passthroughFields->contains(keyColumn)) {
                return false;
            }
        }
    }

    auto lambda = flatmap.Lambda();
    if (!lambda.Ref().IsComplete()) {
        return false;
    }

    TMaybeNode<TExprBase> notSafeNode;
    // white list of callables in lambda
    VisitExpr(lambda.Body().Ptr(),
        [&notSafeNode] (const TExprNode::TPtr&) {
            return !notSafeNode;
        },
        [&notSafeNode](const TExprNode::TPtr& node) {
            if (notSafeNode) {
                return false;
            }
            if (node->IsCallable()) {
                DBG << " --> visit: " << node->Content();

                auto expr = TExprBase(node);

                if (expr.Maybe<TCoDataCtor>()) {
                    return true;
                }
                if (expr.Maybe<TCoCompare>()) {
                    return true;
                }
                if (expr.Maybe<TCoAnd>()) {
                    return true;
                }
                if (expr.Maybe<TCoOr>()) {
                    return true;
                }
                if (expr.Maybe<TCoBinaryArithmetic>()) {
                    return true;
                }
                if (expr.Maybe<TCoCountBase>()) {
                    return true;
                }

                if (SafeCallables.contains(node->Content())) {
                    return true;
                }

                // TODO: allowed UDFs

                notSafeNode = expr;
                DBG << " --> not safe node: " << node->Content();
                return false;
            }

            return true;
        });

    return !notSafeNode;

#undef DBG
}

TDqPhyPrecompute BuildPrecomputeStage(TExprBase expr, TExprContext& ctx) {
    Y_DEBUG_ABORT_UNLESS(IsDqPureExpr(expr));

    auto pureStage = Build<TDqStage>(ctx, expr.Pos())
        .Inputs()
            .Build()
        .Program()
            .Args({})
            .Body<TCoToStream>()
                .Input<TCoJust>()
                    .Input(expr)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    auto dqValue = Build<TDqCnValue>(ctx, expr.Pos())
        .Output()
            .Stage(pureStage)
            .Index().Build("0")
            .Build()
        .Done();

    return Build<TDqPhyPrecompute>(ctx, expr.Pos())
        .Connection(dqValue)
        .Done();
}

bool BuildUpsertRowsEffect(const TKqlUpsertRows& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TCoArgument& inputArg, TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect)
{
    TKqpUpsertRowsSettings settings;
    if (node.Settings()) {
        settings = TKqpUpsertRowsSettings::Parse(node.Settings().Cast());
    }
    if (IsDqPureExpr(node.Input())) {
        stageInput = BuildPrecomputeStage(node.Input(), ctx);

        effect = Build<TKqpUpsertRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoIterator>()
                .List(inputArg)
                .Build()
            .Columns(node.Columns())
            .Settings(settings.BuildNode(ctx, node.Pos()))
            .Done();
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }

    auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());

    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
    auto program = dqUnion.Output().Stage().Program();
    auto input = program.Body();

    if (InplaceUpdateEnabled(*kqpCtx.Config, table, node.Columns()) && IsMapWrite(table, input, ctx)) {
        stageInput = Build<TKqpCnMapShard>(ctx, node.Pos())
            .Output()
                .Stage(dqUnion.Output().Stage())
                .Index(dqUnion.Output().Index())
                .Build()
            .Done();

        settings.SetInplace();

        effect = Build<TKqpUpsertRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoFromFlow>()
                .Input(inputArg)
                .Build()
            .Columns(node.Columns())
            .Settings(settings.BuildNode(ctx, node.Pos()))
            .Done();
    } else {
        stageInput = Build<TDqPhyPrecompute>(ctx, node.Pos())
            .Connection(dqUnion)
            .Done();

        effect = Build<TKqpUpsertRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoIterator>()
                .List(inputArg)
                .Build()
            .Columns(node.Columns())
            .Settings(settings.BuildNode(ctx, node.Pos()))
            .Done();
    }

    return true;
}

bool BuildDeleteRowsEffect(const TKqlDeleteRows& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TCoArgument& inputArg, TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect)
{
    if (IsDqPureExpr(node.Input())) {
        stageInput = BuildPrecomputeStage(node.Input(), ctx);

        effect = Build<TKqpDeleteRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoIterator>()
                .List(inputArg)
                .Build()
            .Done();
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }

    auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());

    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
    auto input = dqUnion.Output().Stage().Program().Body();

    if (InplaceUpdateEnabled(*kqpCtx.Config) && IsMapWrite(table, input, ctx)) {
        stageInput = Build<TKqpCnMapShard>(ctx, node.Pos())
            .Output()
                .Stage(dqUnion.Output().Stage())
                .Index(dqUnion.Output().Index())
                .Build()
            .Done();

        effect = Build<TKqpDeleteRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoFromFlow>()
                .Input(inputArg)
                .Build()
            .Done();
    } else {
        stageInput = Build<TDqPhyPrecompute>(ctx, node.Pos())
            .Connection(dqUnion)
            .Done();

        effect = Build<TKqpDeleteRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoIterator>()
                .List(inputArg)
                .Build()
            .Done();
    }

    return true;
}

bool BuildEffects(TPositionHandle pos, const TVector<TExprBase>& effects,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TVector<TExprBase>& builtEffects)
{
    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputs;
    TVector<TExprBase> newEffects;
    TVector<TExprBase> newSinkEffects;
    newEffects.reserve(effects.size());
    newSinkEffects.reserve(effects.size());

    for (const auto& effect : effects) {
        TMaybeNode<TExprBase> newEffect;
        bool sinkEffect = false;
        YQL_ENSURE(effect.Maybe<TKqlEffectBase>());
        if (effect.Maybe<TKqlTableEffect>()) {
            TMaybeNode<TExprBase> input;
            TCoArgument inputArg = Build<TCoArgument>(ctx, pos)
                .Name("inputArg")
                .Done();

            if (auto maybeUpsertRows = effect.Maybe<TKqlUpsertRows>()) {
                if (!BuildUpsertRowsEffect(maybeUpsertRows.Cast(), ctx, kqpCtx, inputArg, input, newEffect)) {
                    return false;
                }
            }

            if (auto maybeDeleteRows = effect.Maybe<TKqlDeleteRows>()) {
                if (!BuildDeleteRowsEffect(maybeDeleteRows.Cast(), ctx, kqpCtx, inputArg, input, newEffect)) {
                    return false;
                }
            }

            if (input) {
                inputArgs.push_back(inputArg);
                inputs.push_back(input.Cast());
            }
        } else if (auto maybeExt = effect.Maybe<TKqlExternalEffect>()) {
            sinkEffect = true;
            TKqlExternalEffect externalEffect = maybeExt.Cast();
            TExprBase input = externalEffect.Input();
            auto maybeStage = input.Maybe<TDqStageBase>();
            if (!maybeStage) {
                return false;
            }
            auto stage = maybeStage.Cast();
            const auto outputsList = stage.Outputs();
            if (!outputsList) {
                return false;
            }
            TDqStageOutputsList outputs = outputsList.Cast();
            YQL_ENSURE(outputs.Size() == 1, "Multiple sinks are not supported yet");
            TDqOutputAnnotationBase output = outputs.Item(0);
            if (!output.Maybe<TDqSink>()) {
                return false;
            }
            newEffect = Build<TKqpSinkEffect>(ctx, effect.Pos())
                .Stage(maybeStage.Cast().Ptr())
                .SinkIndex().Build("0")
                .Done();
        }

        YQL_ENSURE(newEffect);
        if (sinkEffect) {
            newSinkEffects.push_back(newEffect.Cast());
        } else {
            newEffects.push_back(newEffect.Cast());
        }
    }

    if (!newEffects.empty()) {
        auto stage = Build<TDqStage>(ctx, pos)
            .Inputs()
                .Add(inputs)
                .Build()
            .Program()
                .Args(inputArgs)
                .Body<TKqpEffects>()
                    .Add(newEffects)
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        for (ui32 i = 0; i < newEffects.size(); ++i) {
            auto effect = Build<TDqOutput>(ctx, pos)
                .Stage(stage)
                .Index().Build(ToString(0))
                .Done();

            builtEffects.push_back(effect);
        }
    }

    if (!newSinkEffects.empty()) {
        builtEffects.insert(builtEffects.end(), newSinkEffects.begin(), newSinkEffects.end());
    }

    return true;
}

template <bool GroupEffectsByTable>
TMaybeNode<TKqlQuery> BuildEffects(const TKqlQuery& query, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    TVector<TExprBase> builtEffects;

    if constexpr (GroupEffectsByTable) {
        TMap<TStringBuf, TVector<TExprBase>> tableEffectsMap;
        for (const auto& maybeEffect : query.Effects()) {
            if (const auto maybeList = maybeEffect.Maybe<TExprList>()) {
                for (const auto effect : maybeList.Cast()) {
                    YQL_ENSURE(effect.Maybe<TKqlTableEffect>());
                    auto tableEffect = effect.Cast<TKqlTableEffect>();

                    tableEffectsMap[tableEffect.Table().Path()].push_back(effect);
                }
            } else {
                YQL_ENSURE(maybeEffect.Maybe<TKqlTableEffect>());
                auto tableEffect = maybeEffect.Cast<TKqlTableEffect>();

                tableEffectsMap[tableEffect.Table().Path()].push_back(maybeEffect);
            }
        }

        for (const auto& pair: tableEffectsMap) {
            if (!BuildEffects(query.Pos(), pair.second, ctx, kqpCtx, builtEffects)) {
                return {};
            }
        }
    } else {
        builtEffects.reserve(query.Effects().Size() * 2);

        for (const auto& maybeEffect : query.Effects()) {
            if (const auto maybeList = maybeEffect.Maybe<TExprList>()) {
                for (const auto effect : maybeList.Cast()) {
                    if (!BuildEffects(query.Pos(), {effect}, ctx, kqpCtx, builtEffects)) {
                        return {};
                    }
                }
            } else {
                if (!BuildEffects(query.Pos(), {maybeEffect}, ctx, kqpCtx, builtEffects)) {
                    return {};
                }
            }
        }
    }

    return Build<TKqlQuery>(ctx, query.Pos())
        .Results(query.Results())
        .Effects()
            .Add(builtEffects)
            .Build()
        .Done();
}

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpQueryEffectsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx) {
    return CreateFunctorTransformer([kqpCtx](const TExprNode::TPtr& input, TExprNode::TPtr& output,
        TExprContext& ctx) -> TStatus
    {
        output = input;

        TExprBase inputNode(input);
        YQL_ENSURE(inputNode.Maybe<TKqlQuery>());

        TKqlQuery query = inputNode.Cast<TKqlQuery>();

        bool requireBuild = false;
        bool hasBuilt = false;
        for (const auto& effect : query.Effects()) {
            if (!IsBuiltEffect(effect)) {
                requireBuild = true;
            } else {
                hasBuilt = true;
            }
        }

        if (hasBuilt) {
            YQL_ENSURE(!requireBuild);
        }

        if (!requireBuild) {
            return TStatus::Ok;
        }

        TParentsMap parentsMap;
        GatherParents(*input, parentsMap);

        auto result = BuildEffects<false>(query, ctx, *kqpCtx);
        if (!result) {
            return TStatus::Error;
        }

        output = result.Cast().Ptr();
        return TStatus(TStatus::Repeat, true);
    });
}

} // namespace NKikimr::NKqp::NOpt
