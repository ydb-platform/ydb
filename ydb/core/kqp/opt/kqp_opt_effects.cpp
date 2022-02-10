#include "kqp_opt_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>

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

bool IsMapWrite(const TKikimrTableDescription& table, TExprBase input) {
    // TODO: Check for non-deterministic & unsafe functions (like UDF).
    // TODO: Once we have partitioning constraints implemented in query optimizer,
    // use them to detect map writes.
    if (!input.Maybe<TCoFlatMap>().Input().Maybe<TKqlReadTableBase>()) {
        return false;
    }

    auto flatmap = input.Cast<TCoFlatMap>();
    auto read = flatmap.Input().Cast<TKqlReadTableBase>();

    if (table.Metadata->PathId.ToString() != read.Table().PathId().Value()) {
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

    return true;
}

TDqPhyPrecompute BuildPrecomputeStage(TExprBase expr, TExprContext& ctx) {
    Y_VERIFY_DEBUG(IsDqPureExpr(expr));

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
    if (IsDqPureExpr(node.Input())) {
        stageInput = BuildPrecomputeStage(node.Input(), ctx);

        effect = Build<TKqpUpsertRows>(ctx, node.Pos())
            .Table(node.Table())
            .Input<TCoIterator>()
                .List(inputArg)
                .Build()
            .Columns(node.Columns())
            .Settings().Build()
            .Done();
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }

    auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());

    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
    auto input = dqUnion.Output().Stage().Program().Body();

    if (InplaceUpdateEnabled(*kqpCtx.Config) && IsMapWrite(table, input)) {
        stageInput = Build<TKqpCnMapShard>(ctx, node.Pos())
            .Output()
                .Stage(dqUnion.Output().Stage())
                .Index(dqUnion.Output().Index())
                .Build()
            .Done();

        TKqpUpsertRowsSettings settings;
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
            .Settings().Build()
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

    if (InplaceUpdateEnabled(*kqpCtx.Config) && IsMapWrite(table, input)) {
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

bool BuildEffects(TPositionHandle pos, const TVector<TKqlTableEffect>& effects,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, TVector<TExprBase>& builtEffects)
{
    TVector<TCoArgument> inputArgs;
    TVector<TExprBase> inputs;
    TVector<TExprBase> newEffects;
    newEffects.reserve(effects.size());

    for (const auto& effect : effects) {
        TCoArgument inputArg = Build<TCoArgument>(ctx, pos)
            .Name("inputArg")
            .Done();

        TMaybeNode<TExprBase> input;
        TMaybeNode<TExprBase> newEffect;

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

        YQL_ENSURE(newEffect);
        newEffects.push_back(newEffect.Cast());

        if (input) {
            inputArgs.push_back(inputArg);
            inputs.push_back(input.Cast());
        }
    }

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

    return true;
}

template <bool GroupEffectsByTable>
TMaybeNode<TKqlQuery> BuildEffects(const TKqlQuery& query, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx)
{
    TVector<TExprBase> builtEffects;

    if constexpr (GroupEffectsByTable) {
        TMap<TStringBuf, TVector<TKqlTableEffect>> tableEffectsMap;
        for (const auto& maybeEffect: query.Effects()) {
            if (const auto maybeList = maybeEffect.Maybe<TExprList>()) {
                for (const auto effect : maybeList.Cast()) {
                    YQL_ENSURE(effect.Maybe<TKqlTableEffect>());
                    auto tableEffect = effect.Cast<TKqlTableEffect>();

                    tableEffectsMap[tableEffect.Table().Path()].push_back(tableEffect);
                }
            } else {
                YQL_ENSURE(maybeEffect.Maybe<TKqlTableEffect>());
                auto tableEffect = maybeEffect.Cast<TKqlTableEffect>();

                tableEffectsMap[tableEffect.Table().Path()].push_back(tableEffect);
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
                    YQL_ENSURE(effect.Maybe<TKqlTableEffect>());
                    auto tableEffect = effect.Cast<TKqlTableEffect>();

                    if (!BuildEffects(query.Pos(), {tableEffect}, ctx, kqpCtx, builtEffects)) {
                        return {};
                    }
                }
            } else {
                YQL_ENSURE(maybeEffect.Maybe<TKqlTableEffect>());
                auto tableEffect = maybeEffect.Cast<TKqlTableEffect>();

                if (!BuildEffects(query.Pos(), {tableEffect}, ctx, kqpCtx, builtEffects)) {
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
            if (!effect.Maybe<TDqOutput>()) {
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
