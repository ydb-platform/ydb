#include "kqp_opt_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

TCoAtomList BuildKeyColumnsList(const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx)
{
    TVector<TExprBase> columns;
    for (const auto& name : table.Metadata->KeyColumnNames) {
        columns.emplace_back(Build<TCoAtom>(ctx, pos)
            .Value(name)
            .Done());
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(columns)
        .Done();
}

TDqStage RebuildPureStageWithSink(TExprBase expr, const TKqpTable& table,
        const bool allowInconsistentWrites, const bool enableStreamWrite, bool isBatch,
        const TStringBuf mode, const bool isIndexImplTable, const TVector<TCoNameValueTuple>& settings, const i64 order, TExprContext& ctx) {
    Y_DEBUG_ABORT_UNLESS(IsDqPureExpr(expr));

    auto settingsNode = Build<TCoNameValueTupleList>(ctx, expr.Pos())
        .Add(settings)
        .Done();

    return Build<TDqStage>(ctx, expr.Pos())
        .Inputs()
            .Build()
        .Program()
            .Args({})
            .Body<TCoToFlow>()
                .Input(expr)
                .Build()
            .Build()
        .Outputs<TDqStageOutputsList>()
            .Add<TDqSink>()
                .DataSink<TKqpTableSink>()
                    .Category(ctx.NewAtom(expr.Pos(), NYql::KqpTableSinkName))
                    .Cluster(ctx.NewAtom(expr.Pos(), "db"))
                    .Build()
                .Index().Value("0").Build()
                .Settings<TKqpTableSinkSettings>()
                    .Table(table)
                    .InconsistentWrite(allowInconsistentWrites
                        ? ctx.NewAtom(expr.Pos(), "true")
                        : ctx.NewAtom(expr.Pos(), "false"))
                    .StreamWrite(enableStreamWrite
                        ? ctx.NewAtom(expr.Pos(), "true")
                        : ctx.NewAtom(expr.Pos(), "false"))
                    .Mode(ctx.NewAtom(expr.Pos(), mode))
                    .Priority(ctx.NewAtom(expr.Pos(), ToString(order)))
                    .IsBatch(isBatch
                        ? ctx.NewAtom(expr.Pos(), "true")
                        : ctx.NewAtom(expr.Pos(), "false"))
                    .IsIndexImplTable(isIndexImplTable
                        ? ctx.NewAtom(expr.Pos(), "true")
                        : ctx.NewAtom(expr.Pos(), "false"))
                    .Settings(settingsNode)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
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

bool BuildFillTableEffect(const TKqlFillTable& node, TExprContext& ctx,
    TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect, bool& sinkEffect, const i64 order)
{
    sinkEffect = true;
    const i64 priority = 0;
    AFL_ENSURE(order == 0);

    const TKqpTable table = Build<TKqpTable>(ctx, node.Pos())
        .Path(node.Table())
        .PathId(ctx.NewAtom(node.Pos(), ""))
        .SysView(ctx.NewAtom(node.Pos(), ""))
        .Version(ctx.NewAtom(node.Pos(), ""))
        .Done();

    TVector<TCoNameValueTuple> settings;
    settings.emplace_back(
        Build<TCoNameValueTuple>(ctx, node.Pos())
            .Name().Build("OriginalPath")
            .Value<TCoAtom>().Build(node.OriginalPath())
            .Done());

    if (IsDqPureExpr(node.Input())) {
        stageInput = RebuildPureStageWithSink(
            node.Input(), table,
            /* allowInconsistentWrites */ true, /* useStreamWrite */ true,
            /* isBatch */ false, "fill_table", /* isIndexImplTable */ false, settings,
            priority, ctx);
        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(stageInput.Cast().Ptr())
            .SinkIndex().Build("0")
            .Done();
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }

    auto settingsNode = Build<TCoNameValueTupleList>(ctx, node.Pos())
        .Add(settings)
        .Done();

    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
    auto stage = dqUnion.Output().Stage();
    auto program = stage.Program();
    auto input = program.Body();

    auto sink = Build<TDqSink>(ctx, node.Pos())
        .DataSink<TKqpTableSink>()
            .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
            .Cluster(ctx.NewAtom(node.Pos(), "db"))
            .Build()
        .Index().Value("0").Build()
        .Settings<TKqpTableSinkSettings>()
            .Table(table)
            .InconsistentWrite(ctx.NewAtom(node.Pos(), "true"))
            .StreamWrite(ctx.NewAtom(node.Pos(), "true"))
            .Mode(ctx.NewAtom(node.Pos(), "fill_table"))
            .Priority(ctx.NewAtom(node.Pos(), ToString(priority)))
            .IsBatch(ctx.NewAtom(node.Pos(), "false"))
            .IsIndexImplTable(ctx.NewAtom(node.Pos(), "false"))
            .Settings(settingsNode)
            .Build()
        .Done();

    const auto rowArgument = Build<TCoArgument>(ctx, node.Pos())
        .Name("row")
        .Done();

    auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
        .Output(dqUnion.Output())
        .Done();
    stageInput = Build<TDqStage>(ctx, node.Pos())
        .Inputs()
            .Add(mapCn)
            .Build()
        .Program()
            .Args({rowArgument})
            .Body<TCoToFlow>()
                .Input(rowArgument)
                .Build()
            .Build()
        .Outputs<TDqStageOutputsList>()
            .Add(sink)
            .Build()
        .Settings().Build()
        .Done();

    effect = Build<TKqpSinkEffect>(ctx, node.Pos())
        .Stage(stageInput.Cast().Ptr())
        .SinkIndex().Build("0")
        .Done();

    return true;
}

bool BuildUpsertRowsEffect(const TKqlUpsertRows& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const TCoArgument& inputArg, TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect, bool& sinkEffect, const i64 order)
{
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());

    TKqpUpsertRowsSettings settings;
    if (node.Settings()) {
        settings = TKqpUpsertRowsSettings::Parse(node.Settings().Cast());
    }

    sinkEffect = NeedSinks(table, kqpCtx) || (kqpCtx.IsGenericQuery() && settings.AllowInconsistentWrites);

    const bool useStreamWriteForConsistentSink = CanEnableStreamWrite(table, kqpCtx)
        && (!HasReadTable(node.Table().PathId().Value(), node.Input().Ptr()) || settings.IsConditionalUpdate);
    const bool useStreamWrite = sinkEffect && (settings.AllowInconsistentWrites || useStreamWriteForConsistentSink);
    const bool isIndexImplTable = table.Metadata->IsIndexImplTable;

    const bool isOlap = (table.Metadata->Kind == EKikimrTableKind::Olap);
    const i64 priority = isOlap ? 0 : order;

    if (IsDqPureExpr(node.Input())) {
        if (sinkEffect) {
            stageInput = RebuildPureStageWithSink(
                node.Input(), node.Table(),
                settings.AllowInconsistentWrites, useStreamWrite,
                node.IsBatch() == "true", settings.Mode, isIndexImplTable, {}, priority, ctx);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Cast().Ptr())
                .SinkIndex().Build("0")
                .Done();
        } else {
            stageInput = BuildPrecomputeStage(node.Input(), ctx);
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

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }

    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
    auto stage = dqUnion.Output().Stage();
    auto program = stage.Program();
    auto input = program.Body();

    if (sinkEffect) {
        auto sink = Build<TDqSink>(ctx, node.Pos())
            .DataSink<TKqpTableSink>()
                .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                .Cluster(ctx.NewAtom(node.Pos(), "db"))
                .Build()
            .Index().Value("0").Build()
            .Settings<TKqpTableSinkSettings>()
                .Table(node.Table())
                .InconsistentWrite(settings.AllowInconsistentWrites
                    ? ctx.NewAtom(node.Pos(), "true")
                    : ctx.NewAtom(node.Pos(), "false"))
                .StreamWrite(useStreamWrite
                    ? ctx.NewAtom(node.Pos(), "true")
                    : ctx.NewAtom(node.Pos(), "false"))
                .Mode(ctx.NewAtom(node.Pos(), settings.Mode))
                .Priority(ctx.NewAtom(node.Pos(), ToString(priority)))
                .IsBatch(node.IsBatch())
                .IsIndexImplTable(isIndexImplTable
                    ? ctx.NewAtom(node.Pos(), "true")
                    : ctx.NewAtom(node.Pos(), "false"))
                .Settings()
                    .Build()
                .Build()
            .Done();

        const auto rowArgument = Build<TCoArgument>(ctx, node.Pos())
            .Name("row")
            .Done();

        if ((table.Metadata->Kind == EKikimrTableKind::Olap && useStreamWrite)
                || settings.AllowInconsistentWrites) {
            auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
                .Output(dqUnion.Output())
                .Done();
            stageInput = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Add(mapCn)
                    .Build()
                .Program()
                    .Args({rowArgument})
                    .Body<TCoToFlow>()
                        .Input(rowArgument)
                        .Build()
                    .Build()
                .Outputs<TDqStageOutputsList>()
                    .Add(sink)
                    .Build()
                .Settings().Build()
                .Done();
        } else {
            // OLTP is expected to mostly use just few shards,
            // so we use union all + one sink. It's important for write optimizations support.
            // NOTE: OLTP large writes expected to fail anyway due to problems with locks/splits.

            stageInput = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Add(dqUnion)
                    .Build()
                .Program()
                    .Args({rowArgument})
                    .Body<TCoToFlow>()
                        .Input(rowArgument)
                        .Build()
                    .Build()
                .Outputs<TDqStageOutputsList>()
                    .Add(sink)
                    .Build()
                .Settings().Build()
                .Done();
        }

        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(stageInput.Cast().Ptr())
            .SinkIndex().Build("0")
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
    const TCoArgument& inputArg, TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect, bool& sinkEffect, const i64 order)
{
    TKqpDeleteRowsSettings settings;
    if (node.Settings()) {
        settings = TKqpDeleteRowsSettings::Parse(node.Settings().Cast());
    }

    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());
    sinkEffect = NeedSinks(table, kqpCtx);

    const bool useStreamWriteForConsistentSink = CanEnableStreamWrite(table, kqpCtx)
        && (!HasReadTable(node.Table().PathId().Value(), node.Input().Ptr()) || settings.IsConditionalDelete);
    const bool useStreamWrite = sinkEffect && useStreamWriteForConsistentSink;
    const bool isIndexImplTable = table.Metadata->IsIndexImplTable;

    const bool isOlap = (table.Metadata->Kind == EKikimrTableKind::Olap);
    const i64 priority = isOlap ? 0 : order;

    if (IsDqPureExpr(node.Input())) {
        if (sinkEffect) {
            const auto keyColumns = BuildKeyColumnsList(table, node.Pos(), ctx);
            stageInput = RebuildPureStageWithSink(
                node.Input(), node.Table(),
                false, useStreamWrite, node.IsBatch() == "true",
                "delete", isIndexImplTable, {}, priority, ctx);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Cast().Ptr())
                .SinkIndex().Build("0")
                .Done();
        } else {
            stageInput = BuildPrecomputeStage(node.Input(), ctx);

            effect = Build<TKqpDeleteRows>(ctx, node.Pos())
                .Table(node.Table())
                .Input<TCoIterator>()
                    .List(inputArg)
                    .Build()
                .Done();
        }
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }


    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();
    auto input = dqUnion.Output().Stage().Program().Body();

    if (sinkEffect) {
        auto sink = Build<TDqSink>(ctx, node.Pos())
            .DataSink<TKqpTableSink>()
                .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                .Cluster(ctx.NewAtom(node.Pos(), "db"))
                .Build()
            .Index().Value("0").Build()
            .Settings<TKqpTableSinkSettings>()
                .Table(node.Table())
                .InconsistentWrite(ctx.NewAtom(node.Pos(), "false"))
                .StreamWrite(useStreamWrite
                        ? ctx.NewAtom(node.Pos(), "true")
                        : ctx.NewAtom(node.Pos(), "false"))
                .Mode(ctx.NewAtom(node.Pos(), "delete"))
                .Priority(ctx.NewAtom(node.Pos(), ToString(priority)))
                .IsBatch(node.IsBatch())
                .IsIndexImplTable(isIndexImplTable
                        ? ctx.NewAtom(node.Pos(), "true")
                        : ctx.NewAtom(node.Pos(), "false"))
                .Settings()
                    .Build()
                .Build()
            .Done();

        const auto rowArgument = Build<TCoArgument>(ctx, node.Pos())
            .Name("row")
            .Done();

        if (table.Metadata->Kind == EKikimrTableKind::Olap) {
            auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
                .Output(dqUnion.Output())
                .Done();
            stageInput = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Add(mapCn)
                    .Build()
                .Program()
                    .Args({rowArgument})
                    .Body<TCoToFlow>()
                        .Input(rowArgument)
                        .Build()
                    .Build()
                .Outputs<TDqStageOutputsList>()
                    .Add(sink)
                    .Build()
                .Settings().Build()
                .Done();
        } else {
            stageInput = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Add(dqUnion)
                    .Build()
                .Program()
                    .Args({rowArgument})
                    .Body<TCoToFlow>()
                        .Input(rowArgument)
                        .Build()
                    .Build()
                .Outputs<TDqStageOutputsList>()
                    .Add(sink)
                    .Build()
                .Settings().Build()
                .Done();
        }

        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(stageInput.Cast().Ptr())
            .SinkIndex().Build("0")
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
    i64 order = builtEffects.size();

    for (const auto& effect : effects) {
        TMaybeNode<TExprBase> newEffect;
        bool sinkEffect = false;
        if (effect.Maybe<TKqlFillTable>()) {
            TMaybeNode<TExprBase> input;
            TCoArgument inputArg = Build<TCoArgument>(ctx, pos)
                .Name("inputArg")
                .Done();
            const auto maybeFillTable = effect.Maybe<TKqlFillTable>();
            AFL_ENSURE(maybeFillTable);
            if (!BuildFillTableEffect(maybeFillTable.Cast(), ctx, input, newEffect, sinkEffect, order)) {
                return false;
            }
            ++order;

            if (input) {
                inputArgs.push_back(inputArg);
                inputs.push_back(input.Cast());
            }
        } else if (effect.Maybe<TKqlTableEffect>()) {
            TMaybeNode<TExprBase> input;
            TCoArgument inputArg = Build<TCoArgument>(ctx, pos)
                .Name("inputArg")
                .Done();

            if (auto maybeUpsertRows = effect.Maybe<TKqlUpsertRows>()) {
                if (!BuildUpsertRowsEffect(maybeUpsertRows.Cast(), ctx, kqpCtx, inputArg, input, newEffect, sinkEffect, order)) {
                    return false;
                }
                ++order;
            }

            if (auto maybeDeleteRows = effect.Maybe<TKqlDeleteRows>()) {
                if (!BuildDeleteRowsEffect(maybeDeleteRows.Cast(), ctx, kqpCtx, inputArg, input, newEffect, sinkEffect, order)) {
                    return false;
                }
                ++order;
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

template<typename Visitor>
bool ExploreEffectLists(TExprBase expr, Visitor visitor) {
    if (auto list = expr.Maybe<TExprList>()) {
        for (auto&& item : list.Cast()) {
            if (!ExploreEffectLists(item, visitor)) {
                return false;
            }
        }
    } else {
        if (!visitor(expr)) {
            return false;
        }
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
        ExploreEffectLists(
            query.Effects(),
            [&](TExprBase effect) {
                auto tableEffect = effect.Maybe<TKqlTableEffect>();
                YQL_ENSURE(tableEffect);

                tableEffectsMap[tableEffect.Cast().Table().Path()].push_back(effect);

                return true;
            });

        for (const auto& pair: tableEffectsMap) {
            if (!BuildEffects(query.Pos(), pair.second, ctx, kqpCtx, builtEffects)) {
                return {};
            }
        }
    } else {
        builtEffects.reserve(query.Effects().Size() * 2);

        auto result = ExploreEffectLists(
            query.Effects(),
            [&](TExprBase effect) {
                return BuildEffects(query.Pos(), {effect}, ctx, kqpCtx, builtEffects);
            });

        if (!result) {
            return {};
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
