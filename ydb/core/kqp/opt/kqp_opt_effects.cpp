#include "kqp_opt_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

const TTypeAnnotationNode* BuildReturningType(const TCoAtomList& returningColumns, const TKikimrTableDescription& tableDescription, TExprContext& ctx) {
    TVector<const TItemExprType*> rowItems;
    rowItems.reserve(returningColumns.Size());

    for (const auto& column : returningColumns) {
        const auto* columnType = tableDescription.GetColumnType(column.StringValue());
        rowItems.emplace_back(ctx.MakeType<TItemExprType>(column.StringValue(), columnType));
    }
    return ctx.MakeType<TStructExprType>(rowItems);
}

TDqStage RebuildPureStageWithSink(TExprBase expr, const TKqpTable& table,
        const bool allowInconsistentWrites, const bool enableStreamWrite, bool isBatch,
        const TStringBuf mode, const bool isIndexImplTable, const TCoAtomList& defaultColumns,
        const TVector<TCoNameValueTuple>& settings, const i64 order, TExprContext& ctx) {
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
                    .DefaultColumns(defaultColumns)
                    .ReturningColumns(ctx.NewList(expr.Pos(), {}))
                    .Settings(settingsNode)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();
}

TDqStage RebuildReturningPureStageWithSink(TExprNode::TPtr& returning, TExprBase expr, const TKikimrTableDescription& tableDescription, const TKqpTable& table,
        const bool allowInconsistentWrites, const bool enableStreamWrite, bool isBatch,
        const TStringBuf mode, const bool isIndexImplTable, const TCoAtomList& defaultColumns, 
        const TCoAtomList& returningColumns, const TVector<TCoNameValueTuple>& settings,
        const i64 order, TExprContext& ctx) {
    Y_DEBUG_ABORT_UNLESS(IsDqPureExpr(expr));
    auto settingsNode = Build<TCoNameValueTupleList>(ctx, expr.Pos())
        .Add(settings)
        .Done();

    auto stage = Build<TDqStage>(ctx, expr.Pos())
        .Inputs()
            .Build()
        .Program()
            .Args({})
            .Body<TCoToFlow>()
                .Input(expr)
                .Build()
            .Build()
        .Outputs()
            .Add<TDqTransform>()
                .Index().Build("0")
                .DataSink<TKqpTableSink>()
                    .Category(ctx.NewAtom(expr.Pos(), NYql::KqpTableSinkName))
                    .Cluster(ctx.NewAtom(expr.Pos(), "db"))
                    .Build()
                .Type<TCoAtom>()
                    .Build("ReturningSink")
                .InputType(ExpandType(expr.Pos(), GetSeqItemType(*expr.Ref().GetTypeAnn()), ctx))
                .OutputType(ExpandType(expr.Pos(), *BuildReturningType(returningColumns, tableDescription, ctx), ctx))
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
                    .DefaultColumns(defaultColumns)
                    .ReturningColumns(returningColumns)
                    .Settings(settingsNode)
                    .Build()
                .Build()
            .Build()
        .Settings().Build()
        .Done();

    returning = Build<TDqCnUnionAll>(ctx, expr.Pos())
        .Output()
            .Stage(stage)
            .Index().Build("0")
            .Build()
        .Done().Ptr();

    return stage;
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
            /* isBatch */ false, "fill_table", /* isIndexImplTable */ false,
            Build<TCoAtomList>(ctx, node.Pos()).Done(), settings,
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
            .DefaultColumns<TCoAtomList>().Build()
            .ReturningColumns(ctx.NewList(node.Pos(), {}))
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
    const TCoArgument& inputArg, TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect, TExprNode::TPtr& returning, bool& sinkEffect, const i64 order)
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
    const i64 priority = (isOlap || settings.AllowInconsistentWrites) ? 0 : order;

    if (isOlap && !(kqpCtx.IsGenericQuery() || (kqpCtx.IsDataQuery() && kqpCtx.Config->GetAllowOlapDataQuery()))) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Data manipulation queries with column-oriented tables are supported only by API QueryService."));
        return false;
    }
    if (isOlap && !kqpCtx.Config->GetEnableOlapSink()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Data manipulation queries with column-oriented tables are disabled."));
        return false;
    }

    if (IsDqPureExpr(node.Input())) {
        if (sinkEffect && kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty()) {
            stageInput = RebuildReturningPureStageWithSink(
                returning, node.Input(), table, node.Table(),
                settings.AllowInconsistentWrites, useStreamWrite,
                node.IsBatch() == "true", settings.Mode, isIndexImplTable,
                node.DefaultColumns(), node.ReturningColumns(), {}, priority, ctx);
            AFL_ENSURE(returning);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Cast().Ptr())
                .SinkIndex().Build("0")
                .Done();
        } else if (sinkEffect) {
            stageInput = RebuildPureStageWithSink(
                node.Input(), node.Table(),
                settings.AllowInconsistentWrites, useStreamWrite,
                node.IsBatch() == "true", settings.Mode, isIndexImplTable,
                node.DefaultColumns(), {}, priority, ctx);
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

    if (sinkEffect) {
        auto sinkSettings = Build<TKqpTableSinkSettings>(ctx, node.Pos())
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
            .DefaultColumns(node.DefaultColumns())
            .ReturningColumns(node.ReturningColumns())
            .Settings()
                .Build()
            .Done();
        auto sink = [&ctx, &node, &sinkSettings, &table](bool needOutputTransform) {
            if (!needOutputTransform) {
                return Build<TDqSink>(ctx, node.Pos())
                    .DataSink<TKqpTableSink>()
                        .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                        .Cluster(ctx.NewAtom(node.Pos(), "db"))
                        .Build()
                    .Index().Value("0").Build()
                    .Settings(sinkSettings)
                    .Done().Ptr();
            } else {
                return Build<TDqTransform>(ctx, node.Pos())
                    .Index().Build("0")
                    .DataSink<TKqpTableSink>()
                        .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                        .Cluster(ctx.NewAtom(node.Pos(), "db"))
                        .Build()
                    .Type<TCoAtom>()
                        .Build("ReturningSink")
                    .InputType(ExpandType(node.Pos(), GetSeqItemType(*node.Input().Ref().GetTypeAnn()), ctx))
                    .OutputType(ExpandType(node.Pos(), *BuildReturningType(node.ReturningColumns(), table, ctx), ctx))
                    .Settings(sinkSettings)
                    .Done().Ptr();
            }
        }(kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty());

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

        if (kqpCtx.Config->GetEnableIndexStreamWrite()) {
            returning = Build<TDqCnUnionAll>(ctx, node.Pos())
                .Output()
                    .Stage(stageInput.Cast().Ptr())
                    .Index().Build("0")
                    .Build()
                .Done().Ptr();
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
    const TCoArgument& inputArg, TMaybeNode<TExprBase>& stageInput, TMaybeNode<TExprBase>& effect, TExprNode::TPtr& returning, bool& sinkEffect, const i64 order)
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

    if (isOlap && !(kqpCtx.IsGenericQuery() || (kqpCtx.IsDataQuery() && kqpCtx.Config->GetAllowOlapDataQuery()))) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Data manipulation queries with column-oriented tables are supported only by API QueryService."));
        return false;
    }
    if (isOlap && !kqpCtx.Config->GetEnableOlapSink()) {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
            TStringBuilder() << "Data manipulation queries with column-oriented tables are disabled."));
        return false;
    }

    if (IsDqPureExpr(node.Input())) {
        if (sinkEffect && kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty()) {
            stageInput = RebuildReturningPureStageWithSink(
                returning, node.Input(), table, node.Table(),
                false, useStreamWrite,
                node.IsBatch() == "true", "delete", isIndexImplTable,
                Build<TCoAtomList>(ctx, node.Pos()).Done(), node.ReturningColumns(), {}, priority, ctx);
            AFL_ENSURE(returning);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Cast().Ptr())
                .SinkIndex().Build("0")
                .Done();
        } else if (sinkEffect) {
            stageInput = RebuildPureStageWithSink(
                node.Input(), node.Table(),
                false, useStreamWrite, node.IsBatch() == "true",
                "delete", isIndexImplTable,
                Build<TCoAtomList>(ctx, node.Pos()).Done(), {}, priority, ctx);
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

    if (sinkEffect) {
        auto sinkSettings = Build<TKqpTableSinkSettings>(ctx, node.Pos())
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
            .DefaultColumns<TCoAtomList>().Build()
            .ReturningColumns(node.ReturningColumns())
            .Settings()
                .Build()
            .Done();
        auto sink = [&ctx, &node, &sinkSettings, &table](bool needOutputTransform) {
            if (!needOutputTransform) {
                return Build<TDqSink>(ctx, node.Pos())
                    .DataSink<TKqpTableSink>()
                        .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                        .Cluster(ctx.NewAtom(node.Pos(), "db"))
                        .Build()
                    .Index().Value("0").Build()
                    .Settings(sinkSettings)
                    .Done().Ptr();
            } else {
                return Build<TDqTransform>(ctx, node.Pos())
                    .Index().Build("0")
                    .DataSink<TKqpTableSink>()
                        .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                        .Cluster(ctx.NewAtom(node.Pos(), "db"))
                        .Build()
                    .Type<TCoAtom>()
                        .Build("ReturningSink")
                    .InputType(ExpandType(node.Pos(), GetSeqItemType(*node.Input().Ref().GetTypeAnn()), ctx))
                    .OutputType(ExpandType(node.Pos(), *BuildReturningType(node.ReturningColumns(), table, ctx), ctx))
                    .Settings(sinkSettings)
                    .Done().Ptr();
            }
        }(kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty());

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

        if (kqpCtx.Config->GetEnableIndexStreamWrite()) {
            returning = Build<TDqCnUnionAll>(ctx, node.Pos())
                .Output()
                    .Stage(stageInput.Cast().Ptr())
                    .Index().Build("0")
                    .Build()
                .Done().Ptr();
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

bool BuildEffects(TPositionHandle pos, const TVector<TExprBase>& effects, TExprNode::TPtr& returning,
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
                if (!BuildUpsertRowsEffect(maybeUpsertRows.Cast(), ctx, kqpCtx, inputArg, input, newEffect, returning, sinkEffect, order)) {
                    return false;
                }
                ++order;
            }

            if (auto maybeDeleteRows = effect.Maybe<TKqlDeleteRows>()) {
                if (!BuildDeleteRowsEffect(maybeDeleteRows.Cast(), ctx, kqpCtx, inputArg, input, newEffect, returning, sinkEffect, order)) {
                    return false;
                }
                ++order;
            }

            if (input && !sinkEffect) {
                inputArgs.push_back(inputArg);
                inputs.push_back(input.Cast());
            }
        } else if (auto maybeExt = effect.Maybe<TKqlExternalEffect>()) {
            sinkEffect = true;

            TExprBase input = maybeExt.Cast().Input();
            ui64 index = 0; // Index of output in DQ stage result
            if (input.Ref().IsList()) {
                YQL_ENSURE(input.Ref().ChildrenSize() == 1, "Expected Tuple(Nth(DQ Stage, output index))");

                const auto maybeNth = TMaybeNode<TCoNth>(input.Ref().Child(0));
                YQL_ENSURE(maybeNth, "Expected Nth(DQ Stage, output index)");
                const auto nth = maybeNth.Cast();

                input = nth.Tuple();
                index = FromString(nth.Index().Value());
            }

            const auto maybeStage = input.Maybe<TDqStageBase>();
            YQL_ENSURE(maybeStage, "External effect should be a DQ stage or Tuple(Nth(DQ Stage, output index))");
            const auto stage = maybeStage.Cast();
            const auto outputsList = stage.Outputs();
            YQL_ENSURE(outputsList, "External effect DQ stage should have at least one output");

            std::optional<ui64> outputIndex; // Index of output in outputsList
            const auto outputs = outputsList.Cast();
            for (ui64 i = 0; i < outputs.Size(); ++i) {
                if (const auto output = outputs.Item(i); FromString<ui64>(output.Index()) == index) {
                    outputIndex = i;
                    YQL_ENSURE(TDqSink::Match(output.Raw()), "External effect DQ stage should have DQ sink as " << i << " output");
                    break;
                }
            }
            YQL_ENSURE(outputIndex, "Unknown stage output index: " << index << ", stage have outputs: " << outputs.Size());

            newEffect = Build<TKqpSinkEffect>(ctx, effect.Pos())
                .Stage(stage.Ptr())
                .SinkIndex()
                    .Build(*outputIndex)
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
    TNodeMap<size_t> returningEffectsMap;
    for (size_t index = 0; index < query.Results().Size(); ++index) {
        const auto& result = query.Results().Item(index);
        VisitExpr(
            result.Ptr(),
            [&](const TExprNode::TPtr& node) {
                auto returning = TExprBase(node).Maybe<TKqlReturningList>();
                if (!returning) {
                    return true;
                }

                TExprBase effect = [&returning]() {
                    if (auto maybeList = returning.Cast().Update().Maybe<TExprList>()) {
                        AFL_ENSURE(maybeList.Cast().Size() == 1);
                        return maybeList.Cast().Item(0);
                    } else {
                        return returning.Cast().Update();
                    }
                }();

                AFL_ENSURE((returningEffectsMap.emplace(effect.Raw(), index)).second);
                return false;
            });
    }

    TVector<TExprBase> builtEffects;
    THashMap<size_t, TExprBase> newReturning;
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
            TExprNode::TPtr returning = nullptr;
            if (!BuildEffects(query.Pos(), pair.second, returning, ctx, kqpCtx, builtEffects)) {
                return {};
            }
            AFL_ENSURE(!returning);
        }
    } else {
        builtEffects.reserve(query.Effects().Size() * 2);

        auto result = ExploreEffectLists(
            query.Effects(),
            [&](TExprBase effect) {
                TExprNode::TPtr returning = nullptr;
                const bool effectsResult = BuildEffects(query.Pos(), {effect}, returning, ctx, kqpCtx, builtEffects);
                if (!effectsResult) {
                    return false;
                }

                if (returning) {
                    AFL_ENSURE(kqpCtx.Config->GetEnableIndexStreamWrite());
                    newReturning.emplace(returningEffectsMap[effect.Raw()], returning);
                }
                
                return true;
            });

        if (!result) {
            return {};
        }
    }

    TVector<TKqlQueryResult> newResults;
    for (size_t index = 0; index < query.Results().Size(); ++index) {
        if (newReturning.contains(index)) {
            auto newResult = Build<TKqlQueryResult>(ctx, query.Pos())
                .Value(newReturning.at(index))
                .ColumnHints(query.Results().Item(index).ColumnHints())
                .Done();

            newResults.emplace_back(newResult);
        } else {
            newResults.emplace_back(query.Results().Item(index));
        }
    }

    auto result = Build<TKqlQuery>(ctx, query.Pos())
        .Results()
            .Add(newResults)
            .Build()
        .Effects()
            .Add(builtEffects)
            .Build()
        .Done();

    return result;
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
