#include "kqp_opt_impl.h"

#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

std::pair<const TTypeAnnotationNode*, TCoAtomList> BuildReturningType(const TCoAtomList& returningColumns, const TKikimrTableDescription& tableDescription, TExprContext& ctx) {
    TVector<const TItemExprType*> rowItems;
    rowItems.reserve(returningColumns.Size());

    for (const auto& column : returningColumns) {
        const auto* columnType = tableDescription.GetColumnType(column.StringValue());
        rowItems.emplace_back(ctx.MakeType<TItemExprType>(column.StringValue(), columnType));
    }
    auto resultStructType = ctx.MakeType<TStructExprType>(rowItems);

    TVector<TCoAtom> returningList;
    for (const auto& item : resultStructType->GetItems()) {
        returningList.emplace_back(Build<TCoAtom>(ctx, returningColumns.Pos()).Value(item->GetName()).Done());
    }

    return {
        static_cast<const TTypeAnnotationNode*>(resultStructType),
        Build<TCoAtomList>(ctx, returningColumns.Pos())
            .Add(returningList)
            .Done()
    };
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

    auto [returningType, returningList] = BuildReturningType(returningColumns, tableDescription, ctx);

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
                .OutputType(ExpandType(expr.Pos(), *returningType, ctx))
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
                    .ReturningColumns(returningList)
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

// Build the common Transform→HashShuffle(ColumnShardHashV1)→Sink pattern for
// CS write affinity.  Used by BuildFillTableEffect, BuildUpsertRowsEffect,
// and BuildDeleteRowsEffect to avoid duplicating the same ~40-line block.
//
// Parameters:
//   ctx          — expression context for builders
//   pos          — source position for error reporting
//   transformStage — already-built Transform Stage (the data producer)
//   keyColumnAtoms — key column atoms for HashShuffle (from table metadata or fallback)
//   sink          — already-built sink node (TDqSink or TDqTransform)
//
// Returns the Sink Stage that receives rows via HashShuffle from the Transform Stage.
static TExprNode::TPtr BuildCsWriteAffinitySinkStage(
    TExprContext& ctx,
    TPositionHandle pos,
    TExprNode::TPtr transformStage,
    const TVector<TCoAtom>& keyColumnAtoms,
    TExprNode::TPtr sinkNode)
{
    auto sinkInput = Build<TDqCnHashShuffle>(ctx, pos)
        .Output<TDqOutput>()
            .Stage(transformStage)
            .Index().Build("0")
            .Build()
        .KeyColumns()
            .Add(keyColumnAtoms)
        .Build()
        .UseSpilling().Build(false)
        .HashFunc().Build("ColumnShardHashV1")
        .Done();

    const auto sinkRowArgument = Build<TCoArgument>(ctx, pos)
        .Name("sinkRow")
        .Done();

    auto sinkStage = Build<TDqStage>(ctx, pos)
        .Inputs()
            .Add(sinkInput)
            .Build()
        .Program()
            .Args({sinkRowArgument})
            .Body<TCoToFlow>()
                .Input(sinkRowArgument)
                .Build()
            .Build()
        .Outputs<TDqStageOutputsList>()
            .Add(sinkNode)
            .Build()
        .Settings().Build()
        .Done();

    return sinkStage.Ptr();
}

bool BuildFillTableEffect(const TKqlFillTable& node, TExprContext& ctx,
    const TKqpOptimizeContext& kqpCtx,
    TMaybeNode<TExprBase>& effect, const i64 order)
{
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
        // For pure stage CTAS (e.g. "AS SELECT 1u As Col1, 1 As Col2"),
        // split into Transform Stage (pure expr, no inputs) → HashShuffle(ColumnShardHashV1)
        // → Sink Stage so that per-shard tasks can be created with proper routing.
        // Without this split, the single stage has no input channels, so CountComputeTasks
        // creates 1 task with all shards in TargetShardIds, breaking the per-shard invariant.
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
                .Settings(Build<TCoNameValueTupleList>(ctx, node.Pos()).Add(settings).Done())
                .Build()
            .Done();

        // Create Transform Stage from the pure expression (no inputs).
        // The program outputs the pure expr as a flow of rows.
        auto transformStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Build()  // No inputs — pure stage
            .Program()
                .Args({})
                .Body<TCoToFlow>()
                    .Input(node.Input())
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        // Get key columns for HashShuffle. For pure stage CTAS, the table doesn't
        // exist in metadata yet. Try to get the first column from the pure expression's
        // type annotation. The pure expr type is typically List(Struct(...)) or Struct(...).
        TVector<TCoAtom> keyColumnAtoms;
        {
            const auto pureType = node.Input().Ref().GetTypeAnn();
            if (pureType) {
                const TTypeAnnotationNode* structType = nullptr;
                if (pureType->GetKind() == ETypeAnnotationKind::Struct) {
                    structType = &(*pureType);
                } else if (pureType->GetKind() == ETypeAnnotationKind::List) {
                    structType = pureType->Cast<TListExprType>()->GetItemType();
                }
                if (structType && structType->GetKind() == ETypeAnnotationKind::Struct) {
                    const auto& st = *structType->Cast<TStructExprType>();
                    if (!st.GetItems().empty()) {
                        keyColumnAtoms.emplace_back(
                            Build<TCoAtom>(ctx, node.Pos()).Value(st.GetItems().front()->GetName()).Done());
                    }
                }
            }
            // Fallback: if type annotation is not available, use empty key columns.
            // The HashShuffle will still work because real columns are resolved at runtime.
        }

        // BuildCsWriteAffinitySinkStage creates HashShuffle(ColumnShardHashV1) + Sink Stage.
        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), keyColumnAtoms, sink.Ptr()))
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

    // QP_FORCE_CS_WRITE_AFFINITY: force the per-shard write affinity mode regardless of the PRAGMA.
    const bool enableCsWriteAffinity =
#ifdef QP_FORCE_CS_WRITE_AFFINITY
        true;
#else
        kqpCtx.Config->EnableCsWriteAffinity.Get().GetOrElse(true);
#endif

#ifdef QP_FORCE_CS_WRITE_AFFINITY
    // kqpCtx is only used to read EnableCsWriteAffinity in the non-force build.
    Y_UNUSED(kqpCtx);
#endif

#ifdef QP_FORCE_CS_WRITE_AFFINITY
    // Invariant: with the force flag, the affinity mode must be enabled.
    AFL_VERIFY(enableCsWriteAffinity)("msg", "QP_FORCE_CS_WRITE_AFFINITY must force affinity mode");
#endif

    // Stage 4: Affinity marker for sink settings
    //
    // At optimization time, ShardIdToNodeId is NOT available. Therefore, we cannot
    // populate TargetShardIds or ExpectedNodeId here. Instead:
    //   - The EnableCsWriteAffinity flag is already in TKqpPhyTx (Stage 1),
    //     accessible in TasksGraph at runtime.
    //   - The sink mode "fill_table" identifies this as a CTAS sink.
    //   - In TasksGraph (Stage 5), the combination of EnableCsWriteAffinity +
    //     fill_table mode triggers multi-task creation with proper shard-to-node
    //     mapping, populating TargetShardIds and ExpectedNodeId per task.
    //
    // No changes to sink settings are needed at this stage.

    if (enableCsWriteAffinity) {
        // Per-node write affinity: WriteActor (sink) is extracted into a separate
        // TDqStage so it can be independently parallelized (M tasks, one per node
        // hosting column shards) and assigned via node affinity.
        //
        //   Transform Stage (mapCn -> ToFlow)         — 1 task (arbitrary node)
        //       | TDqCnHashShuffle (ColumnShardHashV1) — routes rows to correct shard task
        //   Sink Stage (ToFlow -> DqSink -> TKqpDirectWriteActor)  — M tasks, pinned to shard nodes
        //
        // Each Sink task receives only the rows destined for its assigned shards
        // (routed by ColumnShardHashV1 hash on the PK columns).
        //
        // HashShuffle (not Map) is used so that the Sink Stage can have a different,
        // independent task count (M) from the Transform Stage (1).  With Map the two
        // stages would be forced into the same copy-group with equal task counts.
        auto transformStage = Build<TDqStage>(ctx, node.Pos())
            .Inputs()
                .Add(mapCn)
                .Build()
            .Program()
                .Args({rowArgument})
                .Body<TCoToFlow>()
                    .Input(rowArgument)
                    .Build()
                .Build()
            .Settings().Build()
            .Done();

        // TDqCnHashShuffle with ColumnShardHashV1: routes each row to the one Sink task
        // that owns the target shard for that row's PK hash.  This avoids the M× traffic
        // overhead of Broadcast: each row is sent only to the correct Sink task.
        //
        // KeyColumns: The type annotation for TDqCnHashShuffle requires at least 1 key
        // column that exists in the output struct type. For CTAS, the target table may
        // not exist in kqpCtx.Tables yet (it's created by a preceding scheme operation).
        // We use the first column from the SELECT output struct as a placeholder — the
        // actual hash routing columns are determined at runtime by
        // BuildColumnShardHashV1ForWriteAffinity from CsShardingColumns, not by these
        // proto KeyColumns.
        //
        // Try to get key columns from the table metadata first (if the table exists).
        // Fall back to the first column from the input struct type.
        TVector<TCoAtom> keyColumnAtoms;
        if (const auto* tableDesc = kqpCtx.Tables->EnsureTableExists(
                kqpCtx.Cluster, TString(node.Table().Value()), node.Pos(), ctx)) {
            for (const auto& keyCol : tableDesc->Metadata->KeyColumnNames) {
                keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, node.Pos()).Value(keyCol).Done());
            }
        }
        if (keyColumnAtoms.empty()) {
            // Table doesn't exist in metadata yet (CTAS temp table). Use the first
            // column from the input struct type as a placeholder for type validation.
            const auto inputType = mapCn.Output().Stage().Program().Body().Ref().GetTypeAnn();
            Y_ENSURE(inputType && inputType->GetKind() == ETypeAnnotationKind::Flow,
                "Expected flow type for transform stage program body");
            const auto* itemType = inputType->Cast<TFlowExprType>()->GetItemType();
            Y_ENSURE(itemType && itemType->GetKind() == ETypeAnnotationKind::Struct,
                "Expected struct type for transform stage output");
            const auto& structType = *itemType->Cast<TStructExprType>();
            Y_ENSURE(!structType.GetItems().empty(), "Empty struct type for CTAS input");
            keyColumnAtoms.emplace_back(
                Build<TCoAtom>(ctx, node.Pos()).Value(structType.GetItems().front()->GetName()).Done());
        }

        // BuildCsWriteAffinitySinkStage creates HashShuffle(ColumnShardHashV1) + Sink Stage.
        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), keyColumnAtoms, sink.Ptr()))
            .SinkIndex().Build("0")
            .Done();
    } else {
        // Original behavior: transform program and sink in a single stage.
        auto stageInput = Build<TDqStage>(ctx, node.Pos())
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
            .Stage(stageInput.Ptr())
            .SinkIndex().Build("0")
            .Done();
    }

    return true;
}

bool BuildUpsertRowsEffect(const TKqlUpsertRows& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TMaybeNode<TExprBase>& effect, TExprNode::TPtr& returning, const i64 order)
{
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());

    TKqpUpsertRowsSettings settings;
    if (node.Settings()) {
        settings = TKqpUpsertRowsSettings::Parse(node.Settings().Cast());
    }

    const bool useStreamWriteForConsistentSink = CanEnableStreamWrite(table, kqpCtx)
        && (!HasReadTable(node.Table().PathId().Value(), node.Input().Ptr()) || settings.IsConditionalUpdate);
    const bool useStreamWrite = (settings.AllowInconsistentWrites || useStreamWriteForConsistentSink);
    const bool isIndexImplTable = table.Metadata->IsIndexImplTable;

    const bool isOlap = (table.Metadata->Kind == EKikimrTableKind::Olap);
    const i64 priority = (isOlap || settings.AllowInconsistentWrites) ? 0 : order;

    // QP_FORCE_CS_WRITE_AFFINITY: force the per-shard write affinity mode regardless of the PRAGMA.
    const bool enableCsWriteAffinity =
#ifdef QP_FORCE_CS_WRITE_AFFINITY
        true;
#else
        kqpCtx.Config->EnableCsWriteAffinity.Get().GetOrElse(true);
#endif

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
        if (kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty()) {
            auto stageInput = RebuildReturningPureStageWithSink(
                returning, node.Input(), table, node.Table(),
                settings.AllowInconsistentWrites, useStreamWrite,
                node.IsBatch() == "true", settings.Mode, isIndexImplTable,
                node.DefaultColumns(), node.ReturningColumns(), {}, priority, ctx);
            AFL_ENSURE(returning);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Ptr())
                .SinkIndex().Build("0")
                .Done();
        } else if (isOlap && enableCsWriteAffinity) {
            // Pure OLAP + write affinity: split into Transform + Sink stages.
            //
            //   Pure Expression (VALUES)
            //       ↓ (ToFlow)
            //   Transform Stage (pure, 0 inputs, 1 task)
            //       ↓ TDqCnHashShuffle (ColumnShardHashV1)
            //   Sink Stage (N tasks, one per shard, input via HashShuffle)
            //       ↓ TDqSink → TKqpDirectWriteActor
            //
            // TDqCnHashShuffle with ColumnShardHashV1 routes each row to the one Sink
            // task that owns the target shard for that row's PK hash. This avoids the
            // M× traffic overhead of Broadcast: each row is sent only to the correct
            // Sink task. HashShuffle (not Map) also lets the Sink Stage have an
            // independent task count (N, one per shard) from the Transform Stage (1).
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
                .ReturningColumns(ctx.NewList(node.Pos(), {}))
                .Settings()
                    .Build()
                .Done();

            auto sink = Build<TDqSink>(ctx, node.Pos())
                .DataSink<TKqpTableSink>()
                    .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                    .Cluster(ctx.NewAtom(node.Pos(), "db"))
                    .Build()
                .Index().Value("0").Build()
                .Settings(sinkSettings)
                .Done();

            // Transform Stage: pure expression, no inputs, 1 task.
            auto transformStage = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Build()
                .Program()
                    .Args({})
                    .Body<TCoToFlow>()
                        .Input(node.Input())
                        .Build()
                    .Build()
                .Settings().Build()
                .Done();

            // TDqCnHashShuffle with ColumnShardHashV1: routes each row to the one Sink
            // task that owns the target shard for that row's PK hash. KeyColumns come
            // from the table metadata (the table exists for INSERT/REPLACE, unlike CTAS).
            // The actual hash routing columns are determined at runtime by
            // BuildColumnShardHashV1ForWriteAffinity from CsShardingColumns.
            TVector<TCoAtom> keyColumnAtoms;
            for (const auto& keyCol : table.Metadata->KeyColumnNames) {
                keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, node.Pos()).Value(keyCol).Done());
            }
            Y_ENSURE(!keyColumnAtoms.empty(), "Empty key columns for OLAP table with write affinity");

            // BuildCsWriteAffinitySinkStage creates HashShuffle(ColumnShardHashV1) + Sink Stage.
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), keyColumnAtoms, sink.Ptr()))
                .SinkIndex().Build("0")
                .Done();
        } else {
            auto stageInput = RebuildPureStageWithSink(
                node.Input(), node.Table(),
                settings.AllowInconsistentWrites, useStreamWrite,
                node.IsBatch() == "true", settings.Mode, isIndexImplTable,
                node.DefaultColumns(), {}, priority, ctx);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Ptr())
                .SinkIndex().Build("0")
                .Done();
        }
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }

    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();

    {
        auto [returningType, returningList] = BuildReturningType(node.ReturningColumns(), table, ctx);
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
            .ReturningColumns(returningList)
            .Settings()
                .Build()
            .Done();
        auto sink = [&ctx, &node, &sinkSettings, &returningType](bool needOutputTransform) {
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
                    .OutputType(ExpandType(node.Pos(), *returningType, ctx))
                    .Settings(sinkSettings)
                    .Done().Ptr();
            }
        }(kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty());

        const auto rowArgument = Build<TCoArgument>(ctx, node.Pos())
            .Name("row")
            .Done();

        auto stageInput = [&]() -> TExprNode::TPtr {
            if (isOlap && enableCsWriteAffinity) {
                // Non-pure OLAP + write affinity (UPDATE/INSERT with source):
                // split into Transform + Sink stages so that the Sink stage
                // uses wide channels (Multi type) and ColumnShardHashV1 routing
                // can correctly use numeric indices for key columns.
                //
                //   Source (TableFullScan via UnionAll)
                //       ↓ (Map)
                //   Transform Stage (mapCn -> ToFlow)         — 1 task (arbitrary node)
                //       ↓ TDqCnHashShuffle (ColumnShardHashV1) — routes rows to correct shard task
                //   Sink Stage (ToFlow -> DqSink -> TKqpDirectWriteActor)  — N tasks, pinned to shard nodes
                //
                // TDqCnHashShuffle with ColumnShardHashV1 routes each row to the one Sink
                // task that owns the target shard. KeyColumns come from the table metadata.
                // The actual hash routing columns are determined at runtime by
                // BuildColumnShardHashV1ForWriteAffinity from CsShardingColumns.
                // HashShuffle (not Map) lets the Sink Stage have an independent task count
                // (N, one per shard) from the Transform Stage (1 task).
                // The Transform stage output uses wide channels (Multi type) so that
                // ColumnShardHashV1 can use numeric indices for key column resolution.

                auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
                    .Output(dqUnion.Output())
                    .Done();

                auto transformStage = Build<TDqStage>(ctx, node.Pos())
                    .Inputs()
                        .Add(mapCn)
                        .Build()
                    .Program()
                        .Args({rowArgument})
                        .Body<TCoToFlow>()
                            .Input(rowArgument)
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Done();

                // TDqCnHashShuffle with ColumnShardHashV1: routes each row to the one Sink
                // task that owns the target shard for that row's PK hash.
                TVector<TCoAtom> keyColumnAtoms;
                for (const auto& keyCol : table.Metadata->KeyColumnNames) {
                    keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, node.Pos()).Value(keyCol).Done());
                }
                Y_ENSURE(!keyColumnAtoms.empty(), "Empty key columns for OLAP table with write affinity");

                // BuildCsWriteAffinitySinkStage creates HashShuffle(ColumnShardHashV1) + Sink Stage.
                return BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), keyColumnAtoms, sink);
            } else if ((table.Metadata->Kind == EKikimrTableKind::Olap && useStreamWrite)
                    || settings.AllowInconsistentWrites) {
                auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
                    .Output(dqUnion.Output())
                    .Done();
                return Build<TDqStage>(ctx, node.Pos())
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
                    .Done().Ptr();
            } else {
                // OLTP is expected to mostly use just few shards,
                // so we use union all + one sink. It's important for write optimizations support.
                // NOTE: OLTP large writes expected to fail anyway due to problems with locks/splits.

                return Build<TDqStage>(ctx, node.Pos())
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
                    .Done().Ptr();
            }
        }();

        if (kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty()) {
            returning = Build<TDqCnUnionAll>(ctx, node.Pos())
                .Output()
                    .Stage(stageInput)
                    .Index().Build("0")
                    .Build()
                .Done().Ptr();
        }

        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(stageInput)
            .SinkIndex().Build("0")
            .Done();
    }

    return true;
}

bool BuildDeleteRowsEffect(const TKqlDeleteRows& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TMaybeNode<TExprBase>& effect, TExprNode::TPtr& returning, const i64 order)
{
    TKqpDeleteRowsSettings settings;
    if (node.Settings()) {
        settings = TKqpDeleteRowsSettings::Parse(node.Settings().Cast());
    }

    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, node.Table().Path());

    const bool useStreamWriteForConsistentSink = CanEnableStreamWrite(table, kqpCtx)
        && (!HasReadTable(node.Table().PathId().Value(), node.Input().Ptr()) || settings.IsConditionalDelete);
    const bool useStreamWrite = useStreamWriteForConsistentSink;
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
        if (kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty()) {
            auto stageInput = RebuildReturningPureStageWithSink(
                returning, node.Input(), table, node.Table(),
                false, useStreamWrite,
                node.IsBatch() == "true", "delete", isIndexImplTable,
                Build<TCoAtomList>(ctx, node.Pos()).Done(), node.ReturningColumns(), {}, priority, ctx);
            AFL_ENSURE(returning);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Ptr())
                .SinkIndex().Build("0")
                .Done();
        } else if (isOlap && kqpCtx.Config->EnableCsWriteAffinity.Get().GetOrElse(true)) {
            // For pure stage OLAP DELETE with write affinity, split into
            // Transform Stage (pure expr, no inputs) → HashShuffle(ColumnShardHashV1)
            // → Sink Stage so that per-shard tasks can be created with proper routing.
            // Without this split, the single stage has no input channels, so CountComputeTasks
            // creates 1 task with all shards in TargetShardIds, breaking the per-shard invariant.

            auto [returningType, returningList] = BuildReturningType(node.ReturningColumns(), table, ctx);
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
                .ReturningColumns(returningList)
                .Settings()
                    .Build()
                .Done();

            auto sink = Build<TDqSink>(ctx, node.Pos())
                .DataSink<TKqpTableSink>()
                    .Category(ctx.NewAtom(node.Pos(), NYql::KqpTableSinkName))
                    .Cluster(ctx.NewAtom(node.Pos(), "db"))
                    .Build()
                .Index().Value("0").Build()
                .Settings(sinkSettings)
                .Done();

            // Create Transform Stage from the pure expression (no inputs).
            auto transformStage = Build<TDqStage>(ctx, node.Pos())
                .Inputs()
                    .Build()  // No inputs — pure stage
                .Program()
                    .Args({})
                    .Body<TCoToFlow>()
                        .Input(node.Input())
                        .Build()
                    .Build()
                .Settings().Build()
                .Done();

            // Get key columns for HashShuffle from table metadata.
            TVector<TCoAtom> keyColumnAtoms;
            for (const auto& keyCol : table.Metadata->KeyColumnNames) {
                keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, node.Pos()).Value(keyCol).Done());
            }
            Y_ENSURE(!keyColumnAtoms.empty(), "Empty key columns for OLAP table with write affinity");

            // BuildCsWriteAffinitySinkStage creates HashShuffle(ColumnShardHashV1) + Sink Stage.
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), keyColumnAtoms, sink.Ptr()))
                .SinkIndex().Build("0")
                .Done();
        } else {
            auto stageInput = RebuildPureStageWithSink(
                node.Input(), node.Table(),
                false, useStreamWrite, node.IsBatch() == "true",
                "delete", isIndexImplTable,
                Build<TCoAtomList>(ctx, node.Pos()).Done(), {}, priority, ctx);
            effect = Build<TKqpSinkEffect>(ctx, node.Pos())
                .Stage(stageInput.Ptr())
                .SinkIndex().Build("0")
                .Done();
        }
        return true;
    }

    if (!EnsureDqUnion(node.Input(), ctx)) {
        return false;
    }


    auto dqUnion = node.Input().Cast<TDqCnUnionAll>();

    {
        auto [returningType, returningList] = BuildReturningType(node.ReturningColumns(), table, ctx);
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
            .ReturningColumns(returningList)
            .Settings()
                .Build()
            .Done();
        auto sink = [&ctx, &node, &sinkSettings, &returningType](bool needOutputTransform) {
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
                    .OutputType(ExpandType(node.Pos(), *returningType, ctx))
                    .Settings(sinkSettings)
                    .Done().Ptr();
            }
        }(kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty());

        const auto rowArgument = Build<TCoArgument>(ctx, node.Pos())
            .Name("row")
            .Done();

        auto stageInput = [&]() -> TExprNode::TPtr {
            const bool enableCsWriteAffinity =
                kqpCtx.Config->EnableCsWriteAffinity.Get().GetOrElse(true);
            
            if (isOlap && enableCsWriteAffinity) {
                // Non-pure OLAP + write affinity (DELETE with source):
                // split into Transform + Sink stages so that the Sink stage
                // uses wide channels (Multi type) and ColumnShardHashV1 routing
                // can correctly use numeric indices for key columns.
                //
                // TDqCnHashShuffle with ColumnShardHashV1 routes each row to the one Sink
                // task that owns the target shard. KeyColumns come from the table metadata.

                auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
                    .Output(dqUnion.Output())
                    .Done();

                auto transformStage = Build<TDqStage>(ctx, node.Pos())
                    .Inputs()
                        .Add(mapCn)
                        .Build()
                    .Program()
                        .Args({rowArgument})
                        .Body<TCoToFlow>()
                            .Input(rowArgument)
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Done();

                // TDqCnHashShuffle with ColumnShardHashV1: routes each row to the one Sink
                // task that owns the target shard for that row's PK hash.
                TVector<TCoAtom> keyColumnAtoms;
                for (const auto& keyCol : table.Metadata->KeyColumnNames) {
                    keyColumnAtoms.emplace_back(Build<TCoAtom>(ctx, node.Pos()).Value(keyCol).Done());
                }
                Y_ENSURE(!keyColumnAtoms.empty(), "Empty key columns for OLAP table with write affinity");

                // BuildCsWriteAffinitySinkStage creates HashShuffle(ColumnShardHashV1) + Sink Stage.
                return BuildCsWriteAffinitySinkStage(ctx, node.Pos(), transformStage.Ptr(), keyColumnAtoms, sink);
            } else if (table.Metadata->Kind == EKikimrTableKind::Olap) {
                auto mapCn = Build<TDqCnMap>(ctx, node.Pos())
                    .Output(dqUnion.Output())
                    .Done();
                return Build<TDqStage>(ctx, node.Pos())
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
                    .Done().Ptr();
            } else {
                return Build<TDqStage>(ctx, node.Pos())
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
                    .Done().Ptr();
            }
        }();

        if (kqpCtx.Config->GetEnableIndexStreamWrite() && !node.ReturningColumns().Empty()) {
            returning = Build<TDqCnUnionAll>(ctx, node.Pos())
                .Output()
                    .Stage(stageInput)
                    .Index().Build("0")
                    .Build()
                .Done().Ptr();
        }

        effect = Build<TKqpSinkEffect>(ctx, node.Pos())
            .Stage(stageInput)
            .SinkIndex().Build("0")
            .Done();
    }

    return true;
}

bool BuildEffects(const TVector<TExprBase>& effects, TExprNode::TPtr& returning,
    TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    TVector<TExprBase>& builtEffects)
{
    for (const auto& effect : effects) {
        TMaybeNode<TExprBase> newEffect;
        if (effect.Maybe<TKqlFillTable>()) {
            const auto maybeFillTable = effect.Maybe<TKqlFillTable>();
            AFL_ENSURE(maybeFillTable);
            if (!BuildFillTableEffect(maybeFillTable.Cast(), ctx, kqpCtx, newEffect, builtEffects.size())) {
                return false;
            }
        } else if (effect.Maybe<TKqlTableEffect>()) {
            if (auto maybeUpsertRows = effect.Maybe<TKqlUpsertRows>()) {
                if (!BuildUpsertRowsEffect(maybeUpsertRows.Cast(), ctx, kqpCtx, newEffect, returning, builtEffects.size())) {
                    return false;
                }
            }

            if (auto maybeDeleteRows = effect.Maybe<TKqlDeleteRows>()) {
                if (!BuildDeleteRowsEffect(maybeDeleteRows.Cast(), ctx, kqpCtx, newEffect, returning, builtEffects.size())) {
                    return false;
                }
            }
        } else if (auto maybeExt = effect.Maybe<TKqlExternalEffect>()) {
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
        builtEffects.push_back(newEffect.Cast());
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
            if (!BuildEffects(pair.second, returning, ctx, kqpCtx, builtEffects)) {
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
                const bool effectsResult = BuildEffects({effect}, returning, ctx, kqpCtx, builtEffects);
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

} // anonymous namespace

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
