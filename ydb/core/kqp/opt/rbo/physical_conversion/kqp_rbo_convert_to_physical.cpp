#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include "kqp_rbo_physical_sort_builder.h"
#include "kqp_rbo_physical_window_builder.h"
#include "kqp_rbo_physical_aggregation_builder.h"
#include "kqp_rbo_physical_map_builder.h"
#include "kqp_rbo_physical_union_all_builder.h"
#include "kqp_rbo_physical_join_builder.h"
#include "kqp_rbo_physical_lookup_join_builder.h"
#include "kqp_rbo_physical_filter_builder.h"
#include "kqp_rbo_physical_source_builder.h"
#include "kqp_rbo_physical_table_effect_builder.h"
#include "kqp_rbo_physical_query_builder.h"

#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

#include <optional>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp {

TExprNode::TPtr ConvertToPhysical(const TVector<TIntrusivePtr<TOpRoot>>& roots, TRBOContext& rboCtx) {
    TExprContext& ctx = rboCtx.ExprCtx;
    ui32 stageInputCounter = 0;
    TCompileTraceScope traceScope(rboCtx.GetCompileTrace(), TCompileTraceLevels::Phase, "Convert to physical plan");
    std::optional<TCompileTraceScope> convertOperatorsTraceScope(std::in_place, rboCtx.GetCompileTrace(),
        TCompileTraceLevels::Step, "Convert operators");

    if (rboCtx.NeedToLog()) {
        rboCtx.TraceLog.stage("Physical AST generation");
    }

    TVector<TStageGraph> queryGraphs;
    TVector<THashMap<ui32, TExprNode::TPtr>> allStages;
    TVector<THashMap<ui32, TVector<TExprNode::TPtr>>> allStageArgs;
    TVector<THashMap<ui32, TPositionHandle>> allStagePos; 

    // As in the old optimizer, PRAGMA OptForceOlapPushdownDistinct marks a
    // DISTINCT key in the read program unless aggregates are pushed down.
    const auto& config = *rboCtx.KqpCtx.Config;
    std::optional<TString> forcedDistinctKey;
    if (const auto key = config.OptForceOlapPushdownDistinct.Get();
        key && !key->empty() && config.HasOptEnableOlapPushdown() && !config.GetEnableOlapPushdownAggregate()) {
        forcedDistinctKey = *key;
    }
    const ui64 forcedDistinctLimit = config.OptForceOlapPushdownDistinctLimit.Get().GetOrElse(0);

    TVector<TPhysicalNames> allNames;
    allNames.reserve(roots.size());
    for (auto & root: roots) {
        const auto& names = allNames.emplace_back(root->PlanProps.InfoUnitRegistry);
        // Bodies are handed from one operator to the next in whichever form - wide flow or
        // narrow stream - saves a conversion, and are materialized as narrow stage programs
        // once every operator has been emitted.
        THashMap<ui32, NPhysicalConvertionUtils::TStageBody> stageBodies;
        THashMap<ui32, TVector<TExprNode::TPtr>> stageArgs;
        THashMap<ui32, TPositionHandle> stagePos;
        // Types of the left rows passed through lookups in join mode; a lookup by keys of the input lookup reuses them.
        THashMap<const IOperator*, const TStructExprType*> lookupJoinLeftRowTypes;
        auto& graph = root->PlanProps.StageGraph;
        THashSet<const TReplicate*> builtReplicates;
        Y_ENSURE(graph.IsConnected(), "Stage graph must be connected");
        Y_DEBUG_ABORT_UNLESS(!graph.ContainsCycles(), "Stage graph must be acyclic");
        THashMap<TStageEdgeId, TExprNode::TPtr> inputArguments;
        auto makeStageInput = [&](const IOperator& op, ui32 childIndex = 0) {
            auto* edge = root->PlanProps.GetStageInputEdge(op, childIndex);
            Y_ENSURE(edge, "Physical argument requires a stage input edge");
            auto& arg = inputArguments[edge->Id()];
            if (!arg) {
                arg = ctx.NewArgument(op.Pos, "input_arg_" + ToString(stageInputCounter++));
            }
            return arg;
        };

        // A read program computes each aggregate pushed to column shards. The
        // aggregate builds that read, its read and map have no lowering of their own.
        THashMap<const IOperator*, TOlapAggregate> olapAggregates;
        THashSet<const IOperator*> olapAggregateInputs;
        // By read, the trivial DISTINCT whose key the read program marks.
        THashMap<const IOperator*, TOlapDistinct> olapDistincts;
        for (const auto& iter : *root) {
            const auto* op = iter.Current;
            if (op->Kind != EOperator::Aggregate) {
                continue;
            }
            const auto& aggregate = CastOperator<TOpAggregate>(*op);
            if (!aggregate.IsPushedToOlap()) {
                if (forcedDistinctKey) {
                    if (auto olapDistinct = MatchOlapDistinct(aggregate, *forcedDistinctKey, root->PlanProps.InfoUnitRegistry)) {
                        olapDistincts.emplace(olapDistinct->Read, *olapDistinct);
                    }
                }
                continue;
            }
            auto olapAggregate = MatchOlapAggregate(aggregate, ctx);
            Y_ENSURE(olapAggregate, "An aggregate pushed to column shards no longer matches its read");
            olapAggregateInputs.insert(olapAggregate->Read);
            if (olapAggregate->Map) {
                olapAggregateInputs.insert(olapAggregate->Map);
            }
            olapAggregates.emplace(op, std::move(*olapAggregate));
        }

        auto buildSource = [&](TOpRead& opRead, const TOlapAggregate* olapAggregate) {
            const auto& table = rboCtx.KqpCtx.Tables->ExistingTable(
                rboCtx.KqpCtx.Cluster, TKqpTable(opRead.TableCallable).Path().Value());

            TString carrierColumn;
            if (opRead.GetColumns().Empty() && opRead.GetTableStorageType() == NYql::EStorageType::ColumnStorage) {
                Y_ENSURE(!table.Metadata->KeyColumnNames.empty(), "An OLAP table needs a primary key");
                carrierColumn = table.Metadata->KeyColumnNames.front();
            }
            TPhysicalSourceBuilder builder(opRead, ctx, opRead.Pos, names, root->PlanProps.InfoUnitRegistry,
                root->PlanProps.GetStage(opRead).Guid(), table.Metadata->Kind == NYql::EKikimrTableKind::SysView,
                std::move(carrierColumn));
            if (olapAggregate) {
                builder.WithOlapAggregate(*olapAggregate);
            } else if (const auto* olapDistinct = olapDistincts.FindPtr(&opRead)) {
                builder.WithOlapDistinct(*olapDistinct, forcedDistinctLimit);
            }
            return builder.BuildPhysicalOp();
        };

        for (const auto& iter : *root) {
            auto op = iter.Current;
            auto opStageId = *(op->Props.StageId);
            if (olapAggregateInputs.contains(op)) {
                continue;
            }

            NPhysicalConvertionUtils::TStageBody currentStageBody;
            if (stageBodies.contains(opStageId)) {
                currentStageBody = stageBodies.at(opStageId);
            }

            if (op->Kind == EOperator::Replicate) {
                auto& replicate = CastOperator<TOpReplicate>(*op).GetReplicate();
                if (!builtReplicates.insert(&replicate).second) {
                    continue;
                }
                if (!currentStageBody) {
                    const auto input = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(input);
                }
                stageBodies[opStageId] = NPhysicalConvertionUtils::TStageBody::Narrow(NPhysicalConvertionUtils::BuildSwitch(
                    currentStageBody.AsNarrow(ctx), replicate, names, ctx));
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::EmptySource) {
                const auto emptySource = CastOperator<TOpEmptySource>(op);
                if (emptySource->Input) {
                    auto iterator = Build<TCoIterator>(ctx, op->Pos)
                        .List(emptySource->Input)
                        .Done().Ptr();
                    TVector<std::pair<TString, TString>> columns;
                    for (const auto id : emptySource->GetOutputIUs()) {
                        columns.emplace_back(root->PlanProps.InfoUnitRegistry.Get(id).GetFullName(), names.Get(id));
                    }
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                        NPhysicalConvertionUtils::BuildRenameMap(iterator, columns, ctx));
                } else {
                    TVector<TExprBase> listElements;
                    listElements.push_back(Build<TCoAsStruct>(ctx, op->Pos).Done());

                    // clang-format off
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                        Build<TCoIterator>(ctx, op->Pos)
                            .List<TCoAsList>()
                                .Add(listElements)
                            .Build()
                        .Done().Ptr());
                    // clang-format on
                }
                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Empty Source " << opStageId;
            } else if (op->Kind == EOperator::Source) {
                currentStageBody = buildSource(CastOperator<TOpRead>(*op), nullptr);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
            } else if (op->Kind == EOperator::Filter) {
                auto& filter = CastOperator<TOpFilter>(*op);

                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }

                currentStageBody = Build<TPhysicalFilterBuilder>(filter, ctx, op->Pos, names, currentStageBody);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Filter " << opStageId;
            } else if (op->Kind == EOperator::Map) {
                auto& map = CastOperator<TOpMap>(*op);

                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }

                currentStageBody = Build<TPhysicalMapBuilder>(map, ctx, op->Pos, names, currentStageBody);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Map " << opStageId;
            } else if (op->Kind == EOperator::Limit) {
                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }

                auto& limit = CastOperator<TOpLimit>(*op);

                // Take/Skip are generic over the sequence, so keep the body wide through the limit
                // and express the projection as a wide map instead of ExtractMembers over structs.
                const auto limitInputs = NPhysicalConvertionUtils::GetFullNames(NPhysicalConvertionUtils::GetLiveInputIUs(limit, 0), names);
                auto limitBody = currentStageBody.AsWide(limitInputs, ctx, names);
                if (limit.HasOffset()) {
                    // clang-format off
                    limitBody = Build<TCoSkip>(ctx, op->Pos)
                        .Input(limitBody)
                        .Count(limit.GetOffsetCond()->GetExpressionBody())
                    .Done().Ptr();
                    // clang-format on
                }

                // clang-format off
                limitBody = Build<TCoTake>(ctx, op->Pos)
                    .Input(limitBody)
                    .Count(limit.LimitCond.GetExpressionBody())
                .Done().Ptr();
                // clang-format on

                currentStageBody = NPhysicalConvertionUtils::BuildLiveWideBody(limitBody, limitInputs,
                    NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(limit), names), ctx);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId;
            } else if (op->Kind == EOperator::Sort) {
                auto& sort = CastOperator<TOpSort>(*op);
                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }
                currentStageBody = Build<TPhysicalSortBuilder>(sort, ctx, op->Pos, names, currentStageBody);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Sort " << opStageId;
            } else if (op->Kind == EOperator::Window) {
                auto& window = CastOperator<TOpWindow>(*op);
                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }
                currentStageBody = Build<TPhysicalWindowBuilder>(window, ctx, op->Pos, names, currentStageBody);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Window " << opStageId;
            } else if (op->Kind == EOperator::Join) {
                auto& join = CastOperator<TOpJoin>(*op);
                Y_ENSURE(join.Props.UseBlockHashJoin.has_value(), "Physical join implementation has not been selected");

                const auto leftInput = makeStageInput(*op);
                const auto rightInput = makeStageInput(*op, 1);
                currentStageBody = Build<TPhysicalJoinBuilder>(join, ctx, op->Pos, names, leftInput, rightInput, *join.Props.UseBlockHashJoin, rboCtx.TypeCtx);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId;
            } else if (op->Kind == EOperator::UnionAll) {
                auto& unionAll = CastOperator<TOpUnionAll>(*op);

                TVector<TExprNode::TPtr> inputs(unionAll.GetChildren().size());
                for (ui32 childIndex = 0; childIndex < unionAll.GetChildren().size(); ++childIndex) {
                    const auto input = makeStageInput(*op, childIndex);
                    inputs[childIndex] = input;
                }

                currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                    Build<TPhysicalUnionAllBuilder>(unionAll, ctx, op->Pos, names, inputs));

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted UnionAll " << opStageId;
            } else if (op->Kind == EOperator::Aggregate) {
                auto& aggregate = CastOperator<TOpAggregate>(*op);

                if (const auto* olapAggregate = olapAggregates.FindPtr(op)) {
                    Y_ENSURE(!currentStageBody, "A pushed aggregate must start the stage of its read");
                    currentStageBody = buildSource(*olapAggregate->Read, olapAggregate);

                    stageBodies[opStageId] = currentStageBody;
                    stagePos[opStageId] = op->Pos;
                    YQL_CLOG(TRACE, CoreDq) << "Converted Aggregate pushed to column shards " << opStageId;
                    continue;
                }

                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }

                std::optional<i64> memLimit;
                if (auto memLimitSetting = rboCtx.KqpCtx.Config->_KqpYqlCombinerMemoryLimit.Get()) {
                    memLimit = -i64(*memLimitSetting);
                }

                // The full physical-stage peephole performs this pruning later.
                const bool pruneUnusedOutputs = !rboCtx.KqpCtx.Config->GetEnableNewRBOPhysicalStagePeephole();
                currentStageBody = NPhysicalConvertionUtils::TransformStageOutput(currentStageBody, [&](const NPhysicalConvertionUtils::TStageBody& body) {
                    return TPhysicalAggregationBuilder(aggregate, ctx, op->Pos, names, pruneUnusedOutputs,
                        rboCtx.KqpCtx.Config->GetDqHashOperatorsUseBlocks()).BuildPhysicalOp(body, memLimit);
                }, GetReplicateOutputIndex(aggregate), ctx);

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::TableLookup) {
                auto& lookup = CastOperator<TOpTableLookup>(*op);

                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }

                const auto inputStageId = *lookup.GetInput()->Props.StageId;
                auto* edge = root->PlanProps.GetStageInputEdge(lookup, 0);
                Y_ENSURE(edge && IsConnection<TStreamLookupConnection>(edge->Connection()),
                    "A table lookup must be fed by a stream lookup connection");
                if (lookup.KeysFromInputLookup) {
                    // The output of the input lookup is passed as is: the rows it fetched are the lookup keys.
                    const auto inputLookup = CastOperator<TOpTableLookup>(lookup.GetInput());
                    const auto* leftRowType = lookupJoinLeftRowTypes.at(inputLookup.Get());
                    lookupJoinLeftRowTypes[&lookup] = leftRowType;
                    edge->SetStreamLookupInputType(NLookupJoinBuilder::BuildKeysFromInputLookupType(
                        *inputLookup, leftRowType, names, root->PlanProps.InfoUnitRegistry, ctx));
                } else {
                    auto keys = NLookupJoinBuilder::BuildLookupKeys(lookup, stageBodies.at(inputStageId).AsNarrow(ctx), ctx, names);
                    stageBodies[inputStageId] = NPhysicalConvertionUtils::TStageBody::Narrow(keys.InputStage);
                    edge->SetStreamLookupInputType(keys.InputType);
                    lookupJoinLeftRowTypes[&lookup] = keys.LeftRowType;
                }

                if (lookup.IsJoin()) {
                    YQL_CLOG(TRACE, CoreDq) << "Converted TableLookupJoin " << opStageId;
                } else {
                    auto streamInput = Build<TCoToStream>(ctx, op->Pos).Input(currentStageBody.AsNarrow(ctx)).Done().Ptr();
                    TVector<std::pair<TString, TString>> renames;
                    for (const auto id : lookup.GetColumns()) {
                        renames.emplace_back(root->PlanProps.InfoUnitRegistry.Get(id).GetColumnName(), names.Get(id));
                    }
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                        NPhysicalConvertionUtils::BuildRenameMap(streamInput, renames, ctx));

                    YQL_CLOG(TRACE, CoreDq) << "Converted TableLookup " << opStageId;
                }

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::IndexLookupJoin) {
                auto& lookupJoin = CastOperator<TOpIndexLookupJoin>(*op);
                Y_ENSURE(currentStageBody, "A lookup join must share the stage of its table lookup");

                currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                    TPhysicalIndexLookupJoinBuilder(lookupJoin, ctx, op->Pos, names, root->PlanProps.InfoUnitRegistry)
                        .BuildPhysicalOp(currentStageBody.AsNarrow(ctx)));

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted IndexLookupJoin " << opStageId;
            } else if (op->Kind == EOperator::Sequencer) {
                auto& sequencer = CastOperator<TOpSequencer>(*op);
                const auto inputStageId = *sequencer.GetInput()->Props.StageId;
                TVector<std::pair<TString, TString>> inputRenames;
                TVector<std::pair<TString, TString>> outputRenames;
                for (const auto& [id, name] : sequencer.GetInputColumns().Items()) {
                    inputRenames.emplace_back(names.Get(id), name);
                    outputRenames.emplace_back(name, names.Get(id));
                }
                for (const auto& [id, name] : sequencer.GetGeneratedColumns().Items()) {
                    outputRenames.emplace_back(name, names.Get(id));
                }
                // The existing sequencer connection uses storage column names.
                // Restore IU names immediately after it so ordinary Maps can
                // compute expressions that depend on the generated values.
                stageBodies[inputStageId] = NPhysicalConvertionUtils::TransformStageOutput(stageBodies.at(inputStageId),
                    [&](const NPhysicalConvertionUtils::TStageBody& body) {
                        return NPhysicalConvertionUtils::TStageBody::Narrow(
                            NPhysicalConvertionUtils::BuildRenameMap(body.AsNarrow(ctx), inputRenames, ctx, /*ordered=*/true));
                    }, GetReplicateOutputIndex(*sequencer.GetInput()), ctx);
                const auto stageInput = makeStageInput(*op);
                auto stream = Build<TCoToStream>(ctx, op->Pos).Input(stageInput).Done().Ptr();
                currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                    NPhysicalConvertionUtils::BuildRenameMap(stream, outputRenames, ctx, /*ordered=*/true));
                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::TableEffect) {
                auto& tableEffect = CastOperator<TOpTableEffect>(*op);

                if (!currentStageBody) {
                    const auto stageInput = makeStageInput(*op);
                    currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(stageInput);
                }

                currentStageBody = NPhysicalConvertionUtils::TStageBody::Narrow(
                    Build<TPhysicalTableEffectBuilder>(tableEffect, ctx, op->Pos, names, currentStageBody.AsNarrow(ctx)));

                stageBodies[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted TableEffect " << opStageId;
            }
            else {
                Y_ENSURE(false, "Could not generate physical plan");
            }
        }

        THashMap<ui32, TExprNode::TPtr> stages;
        stages.reserve(stageBodies.size());
        for (const auto& [id, body] : stageBodies) {
            stages[id] = body.AsNarrow(ctx);
        }

        for (const auto* stage : graph.Stages()) {
            auto& args = stageArgs[stage->Id()];
            for (auto* edge : graph.IncomingEdges(*stage)) {
                args.push_back(inputArguments.at(edge->Id()));
            }
        }
        queryGraphs.push_back(std::move(graph));
        allStages.push_back(std::move(stages));
        allStageArgs.push_back(std::move(stageArgs));
        allStagePos.push_back(std::move(stagePos));
    }
    convertOperatorsTraceScope.reset();

    return TPhysicalQueryBuilder(roots, std::move(queryGraphs), std::move(allStages), std::move(allStageArgs), std::move(allStagePos), std::move(allNames), rboCtx).BuildPhysicalQuery();

}

} // namespace NKikimr::NKqp
