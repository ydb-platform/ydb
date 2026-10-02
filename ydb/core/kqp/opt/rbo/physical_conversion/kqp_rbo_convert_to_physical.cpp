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
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp {

namespace {

/**
 * Order in which the union inputs are consumed by the stage: inputs sharing a stage are grouped
 * together at the position of the first child using that stage, keeping child order within a group.
 * This mirrors the way TPhysicalQueryBuilder::BuildPhysicalStageGraph builds the stage connections,
 * which are paired with the stage arguments positionally.
 */
TVector<ui32> GetUnionAllInputArgumentOrder(TOpUnionAll& unionAll) {
    TVector<ui32> stageOrder;
    THashMap<ui32, TVector<ui32>> childrenByStage;
    for (ui32 childIndex = 0; childIndex < unionAll.GetChildren().size(); ++childIndex) {
        const auto childStageId = *unionAll.GetChildren()[childIndex]->Props.StageId;
        auto [it, inserted] = childrenByStage.emplace(childStageId, TVector<ui32>());
        if (inserted) {
            stageOrder.push_back(childStageId);
        }
        it->second.push_back(childIndex);
    }

    TVector<ui32> result;
    result.reserve(unionAll.GetChildren().size());
    for (const auto stageId : stageOrder) {
        for (const auto childIndex : childrenByStage.at(stageId)) {
            result.push_back(childIndex);
        }
    }
    return result;
}

} // anonymous namespace

TExprNode::TPtr ConvertToPhysical(const TVector<TIntrusivePtr<TOpRoot>>& roots, TRBOContext& rboCtx) {
    TExprContext& ctx = rboCtx.ExprCtx;
    ui32 stageInputCounter = 0;

    if (rboCtx.NeedToLog()) {
        rboCtx.TraceLog.stage("Physical AST generation");
    }

    TVector<TStageGraph> queryGraphs;
    TVector<THashMap<ui32, TExprNode::TPtr>> allStages;
    TVector<THashMap<ui32, TVector<TExprNode::TPtr>>> allStageArgs;
    TVector<THashMap<ui32, TPositionHandle>> allStagePos; 

    TVector<TPhysicalNames> allNames;
    allNames.reserve(roots.size());
    for (auto & root: roots) {
        const auto& names = allNames.emplace_back(root->PlanProps.InfoUnitRegistry);
        THashMap<ui32, TExprNode::TPtr> stages;
        THashMap<ui32, TVector<TExprNode::TPtr>> stageArgs;
        THashMap<ui32, TPositionHandle> stagePos;
        auto& graph = root->PlanProps.StageGraph;
        THashSet<const TReplicate*> builtReplicates;
        for (auto id : graph.StageIds) {
            stageArgs[id] = TVector<TExprNode::TPtr>();
        }

        for (const auto& iter : *root) {
            auto op = iter.Current;
            auto opStageId = *(op->Props.StageId);

            TExprNode::TPtr currentStageBody;
            if (stages.contains(opStageId)) {
                currentStageBody = stages.at(opStageId);
            }

            if (op->Kind == EOperator::Replicate) {
                auto& replicate = CastOperator<TOpReplicate>(*op).GetReplicate();
                if (!builtReplicates.insert(&replicate).second) {
                    continue;
                }
                if (!currentStageBody) {
                    auto [arg, input] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(arg);
                    currentStageBody = input;
                }
                stages[opStageId] = NPhysicalConvertionUtils::BuildSwitch(
                    currentStageBody, replicate, names, ctx);
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::EmptySource) {
                const auto emptySource = CastOperator<TOpEmptySource>(op);
                if (emptySource->Input) {
                    currentStageBody = Build<TCoIterator>(ctx, op->Pos)
                        .List(emptySource->Input)
                        .Done().Ptr();
                    TVector<std::pair<TString, TString>> columns;
                    for (const auto id : emptySource->GetOutputIUs()) {
                        columns.emplace_back(root->PlanProps.InfoUnitRegistry.Get(id).GetFullName(), names.Get(id));
                    }
                    currentStageBody = NPhysicalConvertionUtils::BuildRenameMap(currentStageBody, columns, ctx);
                } else {
                    TVector<TExprBase> listElements;
                    listElements.push_back(Build<TCoAsStruct>(ctx, op->Pos).Done());

                    // clang-format off
                    currentStageBody = Build<TCoIterator>(ctx, op->Pos)
                        .List<TCoAsList>()
                            .Add(listElements)
                        .Build()
                    .Done().Ptr();
                    // clang-format on
                }
                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Empty Source " << opStageId;
            } else if (op->Kind == EOperator::Source) {
                auto& opRead = CastOperator<TOpRead>(*op);

                TString carrierColumn;
                if (opRead.GetColumns().Empty() && opRead.GetTableStorageType() == NYql::EStorageType::ColumnStorage) {
                    const auto& table = rboCtx.KqpCtx.Tables->ExistingTable(
                        rboCtx.KqpCtx.Cluster, TKqpTable(opRead.TableCallable).Path().Value());
                    Y_ENSURE(!table.Metadata->KeyColumnNames.empty(), "An OLAP table needs a primary key");
                    carrierColumn = table.Metadata->KeyColumnNames.front();
                }
                currentStageBody = TPhysicalSourceBuilder(opRead, ctx, op->Pos, names, root->PlanProps.InfoUnitRegistry,
                    graph.StageGUIDs.at(opStageId), std::move(carrierColumn)).BuildPhysicalOp();

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
            } else if (op->Kind == EOperator::Filter) {
                auto& filter = CastOperator<TOpFilter>(*op);

                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }

                currentStageBody = Build<TPhysicalFilterBuilder>(filter, ctx, op->Pos, names, currentStageBody);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Filter " << opStageId;
            } else if (op->Kind == EOperator::Map) {
                auto& map = CastOperator<TOpMap>(*op);

                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }

                currentStageBody = Build<TPhysicalMapBuilder>(map, ctx, op->Pos, names, currentStageBody);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Map " << opStageId;
            } else if (op->Kind == EOperator::Limit) {
                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }

                auto& limit = CastOperator<TOpLimit>(*op);

                if (limit.HasOffset()) {
                    // clang-format off
                    currentStageBody = Build<TCoSkip>(ctx, op->Pos)
                        .Input(currentStageBody)
                        .Count(limit.GetOffsetCond()->GetExpressionBody())
                    .Done().Ptr();
                    // clang-format on
                }

                // clang-format off
                currentStageBody = Build<TCoTake>(ctx, op->Pos)
                    .Input(currentStageBody)
                    .Count(limit.LimitCond.GetExpressionBody())
                .Done().Ptr();
                // clang-format on

                currentStageBody = NPhysicalConvertionUtils::ExtractMembers(
                    currentStageBody,
                    ctx,
                    NPhysicalConvertionUtils::GetLiveOutputIUs(limit), names);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId;
            } else if (op->Kind == EOperator::Sort) {
                auto& sort = CastOperator<TOpSort>(*op);
                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }
                currentStageBody = Build<TPhysicalSortBuilder>(sort, ctx, op->Pos, names, currentStageBody);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Sort " << opStageId;
            } else if (op->Kind == EOperator::Window) {
                auto& window = CastOperator<TOpWindow>(*op);
                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }
                currentStageBody = Build<TPhysicalWindowBuilder>(window, ctx, op->Pos, names, currentStageBody);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Window " << opStageId;
            } else if (op->Kind == EOperator::Join) {
                auto& join = CastOperator<TOpJoin>(*op);
                Y_ENSURE(join.Props.UseBlockHashJoin.has_value(), "Physical join implementation has not been selected");

                auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(leftArg);
                auto [rightArg, rightInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(rightArg);

                currentStageBody = Build<TPhysicalJoinBuilder>(join, ctx, op->Pos, names, leftInput, rightInput, *join.Props.UseBlockHashJoin, rboCtx.TypeCtx);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId;
            } else if (op->Kind == EOperator::UnionAll) {
                auto& unionAll = CastOperator<TOpUnionAll>(*op);

                TVector<TExprNode::TPtr> inputs(unionAll.GetChildren().size());
                for (const auto childIndex : GetUnionAllInputArgumentOrder(unionAll)) {
                    auto [arg, input] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(arg);
                    inputs[childIndex] = input;
                }

                currentStageBody = Build<TPhysicalUnionAllBuilder>(unionAll, ctx, op->Pos, names, inputs);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted UnionAll " << opStageId;
            } else if (op->Kind == EOperator::Aggregate) {
                auto& aggregate = CastOperator<TOpAggregate>(*op);

                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }

                std::optional<i64> memLimit;
                if (auto memLimitSetting = rboCtx.KqpCtx.Config->_KqpYqlCombinerMemoryLimit.Get()) {
                    memLimit = -i64(*memLimitSetting);
                }

                // The full physical-stage peephole performs this pruning later.
                const bool pruneUnusedOutputs = !rboCtx.KqpCtx.Config->GetEnableNewRBOPhysicalStagePeephole();
                currentStageBody = TPhysicalAggregationBuilder(aggregate, ctx, op->Pos, names, pruneUnusedOutputs,
                    rboCtx.KqpCtx.Config->GetDqHashOperatorsUseBlocks())
                    .BuildPhysicalOp(currentStageBody, memLimit);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::TableLookup) {
                auto& lookup = CastOperator<TOpTableLookup>(*op);

                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }

                const auto inputStageId = *lookup.GetInput()->Props.StageId;
                const auto connection = graph.TryGetConnection(inputStageId, opStageId);
                auto* streamLookup = dynamic_cast<TStreamLookupConnection*>(connection.Get());
                Y_ENSURE(streamLookup, "A table lookup must be fed by a stream lookup connection");
                auto keys = NLookupJoinBuilder::BuildLookupKeys(lookup, stages.at(inputStageId), ctx, names);
                stages[inputStageId] = keys.InputStage;
                streamLookup->SetInputType(keys.InputType);

                if (lookup.IsJoin()) {
                    YQL_CLOG(TRACE, CoreDq) << "Converted TableLookupJoin " << opStageId;
                } else {
                    auto streamInput = Build<TCoToStream>(ctx, op->Pos).Input(currentStageBody).Done().Ptr();
                    TVector<std::pair<TString, TString>> renames;
                    for (const auto id : lookup.GetColumns()) {
                        renames.emplace_back(root->PlanProps.InfoUnitRegistry.Get(id).GetColumnName(), names.Get(id));
                    }
                    currentStageBody = NPhysicalConvertionUtils::BuildRenameMap(streamInput, renames, ctx);

                    YQL_CLOG(TRACE, CoreDq) << "Converted TableLookup " << opStageId;
                }

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
            } else if (op->Kind == EOperator::IndexLookupJoin) {
                auto& lookupJoin = CastOperator<TOpIndexLookupJoin>(*op);
                Y_ENSURE(currentStageBody, "A lookup join must share the stage of its table lookup");

                currentStageBody = TPhysicalIndexLookupJoinBuilder(lookupJoin, ctx, op->Pos, names, root->PlanProps.InfoUnitRegistry)
                    .BuildPhysicalOp(currentStageBody);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted IndexLookupJoin " << opStageId;
            } else if (op->Kind == EOperator::TableEffect) {
                auto& tableEffect = CastOperator<TOpTableEffect>(*op);

                if (!currentStageBody) {
                    auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                    stageArgs[opStageId].push_back(stageArg);
                    currentStageBody = stageInput;
                }

                currentStageBody = Build<TPhysicalTableEffectBuilder>(tableEffect, ctx, op->Pos, names, currentStageBody);

                stages[opStageId] = currentStageBody;
                stagePos[opStageId] = op->Pos;
                YQL_CLOG(TRACE, CoreDq) << "Converted TableEffect " << opStageId;
            }
            else {
                Y_ENSURE(false, "Could not generate physical plan");
            }
        }

        queryGraphs.push_back(std::move(graph));
        allStages.push_back(std::move(stages));
        allStageArgs.push_back(std::move(stageArgs));
        allStagePos.push_back(std::move(stagePos));
    }

    return TPhysicalQueryBuilder(roots, std::move(queryGraphs), std::move(allStages), std::move(allStageArgs), std::move(allStagePos), std::move(allNames), rboCtx).BuildPhysicalQuery();

}

} // namespace NKikimr::NKqp
