#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include "kqp_rbo_physical_sort_builder.h"
#include "kqp_rbo_physical_aggregation_builder.h"
#include "kqp_rbo_physical_map_builder.h"
#include "kqp_rbo_physical_union_all_builder.h"
#include "kqp_rbo_physical_join_builder.h"
#include "kqp_rbo_physical_lookup_join_builder.h"
#include "kqp_rbo_physical_filter_builder.h"
#include "kqp_rbo_physical_source_builder.h"
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
    for (ui32 childIndex = 0; childIndex < unionAll.Children.size(); ++childIndex) {
        const auto childStageId = *unionAll.Children[childIndex]->Props.StageId;
        auto [it, inserted] = childrenByStage.emplace(childStageId, TVector<ui32>());
        if (inserted) {
            stageOrder.push_back(childStageId);
        }
        it->second.push_back(childIndex);
    }

    TVector<ui32> result;
    result.reserve(unionAll.Children.size());
    for (const auto stageId : stageOrder) {
        for (const auto childIndex : childrenByStage.at(stageId)) {
            result.push_back(childIndex);
        }
    }
    return result;
}

} // anonymous namespace

TExprNode::TPtr ConvertToPhysical(TOpRoot& root, TRBOContext& rboCtx) {
    TExprContext& ctx = rboCtx.ExprCtx;

    if (rboCtx.NeedToLog()) {
        rboCtx.TraceLog.stage("Physical AST generation");
    }

    THashMap<ui32, TExprNode::TPtr> stages;
    THashMap<ui32, TVector<TExprNode::TPtr>> stageArgs;
    THashMap<ui32, TPositionHandle> stagePos;
    auto& graph = root.PlanProps.StageGraph;
    for (auto id : graph.StageIds) {
        stageArgs[id] = TVector<TExprNode::TPtr>();
    }

    ui32 stageInputCounter = 0;
    for (const auto& iter : root) {
        auto op = iter.Current;
        auto opStageId = *(op->Props.StageId);

        TExprNode::TPtr currentStageBody;
        if (stages.contains(opStageId)) {
            currentStageBody = stages.at(opStageId);
        }

        if (op->Kind == EOperator::EmptySource) {
            TVector<TExprBase> listElements;
            listElements.push_back(Build<TCoAsStruct>(ctx, op->Pos).Done());

            // clang-format off
            currentStageBody = Build<TCoIterator>(ctx, op->Pos)
                .List<TCoAsList>()
                    .Add(listElements)
                .Build()
            .Done().Ptr();
            // clang-format on
            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Empty Source " << opStageId;
        } else if (op->Kind == EOperator::Source) {
            auto opRead = CastOperator<TOpRead>(op);

            currentStageBody = Build<TPhysicalSourceBuilder>(opRead, ctx, op->Pos);

            if (!opRead->IsSingleConsumer()) {
                if (opRead->GetTableStorageType() == NYql::EStorageType::RowStorage) {
                    auto existingStage = TDqPhyStage(currentStageBody);
                    auto switchBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(
                        existingStage.Program().Body().Ptr(), opRead->GetNumOfConsumers(), ctx, op->Pos);
                    // clang-format off
                    currentStageBody = Build<TDqPhyStage>(ctx, op->Pos)
                        .InitFrom(existingStage)
                        .Program()
                            .Args(existingStage.Program().Args())
                            .Body(switchBody)
                        .Build()
                    .Done().Ptr();
                    // clang-format on
                } else {
                    currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(
                        currentStageBody, opRead->GetNumOfConsumers(), ctx, op->Pos);
                }
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
        } else if (op->Kind == EOperator::Filter) {
            auto filter = CastOperator<TOpFilter>(op);

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            currentStageBody = Build<TPhysicalFilterBuilder>(filter, ctx, op->Pos, currentStageBody);

            if (!filter->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, filter->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Filter " << opStageId;
        } else if (op->Kind == EOperator::Map) {
            auto map = CastOperator<TOpMap>(op);

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            currentStageBody = Build<TPhysicalMapBuilder>(map, ctx, op->Pos, currentStageBody);

            if (!map->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, map->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Map " << opStageId;
        } else if (op->Kind == EOperator::Limit) {
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto limit = CastOperator<TOpLimit>(op);

            if (limit->HasOffset()) {
                // clang-format off
                currentStageBody = Build<TCoSkip>(ctx, op->Pos)
                    .Input(currentStageBody)
                    .Count(limit->GetOffsetCond()->GetExpressionBody())
                .Done().Ptr();
                // clang-format on
            }

            // clang-format off
            currentStageBody = Build<TCoTake>(ctx, op->Pos)
                .Input(currentStageBody)
                .Count(limit->LimitCond.GetExpressionBody())
            .Done().Ptr();
            // clang-format on

            currentStageBody = NPhysicalConvertionUtils::ExtractMembers(
                currentStageBody,
                ctx,
                NPhysicalConvertionUtils::GetLiveOutputIUs(*limit));

            if (!limit->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, limit->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId;
        } else if (op->Kind == EOperator::Sort) {
            auto sort = CastOperator<TOpSort>(op);
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }
            currentStageBody = Build<TPhysicalSortBuilder>(sort, ctx, op->Pos, currentStageBody);

            if (!sort->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, sort->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Sort " << opStageId;
        } else if (op->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(op);
            Y_ENSURE(join->Props.UseBlockHashJoin.has_value(), "Physical join implementation has not been selected");

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
            stageArgs[opStageId].push_back(leftArg);
            auto [rightArg, rightInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
            stageArgs[opStageId].push_back(rightArg);

            currentStageBody = Build<TPhysicalJoinBuilder>(join, ctx, op->Pos, leftInput, rightInput, *join->Props.UseBlockHashJoin, rboCtx.TypeCtx);

            if (!join->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, join->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId;
        } else if (op->Kind == EOperator::UnionAll) {
            auto unionAll = CastOperator<TOpUnionAll>(op);

            TVector<TExprNode::TPtr> inputs(unionAll->Children.size());
            for (const auto childIndex : GetUnionAllInputArgumentOrder(*unionAll)) {
                auto [arg, input] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(arg);
                inputs[childIndex] = input;
            }

            currentStageBody = Build<TPhysicalUnionAllBuilder>(unionAll, ctx, op->Pos, inputs);

            if (!unionAll->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, unionAll->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted UnionAll " << opStageId;
        } else if (op->Kind == EOperator::Aggregate) {
            const auto aggregate = CastOperator<TOpAggregate>(op);

             if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            std::optional<i64> memLimit;
            if (auto memLimitSetting = rboCtx.KqpCtx.Config->_KqpYqlCombinerMemoryLimit.Get()) {
                memLimit = -i64(*memLimitSetting);
            }

            currentStageBody = Build<TPhysicalAggregationBuilder>(aggregate, ctx, op->Pos, currentStageBody, memLimit);
            if (!aggregate->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, aggregate->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
        } else if (op->Kind == EOperator::TableLookup) {
            auto lookup = CastOperator<TOpTableLookup>(op);

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            if (lookup->IsJoin()) {
                const auto inputStageId = *lookup->GetInput()->Props.StageId;
                const auto connection = graph.TryGetConnection(inputStageId, opStageId);
                auto* streamLookup = dynamic_cast<TStreamLookupConnection*>(connection.Get());
                Y_ENSURE(streamLookup, "A table lookup in join mode must be fed by a stream lookup connection");

                auto keys = NLookupJoinBuilder::BuildLookupKeys(*lookup, stages.at(inputStageId), ctx);
                stages[inputStageId] = keys.InputStage;
                streamLookup->SetInputType(keys.InputType);
                YQL_CLOG(TRACE, CoreDq) << "Converted TableLookupJoin " << opStageId;
            } else {
                auto streamInput = Build<TCoToStream>(ctx, op->Pos).Input(currentStageBody).Done().Ptr();
                TVector<std::pair<TString, TString>> renames;
                for (size_t i = 0; i < lookup->FetchColumns.size(); ++i) {
                    renames.emplace_back(lookup->FetchColumns[i], lookup->OutputIUs[i].GetFullName());
                }
                currentStageBody = NPhysicalConvertionUtils::BuildRenameMap(streamInput, renames, ctx);

                if (!lookup->IsSingleConsumer()) {
                    currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, lookup->GetNumOfConsumers(), ctx, op->Pos);
                }
                YQL_CLOG(TRACE, CoreDq) << "Converted TableLookup " << opStageId;
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
        } else if (op->Kind == EOperator::IndexLookupJoin) {
            auto lookupJoin = CastOperator<TOpIndexLookupJoin>(op);
            Y_ENSURE(currentStageBody, "A lookup join must share the stage of its table lookup");

            currentStageBody = Build<TPhysicalIndexLookupJoinBuilder>(lookupJoin, ctx, op->Pos, currentStageBody);

            if (!lookupJoin->IsSingleConsumer()) {
                currentStageBody =
                    NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, lookupJoin->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted IndexLookupJoin " << opStageId;
        } else {
            Y_ENSURE(false, "Could not generate physical plan");
        }
    }

    return TPhysicalQueryBuilder(root, std::move(graph), std::move(stages), std::move(stageArgs), std::move(stagePos), rboCtx).BuildPhysicalQuery();
}

} // namespace NKikimr::NKqp
