#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include "kqp_rbo_physical_sort_builder.h"
#include "kqp_rbo_physical_aggregation_builder.h"
#include "kqp_rbo_physical_map_builder.h"
#include "kqp_rbo_physical_join_builder.h"
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

TExprNode::TPtr ConvertToPhysical(TOpRoot& root, TRBOContext& rboCtx) {
    TExprContext& ctx = rboCtx.ExprCtx;

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
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, opRead->GetNumOfConsumers(), ctx, op->Pos);
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

            if (limit->GetOutputIUs() != limit->GetInput()->GetOutputIUs()) {
                currentStageBody = NPhysicalConvertionUtils::ExtractMembers(currentStageBody, ctx, limit->GetOutputIUs());
            }

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

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
            stageArgs[opStageId].push_back(leftArg);
            auto [rightArg, rightInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
            stageArgs[opStageId].push_back(rightArg);

            currentStageBody = Build<TPhysicalJoinBuilder>(join, ctx, op->Pos, leftInput, rightInput, rboCtx.KqpCtx.Config->GetUseBlockHashJoin());

            if (!join->IsSingleConsumer()) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, join->GetNumOfConsumers(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId;
        } else if (op->Kind == EOperator::UnionAll) {
            auto unionAll = CastOperator<TOpUnionAll>(op);
            const auto unionOutput = unionAll->GetOutputIUs();

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
            stageArgs[opStageId].push_back(leftArg);

            auto [rightArg, rightInput] = graph.GenerateStageInput(stageInputCounter, op->Pos, ctx);
            stageArgs[opStageId].push_back(rightArg);

            auto projectInput = [&](TExprNode::TPtr input, const TIntrusivePtr<IOperator>& inputOp) {
                auto hasSameType = [&](const TInfoUnit& source, const TInfoUnit& target) {
                    const auto* sourceType = inputOp->GetIUType(source);
                    const auto* targetType = unionAll->GetIUType(target);
                    return sourceType && targetType && IsSameAnnotation(*sourceType, *targetType);
                };

                const auto inputOutput = inputOp->GetOutputIUs();
                if (inputOutput == unionOutput) {
                    return input;
                }
                if (inputOutput.size() == unionOutput.size()) {
                    THashSet<TInfoUnit, TInfoUnit::THashFunction> inputOutputSet;
                    inputOutputSet.insert(inputOutput.begin(), inputOutput.end());
                    TVector<std::pair<TString, TString>> renames;
                    renames.reserve(unionOutput.size());
                    for (size_t i = 0; i < unionOutput.size(); ++i) {
                        TInfoUnit source = inputOutput[i];
                        if (inputOutputSet.contains(unionOutput[i]) && hasSameType(unionOutput[i], unionOutput[i])) {
                            source = unionOutput[i];
                        } else if (!hasSameType(source, unionOutput[i])) {
                            for (const auto& candidate : inputOutput) {
                                if (candidate.GetColumnName() == unionOutput[i].GetColumnName() && hasSameType(candidate, unionOutput[i])) {
                                    source = candidate;
                                    break;
                                }
                            }
                        }
                        renames.emplace_back(source.GetFullName(), unionOutput[i].GetFullName());
                    }
                    return NPhysicalConvertionUtils::BuildRenameMap(input, renames, ctx);
                }
                return NPhysicalConvertionUtils::ExtractMembers(input, ctx, unionOutput);
            };

            TVector<TExprNode::TPtr> extendArgs{
                projectInput(leftArg, unionAll->GetLeftInput()),
                projectInput(rightArg, unionAll->GetRightInput())
            };

            if (unionAll->Ordered) {
                // clang-format off
                currentStageBody = Build<TCoOrderedExtend>(ctx, op->Pos)
                    .Add(extendArgs)
                .Done().Ptr();
                // clang-format on
            } else {
                // clang-format off
                currentStageBody = Build<TCoExtend>(ctx, op->Pos)
                    .Add(extendArgs)
                .Done().Ptr();
                // clang-format on
            }

            if (unionOutput != unionAll->GetLeftInput()->GetOutputIUs()) {
                currentStageBody = NPhysicalConvertionUtils::ExtractMembers(currentStageBody, ctx, unionOutput);
            }

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
        } else {
            Y_ENSURE(false, "Could not generate physical plan");
        }
    }

    return TPhysicalQueryBuilder(root, std::move(graph), std::move(stages), std::move(stageArgs), std::move(stagePos), rboCtx).BuildPhysicalQuery();
}

} // namespace NKikimr::NKqp
