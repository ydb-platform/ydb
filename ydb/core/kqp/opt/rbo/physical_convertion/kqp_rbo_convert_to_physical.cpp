#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include "kqp_rbo_physical_sort_builder.h"
#include "kqp_rbo_physical_aggregation_builder.h"
#include "kqp_rbo_physical_map_builder.h"
#include "kqp_rbo_physical_join_builder.h"
#include "kqp_rbo_physical_filter_builder.h"
#include "kqp_rbo_physical_source_builder.h"
#include "kqp_rbo_physical_query_builder.h"

#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <ydb/library/yql/dq/opt/dq_opt_peephole.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_expr_optimize.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr {
namespace NKqp {

TExprNode::TPtr ConvertToPhysical(TOpRoot& root, TRBOContext& rboCtx) {
    TExprContext& ctx = rboCtx.ExprCtx;

    THashMap<ui32, TExprNode::TPtr> stages;
    THashMap<ui32, TVector<TExprNode::TPtr>> stageArgs;
    THashMap<ui32, TPositionHandle> stagePos;
    auto &graph = root.PlanProps.StageGraph;
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

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Read " << opStageId;
        } else if (op->Kind == EOperator::Filter) {
            auto filter = CastOperator<TOpFilter>(op);

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            currentStageBody = Build<TPhysicalFilterBuilder>(filter, ctx, op->Pos, currentStageBody);

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Filter " << opStageId;
        } else if (op->Kind == EOperator::Map) {
            auto map = CastOperator<TOpMap>(op);

            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            currentStageBody = Build<TPhysicalMapBuilder>(map, ctx, op->Pos, currentStageBody);

            if (NPhysicalConvertionUtils::IsMultiConsumerHandlerNeeded(op)) {
                currentStageBody = NPhysicalConvertionUtils::BuildMultiConsumerHandler(currentStageBody, op->Props.NumOfConsumers.value(), ctx, op->Pos);
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Map " << opStageId;
        } else if (op->Kind == EOperator::Limit) {
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            auto limit = CastOperator<TOpLimit>(op);

            // clang-format off
            currentStageBody = Build<TCoTake>(ctx, op->Pos)
                .Input(TExprBase(currentStageBody))
                .Count(limit->LimitCond.GetExpressionBody())
            .Done().Ptr();
            // clang-format on

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Limit " << opStageId;
        } else if (op->Kind == EOperator::Sort) {
            auto sort = CastOperator<TOpSort>(op);
            if (!currentStageBody) {
                auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *op->Children[0]->Props.StageId);
                stageArgs[opStageId].push_back(stageArg);
                currentStageBody = stageInput;
            }

            currentStageBody = Build<TPhysicalSortBuilder>(sort, ctx, op->Pos, currentStageBody);

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Sort " << opStageId;
        } else if (op->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(op);

            auto [leftArg, leftInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetLeftInput()->Props.StageId);
            stageArgs[opStageId].push_back(leftArg);
            auto [rightArg, rightInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *join->GetRightInput()->Props.StageId);
            stageArgs[opStageId].push_back(rightArg);

            currentStageBody = Build<TPhysicalJoinBuilder>(join, ctx, op->Pos, leftInput, rightInput);

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted Join " << opStageId;
        } else if (op->Kind == EOperator::UnionAll) {
            auto unionAll = CastOperator<TOpUnionAll>(op);

            auto [leftArg, leftInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *unionAll->GetLeftInput()->Props.StageId);
            stageArgs[opStageId].push_back(leftArg);

            auto [rightArg, rightInput] =
                graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *unionAll->GetRightInput()->Props.StageId);
            stageArgs[opStageId].push_back(rightArg);
            TVector<TExprNode::TPtr> extendArgs{leftArg, rightArg};

            if (unionAll->Ordered) {
                // clang-format off
                currentStageBody = Build<TCoOrderedExtend>(ctx, op->Pos)
                    .Add(extendArgs)
                .Done().Ptr();
                // clang-format on
            }
            else {
                // clang-format off
                currentStageBody = Build<TCoExtend>(ctx, op->Pos)
                    .Add(extendArgs)
                .Done().Ptr();
                // clang-format on
            }

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
            YQL_CLOG(TRACE, CoreDq) << "Converted UnionAll " << opStageId;
        } else if (op->Kind == EOperator::Aggregate) {
            auto aggregate = CastOperator<TOpAggregate>(op);

            auto [stageArg, stageInput] = graph.GenerateStageInput(stageInputCounter, root.Node, ctx, *aggregate->GetInput()->Props.StageId);
            stageArgs[opStageId].push_back(stageArg);

            currentStageBody = Build<TPhysicalAggregationBuilder>(aggregate, ctx, op->Pos, stageInput);

            stages[opStageId] = currentStageBody;
            stagePos[opStageId] = op->Pos;
        } else {
            Y_ENSURE(false, "Could not generate physical plan");
        }
    }

    return TPhysicalQueryBuilder(root, std::move(graph), std::move(stages), std::move(stageArgs), std::move(stagePos), rboCtx).BuildPhysicalQuery();
}
} // namespace NKqp
} // namespace NKikimr
