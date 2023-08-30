#include "yql_dq_statistics.h"
#include "yql_dq_state.h"

#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

namespace NYql {

using namespace NNodes;

class TDqsStatisticsTransformer : public TSyncTransformerBase {
public:
    TDqsStatisticsTransformer(const TDqStatePtr& state)
        : State(state)
    { }

    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;

        if (!State->TypeCtx->CostBasedOptimizerType) {
            return IGraphTransformer::TStatus::Ok;
        }

        TOptimizeExprSettings settings(nullptr);

        auto ret = OptimizeExpr(input, output, [*this](const TExprNode::TPtr& input, TExprContext& ctx) {
            Y_UNUSED(ctx);
            auto output = input;
            bool hasDqSource = false;

            if (TCoFlatMap::Match(input.Get())) {
                NDq::InferStatisticsForFlatMap(input, State->TypeCtx);
            } else if(TCoSkipNullMembers::Match(input.Get())) {
                NDq::InferStatisticsForSkipNullMembers(input, State->TypeCtx);
            } else if(TCoExtractMembers::Match(input.Get())){
                NDq::InferStatisticsForExtractMembers(input, State->TypeCtx);
            }
            else if(TCoAggregateCombine::Match(input.Get())){
                NDq::InferStatisticsForAggregateCombine(input, State->TypeCtx);
            }
            else if(TCoAggregateMergeFinalize::Match(input.Get())){
                NDq::InferStatisticsForAggregateMergeFinalize(input, State->TypeCtx);
            } else if (TDqReadWrapBase::Match(input.Get()) || (hasDqSource = TDqSourceWrapBase::Match(input.Get()))) {
                auto node = hasDqSource
                    ? input
                    : input->Child(TDqReadWrapBase::idx_Input);
                auto dataSourceChildIndex = 1;
                YQL_ENSURE(node->ChildrenSize() > 1);
                YQL_ENSURE(node->Child(dataSourceChildIndex)->IsCallable("DataSource"));
                auto dataSourceName = node->Child(dataSourceChildIndex)->Child(0)->Content();
                auto datasource = State->TypeCtx->DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(datasource);
                if (auto dqIntegration = (*datasource)->GetDqIntegration()) {
                    auto stat = dqIntegration->ReadStatistics(node, ctx);
                    if (stat) {
                        State->TypeCtx->SetStats(input.Get(), std::move(std::make_shared<TOptimizerStatistics>(*stat)));
                    }
                }
            } else {
                // default sum propagation
                TOptimizerStatistics stat;
                for (const auto& child : input->Children()) {
                    auto chStat = State->TypeCtx->GetStats(child.Get());
                    if (chStat) {
                        stat += *chStat;
                    }
                }
                if (!stat.Empty()) {
                    State->TypeCtx->SetStats(input.Get(), std::move(std::make_shared<TOptimizerStatistics>(stat)));
                }
            }
            return output;
        }, ctx, settings);

        return ret;
    }

    void Rewind() { }

private:
    TDqStatePtr State;
};

THolder<IGraphTransformer> CreateDqsStatisticsTransformer(TDqStatePtr state) {
    return MakeHolder<TDqsStatisticsTransformer>(state);
}

} // namespace NYql
