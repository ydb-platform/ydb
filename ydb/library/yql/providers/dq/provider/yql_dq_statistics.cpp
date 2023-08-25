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

            if (TCoFlatMap::Match(input.Get())){
                NDq::InferStatisticsForFlatMap(input, State->TypeCtx);
            } else if(TCoSkipNullMembers::Match(input.Get())){
                NDq::InferStatisticsForSkipNullMembers(input, State->TypeCtx);
            } else if (TDqReadWrapBase::Match(input.Get())) {
                auto read = input->Child(TDqReadWrapBase::idx_Input);
                auto dataSourceChildIndex = 1;
                YQL_ENSURE(read->ChildrenSize() > 1);
                YQL_ENSURE(read->Child(dataSourceChildIndex)->IsCallable("DataSource"));
                auto dataSourceName = read->Child(dataSourceChildIndex)->Child(0)->Content();
                auto datasource = State->TypeCtx->DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(datasource);
                if (auto dqIntegration = (*datasource)->GetDqIntegration()) {
                    auto stat = dqIntegration->ReadStatistics(read, ctx);
                    if (stat) {
                        State->TypeCtx->SetStats(input.Get(), std::move(std::make_shared<TOptimizerStatistics>(*stat)));
                    }
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
