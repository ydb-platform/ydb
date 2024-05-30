#include "yql_dq_statistics.h"
#include "yql_dq_state.h"

#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat_transformer_base.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

namespace NYql {

using namespace NNodes;

class TDqsStatisticsTransformer : public NDq::TDqStatisticsTransformerBase {
public:
    TDqsStatisticsTransformer(const TDqStatePtr& state, const IProviderContext& ctx)
        : NDq::TDqStatisticsTransformerBase(state->TypeCtx, ctx)
        , State(state)
    { }

    bool BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) override {
        bool matched = true;
        bool hasDqSource = false;

        if (TDqReadWrapBase::Match(input.Get()) || (hasDqSource = TDqSourceWrapBase::Match(input.Get()))) {
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
                    State->TypeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(*stat)));
                }
            }
        } else {
            matched = false;
        }
        return matched;
    }

    bool AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) override {
        Y_UNUSED(input);
        Y_UNUSED(ctx);
        return false;
    }

private:
    TDqStatePtr State;
};

THolder<IGraphTransformer> CreateDqsStatisticsTransformer(TDqStatePtr state, const IProviderContext& ctx) {
    return MakeHolder<TDqsStatisticsTransformer>(state, ctx);
}

} // namespace NYql
