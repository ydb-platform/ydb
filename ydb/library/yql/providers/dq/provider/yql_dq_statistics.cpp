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

        if (TDqReadWrapBase::Match(input.Get())) {
            if (auto stat = GetStats(input->Child(TDqReadWrapBase::idx_Input), ctx)) {
                State->TypeCtx->SetStats(input.Get(), stat);
            }
        } else if (TDqSourceWrapBase::Match(input.Get())) {
            if (auto stat = GetStats(input, ctx)) {
                State->TypeCtx->SetStats(input.Get(), stat);
                // This node can be split, so to preserve the statistics, we also expose it to settings
                State->TypeCtx->SetStats(input->Child(TDqSourceWrapBase::idx_Settings), stat);
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
    std::shared_ptr<TOptimizerStatistics> GetStats(const TExprNode::TPtr& node, TExprContext& ctx) {
        auto dataSourceChildIndex = 1;
        YQL_ENSURE(node->ChildrenSize() > 1);
        YQL_ENSURE(node->Child(dataSourceChildIndex)->IsCallable("DataSource"));
        auto dataSourceName = node->Child(dataSourceChildIndex)->Child(0)->Content();
        auto datasource = State->TypeCtx->DataSourceMap.FindPtr(dataSourceName);
        if (auto dqIntegration = (*datasource)->GetDqIntegration()) {
            auto stat = dqIntegration->ReadStatistics(node, ctx);
            if (stat) {
                return std::make_shared<TOptimizerStatistics>(std::move(*stat));
            }
        }
        return {};
    }

    TDqStatePtr State;
};

THolder<IGraphTransformer> CreateDqsStatisticsTransformer(TDqStatePtr state, const IProviderContext& ctx) {
    return MakeHolder<TDqsStatisticsTransformer>(state, ctx);
}

} // namespace NYql
