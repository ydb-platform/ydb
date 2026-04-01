#pragma once

#include "kqp_opt.h"

#include <yql/essentials/core/yql_statistics.h>

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat_transformer_base.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

/***
 * Statistics transformer is a transformer that propagates statistics and costs from
 * the leaves of the plan DAG up to the root of the DAG. It handles a number of operators,
 * but will simply stop propagation if in encounters an operator that it has no rules for.
 * One of such operators is EquiJoin, but there is a special rule to handle EquiJoin.
*/
class TKqpStatisticsTransformer : public NYql::NDq::TDqStatisticsTransformerBase {

    const TKikimrConfiguration::TPtr& Config;
    TKqpOptimizeContext& KqpCtx;
    TKqpStatsStore* KqpStats;
    const TKqpProviderContext& KqpPctx;
    TVector<TVector<std::shared_ptr<TOptimizerStatistics>>> TxStats;

    THashMap<std::shared_ptr<TOptimizerStatistics>, TString, std::hash<std::shared_ptr<TOptimizerStatistics>>> TablePathByStats;

    public:
        TKqpStatisticsTransformer(
            const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
            TTypeAnnotationContext& typeCtx,
            const TKikimrConfiguration::TPtr& config,
            const TKqpProviderContext& pctx
        ) :
            NYql::NDq::TDqStatisticsTransformerBase(&typeCtx, true),
            Config(config),
            KqpCtx(*kqpCtx),
            KqpStats(&kqpCtx->KqpStats),
            KqpPctx(pctx)
        {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;

    private:
        bool BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) final;
        bool AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx) final;

        bool BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx) override;
        bool BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx) override;
        bool AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx) override;
        void OnPropagateToLambdaArgument(const TExprNode::TPtr& input) override;
        void OnPropagateTableAliases(const TExprNode::TPtr& input) override;
};

TAutoPtr<IGraphTransformer> CreateKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config, const TKqpProviderContext& pctx);
}
}
