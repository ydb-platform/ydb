#pragma once

#include <util/generic/ptr.h>

namespace NYql {

class IGraphTransformer;
struct TTypeAnnotationContext;
struct TKikimrConfiguration;

} // namespace NYql

namespace NKikimr::NKqp {

namespace NOpt {

struct TKqpOptimizeContext;
struct TKqpProviderContext;

} // namespace NOpt

/***
 * Statistics transformer is a transformer that propagates statistics and costs from
 * the leaves of the plan DAG up to the root of the DAG. It handles a number of operators,
 * but will simply stop propagation if in encounters an operator that it has no rules for.
 * One of such operators is EquiJoin, but there is a special rule to handle EquiJoin.
*/
TAutoPtr<NYql::IGraphTransformer> CreateKqpStatisticsTransformer(const TIntrusivePtr<NOpt::TKqpOptimizeContext>& kqpCtx,
    NYql::TTypeAnnotationContext& typeCtx, const TIntrusivePtr<NYql::TKikimrConfiguration>& config, const NOpt::TKqpProviderContext& pctx);

    TTypeAnnotationContext* TypeCtx;
    const TKikimrConfiguration::TPtr& Config;
    TKqpOptimizeContext& KqpCtx;
    TKqpStatsStore* KqpStats;
    const TKqpProviderContext& KqpPctx;
    [[maybe_unused]] const NKikimr::NMiniKQL::IFunctionRegistry* FuncRegistry;
    TVector<TVector<std::shared_ptr<TOptimizerStatistics>>> TxStats;

    THashMap<std::shared_ptr<TOptimizerStatistics>, TString, std::hash<std::shared_ptr<TOptimizerStatistics>>> TablePathByStats;

    public:
        TKqpStatisticsTransformer(
            const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
            TTypeAnnotationContext& typeCtx,
            const TKikimrConfiguration::TPtr& config,
            const TKqpProviderContext& pctx,
            const NMiniKQL::IFunctionRegistry* funcRegistry
        ) :
            TypeCtx(&typeCtx),
            Config(config),
            KqpCtx(*kqpCtx),
            KqpStats(&kqpCtx->KqpStats),
            KqpPctx(pctx),
            FuncRegistry(funcRegistry)
        {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override;
        void Rewind() override {};

    private:
        bool BeforeLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx);
        bool AfterLambdasSpecific(const TExprNode::TPtr& input, TExprContext& ctx);

        bool BeforeLambdas(const TExprNode::TPtr& input, TExprContext& ctx);
        bool BeforeLambdasUnmatched(const TExprNode::TPtr& input, TExprContext& ctx);
        bool AfterLambdas(const TExprNode::TPtr& input, TExprContext& ctx);
};

TAutoPtr<IGraphTransformer> CreateKqpStatisticsTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config, const TKqpProviderContext& pctx, const NMiniKQL::IFunctionRegistry* funcRegistry);
}
}
