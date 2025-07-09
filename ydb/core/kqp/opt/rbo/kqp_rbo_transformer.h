#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

class TKqpPgRewriteTransformer : public TSyncTransformerBase {
    public:
        TKqpPgRewriteTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) : 
            TypeCtx(typeCtx),
            KqpCtx(*kqpCtx) {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
        void Rewind() override;
        
    private:
        TTypeAnnotationContext& TypeCtx;
        const TKqpOptimizeContext& KqpCtx;
};

TAutoPtr<IGraphTransformer> CreateKqpPgRewriteTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx);

class TKqpNewRBOTransformer : public TSyncTransformerBase {
    public:
        TKqpNewRBOTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config, TAutoPtr<IGraphTransformer> typeAnnTransformer, TAutoPtr<IGraphTransformer> peephole) : 
            TypeCtx(typeCtx),
            KqpCtx(*kqpCtx),
            RBO({RuleStage1, RuleStage2}, kqpCtx, typeCtx, config, typeAnnTransformer, peephole) {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
        void Rewind() override;
        
    private:
        TTypeAnnotationContext& TypeCtx;
        const TKqpOptimizeContext& KqpCtx;
        TRuleBasedOptimizer RBO;
};

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config, TAutoPtr<IGraphTransformer> typeAnnTransformer, TAutoPtr<IGraphTransformer> peepholeTransformer);

class TKqpRBOCleanupTransformer : public TSyncTransformerBase {
    public:
        TKqpRBOCleanupTransformer(TTypeAnnotationContext& typeCtx) : 
            TypeCtx(typeCtx)
            {}

        // Main method of the transformer
        IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final;
        void Rewind() override;
        
    private:
        TTypeAnnotationContext& TypeCtx;
};

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext& typeCtx);

}
}