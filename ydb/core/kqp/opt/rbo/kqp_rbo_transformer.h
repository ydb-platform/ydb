#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NOpt;

class TKqpRewriteSelectTransformer : public TSyncTransformerBase {
  public:
    TKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx, TTypeAnnotationContext &typeCtx)
        : TypeCtx(typeCtx), KqpCtx(*kqpCtx), UniqueSourceIdCounter(0) {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) final;
    void Rewind() override;

  private:
    TTypeAnnotationContext &TypeCtx;
    const TKqpOptimizeContext &KqpCtx;
    ui64 UniqueSourceIdCounter = 0;
};

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                                                             TTypeAnnotationContext &typeCtx);

class TKqpNewRBOTransformer: public TSyncTransformerBase {
public:
    TKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, TAutoPtr<IGraphTransformer> rboTypeAnnTransformer,
                          TAutoPtr<IGraphTransformer> typeAnnTransformer, TAutoPtr<IGraphTransformer> peephole, const NMiniKQL::IFunctionRegistry& funcRegistry)
        : TypeCtx(typeCtx)
        , KqpCtx(*kqpCtx)
        , RBO(kqpCtx, typeCtx, rboTypeAnnTransformer, typeAnnTransformer, peephole, funcRegistry) {
        // Initial stages.
        TVector<std::shared_ptr<IRule>> inlineScalarSubPlanStageRules{std::make_shared<TInlineScalarSubplanRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Inline scalar subplans", std::move(inlineScalarSubPlanStageRules)));
        RBO.AddStage(std::make_shared<TRenameStage>());
        RBO.AddStage(std::make_shared<TConstantFoldingStage>());
        RBO.AddStage(std::make_shared<TPruneColumnsStage>());
        // Logical stage.
        TVector<std::shared_ptr<IRule>> logicalStageRules = {std::make_shared<TRemoveIdenityMapRule>(), std::make_shared<TExtractJoinExpressionsRule>(),
                                                             std::make_shared<TPushMapRule>(), std::make_shared<TPushFilterRule>(),
                                                             std::make_shared<TPushLimitIntoSortRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Logical rewrites I", std::move(logicalStageRules)));
        // Physical stage.
        TVector<std::shared_ptr<IRule>> physicalStageRules = {std::make_shared<TPushOlapFilterRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Physical rewrites I", std::move(physicalStageRules)));
        // CBO stages.
        TVector<std::shared_ptr<IRule>> initialCBOStageRules = {std::make_shared<TBuildInitialCBOTreeRule>(), std::make_shared<TExpandCBOTreeRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Prepare for CBO", std::move(initialCBOStageRules)));
        TVector<std::shared_ptr<IRule>> cboStageRules = {std::make_shared<TOptimizeCBOTreeRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Invoke CBO", std::move(cboStageRules)));
        TVector<std::shared_ptr<IRule>> cleanUpCBOStageRules = {std::make_shared<TInlineCBOTreeRule>(), std::make_shared<TPushFilterRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Clean up after CBO", std::move(cleanUpCBOStageRules)));
        // Assign physical stages.
        TVector<std::shared_ptr<IRule>> assignPhysicalStageRules = {std::make_shared<TAssignStagesRule>()};
        RBO.AddStage(std::make_shared<TRuleBasedStage>("Assign stages", std::move(assignPhysicalStageRules)));
    }

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) final;
    void Rewind() override;

  private:
    TTypeAnnotationContext &TypeCtx;
    TKqpOptimizeContext &KqpCtx;
    TRuleBasedOptimizer RBO;
};

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext> &kqpCtx, 
                                                      TTypeAnnotationContext &typeCtx,
                                                      TAutoPtr<IGraphTransformer> rboTypeAnnTransformer,
                                                      TAutoPtr<IGraphTransformer> typeAnnTransformer,
                                                      TAutoPtr<IGraphTransformer> peepholeTransformer,
                                                      const NMiniKQL::IFunctionRegistry& funcRegistry);

class TKqpRBOCleanupTransformer : public TSyncTransformerBase {
  public:
    TKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx) : TypeCtx(typeCtx) {}

    // Main method of the transformer
    IGraphTransformer::TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) final;
    void Rewind() override;

  private:
    TTypeAnnotationContext &TypeCtx;
};

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx);

TExprNode::TPtr RewriteSelect(const TExprNode::TPtr &node, TExprContext &ctx, const TTypeAnnotationContext &typeCtx, const TKqpOptimizeContext& kqpCtx, ui64& uniqueSourceIdCounter, bool pgSyntax=false);

} // namespace NKqp
} // namespace NKikimr