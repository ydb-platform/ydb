#include "kqp_rbo_transformer.h"
#include "kqp_operator.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

TExprNode::TPtr PushTakeIntoPlan(const TExprNode::TPtr &node, TExprContext &ctx, const TTypeAnnotationContext &typeCtx) {
    Y_UNUSED(typeCtx);
    auto take = TCoTake(node);
    auto takeInput = take.Input();
    if (takeInput.Maybe<TCoUnordered>()) {
        takeInput = takeInput.Cast<TCoUnordered>().Input();
    }

    if (auto root = takeInput.Maybe<TKqpOpRoot>()) {
        // clang-format off
        return Build<TKqpOpRoot>(ctx, node->Pos())
            .Input<TKqpOpLimit>()
                .Input(root.Cast().Input())
                .Count(take.Count())
            .Build()
            .ColumnOrder(root.Cast().ColumnOrder())
            .PgSyntax(root.Cast().PgSyntax())
        .Done().Ptr();
        // clang-format on
    } else {
        return node;
    }
}

TExprNode::TPtr RewriteSublink(const TExprNode::TPtr &node, TExprContext &ctx, bool pgSyntax) {
    if (node->Child(0)->Content() == "expr") {
        // clang-format off
        return Build<TKqpExprSublink>(ctx, node->Pos())
            .Subquery(node->Child(4))
            .Done().Ptr();
        // clang-format on
    } else if (node->Child(0)->Content() == "any") {
        // clang-format off
        return Build<TKqpInSublink>(ctx, node->Pos())
            .Subquery(node->Child(4))
            .ReturnPgBool().Value(std::to_string(pgSyntax)).Build()
            .InTuple(node->Child(2))
            .Done().Ptr();
        // clang-format on
    } else if (node->Child(0)->Content() == "exists") {
        // clang-format off
        return Build<TKqpExistsSublink>(ctx, node->Pos())
            .Subquery(node->Child(4))
            .ReturnPgBool().Value(std::to_string(pgSyntax)).Build()
            .Done().Ptr();
        // clang-format on
    }
    else {
        Y_ENSURE(false, "Uknown sublink type in query");
    }

}

TExprNode::TPtr RemoveRootFromSublink(const TExprNode::TPtr &node, TExprContext &ctx) {
    auto sublink = TKqpSublinkBase(node);
    if (auto root = sublink.Subquery().Maybe<TKqpOpRoot>()) {
        if (TKqpExprSublink::Match(node.Get())) {
            // clang-format off
            return Build<TKqpExprSublink>(ctx, node->Pos())
                .Subquery(root.Cast().Input())
                .Done().Ptr();
            // clang-format on
        } else if (TKqpExistsSublink::Match(node.Get())) {
            // clang-format off
            return Build<TKqpExistsSublink>(ctx, node->Pos())
                .Subquery(root.Cast().Input())
                .ReturnPgBool(node->Child(TKqpExistsSublink::idx_ReturnPgBool))
                .Done().Ptr();
            // clang-format on
        } else if (TKqpInSublink::Match(node.Get())) {
            // clang-format off
            return Build<TKqpInSublink>(ctx, node->Pos())
                .Subquery(root.Cast().Input())
                .ReturnPgBool(node->Child(TKqpInSublink::idx_ReturnPgBool))
                .InTuple(sublink.Cast<TKqpInSublink>().InTuple())
                .Done().Ptr();
            // clang-format on
        }
    }
    return node;
}
} // namespace

namespace NKikimr {
namespace NKqp {

IGraphTransformer::TStatus TKqpRewriteSelectTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(
        output, output,
        [](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            if (node->IsCallable("PgSubLink")) {
                return RewriteSublink(node, ctx, true);
            } else if (node->IsCallable("YqlSubLink")) {
                return RewriteSublink(node, ctx, false);
            } else {
                return node;
            }
        },
        ctx, settings);
    
    if (status != TStatus::Ok) {
        return status;
    }

    status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            // PostgreSQL AST rewrtiting
            if (TCoPgSelect::Match(node.Get())) {
                return RewriteSelect(node, ctx, TypeCtx, KqpCtx, UniqueSourceIdCounter,  true);
            }
            
            // YQL AST rewriting
            else if (TCoYqlSelect::Match(node.Get())) {
                return RewriteSelect(node, ctx, TypeCtx, KqpCtx, UniqueSourceIdCounter, false);
            } else if (TKqpSublinkBase::Match(node.Get())) {
                return RemoveRootFromSublink(node, ctx);
            }  else if (TCoTake::Match(node.Get())) {
                return PushTakeIntoPlan(node, ctx, TypeCtx);
            } else {
                return node;
            }
        },
        ctx, settings);

    return status;
}

void TKqpRewriteSelectTransformer::Rewind() {}

IGraphTransformer::TStatus TKqpNewRBOTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(
        output, output,
        [this](const TExprNode::TPtr &node, TExprContext &ctx) -> TExprNode::TPtr {
            Y_UNUSED(ctx);
            if (TKqpOpRoot::Match(node.Get())) {
                auto root = PlanConverter(TypeCtx, ctx).ConvertRoot(node);
                root.ComputeParents();
                return RBO.Optimize(root, ctx);
            } else {
                return node;
            }
        },
        ctx, settings);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    return IGraphTransformer::TStatus::Ok;
}

void TKqpNewRBOTransformer::Rewind() {}

IGraphTransformer::TStatus TKqpRBOCleanupTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr &output, TExprContext &ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    Y_UNUSED(ctx);

    YQL_CLOG(TRACE, CoreDq) << "Cleanup input plan: " << KqpExprToPrettyString(TExprBase(output), ctx) << Endl;


    if (output->IsList() && output->ChildrenSize() >= 1) {
        auto child_level_1 = output->Child(0);
        YQL_CLOG(TRACE, CoreDq) << "Matched level 0";

        if (child_level_1->IsList() && child_level_1->ChildrenSize() >= 1) {
            auto child_level_2 = child_level_1->Child(0);
            YQL_CLOG(TRACE, CoreDq) << "Matched level 1";

            if (child_level_2->IsList() && child_level_2->ChildrenSize() >= 1) {
                auto child_level_3 = child_level_2->Child(0);
                YQL_CLOG(TRACE, CoreDq) << "Matched level 2";

                if (child_level_3->IsList() && child_level_2->ChildrenSize() >= 1) {
                    auto maybeQuery = child_level_3->Child(0);

                    if (TKqpPhysicalQuery::Match(maybeQuery)) {
                        YQL_CLOG(TRACE, CoreDq) << "Found query node";
                        output = maybeQuery;
                    }
                }
            }
        }
    }

    return IGraphTransformer::TStatus::Ok;
}

TKqpNewRBOTransformer::TKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                             TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer, TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer,
                                             const NMiniKQL::IFunctionRegistry& funcRegistry)
    : TypeCtx(typeCtx)
    , KqpCtx(*kqpCtx)
    , RBO(kqpCtx, typeCtx, std::move(rboTypeAnnTransformer), std::move(peepholeTypeAnnTransformer), funcRegistry) {
    // Predicate pull-up stage
    TVector<std::shared_ptr<IRule>> filterPullUpRules{std::make_shared<TPullUpCorrelatedFilterRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Correlated predicte pullup", std::move(filterPullUpRules)));
    // Initial stages.
    TVector<std::shared_ptr<IRule>> inlineScalarSubPlanStageRules{std::make_shared<TInlineScalarSubplanRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Inline scalar subplans", std::move(inlineScalarSubPlanStageRules)));
    RBO.AddStage(std::make_shared<TRenameStage>());
    RBO.AddStage(std::make_shared<TConstantFoldingStage>());
    // Logical stage.
    TVector<std::shared_ptr<IRule>> logicalStageRules = {std::make_shared<TRemoveIdenityMapRule>(), std::make_shared<TExtractJoinExpressionsRule>(),
                                                         std::make_shared<TPushMapRule>(), std::make_shared<TPushFilterIntoJoinRule>(),
                                                         std::make_shared<TPushFilterUnderMapRule>(),
                                                         std::make_shared<TPushLimitIntoSortRule>(), 
                                                         std::make_shared<TInlineSimpleInExistsSubplanRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Logical rewrites I", std::move(logicalStageRules)));
    RBO.AddStage(std::make_shared<TPruneColumnsStage>());
    // Physical stage.
    TVector<std::shared_ptr<IRule>> physicalStageRules = {std::make_shared<TPeepholePredicate>(), std::make_shared<TPushOlapFilterRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Physical rewrites I", std::move(physicalStageRules)));
    // CBO stages.
    TVector<std::shared_ptr<IRule>> initialCBOStageRules = {std::make_shared<TBuildInitialCBOTreeRule>(), std::make_shared<TExpandCBOTreeRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Prepare for CBO", std::move(initialCBOStageRules)));
    TVector<std::shared_ptr<IRule>> cboStageRules = {std::make_shared<TOptimizeCBOTreeRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Invoke CBO", std::move(cboStageRules)));
    TVector<std::shared_ptr<IRule>> cleanUpCBOStageRules = {std::make_shared<TInlineCBOTreeRule>(), std::make_shared<TPushFilterIntoJoinRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Clean up after CBO", std::move(cleanUpCBOStageRules)));
    // Assign physical stages.
    TVector<std::shared_ptr<IRule>> assignPhysicalStageRules = {std::make_shared<TAssignStagesRule>()};
    RBO.AddStage(std::make_shared<TRuleBasedStage>("Assign stages", std::move(assignPhysicalStageRules)));
}

void TKqpRBOCleanupTransformer::Rewind() {
}

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) {
    return new TKqpRewriteSelectTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx,
                                                       TAutoPtr<IGraphTransformer>&& rboTypeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer>&& peepholeTypeAnnTransformer,
                                                       const NMiniKQL::IFunctionRegistry& funcRegistry) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx, std::move(rboTypeAnnTransformer), std::move(peepholeTypeAnnTransformer), funcRegistry);
}

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx) {
    return new TKqpRBOCleanupTransformer(typeCtx);
}

} // namespace NKqp
} // namespace NKikimr