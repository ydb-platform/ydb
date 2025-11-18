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
    if (auto root = take.Input().Maybe<TKqpOpRoot>()) {
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

TExprNode::TPtr RewriteSublink(const TExprNode::TPtr &node, TExprContext &ctx) {
    if (node->Child(0)->Content() != "expr") {
        return node;
    }

    // clang-format off
    return Build<TKqpExprSublink>(ctx, node->Pos())
        .Expr(node->Child(4))
        .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr RemoveRootFromSublink(const TExprNode::TPtr &node, TExprContext &ctx) {
    auto sublink = TKqpExprSublink(node);
    if (auto root = sublink.Expr().Maybe<TKqpOpRoot>()) {
        // clang-format off
        return Build<TKqpExprSublink>(ctx, node->Pos())
            .Expr(root.Cast().Input())
            .Done().Ptr();
        // clang-format on
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
                return RewriteSublink(node, ctx);
            } else if (node->IsCallable("YqlSubLink")) {
                return RewriteSublink(node, ctx);
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
                return RewriteSelect(node, ctx, TypeCtx, true);
            }
            
            // YQL AST rewriting
            else if (TCoYqlSelect::Match(node.Get())) {
                return RewriteSelect(node, ctx, TypeCtx, false);
            } else if (TKqpExprSublink::Match(node.Get())) {
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

void TKqpRBOCleanupTransformer::Rewind() {}

TAutoPtr<IGraphTransformer> CreateKqpRewriteSelectTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx,
                                                          TTypeAnnotationContext &typeCtx) {
    return new TKqpRewriteSelectTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(const TIntrusivePtr<TKqpOptimizeContext> &kqpCtx, TTypeAnnotationContext &typeCtx,
                                                       TAutoPtr<IGraphTransformer> rboTypeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer> typeAnnTransformer,
                                                       TAutoPtr<IGraphTransformer> peephole,
                                                       const NMiniKQL::IFunctionRegistry& funcRegistry) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx, rboTypeAnnTransformer, typeAnnTransformer, peephole, funcRegistry);
}

TAutoPtr<IGraphTransformer> CreateKqpRBOCleanupTransformer(TTypeAnnotationContext &typeCtx) {
    return new TKqpRBOCleanupTransformer(typeCtx);
}

} // namespace NKqp
} // namespace NKikimr