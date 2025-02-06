#include "kqp_new_rbo_transformer.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

TExprNode::TPtr RewritePgSelect(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& typeCtx) {

    auto setItems = GetSetting(node->Head(), "set_items");
    
    TVector<TExprNode::TPtr> resultElements;

    TExprNode::TPtr joinExpr;
    TExprNode::TPtr filterExpr;
    TExprNode::TPtr lastAlias;


    auto setItem = setItems->Tail().ChildPtr(0);

    auto from = GetSetting(setItem->Tail(), "from");

    if (from) {
        for (auto fromItem : from->Child(1)->Children()) {
            auto readExpr = TKqlReadTableRanges(fromItem->Child(0));
            auto alias = fromItem->Child(1);

            auto opRead = Build<TKqpOpRead>(ctx, node->Pos())
                .Table(readExpr.Table())
                .Alias(alias)
                .Columns(readExpr.Columns())
                .Done().Ptr();

            if (!joinExpr) {
                joinExpr = opRead;
            } 
            else {
                joinExpr = Build<TKqpOpJoin>(ctx, node->Pos())
                    .LeftInput(joinExpr)
                    .RightInput(opRead)
                    .LeftLabel(lastAlias)
                    .RightLabel(alias)
                    .JoinKind().Value("Inner").Build()
                    .Done().Ptr();
            }
            lastAlias = alias;
        }
    }

    filterExpr = joinExpr;

    auto where = GetSetting(setItem->Tail(), "where");

    if (where) {
        auto lambda = where->Child(1)->Child(1);
        filterExpr = Build<TKqpOpFilter>(ctx, node->Pos())
            .Input(filterExpr)
            .Lambda(lambda)
            .Done().Ptr();
    }

    if (!filterExpr) {
        filterExpr = Build<TKqpOpEmptySource>(ctx, node->Pos()).Done().Ptr();
    }

    auto result = GetSetting(setItem->Tail(), "result");

    TExprNode::TPtr resultExpr = filterExpr;

    for (auto resultItem : result->Child(1)->Children()) {
        auto variable = Build<TCoAtom>(ctx, node->Pos()).Value(resultItem->Child(0)->Content()).Done();

        resultElements.push_back(Build<TKqpOpMapElement>(ctx, node->Pos())
            .Input(resultExpr)
            .Variable(variable)
            .Lambda(ctx.DeepCopyLambda(*(resultItem->Child(2))))
            .Done().Ptr());
    }

    return Build<TKqpOpMap>(ctx, node->Pos())
            .Input(resultExpr)
            .MapElements()
                .Add(resultElements)
            .Build()
            .Done().Ptr();
}

}

IGraphTransformer::TStatus TKqpPgRewriteTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(output, output, [this] (const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (TCoPgSelect::Match(node.Get())) {
            return RewritePgSelect(node, ctx, TypeCtx);
        } else {
            return node;
        }}, ctx, settings);

    return status;
}

void TKqpPgRewriteTransformer::Rewind() {
}


IGraphTransformer::TStatus TKqpNewRBOTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    return IGraphTransformer::TStatus::Ok;
}

void TKqpNewRBOTransformer::Rewind() {
}

TAutoPtr<IGraphTransformer> NKikimr::NKqp::CreateKqpPgRewriteTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) {
    return new TKqpPgRewriteTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> NKikimr::NKqp::CreateKqpNewRBOTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx);
}