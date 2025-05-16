#include "kqp_rbo_transformer.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

TExprNode::TPtr RewritePgSelect(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& typeCtx) {
    Y_UNUSED(typeCtx);
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
                auto joinKeys = Build<TDqJoinKeyTupleList>(ctx, node->Pos()).Done();

                joinExpr = Build<TKqpOpJoin>(ctx, node->Pos())
                    .LeftInput(joinExpr)
                    .RightInput(opRead)
                    .JoinKind().Value("Cross").Build()
                    .JoinKeys(joinKeys)
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
    auto finalType = node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    TExprNode::TPtr resultExpr = filterExpr;

    for (auto resultItem : result->Child(1)->Children()) {
        auto column = resultItem->Child(0);
        auto columnName = column->Content();
        auto variable = Build<TCoAtom>(ctx, node->Pos()).Value(columnName).Done();

        const auto expectedTypeNode = finalType->FindItemType(columnName);
        Y_ENSURE(expectedTypeNode);
        const auto expectedType = expectedTypeNode->Cast<TPgExprType>();
        const auto actualTypeNode = resultItem->GetTypeAnn();

        YQL_CLOG(TRACE, CoreDq) << "Actual type for column: " << columnName << " is: " << *actualTypeNode;

        ui32 actualPgTypeId;
        bool convertToPg;
        Y_ENSURE(ExtractPgType(actualTypeNode, actualPgTypeId, convertToPg, node->Pos(), ctx));

        auto needPgCast = (expectedType->GetId() != actualPgTypeId);
        auto lambda = TCoLambda(ctx.DeepCopyLambda(*(resultItem->Child(2))));

        if (convertToPg) {
            Y_ENSURE(!needPgCast, TStringBuilder()
                 << "Conversion to PG type is different at typization (" << expectedType->GetId()
                 << ") and optimization (" << actualPgTypeId << ") stages.");

            auto toPg = ctx.NewCallable(node->Pos(), "ToPg", {lambda.Body().Ptr()});

            lambda = Build<TCoLambda>(ctx, node->Pos())
                .Args(lambda.Args())
                .Body(toPg)
                .Done();
        }
        else if (needPgCast) {
            auto pgType = ctx.NewCallable(node->Pos(), "PgType", {ctx.NewAtom(node->Pos(), NPg::LookupType(expectedType->GetId()).Name)});
            auto pgCast = ctx.NewCallable(node->Pos(), "PgCast", {lambda.Body().Ptr(), pgType});

            lambda = Build<TCoLambda>(ctx, node->Pos())
                .Args(lambda.Args())
                .Body(pgCast)
                .Done();
        }

        resultElements.push_back(Build<TKqpOpMapElement>(ctx, node->Pos())
            .Input(resultExpr)
            .Variable(variable)
            .Lambda(lambda)
            .Done().Ptr());
    }

    return Build<TKqpOpRoot>(ctx, node->Pos())
            .Input<TKqpOpMap>()
                .Input(resultExpr)
                .MapElements()
                    .Add(resultElements)
                .Build()
            .Build()
            .Done().Ptr();
}

TExprNode::TPtr PushTakeIntoPlan(const TExprNode::TPtr& node, TExprContext& ctx, const TTypeAnnotationContext& typeCtx) {
    Y_UNUSED(typeCtx);
    auto take = TCoTake(node);
    if (auto root = take.Input().Maybe<TKqpOpRoot>()){
        return Build<TKqpOpRoot>(ctx, node->Pos())
            .Input<TKqpOpLimit>()
                .Input(root.Cast().Input())
                .Count(take.Count())
            .Build()
            .Done().Ptr();
    }
    else {
        return node;
    }
}
}

namespace NKikimr {
namespace NKqp {

IGraphTransformer::TStatus TKqpPgRewriteTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(output, output, [this] (const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (TCoPgSelect::Match(node.Get())) {
            return RewritePgSelect(node, ctx, TypeCtx);
        } if (TCoTake::Match(node.Get())) {
            return PushTakeIntoPlan(node, ctx, TypeCtx);
        }
        else {
            return node;
        }}, ctx, settings);

    return status;
}

void TKqpPgRewriteTransformer::Rewind() {
}


IGraphTransformer::TStatus TKqpNewRBOTransformer::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;
    TOptimizeExprSettings settings(&TypeCtx);

    auto status = OptimizeExpr(output, output, [this] (const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        if (TKqpOpRoot::Match(node.Get())) {
            auto root = TOpRoot(node);
            return RBO.Optimize(root, ctx);
        } else {
            return node;
        }}, ctx, settings);

    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    return IGraphTransformer::TStatus::Ok;
}

void TKqpNewRBOTransformer::Rewind() {
}

TAutoPtr<IGraphTransformer> CreateKqpPgRewriteTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx) {
    return new TKqpPgRewriteTransformer(kqpCtx, typeCtx);
}

TAutoPtr<IGraphTransformer> CreateKqpNewRBOTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config) {
    return new TKqpNewRBOTransformer(kqpCtx, typeCtx, config);
}

}
}