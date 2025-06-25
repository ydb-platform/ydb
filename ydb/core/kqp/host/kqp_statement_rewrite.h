#pragma once


#include <yql/essentials/ast/yql_expr.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

namespace NKikimr {
namespace NKqp {

bool NeedToSplit(
    const NYql::TExprNode::TPtr& root,
    NYql::TExprContext& exprCtx);

bool CheckRewrite(
    const NYql::TExprNode::TPtr& root,
    NYql::TExprContext& exprCtx);

struct TPrepareRewriteInfo {
    NYql::TExprNode::TPtr InputExpr;
    TAutoPtr<NYql::IGraphTransformer> Transformer; 
};

TPrepareRewriteInfo PrepareRewrite(
    const NYql::TExprNode::TPtr& root,
    NYql::TExprContext& exprCtx,
    NYql::TTypeAnnotationContext& typeCtx,
    const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
    const NMiniKQL::IFunctionRegistry& funcRegistry,
    const TString& cluster);

TVector<NYql::TExprNode::TPtr> RewriteExpression(
    const NYql::TExprNode::TPtr& root,
    NYql::TExprContext& ctx,
    const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
    NYql::TExprNode::TPtr insertDataPtr);

}
}
