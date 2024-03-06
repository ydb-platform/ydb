#pragma once


#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

namespace NKikimr {
namespace NKqp {

TVector<NYql::TExprNode::TPtr> RewriteExpression(
    const NYql::TExprNode::TPtr& root,
    NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typeCtx,
    const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
    const TString& cluster);

}
}
