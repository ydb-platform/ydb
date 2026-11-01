#include "secrets.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NKikimr::NKqp {

void FillRemoteEmbedderSecureParams(const NYql::TExprNode::TPtr& node, THashMap<TString, TString>& secureParams) {
    NYql::VisitExpr(node, [&](const NYql::TExprNode::TPtr& expr) {
        if (!expr->IsCallable("Apply") || expr->ChildrenSize() < 4 || !expr->Head().IsCallable("Udf")) {
            return true;
        }
        const auto name = expr->Head().Head().Content();
        if (name != "RemoteEmbedder.ApplyRemoteEmbedder" && name != "RemoteEmbedder.ApplyRemoteEmbedderStream") {
            return true;
        }
        const auto& secret = *expr->Child(3);
        YQL_ENSURE(secret.IsCallable("String") && secret.ChildrenSize() == 1 && secret.Head().IsAtom() && !secret.Head().Content().empty(),
            "RemoteEmbedder: AuthSecretName must be a non-empty String literal");
        const TString secretName(secret.Head().Content());
        secureParams.emplace(secretName, NYql::TStructuredTokenBuilder().SetTokenAuthWithSecret(secretName, "").ToJson());
        return true;
    });
}

} // namespace NKikimr::NKqp
