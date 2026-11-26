#include "secrets.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/public/issue/protos/issue_id.pb.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <util/string/ascii.h>

namespace NKikimr::NKqp {
namespace {

const NYql::TExprNode* GetSecretArgument(const NYql::TExprNode& call) {
    if (call.IsCallable("Apply")) {
        return call.ChildrenSize() > 4 ? call.Child(4) : nullptr;
    }
    if (call.ChildrenSize() >= 3) {
        const auto& positional = *call.Child(1);
        if (!positional.IsList() && !positional.IsCallable({"AsTuple", "Tuple"})) {
            return nullptr;
        }
        if (positional.ChildrenSize() > 3) {
            return positional.Child(3);
        }
        const auto& named = *call.Child(2);
        if (named.IsCallable("AsStruct")) {
            for (const auto& field : named.Children()) {
                if (field->IsList() && field->ChildrenSize() == 2 && field->Head().IsAtom()
                    && AsciiEqualsIgnoreCase(field->Head().Content(), "AuthSecretName")) {
                    return field->Child(1);
                }
            }
        }
    }
    return nullptr;
}

template <class TCallback>
void VisitSecretArguments(const NYql::TExprNode::TPtr& node, TCallback&& callback) {
    NYql::VisitExpr(node, [&](const NYql::TExprNode::TPtr& expr) {
        if (expr->IsCallable({"Apply", "NamedApply"}) && expr->ChildrenSize()
            && expr->Head().IsCallable("Udf") && expr->Head().ChildrenSize()
            && expr->Head().Head().Content() == "Knn.ApplyRemoteEmbedder") {
            callback(*expr, GetSecretArgument(*expr));
        }
        return true;
    });
}

bool IsAbsoluteSecretPathLiteral(const NYql::TExprNode* secret) {
    return secret && secret->IsCallable("String") && secret->ChildrenSize() == 1
        && secret->Head().IsAtom() && secret->Head().Content().StartsWith('/') && secret->Head().Content().size() > 1;
}

} // namespace

bool ValidateRemoteEmbedderSecretNames(const NYql::TExprNode::TPtr& node, NYql::TExprContext& ctx) {
    bool valid = true;
    VisitSecretArguments(node, [&](const auto& call, const auto* secret) {
        if (!IsAbsoluteSecretPathLiteral(secret)) {
            NYql::TIssue issue(ctx.GetPosition(secret ? secret->Pos() : call.Pos()),
                "RemoteEmbedder: AuthSecretName must be a non-empty String literal with an absolute schema secret path");
            issue.SetCode(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, NYql::TSeverityIds::S_ERROR);
            ctx.AddError(issue);
            valid = false;
        }
    });
    return valid;
}

void FillRemoteEmbedderSecureParams(const NYql::TExprNode::TPtr& node, THashMap<TString, TString>& secureParams) {
    VisitSecretArguments(node, [&](const auto&, const auto* secret) {
        if (IsAbsoluteSecretPathLiteral(secret)) {
            const TString secretName(secret->Head().Content());
            secureParams.emplace(secretName, NYql::TStructuredTokenBuilder().SetTokenAuthWithSecret(secretName, "").ToJson());
        }
    });
}

} // namespace NKikimr::NKqp
