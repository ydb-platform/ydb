#include "optimization.h"
#include "object.h"

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

struct TStreamingExploreCtx {
    TExprContext& Ctx;
    std::unordered_set<const TExprNode*> Visited;
    ui64 StreamingReads = 0;
    ui64 Writes = 0;
};

bool ExploreStreamingQueryNode(TExprNode::TPtr node, TStreamingExploreCtx& res) {
    if (!res.Visited.emplace(node.Get()).second) {
        return true;
    }

    // Check only read / write nodes
    if (node->ChildrenSize() < 2) {
        return true;
    }

    if (TMaybeNode<TCoConfigure>(node) || TMaybeNode<TCoCommit>(node)) {
        return true;
    }

    const auto providerArg = node->ChildPtr(1);
    if (const auto maybeDataSource = TMaybeNode<TCoDataSource>(providerArg)) {
        const auto dataSourceCategory = maybeDataSource.Cast().Category().Value();
        if (dataSourceCategory == NYql::PqProviderName) {
            ++res.StreamingReads;
            return true;
        }

        if (IsIn({NYql::S3ProviderName, NYql::GenericProviderName}, dataSourceCategory)) {
            return true;
        }

        if (dataSourceCategory == NYql::KikimrProviderName) {
            res.Ctx.AddError(NYql::TIssue(res.Ctx.GetPosition(node->Pos()), "Reading from YDB tables is not supported now for streaming queries"));
        } else {
            res.Ctx.AddError(NYql::TIssue(res.Ctx.GetPosition(node->Pos()), TStringBuilder() << "Reading from data source " << dataSourceCategory << " is not supported now for streaming queries"));
        }

        return false;
    }

    if (const auto maybeDataSink = TMaybeNode<TCoDataSink>(providerArg)) {
        ++res.Writes;

        const auto dataSinkCategory = maybeDataSink.Cast().Category().Value();
        if (IsIn({NYql::PqProviderName, NYql::SolomonProviderName}, dataSinkCategory)) {
            return true;
        }

        if (dataSinkCategory == NYql::KikimrProviderName) {
            const auto maybeYdbWrite = TMaybeNode<TKiWriteTable>(node);
            if (!maybeYdbWrite) {
                res.Ctx.AddError(NYql::TIssue(res.Ctx.GetPosition(node->Pos()), "Operations with YDB objects is not allowed inside streaming queries"));
                return false;
            }

            const auto ydbWrite = maybeYdbWrite.Cast();
            if (const TString mode(ydbWrite.Mode()); mode != "upsert") {
                res.Ctx.AddError(NYql::TIssue(res.Ctx.GetPosition(node->Pos()), TStringBuilder() << "Only UPSERT writing mode is supported for YDB writes inside streaming queries, got mode: " << to_upper(mode)));
                return false;
            }

            return true;
        }

        if (dataSinkCategory == NYql::ResultProviderName) {
            res.Ctx.AddError(NYql::TIssue(res.Ctx.GetPosition(node->Pos()), "Results is not allowed for streaming queries, please use INSERT to record the query result"));
        } else {
            res.Ctx.AddError(NYql::TIssue(res.Ctx.GetPosition(node->Pos()), TStringBuilder() << "Writing into data sink " << dataSinkCategory << " is not supported now for streaming queries"));
        }

        return false;
    }

    return true;
}

bool CheckStreamingQueryAst(TExprNode::TPtr ast, TExprContext& ctx) {
    bool hasErrors = false;
    TStreamingExploreCtx res = {.Ctx = ctx};
    VisitExpr(ast, [&hasErrors, &res](const TExprNode::TPtr& node) {
        if (hasErrors) {
            return false;
        }

        if (!ExploreStreamingQueryNode(node, res)) {
            hasErrors = true;
            return false;
        }

        return true;
    });

    if (hasErrors) {
        return false;
    }

    if (res.StreamingReads == 0) {
        ctx.AddError(NYql::TIssue(ctx.GetPosition(ast->Pos()), "Streaming query must have at least one streaming read from topic"));
        return false;
    }

    if (res.Writes > 1) {
        ctx.AddError(NYql::TIssue(ctx.GetPosition(ast->Pos()), "Streaming query with more than one write is not supported now"));
        return false;
    }

    return true;
}

}  // anonymous namespace

TExprNode::TPtr TStreamingQueryOptimizer::ExtractWorldFeatures(TCoNameValueTupleList& features, TExprContext& ctx) const {
    TExprNode::TPtr ast;
    TVector<TCoNameValueTuple> filteredFeatures;
    filteredFeatures.reserve(features.Size());
    for (const auto& feature : features) {
        if (feature.Name() == TStreamingQueryConfig::TSqlSettings::QUERY_AST_FEATURE) {
            if (const auto& astNode = feature.Value()) {
                ast = astNode.Cast().Ptr();
            }
        } else {
            filteredFeatures.emplace_back(feature);
        }
    }

    features = Build<TCoNameValueTupleList>(ctx, features.Pos())
        .Add(std::move(filteredFeatures))
        .Done();

    if (!ast) {
        ast = Build<TCoVoid>(ctx, features.Pos())
            .Done()
            .Ptr();
    }

    return ast;
}

TStatus TStreamingQueryOptimizer::ValidateObjectNodeAnnotation(TExprNode::TPtr node, TExprContext& ctx) const {
    TExprNode::TPtr ast;
    size_t astIdx = 0;

    if (auto createObject = TMaybeNode<TKiCreateObject>(node)) {
        ast = createObject.Cast().Ast().Ptr();
        astIdx = TKiCreateObject::idx_Ast;
    } else if (auto alterObject = TMaybeNode<TKiAlterObject>(node)) {
        ast = alterObject.Cast().Ast().Ptr();
        astIdx = TKiAlterObject::idx_Ast;
    } else {
        return TStatus::Ok;
    }

    if (TMaybeNode<TCoVoid>(ast)) {
        return TStatus::Ok;
    }

    if (!EnsureWorldType(*ast, ctx)) {
        return TStatus::Error;
    }

    TIssueScopeGuard issueScopeObject(ctx.IssueManager, [&]() {
        return MakeIntrusive<TIssue>(ctx.GetPosition(node->Pos()), "At STREAMING_QUERY object operation");
    });

    if (!CheckStreamingQueryAst(ast, ctx)) {
        return TStatus::Error;
    }

    node->ChildRef(astIdx) = Build<TCoVoid>(ctx, ast->Pos())
        .Done()
        .Ptr();

    return TStatus::Repeat;
}

}  // namespace NKikimr::NKqp
