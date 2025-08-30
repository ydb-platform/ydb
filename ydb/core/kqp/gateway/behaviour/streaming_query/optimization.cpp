#include "optimization.h"
#include "object.h"

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

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
        ast = ctx.NewWorld(features.Pos());
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

    EnsureWorldType(*ast, ctx);

    if (TMaybeNode<TCoWorld>(ast)) {
        return TStatus::Ok;
    }

    node->ChildRef(astIdx) = ctx.NewWorld(ast->Pos());

    return TStatus::Repeat;
}

}  // namespace NKikimr::NKqp
