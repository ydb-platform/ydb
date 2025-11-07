#include "kqp_rbo_rules.h"
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <typeinfo>

using namespace NYql::NNodes;

namespace {

TNodeOnNodeOwnedMap ExtractConstantExprs(const TExprNode::TPtr& input, TExprContext& ctx, bool foldUdfs = true) {
    TNodeOnNodeOwnedMap result;
    ExtractConstantExprs(lambda.Body().Ptr(), result, ctx.ExprCtx, foldUdfs);   
    return result;
}

}

namespace NKikimr {
namespace NKqp {

void TConstantFoldingStage::RunStage(TOpRoot &root, TRBOContext &ctx) {
    TVector<TExprNode::TPtr> lambdasWithConstExpr;
    bool foldUdfs = ctx.KqpCtx.Config->EnableFoldUdfs();

    // Iterate through all operators that contain lambdas with potential constant expression
    TNodeOnNodeOwnedMap replaces;
    TVector<std::shared_ptr<IOperator>> affectedOps;

    for (auto it : root) {
        if (!it.Current->GetLambdas().empty()) {
            auto lambdas = it.Current->GetLambdas();
            bool affected = false;
            for (auto l : lambdas) {
                auto lambda = TCoLambda(l);
                auto extractedMap = ExtractConstantExprs(lambda.Body().Ptr(), ctx.ExprCtx, foldUdfs);
                if (!extracted.empty()) {
                    affected = true;
                }
            }

            if (affected) {
                affectedOps.push_back(it.Current);
            }
        }
    }

    if (replaces.empty()) {
        return;
    }

    // Build a list of eval expressions

    TExprNode::TListType lambdaList;
    TExprNode::TListType evalElements;
    for (auto & [k, v] : replaces) {
        lambdaList.push_back(k);
        evalElements.push_back(v);
    }

    auto evalList = ctx.ExprCtx.NewList(root.Pos, std::move(evalList));

    // Run optimizer with eval on the evalList
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    settings.VisitTuples = false;
    ctx.ExprCtx.Step.Repeat(TExprStep::ExprEval);
    auto status = RemapExpr(evalList, evalList, replaces, ctx.ExprCtx, settings);

    // Iterate over affected operators and modify their expressions with folded expressions
    replaces.clear();
    for (size_t i=0; i<lambdaList.size(); i++) {
        replaces[lambdaList[i]] = evalList->Child(i);
    }

    for (auto op : affectedOps) {
        op->ApplyReplaceMap(replaces);
    }
}
}
}