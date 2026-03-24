#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <typeinfo>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NYql::NDq;

namespace {

    THashSet<TString> notAllowedDataTypeForSafeCast{"JsonDocument", "DyNumber"};

    bool IsSuitableToExtractExpr(const TExprNode::TPtr &input) {
        if (auto maybeSafeCast = TExprBase(input).Maybe<TCoSafeCast>()) {
            auto maybeDataType = maybeSafeCast.Cast().Type().Maybe<TCoDataType>();
            if (!maybeDataType) {
                if (const auto maybeOptionalType = maybeSafeCast.Cast().Type().Maybe<TCoOptionalType>()) {
                    maybeDataType = maybeOptionalType.Cast().ItemType().Maybe<TCoDataType>();
                }
            }
            return (maybeDataType && !notAllowedDataTypeForSafeCast.contains(maybeDataType.Cast().Type().Value()));
        }
        return true;
    }

    /**
     * Traverse a lambda and extract a list of constant expressions
     */
    void ExtractConstantExprs(const TExprNode::TPtr& input, TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>>& exprs, TExprContext& ctx, bool foldUdfs = true) {
        if (!IsSuitableToExtractExpr(input)) {
            return;
        }

        if (TCoLambda::Match(input.Get())) {
            auto lambda = TExprBase(input).Cast<TCoLambda>();
            return ExtractConstantExprs(lambda.Body().Ptr(), exprs, ctx);
        }

        if (IsDataOrOptionalOfData(input->GetTypeAnn()) && !NeedCalc(TExprBase(input))) {
            return;
        }

        if (IsConstantExpr(input, foldUdfs) && !input->IsCallable("PgConst")) {
            TNodeOnNodeOwnedMap deepClones;
            auto inputClone = ctx.DeepCopy(*input, ctx, deepClones, false, true, true);
            exprs.push_back(std::make_pair(input, inputClone));
            return;
        }

        if (TCoAsStruct::Match(input.Get())) {
            for (auto child : TExprBase(input).Cast<TCoAsStruct>()) {
                ExtractConstantExprs(child.Item(1).Ptr(), exprs, ctx);
            }
            return;
        }

        if (input->IsCallable() && input->Content() != "EvaluateExpr") {
            if (input->ChildrenSize() >= 1) {
                for (size_t i = 0; i < input->ChildrenSize(); i++) {
                    ExtractConstantExprs(input->Child(i), exprs, ctx);
                }
            }
        }

        return;
    }

}

namespace NKikimr {
namespace NKqp {

TConstantFoldingStage::TConstantFoldingStage() : IRBOStage("Constant folding stage") {
    Props = ERuleProperties::RequireParents |  ERuleProperties::RequireTypes;
}

void TConstantFoldingStage::RunStage(TOpRoot &root, TRBOContext &ctx) {
    TVector<TExprNode::TPtr> lambdasWithConstExpr;
    bool foldUdfs = ctx.KqpCtx.Config->GetEnableFoldUdfs();

    // Iterate through all operators that contain lambdas with potential constant expression

    // Internal map for remap operation
    TNodeOnNodeOwnedMap replaces;

    // Actual map used in the optimizer
    TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>> globalExtractedExprs;
    TVector<TIntrusivePtr<IOperator>> affectedOps;

    for (auto it : root) {
        if (!it.Current->GetExpressions().empty()) {
            auto expressions = it.Current->GetExpressions();
            bool affected = false;
            for (auto e : expressions) {
                auto lambda = TCoLambda(e.get().Node);
                TVector<std::pair<TExprNode::TPtr, TExprNode::TPtr>> extractedExprs;
                ExtractConstantExprs(lambda.Body().Ptr(), extractedExprs, ctx.ExprCtx, foldUdfs);
                if (!extractedExprs.empty()) {
                    affected = true;
                    globalExtractedExprs.insert(globalExtractedExprs.end(), extractedExprs.begin(), extractedExprs.end());
                }
            }

            if (affected) {
                affectedOps.push_back(it.Current);
            }
        }
    }

    if (globalExtractedExprs.empty()) {
        return;
    }

    // Build a list of eval expressions

    TExprNode::TListType lambdaList;
    TExprNode::TListType evalElements;
    for (auto & [k, v] : globalExtractedExprs) {
        lambdaList.push_back(k);
        evalElements.push_back(v);
    }

    // Evaluate all the constant expressions at once
    auto evalList = ctx.ExprCtx.NewList(root.Pos, std::move(evalElements));
    evalList = ctx.ExprCtx.NewCallable(root.Pos, "EvaluateExpr", { evalList });

    auto evaluator = TTransformationPipeline(&ctx.TypeCtx)
        .AddServiceTransformers()
        .AddPreTypeAnnotation()
        .AddExpressionEvaluation(ctx.FuncRegistry).Build(false);

    ctx.ExprCtx.Step.Repeat(TExprStep::ExprEval);
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = evaluator->Transform(evalList, evalList, ctx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    // Iterate over affected operators and modify their expressions with folded expressions
    for (size_t i=0; i<lambdaList.size(); i++) {
        replaces[lambdaList[i].Get()] = evalList->Child(i);
    }

    for (auto op : affectedOps) {
        op->ApplyReplaceMap(replaces, ctx);
    }
}
}
}