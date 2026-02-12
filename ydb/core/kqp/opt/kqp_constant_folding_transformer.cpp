#include "kqp_constant_folding_transformer.h"

#include <ydb/library/yql/dq/opt/dq_opt_stat.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
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
     * Traverse a lambda and create a mapping from nodes to nodes wrapped in EvaluateExpr callable
     * We check for literals specifically, since they shouldn't be evaluated
     */
    void ExtractConstantExprs(const TExprNode::TPtr& input, TNodeOnNodeOwnedMap& replaces, TExprContext& ctx, bool foldUdfs = true) {
        if (!IsSuitableToExtractExpr(input)) {
            return;
        }

        if (TCoLambda::Match(input.Get())) {
            auto lambda = TExprBase(input).Cast<TCoLambda>();
            return ExtractConstantExprs(lambda.Body().Ptr(), replaces, ctx);
        }

        if (IsDataOrOptionalOfData(input->GetTypeAnn()) && !NeedCalc(TExprBase(input))) {
            return;
        }

        if (IsConstantExpr(input, foldUdfs) && !input->IsCallable("PgConst")) {
            TNodeOnNodeOwnedMap deepClones;
            auto inputClone = ctx.DeepCopy(*input, ctx, deepClones, false, true, true);

            auto replaceExpr = ctx.Builder(input->Pos())
                                   .Callable("EvaluateExpr")
                                   .Add(0, inputClone)
                                   .Seal()
                                   .Build();

            replaces[input.Get()] = replaceExpr;

            return;
        }

        if (TCoAsStruct::Match(input.Get())) {
            for (auto child : TExprBase(input).Cast<TCoAsStruct>()) {
                ExtractConstantExprs(child.Item(1).Ptr(), replaces, ctx);
            }
            return;
        }

        if (input->IsCallable() && input->Content() != "EvaluateExpr") {
            if (input->ChildrenSize() >= 1) {
                for (size_t i = 0; i < input->ChildrenSize(); i++) {
                    ExtractConstantExprs(input->Child(i), replaces, ctx);
                }
            }
        }

        return;
    }

}

/**
 * Constant folding transformer finds constant expressions in FlatMaps, evaluates them and
 * substitutes the result in the AST
 */
IGraphTransformer::TStatus TKqpConstantFoldingTransformer::DoTransform(TExprNode::TPtr input,
    TExprNode::TPtr& output, TExprContext& ctx) {
    output = input;

    bool foldUdfs = Config->GetEnableFoldUdfs();

    TNodeOnNodeOwnedMap replaces;

    VisitExpr(input, [&](const TExprNode::TPtr& node) {
        if (!replaces.empty()) {
            return false;
        }

        if (TCoFlatMap::Match(node.Get())) {
            auto flatmap = TExprBase(node).Cast<TCoFlatMap>();

            if (!IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
                return true;
            }

            ExtractConstantExprs(flatmap.Lambda().Body().Ptr(), replaces, ctx, foldUdfs);

            return replaces.empty();
        }

        return true;
    });

    if (replaces.empty()) {
        return IGraphTransformer::TStatus::Ok;
        ;
    } else {
        TOptimizeExprSettings settings(&TypeCtx);
        settings.VisitTuples = false;
        ctx.Step.Repeat(TExprStep::ExprEval);

        auto status = RemapExpr(input, output, replaces, ctx, settings);

        return status.Combine(IGraphTransformer::TStatus(IGraphTransformer::TStatus::Repeat, true));
    }

    return IGraphTransformer::TStatus::Ok;
}

void TKqpConstantFoldingTransformer::Rewind() {
}

TAutoPtr<IGraphTransformer> NKikimr::NKqp::CreateKqpConstantFoldingTransformer(const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx,
    TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config) {
    return THolder<IGraphTransformer>(new TKqpConstantFoldingTransformer(kqpCtx, typeCtx, config));
}
