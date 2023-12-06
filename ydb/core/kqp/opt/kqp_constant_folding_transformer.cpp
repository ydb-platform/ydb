#include "kqp_constant_folding_transformer.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;
using namespace NYql::NDq;

namespace {

    /***
     * We maintain a white list of callables that we consider part of constant expressions
     * All other callables will not be evaluated
     */
    THashSet<TString> constantFoldingWhiteList = {
        "Concat", "Just", "Optional","SafeCast",
        "+", "-", "*", "/", "%"};

    bool NeedCalc(NNodes::TExprBase node) {
        auto type = node.Ref().GetTypeAnn();
        if (type->IsSingleton()) {
            return false;
        }

        if (type->GetKind() == ETypeAnnotationKind::Optional) {
            if (node.Maybe<TCoNothing>()) {
                return false;
            }
            if (auto maybeJust = node.Maybe<TCoJust>()) {
                return NeedCalc(maybeJust.Cast().Input());
            }
            return true;
        }

        if (type->GetKind() == ETypeAnnotationKind::Tuple) {
            if (auto maybeTuple = node.Maybe<TExprList>()) {
                return AnyOf(maybeTuple.Cast(), [](const auto& item) { return NeedCalc(item); });
            }
            return true;
        }

        if (type->GetKind() == ETypeAnnotationKind::List) {
            if (node.Maybe<TCoList>()) {
                YQL_ENSURE(node.Ref().ChildrenSize() == 1, "Should be rewritten to AsList");
                return false;
            }
            if (auto maybeAsList = node.Maybe<TCoAsList>()) {
                return AnyOf(maybeAsList.Cast().Args(), [](const auto& item) { return NeedCalc(NNodes::TExprBase(item)); });
            }
            return true;
        }

        YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Data,
                   "Object of type " << *type << " should not be considered for calculation");

        return !node.Maybe<TCoDataCtor>();
    }

    /***
     * Check if the expression is a constant expression
     * Its type annotation need to specify that its a data type, and then we check:
     *   - If its a literal, its a constant expression
     *   - If its a callable in the while list and all children are constant expressions, then its a constant expression
     *   - If one of the child is a type expression, it also passes the check
     */
    bool IsConstantExpr(const TExprNode::TPtr& input) {
        if (!IsDataOrOptionalOfData(input->GetTypeAnn())) {
            return false;
        }

        if (!NeedCalc(TExprBase(input))) {
            return true;
        }

        else if (input->IsCallable(constantFoldingWhiteList)) {
            for (size_t i = 0; i < input->ChildrenSize(); i++) {
                auto callableInput = input->Child(i);
                if (callableInput->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type && !IsConstantExpr(callableInput)) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    /**
     * Traverse a lambda and create a mapping from nodes to nodes wrapped in EvaluateExpr callable
     * We check for literals specifically, since they shouldn't be evaluated
     */
    void ExtractConstantExprs(const TExprNode::TPtr& input, TNodeOnNodeOwnedMap& replaces, TExprContext& ctx) {
        if (TCoLambda::Match(input.Get())) {
            auto lambda = TExprBase(input).Cast<TCoLambda>();
            return ExtractConstantExprs(lambda.Body().Ptr(), replaces, ctx);
        }

        if (IsDataOrOptionalOfData(input->GetTypeAnn()) && !NeedCalc(TExprBase(input))) {
            return;
        }

        if (IsConstantExpr(input)) {
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

    if (!Config->HasOptEnableConstantFolding()) {
        return IGraphTransformer::TStatus::Ok;
    }

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

            ExtractConstantExprs(flatmap.Lambda().Body().Ptr(), replaces, ctx);

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
