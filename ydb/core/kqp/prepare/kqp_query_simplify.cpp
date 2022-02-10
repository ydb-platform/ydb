#include "kqp_prepare_impl.h"

#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

template<typename TMapNode>
TExprBase BuildMap(TExprBase input, TExprBase body, TCoArgument arg, TExprContext& ctx) {
    return Build<TMapNode>(ctx, input.Pos())
        .Input(input)
        .Lambda()
            .Args({"item"})
            .template Body<TExprApplier>()
                .Apply(body)
                .With(arg, "item")
                .Build()
            .Build()
        .Done();
}

TExprNode::TPtr ExtractFilter(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoFlatMap>()) {
        return node.Ptr();
    }

    auto flatmap = node.Cast<TCoFlatMap>();
    auto flatmapArg = flatmap.Lambda().Args().Arg(0);

    if (flatmap.Input().Ref().GetTypeAnn()->GetKind() != ETypeAnnotationKind::List) {
        return node.Ptr();
    }

    if (auto maybeConditional = flatmap.Lambda().Body().Maybe<TCoConditionalValueBase>()) {
        auto conditional = maybeConditional.Cast();

        auto blacklistedNode = FindNode(conditional.Predicate().Ptr(), [](const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            if (node.Maybe<TKiSelectRow>() || node.Maybe<TKiSelectRangeBase>()) { 
                return true;
            }

            return false;
        });

        if (blacklistedNode) {
            return node.Ptr();
        }

        auto filter = Build<TCoFilter>(ctx, node.Pos())
            .Input(flatmap.Input())
            .Lambda()
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(conditional.Predicate())
                    .With(flatmapArg, "item")
                    .Build()
                .Build()
            .Done();

        auto value = conditional.Value();

        if (conditional.Maybe<TCoListIf>() || conditional.Maybe<TCoOptionalIf>()) {
            return BuildMap<TCoMap>(filter, value, flatmapArg, ctx).Ptr();
        } else {
            return BuildMap<TCoFlatMap>(filter, value, flatmapArg, ctx).Ptr();
        }
    }

    if (auto maybeConditional = flatmap.Lambda().Body().Maybe<TCoIf>()) {
        auto conditional = maybeConditional.Cast();

        auto elseValue = conditional.ElseValue();
        if (!elseValue.Maybe<TCoList>() && !elseValue.Maybe<TCoNothing>()) {
            return node.Ptr();
        }

        auto blacklistedNode = FindNode(conditional.Predicate().Ptr(), [](const TExprNode::TPtr& exprNode) {
            auto node = TExprBase(exprNode);

            if (node.Maybe<TKiSelectRow>() || node.Maybe<TKiSelectRangeBase>()) { 
                return true;
            }

            return false;
        });

        if (blacklistedNode) {
            return node.Ptr();
        }

        auto filter = Build<TCoFilter>(ctx, node.Pos())
            .Input(flatmap.Input())
            .Lambda()
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(conditional.Predicate())
                    .With(flatmapArg, "item")
                    .Build()
                .Build()
            .Done();

        auto value = conditional.ThenValue();

        return BuildMap<TCoFlatMap>(filter, value, flatmapArg, ctx).Ptr();
    }

    return node.Ptr();
}

TExprNode::TPtr ExtractCombineByKeyPreMap(TExprBase node, TExprContext& ctx) {
    if (auto maybeCombine = node.Maybe<TCoCombineByKey>()) {
        auto combine = maybeCombine.Cast();
        if (!IsKqlPureLambda(combine.PreMapLambda())) {
            return Build<TCoCombineByKey>(ctx, node.Pos())
                .Input<TCoMap>()
                    .Input(combine.Input())
                    .Lambda(combine.PreMapLambda())
                    .Build()
                .PreMapLambda()
                    .Args({"item"})
                    .Body("item")
                    .Build()
                .KeySelectorLambda(combine.KeySelectorLambda())
                .InitHandlerLambda(combine.InitHandlerLambda())
                .UpdateHandlerLambda(combine.UpdateHandlerLambda())
                .FinishHandlerLambda(combine.FinishHandlerLambda())
                .Done()
                .Ptr();
        }
    }

    return node.Ptr();
}

template <class TPartitionsType>
TExprNode::TPtr ExtractPartitionByKeyListHandler(TExprBase node, TExprContext& ctx) {
    if (auto maybePartition = node.Maybe<TPartitionsType>()) {
        auto partition = maybePartition.Cast();
        if (!IsKqlPureLambda(partition.ListHandlerLambda())) {
            auto newPartition = Build<TPartitionsType>(ctx, node.Pos())
                .Input(partition.Input())
                .KeySelectorLambda(partition.KeySelectorLambda())
                .SortDirections(partition.SortDirections())
                .SortKeySelectorLambda(partition.SortKeySelectorLambda())
                .ListHandlerLambda()
                    .Args({"list"})
                    .Body("list")
                    .Build()
                .Done();

            return Build<TExprApplier>(ctx, node.Pos())
                .Apply(partition.ListHandlerLambda().Body())
                .With(partition.ListHandlerLambda().Args().Arg(0), newPartition)
                .Done()
                .Ptr();
        }
    }

    return node.Ptr();
}

template<typename TMapType>
TExprNode::TPtr MergeMapsWithSameLambda(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtend>()) {
        return node.Ptr();
    }
 
    bool hasInputsToMerge = false;
    TVector<std::pair<TMaybeNode<TCoLambda>, TVector<TExprBase>>> inputs;
 
    auto extend = node.Cast<TCoExtend>();
    for (const auto& list : extend) {
        TMaybeNode<TExprBase> input;
        TMaybeNode<TCoLambda> lambda;

        bool buildList = false;
        if (auto maybeMap = list.Maybe<TMapType>()) {
            lambda = maybeMap.Cast().Lambda();
            input = maybeMap.Cast().Input();

            YQL_ENSURE(input.Ref().GetTypeAnn());
            buildList = input.Cast().Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Optional;
        } else if (auto maybeMap = list.Maybe<TCoToList>().Optional().Maybe<TMapType>()) {
            lambda = maybeMap.Cast().Lambda();
            input = maybeMap.Cast().Input();
            buildList = true;
        } 
 
        if (buildList) {
            input = Build<TCoToList>(ctx, node.Pos())
                .Optional(input.Cast())
                .Done(); 
        } 
 
        if (lambda && !IsKqlPureLambda(lambda.Cast())) {
            if (!inputs.empty() && inputs.back().first && inputs.back().first.Cast().Raw() == lambda.Cast().Raw()) {
                inputs.back().second.push_back(input.Cast());
                hasInputsToMerge = true;
            } else {
                inputs.emplace_back(lambda, TVector<TExprBase>{input.Cast()});
            }
        } else { 
            inputs.emplace_back(TMaybeNode<TCoLambda>(), TVector<TExprBase>{list});
        } 
    } 

    if (!hasInputsToMerge) {
        return node.Ptr();
    }

    TVector<TExprBase> merged;
    for (const auto& pair : inputs) {
        const auto& lambda = pair.first;
        const auto& lists = pair.second;

        if (lambda) {
            YQL_ENSURE(!lists.empty());
            auto mergedInput = lists.front();

            if (lists.size() > 1) {
                mergedInput = Build<TCoExtend>(ctx, node.Pos())
                    .Add(lists)
                    .Done();
            }

            auto mergedMap = Build<TMapType>(ctx, node.Pos())
                .Input(mergedInput)
                .Lambda()
                    .Args({"item"})
                    .template Body<TExprApplier>()
                        .Apply(lambda.Cast())
                        .With(0, "item")
                        .Build()
                    .Build()
                .Done();

            merged.push_back(mergedMap);
        } else {
            YQL_ENSURE(lists.size() == 1);
            merged.push_back(lists.front());
        }
    }

    YQL_ENSURE(!merged.empty());
    auto ret = merged.front();
    if (merged.size() > 1) {
        ret = Build<TCoExtend>(ctx, node.Pos())
            .Add(merged)
            .Done();
    }

    return ret.Ptr();
} 
 
TExprNode::TPtr RewritePresentIfToFlatMap(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoIfPresent>()) {
        return node.Ptr();
    }
    auto ifPresent = node.Cast<TCoIfPresent>();

    if (IsKqlPureLambda(ifPresent.PresentHandler())) {
        return node.Ptr();
    }

    if (!ifPresent.MissingValue().Maybe<TCoNothing>()) {
        return node.Ptr();
    }

    return Build<TCoFlatMap>(ctx, node.Pos())
            .Input(ifPresent.Optional())
            .Lambda()
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(ifPresent.PresentHandler())
                    .With(ifPresent.PresentHandler().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done()
            .Ptr();
}

class TKqpSimplifyTransformer : public TSyncTransformerBase {
public:
    TKqpSimplifyTransformer()
        : Simplified(false) {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        if (Simplified) {
            return TStatus::Ok;
        }

        TOptimizeExprSettings optSettings(nullptr);
        optSettings.VisitChanges = false;
        TStatus status = OptimizeExpr(input, output,
            [](const TExprNode::TPtr& input, TExprContext& ctx) {
                auto ret = input;
                TExprBase node(input);

                ret = MergeMapsWithSameLambda<TCoMap>(node, ctx);
                if (ret != input) { 
                    return ret; 
                } 
 
                ret = MergeMapsWithSameLambda<TCoFlatMap>(node, ctx);
                if (ret != input) {
                    return ret;
                }

                ret = ExtractFilter(node, ctx);
                if (ret != input) {
                    return ret;
                }

                ret = ExtractCombineByKeyPreMap(node, ctx);
                if (ret != input) {
                    return ret;
                }

                ret = ExtractPartitionByKeyListHandler<TCoPartitionByKey>(node, ctx);
                if (ret != input) {
                    return ret;
                }

                ret = ExtractPartitionByKeyListHandler<TCoPartitionsByKeys>(node, ctx);
                if (ret != input) {
                    return ret;
                }

                ret = RewritePresentIfToFlatMap(node, ctx);
                if (ret != input) {
                    return ret;
                }

                return ret;
            }, ctx, optSettings);

        if (input != output) {
            return TStatus(TStatus::Repeat, true);
        }

        if (status == TStatus::Ok) {
            Simplified = true;
        }

        return status;
    }

    void Rewind() override {
        Simplified = false;
    }

private:
    bool Simplified;
};

} // namespace

TAutoPtr<IGraphTransformer> CreateKqpSimplifyTransformer() {
    return new TKqpSimplifyTransformer();
}

} // namespace NKqp
} // namespace NKikimr
