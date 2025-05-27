#include "yql_opt_match_recognize.h"

#include "yql_opt_utils.h"

#include <yql/essentials/core/sql_types/time_order_recover.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <ranges>

namespace NYql {

using namespace NNodes;

namespace {

bool IsStreaming(const TExprNode::TPtr& input, const TTypeAnnotationContext& typeAnnCtx) {
    switch (typeAnnCtx.MatchRecognizeStreaming) {
        case EMatchRecognizeStreamingMode::Disable:
            return false;
        case EMatchRecognizeStreamingMode::Force:
            return true;
        case EMatchRecognizeStreamingMode::Auto: {
            bool hasPq = false;
            NYql::VisitExpr(input, [&hasPq](const TExprNode::TPtr& node) {
                if (auto maybeDataSource = TExprBase(node).Maybe<TCoDataSource>()) {
                    hasPq = maybeDataSource.Cast().Category().Value() == "pq";
                }
                return !hasPq;
            });
            return hasPq;
        }
    }
}

TExprNode::TPtr ExpandMatchRecognizeMeasuresCallables(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& /* typeAnnCtx */) {
    YQL_CLOG(DEBUG, Core) << "Expand " << node->Content();
    static constexpr size_t MeasuresLambdasStartPos = 3;
    return ctx.Builder(node->Pos())
        .Callable("MatchRecognizeMeasures")
            .Add(0, node->ChildPtr(0))
            .Add(1, node->ChildPtr(1))
            .Add(2, node->ChildPtr(2))
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                const auto aggregatesItems = node->Child(3);
                for (size_t i = 0; i < aggregatesItems->ChildrenSize(); ++i) {
                    const auto item = aggregatesItems->Child(i);
                    auto lambda = item->ChildPtr(0);
                    const auto vars = item->Child(1);
                    const auto aggregates = item->Child(2);
                    parent.Lambda(MeasuresLambdasStartPos + i, lambda->Pos())
                        .Param("data")
                        .Param("vars")
                        .Apply(std::move(lambda))
                            .With(0)
                                .Callable("FlattenMembers")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        for (size_t i = 0; i < aggregates->ChildrenSize(); ++i) {
                                            const auto var = vars->Child(i)->Content();
                                            auto aggregate = aggregates->Child(i);
                                            parent
                                                .List(i)
                                                    .Atom(0, "")
                                                    .Callable(1, "Head")
                                                        .Callable(0, "Aggregate")
                                                            .Callable(0, "OrderedMap")
                                                                .Callable(0, "OrderedFlatMap")
                                                                    .Callable(0, "Member")
                                                                        .Arg(0, "vars")
                                                                        .Atom(1, var)
                                                                    .Seal()
                                                                    .Lambda(1)
                                                                        .Param("item")
                                                                        .Callable(0, "ListFromRange")
                                                                            .Callable(0, "Member")
                                                                                .Arg(0, "item")
                                                                                .Atom(1, "From")
                                                                            .Seal()
                                                                            .Callable(1, "+MayWarn")
                                                                                .Callable(0, "Member")
                                                                                    .Arg(0, "item")
                                                                                    .Atom(1, "To")
                                                                                .Seal()
                                                                                .Callable(1, "Uint64")
                                                                                    .Atom(0, "1")
                                                                                .Seal()
                                                                            .Seal()
                                                                        .Seal()
                                                                    .Seal()
                                                                .Seal()
                                                                .Lambda(1)
                                                                    .Param("index")
                                                                    .Callable(0, "Unwrap")
                                                                        .Callable(0, "Lookup")
                                                                            .Callable(0, "ToIndexDict")
                                                                                .Arg(0, "data")
                                                                            .Seal()
                                                                            .Arg(1, "index")
                                                                        .Seal()
                                                                    .Seal()
                                                                .Seal()
                                                            .Seal()
                                                            .List(1).Seal()
                                                            .List(2)
                                                                .Add(0, std::move(aggregate))
                                                            .Seal()
                                                            .List(3).Seal()
                                                        .Seal()
                                                    .Seal()
                                                .Seal();
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal();
                }
                return parent;
            })
        .Seal()
    .Build();
}

std::unordered_set<std::string_view> FindUsedVars(const TExprNode::TPtr& params) {
    std::unordered_set<std::string_view> result;

    const auto measures = params->Child(0);
    const auto callablesItems = measures->Child(3);
    for (const auto& item : callablesItems->Children()) {
        const auto vars = item->Child(1);
        for (const auto& var : vars->Children()) {
            result.insert(var->Content());
        }
    }

    const auto defines = params->Child(4);
    static constexpr size_t defineLambdasStartPos = 3;
    for (const auto& define : defines->Children() | std::views::drop(defineLambdasStartPos)) {
        const auto lambda = TCoLambda(define);
        const auto varsArg = lambda.Args().Arg(1).Ptr();
        const auto lambdaBody = lambda.Body().Ptr();
        NYql::VisitExpr(
            lambdaBody,
            [varsArg, &result](const TExprNode::TPtr& node) {
                if (auto maybeMember = TMaybeNode<TCoMember>(node);
                    maybeMember && maybeMember.Cast().Struct().Ptr() == varsArg) {
                    result.insert(maybeMember.Cast().Name().Value());
                    return false;
                }
                return true;
            }
        );
    }

    return result;
}

TExprNode::TPtr MarkUnusedPatternVars(const TExprNode::TPtr& node, TExprContext& ctx, const std::unordered_set<std::string_view>& usedVars, const TExprNode::TPtr& rowsPerMatch) {
    const auto pos = node->Pos();
    if (node->ChildrenSize() == 6 && node->Child(0)->IsAtom()) {
        const auto varName = node->Child(0)->Content();
        const auto output = FromString<bool>(node->Child(4)->Content());
        const auto varUnused = ("RowsPerMatch_AllRows" != rowsPerMatch->Content() || !output) && !usedVars.contains(varName);
        return Build<TExprList>(ctx, pos)
            .Add(node->ChildPtr(0))
            .Add(node->ChildPtr(1))
            .Add(node->ChildPtr(2))
            .Add(node->ChildPtr(3))
            .Add(node->ChildPtr(4))
            .Add<TCoAtom>().Build(ToString(varUnused))
        .Done()
        .Ptr();
    }
    TExprNode::TListType newChildren;
    for (const auto& child : node->Children()) {
        newChildren.push_back(MarkUnusedPatternVars(child, ctx, usedVars, rowsPerMatch));
    }
    return ctx.ChangeChildren(*node, std::move(newChildren));
}

} // anonymous namespace

TExprNode::TPtr ExpandMatchRecognize(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    YQL_CLOG(DEBUG, Core) << "Expand " << node->Content();
    TCoMatchRecognize matchRecognize(node);
    const auto input = matchRecognize.Input().Ptr();
    const auto partitionKeySelector = matchRecognize.PartitionKeySelector().Ptr();
    const auto partitionColumns = matchRecognize.PartitionColumns().Ptr();
    const auto sortTraits = matchRecognize.SortTraits().Ptr();
    const auto params = matchRecognize.Params().Ptr();
    const auto pos = matchRecognize.Pos();

    const auto isStreaming = IsStreaming(input, typeAnnCtx);

    auto newInput = Build<TCoLambda>(ctx, pos)
        .Args({"partition"})
        .Body<TCoToFlow>()
            .Input("partition")
        .Build()
    .Done()
    .Ptr();

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    ExtractSortKeyAndOrder(pos, sortTraits, sortKey, sortOrder, ctx);
    auto timeOrderRecover = [&]() -> TExprNode::TPtr {
        if (!isStreaming) {
            return newInput;
        }
        switch (sortOrder->ChildrenSize()) {
            case 0:
                return newInput;
            case 1: {
                auto timeOrderRecover = ctx.Builder(pos)
                    .Lambda()
                        .Param("partition")
                        .Callable("TimeOrderRecover")
                            .Apply(0, std::move(newInput))
                                .With(0, "partition")
                            .Seal()
                            .Add(1, sortKey)
                            .Callable(2, "Interval")
                                .Atom(0, ToString(typeAnnCtx.TimeOrderRecoverDelay))
                            .Seal()
                            .Callable(3, "Interval")
                                .Atom(0, ToString(typeAnnCtx.TimeOrderRecoverAhead))
                            .Seal()
                            .Callable(4, "Uint32")
                                .Atom(0, ToString(typeAnnCtx.TimeOrderRecoverRowLimit))
                            .Seal()
                        .Seal()
                    .Seal()
                .Build();
                return Build<TCoLambda>(ctx, pos)
                    .Args({"partition"})
                    .Body<TCoOrderedMap>()
                        .Input<TCoOrderedFilter>()
                            .Input<TExprApplier>()
                                .Apply(TCoLambda(timeOrderRecover))
                                .With(0, "partition")
                            .Build()
                            .Lambda<TCoLambda>()
                                .Args({"row"})
                                .Body<TCoNot>()
                                    .Value<TCoMember>()
                                        .Struct("row")
                                        .Name<TCoAtom>().Build(NYql::NTimeOrderRecover::OUT_OF_ORDER_MARKER)
                                    .Build()
                                .Build()
                            .Build()
                        .Build()
                        .Lambda<TCoLambda>()
                            .Args({"row"})
                            .Body<TCoRemoveMember>()
                                .Struct("row")
                                .Name<TCoAtom>().Build(NYql::NTimeOrderRecover::OUT_OF_ORDER_MARKER)
                            .Build()
                        .Build()
                    .Build()
                .Done()
                .Ptr();
            }
            default:
                ctx.AddError(TIssue(ctx.GetPosition(sortTraits->Pos()), "Expected no ORDER BY or ORDER BY timestamp for MATCH_RECOGNIZE"));
                return {};
        }
    }();
    if (!timeOrderRecover) {
        return {};
    }

    auto measures = ExpandMatchRecognizeMeasuresCallables(params->ChildPtr(0), ctx, typeAnnCtx);
    auto rowsPerMatch = params->ChildPtr(1);
    const auto usedVars = FindUsedVars(params);
    auto pattern = MarkUnusedPatternVars(params->ChildPtr(3), ctx, usedVars, rowsPerMatch);
    auto settings = AddSetting(*ctx.NewList(pos, {}), pos, "Streaming", ctx.NewAtom(pos, ToString(isStreaming)), ctx);
    auto newMatchRecognize = ctx.Builder(pos)
        .Lambda()
            .Param("partition")
            .Callable("MatchRecognizeCore")
                .Apply(0, std::move(timeOrderRecover))
                    .With(0, "partition")
                .Seal()
                .Add(1, partitionKeySelector)
                .Add(2, partitionColumns)
                .Callable(3, params->Content())
                    .Add(0, std::move(measures))
                    .Add(1, std::move(rowsPerMatch))
                    .Add(2, params->ChildPtr(2))
                    .Add(3, std::move(pattern))
                    .Add(4, params->ChildPtr(4))
                .Seal()
                .Add(4, std::move(settings))
            .Seal()
        .Seal()
    .Build();

    auto lambda = Build<TCoLambda>(ctx, pos)
        .Args({"partition"})
        .Body<TCoForwardList>()
            .Stream<TExprApplier>()
                .Apply(TCoLambda(newMatchRecognize))
                .With(0, "partition")
            .Build()
        .Build()
    .Done()
    .Ptr();

    if (isStreaming) {
        TExprNode::TPtr keySelector;
        if (partitionColumns->ChildrenSize() != 0) {
            keySelector = std::move(partitionKeySelector);
        } else {
            // Use pseudo partitioning with constant lambda to wrap TimeOrderRecover into DQ stage
            // TODO(zverevgeny): fixme
            keySelector = Build<TCoLambda>(ctx, pos)
                .Args({"row"})
                .Body(MakeBool<true>(pos, ctx))
            .Done()
            .Ptr();
        }

        return Build<TCoShuffleByKeys>(ctx, pos)
            .Input(std::move(input))
            .KeySelectorLambda(std::move(keySelector))
            .ListHandlerLambda(std::move(lambda))
        .Done()
        .Ptr();
    } else { // non-streaming
        return Build<TCoPartitionsByKeys>(ctx, pos)
            .Input(std::move(input))
            .KeySelectorLambda(std::move(partitionKeySelector))
            .SortDirections(std::move(sortOrder))
            .SortKeySelectorLambda(std::move(sortKey))
            .ListHandlerLambda(std::move(lambda))
        .Done()
        .Ptr();
    }
}

} // namespace NYql
