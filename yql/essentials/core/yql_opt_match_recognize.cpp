#include "yql_opt_match_recognize.h"
#include "yql_opt_utils.h"
#include <yql/essentials/core/sql_types/time_order_recover.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

bool IsStreaming(const TExprNode::TPtr& input, const TTypeAnnotationContext& typeAnnCtx) {
    if (EMatchRecognizeStreamingMode::Disable == typeAnnCtx.MatchRecognizeStreaming){
        return false;
    }
    if (EMatchRecognizeStreamingMode::Force == typeAnnCtx.MatchRecognizeStreaming){
        return true;
    }

    YQL_ENSURE(EMatchRecognizeStreamingMode::Auto == typeAnnCtx.MatchRecognizeStreaming, "Internal logic error");

    bool hasPq = false;
    NYql::VisitExpr(input, [&hasPq](const TExprNode::TPtr& node){
        if (node->IsCallable("DataSource")) {
            YQL_ENSURE(node->ChildrenSize() > 0 and node->Child(0)->IsAtom());
            hasPq = node->Child(0)->Content() == "pq";
        }
        return !hasPq;
    });
    return hasPq;
}

TExprNode::TPtr ExpandMatchRecognizeMeasuresAggregates(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& /* typeAnnCtx */) {
    const auto pos = node->Pos();
    const auto vars = node->Child(3);
    static constexpr size_t AggregatesLambdasStartPos = 4;
    static constexpr size_t MeasuresLambdasStartPos = 3;

    return ctx.Builder(pos)
        .Callable("MatchRecognizeMeasures")
            .Add(0, node->ChildPtr(0))
            .Add(1, node->ChildPtr(1))
            .Add(2, node->ChildPtr(2))
            .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                for (size_t i = 0; i < vars->ChildrenSize(); ++i) {
                    const auto var = vars->Child(i)->Content();
                    const auto handler = node->ChildPtr(AggregatesLambdasStartPos + i);
                    if (!var) {
                        auto value = handler->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Optional
                            ? ctx.Builder(pos).Callable("Just").Add(0, handler).Seal().Build()
                            : handler;
                        parent.Add(
                            MeasuresLambdasStartPos + i,
                            ctx.Builder(pos)
                                .Lambda()
                                    .Param("data")
                                    .Param("vars")
                                    .Add(0, std::move(value))
                                .Seal()
                            .Build()
                        );
                        continue;
                    }
                    parent.Add(
                        MeasuresLambdasStartPos + i,
                        ctx.Builder(pos)
                            .Lambda()
                                .Param("data")
                                .Param("vars")
                                .Callable(0, "Member")
                                    .Callable(0, "Head")
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
                                                .Add(0, handler)
                                            .Seal()
                                            .List(3).Seal()
                                        .Seal()
                                    .Seal()
                                    .Atom(1, handler->Child(0)->Content())
                                .Seal()
                            .Seal()
                        .Build()
                    );
                }
                return parent;
            })
        .Seal()
    .Build();
}

THashSet<TStringBuf> FindUsedVars(const TExprNode::TPtr& params) {
    THashSet<TStringBuf> result;

    const auto measures = params->Child(0);
    const auto measuresVars = measures->Child(3);
    for (const auto& var : measuresVars->Children()) {
        result.insert(var->Content());
    }

    const auto defines = params->Child(4);
    static constexpr size_t defineLambdasStartPos = 3;
    for (auto i = defineLambdasStartPos; i < defines->ChildrenSize(); ++i) {
        const auto lambda = defines->Child(i);
        const auto lambdaArgs = lambda->Child(0);
        const auto lambdaBody = lambda->ChildPtr(1);
        const auto varsArg = lambdaArgs->Child(1);
        NYql::VisitExpr(
            lambdaBody,
            [varsArg, &result](const TExprNode::TPtr& node) {
                if (node->IsCallable("Member") && node->Child(0) == varsArg) {
                    result.insert(node->Child(1)->Content());
                    return false;
                }
                return true;
            }
        );
    }

    return result;
}

TExprNode::TPtr MarkUnusedPatternVars(const TExprNode::TPtr& node, TExprContext& ctx, const THashSet<TStringBuf>& usedVars, TStringBuf rowsPerMatch) {
    const auto pos = node->Pos();
    if (node->ChildrenSize() != 0 && node->Child(0)->IsAtom()) {
        const auto varName = node->Child(0)->Content();
        const auto output = FromString<bool>(node->Child(4)->Content());
        const auto varUnused = ("RowsPerMatch_AllRows" != rowsPerMatch || !output) && !usedVars.contains(varName);
        return ctx.Builder(pos)
            .List()
                .Add(0, node->ChildPtr(0))
                .Add(1, node->ChildPtr(1))
                .Add(2, node->ChildPtr(2))
                .Add(3, node->ChildPtr(3))
                .Add(4, node->ChildPtr(4))
                .Add(5, ctx.NewAtom(pos, ToString(varUnused)))
            .Seal()
        .Build();
    }
    TExprNodeList newChildren;
    for (size_t chPos = 0; chPos != node->ChildrenSize(); chPos++) {
        newChildren.push_back(MarkUnusedPatternVars(node->ChildPtr(chPos), ctx, usedVars, rowsPerMatch));
    }
    if (node->IsCallable()) {
        return ctx.Builder(pos).Callable(node->Content()).Add(std::move(newChildren)).Seal().Build();
    } else if (node->IsList()) {
        return ctx.Builder(pos).List().Add(std::move(newChildren)).Seal().Build();
    } else { // Atom
        return node;
    }
}

} // anonymous namespace

TExprNode::TPtr ExpandMatchRecognize(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    YQL_ENSURE(node->IsCallable("MatchRecognize"));
    const auto input = node->Child(0);
    const auto partitionKeySelector = node->Child(1);
    const auto partitionColumns = node->Child(2);
    const auto sortTraits = node->Child(3);
    const auto params = node->Child(4);
    const auto pos = node->Pos();

    const bool isStreaming = IsStreaming(input, typeAnnCtx);

    TExprNode::TPtr settings = AddSetting(*ctx.NewList(pos, {}), pos,
          "Streaming", ctx.NewAtom(pos, ToString(isStreaming)), ctx);

    const auto rowsPerMatch = params->Child(1)->Content();
    auto measures = ExpandMatchRecognizeMeasuresAggregates(params->ChildPtr(0), ctx, typeAnnCtx);
    const auto matchRecognize = ctx.Builder(pos)
        .Lambda()
            .Param("sortedPartition")
            .Callable(0, "ForwardList")
                .Callable(0, "MatchRecognizeCore")
                    .Callable(0, "ToFlow")
                        .Arg(0, "sortedPartition")
                    .Seal()
                    .Add(1, partitionKeySelector)
                    .Add(2, partitionColumns)
                    .Callable(3, params->Content())
                        .Add(0, std::move(measures))
                        .Add(1, params->ChildPtr(1))
                        .Add(2, params->ChildPtr(2))
                        .Add(3, MarkUnusedPatternVars(params->ChildPtr(3), ctx, FindUsedVars(params), rowsPerMatch))
                        .Add(4, params->ChildPtr(4))
                    .Seal()
                    .Add(4, settings)
                .Seal()
            .Seal()
        .Seal()
    .Build();

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    ExtractSortKeyAndOrder(pos, sortTraits, sortKey, sortOrder, ctx);
    TExprNode::TPtr result;
    if (isStreaming) {
        YQL_ENSURE(sortOrder->ChildrenSize() == 1, "Expect ORDER BY timestamp for MATCH_RECOGNIZE");
        const auto reordered = ctx.Builder(pos)
            .Lambda()
            .Param("partition")
                .Callable("ForwardList")
                    .Callable(0, "OrderedMap")
                        .Callable(0, "TimeOrderRecover")
                            .Callable(0, "ToFlow").
                                Arg(0, "partition")
                            .Seal()
                            .Add(1, sortKey)
                            .Callable(2, "Interval")
                                .Add(0, ctx.NewAtom(pos, ToString(typeAnnCtx.TimeOrderRecoverDelay)))
                            .Seal()
                            .Callable(3, "Interval")
                                .Add(0,  ctx.NewAtom(pos, ToString(typeAnnCtx.TimeOrderRecoverAhead)))
                            .Seal()
                            .Callable(4, "Uint32")
                                .Add(0,  ctx.NewAtom(pos, ToString(typeAnnCtx.TimeOrderRecoverRowLimit)))
                            .Seal()
                        .Seal()
                        .Lambda(1)
                            .Param("row")
                            .Callable("RemoveMember")
                                .Arg(0, "row")
                                .Add(1, ctx.NewAtom(pos, NYql::NTimeOrderRecover::OUT_OF_ORDER_MARKER))
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Build();

        const auto matchRecognizeOnReorderedPartition = ctx.Builder(pos)
            .Lambda()
                .Param("partition")
                .Apply(matchRecognize)
                    .With(0)
                        .Apply(reordered)
                            .With(0)
                                .Arg("partition")
                            .Done()
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Build();
        TExprNode::TPtr keySelector;
        if (partitionColumns->ChildrenSize() != 0) {
            keySelector = partitionKeySelector;
        } else {
            //Use pseudo partitioning with constant lambda to wrap TimeOrderRecover into DQ stage
            //TODO(zverevgeny): fixme
            keySelector = ctx.Builder(pos)
                .Lambda()
                    .Param("row")
                    .Callable("Bool")
                        .Add(0, ctx.NewAtom(pos, "true"))
                    .Seal()
                .Seal()
            .Build();
        }
        result = ctx.Builder(pos)
            .Callable("ShuffleByKeys")
                .Add(0, input)
                .Add(1, keySelector)
                .Add(2, matchRecognizeOnReorderedPartition)
            .Seal()
        .Build();
    } else { //non-streaming
        result = ctx.Builder(pos)
            .Callable("PartitionsByKeys")
                .Add(0, input)
                .Add(1, partitionKeySelector)
                .Add(2, sortOrder)
                .Add(3, sortKey)
                .Add(4, matchRecognize)
            .Seal()
        .Build();
    }
    YQL_CLOG(INFO, Core) << "Expanded MatchRecognize";
    return result;
}

} //namespace NYql
