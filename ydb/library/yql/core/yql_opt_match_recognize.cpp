#include "yql_opt_match_recognize.h"
#include "yql_opt_utils.h"
#include <ydb/library/yql/core/sql_types/time_order_recover.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h>

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
            YQL_ENSURE(node->ChildrenSize() > 0 and node->ChildRef(0)->IsAtom());
            hasPq = node->ChildRef(0)->Content() == "pq";
        }
        return !hasPq;
    });
    return hasPq;
}
} //namespace

// returns std::nullopt if all vars could be used
std::optional<TSet<TStringBuf>> FindUsedVars(const TExprNode::TPtr& params) {
    TSet<TStringBuf> usedVars;
    bool allVarsUsed = false;

    const auto createVisitor = [&usedVars, &allVarsUsed](const TExprNode::TPtr& varsArg) {
        return [&varsArg, &usedVars, &allVarsUsed](const TExprNode::TPtr& node) -> bool {
            if (node->IsCallable("Member")) {
                if (node->ChildRef(0) == varsArg) {
                    usedVars.insert(node->ChildRef(1)->Content());
                    return false;
                }
            }
            if (node == varsArg) {
                allVarsUsed = true;
            }
            return true;
        };
    };

    const auto& measures = params->ChildRef(0);
    static constexpr size_t measureLambdasStartPos = 3;
    for (size_t pos = measureLambdasStartPos; pos != measures->ChildrenSize(); pos++) {
        const auto& lambda = measures->ChildRef(pos);
        const auto& lambdaArgs = lambda->ChildRef(0);
        const auto& lambdaBody = lambda->ChildRef(1);
        const auto& varsArg = lambdaArgs->ChildRef(1);
        NYql::VisitExpr(lambdaBody, createVisitor(varsArg));
    }

    const auto& defines = params->ChildRef(4);
    static constexpr size_t defineLambdasStartPos = 3;
    for (size_t pos = defineLambdasStartPos; pos != defines->ChildrenSize(); pos++) {
        const auto& lambda = defines->ChildRef(pos);
        const auto& lambdaArgs = lambda->ChildRef(0);
        const auto& lambdaBody = lambda->ChildRef(1);
        const auto& varsArg = lambdaArgs->ChildRef(1);
        NYql::VisitExpr(lambdaBody, createVisitor(varsArg));
    }

    return allVarsUsed ? std::nullopt : std::make_optional(usedVars);
}

// usedVars can be std::nullopt if all vars could probably be used
TExprNode::TPtr MarkUnusedPatternVars(const TExprNode::TPtr& node, TExprContext& ctx, const std::optional<TSet<TStringBuf>> &usedVars) {
    const auto pos = node->Pos();
    if (node->ChildrenSize() != 0 && node->ChildRef(0)->IsAtom()) {
        const auto& varName = node->ChildRef(0)->Content();
        bool varUsed = !usedVars.has_value() || usedVars.value().contains(varName);
        return ctx.Builder(pos)
            .List()
                .Add(0, node->ChildRef(0))
                .Add(1, node->ChildRef(1))
                .Add(2, node->ChildRef(2))
                .Add(3, node->ChildRef(3))
                .Add(4, node->ChildRef(4))
                .Add(5, ctx.NewAtom(pos, varUsed ? "0" : "1"))
            .Seal()
        .Build();
    }
    TExprNodeList newChildren;
    for (size_t chPos = 0; chPos != node->ChildrenSize(); chPos++) {
        newChildren.push_back(MarkUnusedPatternVars(node->ChildRef(chPos), ctx, usedVars));
    }
    if (node->IsCallable()) {
        return ctx.Builder(pos).Callable(node->Content()).Add(std::move(newChildren)).Seal().Build();
    } else if (node->IsList()) {
        return ctx.Builder(pos).List().Add(std::move(newChildren)).Seal().Build();
    } else { // Atom
        return node;
    }
}

TExprNode::TPtr ExpandMatchRecognize(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typeAnnCtx) {
    YQL_ENSURE(node->IsCallable({"MatchRecognize"}));
    const auto& input = node->ChildRef(0);
    const auto& partitionKeySelector = node->ChildRef(1);
    const auto& partitionColumns = node->ChildRef(2);
    const auto& sortTraits = node->ChildRef(3);
    const auto& params = node->ChildRef(4);
    const auto pos = node->Pos();

    const bool isStreaming = IsStreaming(input, typeAnnCtx);

    TExprNode::TPtr settings = AddSetting(*ctx.NewList(pos, {}), pos,
          "Streaming", ctx.NewAtom(pos, ToString(isStreaming)), ctx);

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
                        .Add(0, params->ChildRef(0))
                        .Add(1, params->ChildRef(1))
                        .Add(2, params->ChildRef(2))
                        .Add(3, MarkUnusedPatternVars(params->ChildRef(3), ctx, FindUsedVars(params)))
                        .Add(4, params->ChildRef(4))
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
