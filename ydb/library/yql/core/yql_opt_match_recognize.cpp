#include "yql_opt_match_recognize.h"
#include "yql_opt_utils.h"

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

TExprNode::TPtr ExpandMatchRecognize(const TExprNode::TPtr& node, TExprContext& ctx) {
    YQL_ENSURE(node->IsCallable({"MatchRecognize"}));
    const auto& input = node->ChildRef(0);
    const auto& partitionKeySelector = node->ChildRef(1);
    const auto& partitionColumns = node->ChildRef(2);
    const auto& sortTraits = node->ChildRef(3);
    const auto& params = node->ChildRef(4);

    const auto pos = node->Pos();

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
                    .Add(3, params)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    ExtractSortKeyAndOrder(pos, sortTraits, sortKey, sortOrder, ctx);

    const auto matchRecognizeOnSortedPartition = sortOrder->ChildrenSize() != 0 ?
            ctx.Builder(pos)
                .Lambda()
                    .Param("partition")
                    .Apply(matchRecognize)
                        .With(0)
                            .Callable("Sort")
                                .Arg(0, "partition")
                                .Add(1, sortOrder)
                                .Add(2, sortKey)
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Build() :
             matchRecognize;

    const auto result = partitionColumns->ChildrenSize() != 0 ?
        ctx.Builder(pos)
            .Callable("ShuffleByKeys")
                .Add(0, input)
                .Add(1, partitionKeySelector)
                .Add(2, matchRecognizeOnSortedPartition)
            .Seal()
            .Build() :
        ctx.Builder(pos)
            .Apply(matchRecognizeOnSortedPartition)
                  .With(0, input)
            .Seal()
            .Build();
    YQL_CLOG(INFO, Core) << "Expanded MatchRecognize";
    return result;
}


} //namespace NYql
