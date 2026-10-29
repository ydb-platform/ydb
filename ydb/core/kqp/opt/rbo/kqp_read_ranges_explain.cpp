#include "kqp_read_ranges_explain.h"

#include "kqp_read_ranges_model.h"
#include "kqp_read_ranges_structure.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NReadRanges;

namespace {

// A bound of a literal range, whose values BuildLiteralRange makes literals, NULLs or parameters.
TBoundary ParseLiteralBound(const TExprNode& bound, bool isLeft) {
    TBoundary result;
    for (const auto& node : bound.Children()) {
        TKeyValue value{.Kind = TKeyValue::EKind::Parameter, .Bytes = "n/a"};
        if (const auto param = TMaybeNode<TCoParameter>(node)) {
            value.Bytes = param.Cast().Name().Value();
        } else if (auto literal = ParseKeyValue(node)) {
            value = std::move(*literal);
        }
        result.Components.push_back({.Value = std::move(value)});
    }
    result.Included = bound.IsCallable("KqlKeyInc") ? (isLeft ? -1 : 1) : 0;
    return result;
}

} // namespace

TString DescribeLiteralKeyRange(const TExprNode::TPtr& keyRange, const TVector<TString>& keyColumns) {
    // A KqlKeyRange is a tuple of the two bounds.
    Y_ENSURE(keyRange->IsList() && keyRange->ChildrenSize() == 2);
    return FormatRange({ParseLiteralBound(keyRange->Head(), /*isLeft=*/true), ParseLiteralBound(keyRange->Tail(), /*isLeft=*/false)},
                       keyColumns);
}

TReadRangesDescription DescribeReadRanges(const TExprNode::TPtr& literalRange, const TExprNode::TPtr& evaluatedRanges,
                                          const TExprNode::TPtr& ranges,
                                          const TVector<TString>& keyColumns) {
    // 1. The literal range, which a row storage source reads as is.
    if (literalRange) {
        return {.Ranges = TVector<TString>{DescribeLiteralKeyRange(literalRange, keyColumns)}};
    }

    // 2. The ranges MiniKQL computed at compile time.
    if (evaluatedRanges) {
        std::optional<TRangeList> list;
        if (evaluatedRanges->IsCallable("AsList") && evaluatedRanges->ChildrenSize() <= MaxExplainedReadRanges) {
            list = ParseAsRange(*evaluatedRanges);
        } else if (evaluatedRanges->IsCallable("List") && evaluatedRanges->ChildrenSize() == 1) {
            list = TRangeList{};
        }
        if (list && list->Width <= keyColumns.size() && list->Ranges.size() <= MaxExplainedReadRanges) {
            TVector<TString> result;
            for (const auto& range : list->Ranges) {
                result.push_back(FormatRange(range, keyColumns));
            }
            return {.Ranges = std::move(result)};
        }
    }

    // 3. The structure of the ranges expression, with the values unknown at compile time shown by name.
    return {.Expression = DescribeRangesStructure(ranges, keyColumns)};
}

} // namespace NKikimr::NKqp::NOpt
