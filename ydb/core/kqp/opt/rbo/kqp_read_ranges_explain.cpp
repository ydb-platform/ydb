#include "kqp_read_ranges_explain.h"

#include "kqp_read_ranges_calculator.h"
#include "kqp_read_ranges_model.h"
#include "kqp_read_ranges_structure.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <util/string/join.h>

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
                                          const TVector<TString>& keyColumns, TMaybe<size_t> expectedMaxRanges) {
    // 1. The literal range, which a row storage source reads as is.
    if (literalRange) {
        return {.Ranges = TVector<TString>{DescribeLiteralKeyRange(literalRange, keyColumns)}};
    }

    // 2a. The ranges MiniKQL computed at compile time.
    std::optional<TVector<TString>> evaluated;
    if (evaluatedRanges) {
        std::optional<TRangeList> list;
        if (evaluatedRanges->IsCallable("AsList")) {
            list = ParseAsRange(*evaluatedRanges);
        } else if (evaluatedRanges->IsCallable("List")) {
            list = TRangeList{};
        }
        if (list && list->Width <= keyColumns.size() && list->Ranges.size() <= MaxExplainedReadRanges) {
            evaluated.emplace();
            for (const auto& range : list->Ranges) {
                evaluated->push_back(FormatRange(range, keyColumns));
            }
        } else {
            Cerr << "RANGE_CMP unparsed: " << (list ? "too many/wide" : "bad form") << Endl;
        }
    }

    // 2. The ranges computed from the ranges expression, when they are known at compile time and few.
    if (ranges && !keyColumns.empty() && (!expectedMaxRanges || *expectedMaxRanges <= MaxExplainedReadRanges)) {
        const auto list = CalculateRanges(ranges);
        if (list && list->Width <= keyColumns.size() && list->Ranges.size() <= MaxExplainedReadRanges) {
            TVector<TString> result;
            for (const auto& range : list->Ranges) {
                result.push_back(FormatRange(range, keyColumns));
            }
            Cerr << "RANGE_CMP " << (!evaluated ? "calc-only" : (*evaluated == result ? "same" : "DIFF")) << " calc=" << JoinSeq(" ", result);
            if (evaluated) {
                Cerr << " eval=" << JoinSeq(" ", *evaluated);
            }
            Cerr << Endl;
            return {.Ranges = evaluated ? std::move(*evaluated) : std::move(result)};
        }
    }
    if (evaluated) {
        Cerr << "RANGE_CMP eval-only eval=" << JoinSeq(" ", *evaluated) << Endl;
        return {.Ranges = std::move(*evaluated)};
    }

    // 3. The structure of the ranges expression, with the values unknown at compile time shown by name.
    return {.Expression = DescribeRangesStructure(ranges, keyColumns)};
}

} // namespace NKikimr::NKqp::NOpt
