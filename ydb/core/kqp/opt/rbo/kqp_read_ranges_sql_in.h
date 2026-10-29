#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp::NOpt::NReadRanges {

// The ranges of IN as the predicate extractor builds them:
//   IfPresent(list, collection -> RangeUnion of the ranges of every item of the collection, or the full range
//             when there are more items than the limit, RangeEmpty(type)).
// The ranges of an item are RangeMultiply(limit, RangeFor(op, value, type), ...), one RangeFor per key column. For IN
// over single values the value is the item. For IN over tuples it is Nth(item, i), after a Map puts the elements of
// every tuple in key order.
struct TSqlInColumn {
    TStringBuf Op;
    NYql::TExprNode::TPtr Type;
    std::optional<ui32> Element; // the element of a tuple in key order, or the whole item
};

struct TSqlInRanges {
    NYql::TExprNode::TPtr List;
    std::optional<NYql::TExprNode::TListType> Items; // when the list is a literal
    TVector<TSqlInColumn> Columns;
    ui64 ItemLimit = 0;
    TVector<ui32> KeyOrder; // the element of a tuple for every element in key order, when the tuples are reordered
    std::optional<size_t> TupleSize; // the original tuple width, before selecting/reordering key columns

    // The value of a column in an item, null when the item is not a literal tuple.
    NYql::TExprNode::TPtr GetValue(const NYql::TExprNode::TPtr& item, const TSqlInColumn& column) const {
        if (!column.Element) {
            return item;
        }
        ui32 element = *column.Element;
        if (!KeyOrder.empty()) {
            if (element >= KeyOrder.size()) {
                return nullptr;
            }
            element = KeyOrder[element];
        }
        if (!item->IsList() || element >= item->ChildrenSize()) {
            return nullptr;
        }
        return item->ChildPtr(element);
    }
};

// nullopt if the node is not the ranges of IN.
std::optional<TSqlInRanges> MatchSqlInRanges(const NYql::TExprNode::TPtr& node);

} // namespace NKikimr::NKqp::NOpt::NReadRanges
