#include "kqp_read_ranges_sql_in.h"

#include <yql/essentials/core/yql_expr_optimize.h>

#include <util/generic/algorithm.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp::NOpt::NReadRanges {

using namespace NYql;

namespace {

bool IsArgumentElement(const TExprNode& node) {
    return node.IsCallable("Nth") && node.ChildrenSize() == 2 && node.Head().Type() == TExprNode::Argument;
}

// The items of a literal list: AsList, a tuple, or a tuple made a list by Map(tuple, t -> AsList(Nth(t, i), ...)).
std::optional<TExprNode::TListType> ParseLiteralList(const TExprNode::TPtr& node) {
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return ParseLiteralList(node->HeadPtr());
    }
    if (node->IsCallable("AsList") || node->IsList()) {
        return node->ChildrenList();
    }
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && node->Tail().IsLambda()) {
        const auto tuple = ParseLiteralList(node->HeadPtr());
        const auto& lambda = node->Tail();
        const auto& body = lambda.Tail();
        if (!tuple || lambda.Head().ChildrenSize() != 1 || !body.IsCallable("AsList")) {
            return std::nullopt;
        }
        TExprNode::TListType result;
        for (const auto& item : body.Children()) {
            ui32 index;
            if (!item->IsCallable("Nth") || item->ChildrenSize() != 2 || item->Head().Type() != TExprNode::Argument
                || !TryFromString(item->Tail().Content(), index) || index >= tuple->size()) {
                return std::nullopt;
            }
            result.push_back((*tuple)[index]);
        }
        return result;
    }
    return std::nullopt;
}

} // namespace

std::optional<TSqlInRanges> MatchSqlInRanges(const TExprNode::TPtr& node) {
    if (!node->IsCallable("IfPresent") || node->ChildrenSize() != 3 || !node->Child(1)->IsLambda()
        || !node->Child(2)->IsCallable("RangeEmpty")) {
        return std::nullopt;
    }
    const auto& body = node->Child(1)->TailPtr();

    auto isItemRangeFor = [](const TExprNode& child) {
        return child.IsCallable("RangeFor") && child.ChildrenSize() == 3
            && (child.Child(1)->Type() == TExprNode::Argument || IsArgumentElement(*child.Child(1)));
    };
    const auto itemRanges = FindNode(body, [&](const TExprNode::TPtr& child) {
        if (!child->IsCallable("RangeMultiply") || child->ChildrenSize() < 2) {
            return false;
        }
        for (size_t i = 1; i < child->ChildrenSize(); ++i) {
            if (!isItemRangeFor(*child->Child(i))) {
                return false;
            }
        }
        return true;
    });
    TSqlInRanges result{.List = node->HeadPtr(), .Items = ParseLiteralList(node->HeadPtr())};
    if (!itemRanges || !itemRanges->Head().IsCallable("Uint64")
        || !TryFromString(itemRanges->Head().Head().Content(), result.ItemLimit)) {
        return std::nullopt;
    }
    for (size_t i = 1; i < itemRanges->ChildrenSize(); ++i) {
        const auto& rangeFor = *itemRanges->Child(i);
        TSqlInColumn column{.Op = rangeFor.Head().Content(), .Type = rangeFor.TailPtr()};
        if (IsArgumentElement(*rangeFor.Child(1))) {
            ui32 element;
            if (!TryFromString(rangeFor.Child(1)->Tail().Content(), element)) {
                return std::nullopt;
            }
            column.Element = element;
        }
        result.Columns.push_back(std::move(column));
    }

    // Map(tuples, t -> '(Nth(t, i), ...)) puts the elements of every tuple in key order.
    const auto keyOrder = FindNode(body, [](const TExprNode::TPtr& child) {
        if (!child->IsCallable("Map") || child->ChildrenSize() != 2 || !child->Tail().IsLambda() || !child->Tail().Tail().IsList()) {
            return false;
        }
        return AllOf(child->Tail().Tail().Children(), [](const TExprNode::TPtr& element) { return IsArgumentElement(*element); });
    });
    if (keyOrder) {
        const auto& input = keyOrder->Head();
        if (input.IsCallable("FlatMap") && input.ChildrenSize() == 2 && input.Tail().IsLambda()) {
            const auto& cast = input.Tail().Tail();
            if (cast.IsCallable("StrictCast") && cast.ChildrenSize() == 2
                && cast.Tail().IsCallable("OptionalType") && cast.Tail().ChildrenSize() == 1
                && cast.Tail().Head().IsCallable("TupleType")) {
                result.TupleSize = cast.Tail().Head().ChildrenSize();
            }
        }
        for (const auto& element : keyOrder->Tail().Tail().Children()) {
            ui32 index;
            if (!TryFromString(element->Tail().Content(), index)) {
                return std::nullopt;
            }
            result.KeyOrder.push_back(index);
        }
    }

    return result;
}

} // namespace NKikimr::NKqp::NOpt::NReadRanges
