#pragma once

#include "public.h"
#include "key_trie.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/range.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Returns a minimal key range that cover both inputs.
TKeyRange Unite(const TKeyRange& first, const TKeyRange& second);
TRowRange Unite(const TRowRange& first, const TRowRange& second);

//! Returns a maximal key range covered by both inputs.
TKeyRange Intersect(const TKeyRange& first, const TKeyRange& second);
TRowRange Intersect(const TRowRange& first, const TRowRange& second);

//! Checks whether key range is empty.
bool IsEmpty(const TKeyRange& keyRange);
bool IsEmpty(const TRowRange& keyRange);

bool IsTrue(TConstExpressionPtr expr);
TConstExpressionPtr MakeAndExpression(TConstExpressionPtr lhs, TConstExpressionPtr rhs);
TConstExpressionPtr MakeOrExpression(TConstExpressionPtr lhs, TConstExpressionPtr rhs);

TConstExpressionPtr EliminatePredicate(
    TRange<TRowRange> keyRanges,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns);

TConstExpressionPtr EliminatePredicate(
    TRange<TRow> lookupKeys,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns);

TConstExpressionPtr ExtractPredicateForColumnSubset(
    TConstExpressionPtr expr,
    const TTableSchema& tableSchema);

std::pair<TConstExpressionPtr, TConstExpressionPtr> SplitPredicateByColumnSubset(
    TConstExpressionPtr root,
    const TTableSchema& tableSchema);

// Wrapper around CompareRowValues that checks that its arguments are not nan.
int CompareRowValuesCheckingNan(const NTableClient::TUnversionedValue& lhs, const NTableClient::TUnversionedValue& rhs);

ui64 GetEvaluatedColumnModulo(const TConstExpressionPtr& expr);

template <class TIter>
TIter MergeOverlappingRanges(TIter begin, TIter end)
{
    if (begin == end) {
        return end;
    }

    auto it = begin;
    auto dest = it;
    ++it;

    for (; it != end; ++it) {
        if (dest->second < it->first) {
            if (++dest != it) {
                *dest = std::move(*it);
            }
        } else if (dest->second < it->second) {
            dest->second = std::move(it->second);
        }
    }

    ++dest;
    return dest;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

