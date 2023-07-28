#pragma once

#include <algorithm>

#include <util/system/compiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TIter, class TPredicate>
TIter LinearSearch(TIter begin, TIter end, TPredicate pred);

// Search returns iterator to the first element that is !pred(it).
// Reverse(Search) returns iterator to the last element that pred(it).

// With default predicate you can use shortcuts LowerBound and UpperBound.
// If custom predicate is needed you should use Binary/ExponentialSearch because LowerBound and UpperBound is
// determined by predicate (see implementation).

// Properties of Binary/ExponentialSearch:
// let Reverse = std::make_reverse_iterator
// let Search = Binary/ExponentialSearch
// Reverse(Search(begin, end, pred)) ~ Search(Reverse(end), Reverse(begin), [&] (auto it) { return !pred(it); })
// Use first case due to reverse iterator contains extra fields (see LWG #2360).
// Do not use it - 1 pattern (i.e. LowerBound(...) - 1). Convert iterator to reverse and compare with .rend()
// instead. it - 1 causes STL debug assert violations when it is equal to .begin().

template <class TIter, class TPredicate>
TIter BinarySearch(TIter begin, TIter end, TPredicate pred);

template <class TIter, class TPredicate>
TIter ExponentialSearch(TIter begin, TIter end, TPredicate pred);

// Properties based on property a <= b ~ !(b < a):
// UpperBound(a < b) ~ LowerBound(a <= b)
// UpperBound(a <= b) ~ LowerBound(a < b)

template <class TIter, class T>
TIter LowerBound(TIter begin, TIter end, const T& value);

template <class TIter, class T>
TIter UpperBound(TIter begin, TIter end, const T& value);

template <class TIter, class T>
TIter ExpLowerBound(TIter begin, TIter end, const T& value);

template <class TIter, class T>
TIter ExpUpperBound(TIter begin, TIter end, const T& value);

//! Similar to std::set_intersection, but returns bool and doesn't output the
//! actual intersection.
//! Input ranges must be sorted.
template <class TInputIt1, class TInputIt2>
bool Intersects(TInputIt1 first1, TInputIt1 last1, TInputIt2 first2, TInputIt2 last2);

//! Runs Fisher--Yates shuffle to pick a random K-subset [begin, end) from [begin,last) (for K = std::distance(begin, end)).
template <class TIter>
void PartialShuffle(TIter begin, TIter end, TIter last);

template <class T, typename TGetKey>
std::pair<const T&, const T&> MinMaxBy(const T& first, const T& second, const TGetKey& getKey);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ALGORITHM_HELPERS_INL_H_
#include "algorithm_helpers-inl.h"
#undef ALGORITHM_HELPERS_INL_H_
