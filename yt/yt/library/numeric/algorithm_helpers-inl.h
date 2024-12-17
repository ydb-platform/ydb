#ifndef ALGORITHM_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include algorithm_helpers.h"
// For the sake of sane code completion.
#include "algorithm_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TIter, class TPredicate>
Y_FORCE_INLINE TIter LinearSearch(TIter begin, TIter end, TPredicate pred)
{
    while (begin != end && pred(begin)) {
        ++begin;
    }

    return begin;
}

template <class TIter, class TPredicate>
TIter BinarySearch(TIter begin, TIter end, TPredicate pred)
{
    size_t count = end - begin;
    while (count != 0) {
        auto half = count / 2;
        auto middle = begin + half;
        if (pred(middle)) {
            begin = ++middle;
            count -= half + 1;
        } else {
            count = half;
        }
    }

    return begin;
}

template <class TPredicate>
size_t BinarySearch(size_t begin, size_t end, TPredicate pred)
{
    return NYT::BinarySearch<size_t, TPredicate>(begin, end, pred);
}

template <class TIter, class TPredicate>
Y_FORCE_INLINE TIter ExponentialSearch(TIter begin, TIter end, TPredicate pred)
{
    size_t step = 1;
    auto next = begin;

    if (begin == end) {
        return begin;
    }

    while (pred(next)) {
        begin = ++next;

        if (step < static_cast<size_t>(end - next)) {
            next += step;
            step *= 3;
        } else {
            next = end;
            break;
        }
    }

    return BinarySearch(begin, next, pred);
}

template <class TIter, class T>
TIter LowerBound(TIter begin, TIter end, const T& value)
{
    return NYT::BinarySearch(begin, end, [&] (TIter it) {
        return *it < value;
    });
}

template <class TIter, class T>
TIter UpperBound(TIter begin, TIter end, const T& value)
{
    return BinarySearch(begin, end, [&] (TIter it) {
        return !(value < *it);
    });
}

template <class TIter, class T>
TIter ExpLowerBound(TIter begin, TIter end, const T& value)
{
    return ExponentialSearch(begin, end, [&] (TIter it) {
        return *it < value;
    });
}

template <class TIter, class T>
TIter ExpUpperBound(TIter begin, TIter end, const T& value)
{
    return ExponentialSearch(begin, end, [&] (TIter it) {
        return !(value < *it);
    });
}

////////////////////////////////////////////////////////////////////////////////

template <class TInputIt1, class TInputIt2>
bool Intersects(TInputIt1 first1, TInputIt1 last1, TInputIt2 first2, TInputIt2 last2)
{
    while (first1 != last1 && first2 != last2) {
        if (*first1 < *first2) {
            ++first1;
        } else if (*first2 < *first1) {
            ++first2;
        } else {
            return true;
        }

    }
    return false;
}

template <class TIter>
void PartialShuffle(TIter begin, TIter end, TIter last)
{
    while (begin != end) {
        std::iter_swap(begin, begin + rand() % std::distance(begin, last));
        ++begin;
    }
}

template <class T, class TGetKey>
std::pair<const T&, const T&> MinMaxBy(const T& first, const T& second, const TGetKey& getKey)
{
    return std::minmax(first, second, [&] (auto&& left, auto&& right) { return getKey(left) < getKey(right); });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
