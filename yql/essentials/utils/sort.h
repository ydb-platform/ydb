#pragma once

#include <contrib/libs/miniselect/include/miniselect/floyd_rivest_select.h>

namespace NYql {

template <class RandomIt>
void FastNthElement(RandomIt first, RandomIt middle, RandomIt last) {
    ::miniselect::floyd_rivest_select(first, middle, last);
}

template <class RandomIt, class Compare>
void FastNthElement(RandomIt first, RandomIt middle, RandomIt last, Compare compare) {
    ::miniselect::floyd_rivest_select(first, middle, last, compare);
}

template <class RandomIt>
void FastPartialSort(RandomIt first, RandomIt middle, RandomIt last) {
    ::miniselect::floyd_rivest_partial_sort(first, middle, last);
}

template <class RandomIt, class Compare>
void FastPartialSort(RandomIt first, RandomIt middle, RandomIt last, Compare compare) {
    ::miniselect::floyd_rivest_partial_sort(first, middle, last, compare);
}

}
