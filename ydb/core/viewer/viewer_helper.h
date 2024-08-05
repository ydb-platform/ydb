#pragma once
#include <util/generic/algorithm.h>

namespace NKikimr::NViewer {
    template<typename TCollection, typename TFunc>
    void SortCollection(TCollection& collection, TFunc&& func, bool ReverseSort = false) {
        using TItem = typename TCollection::value_type;
        bool reverse = ReverseSort;
        ::Sort(collection, [reverse, func](const TItem& a, const TItem& b) {
            auto valueA = func(a);
            auto valueB = func(b);
            return valueA == valueB ? false : reverse ^ (valueA < valueB);
        });
    }
}
