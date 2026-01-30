#include "yql_yt_sort_helper.h"
#include <util/generic/algorithm.h>
#include <algorithm>

namespace NYql::NFmr {

TPermutation TSortHelper::BuildPermutation(const TVector<TRowIndexMarkup>& rows) const {
    if (rows.empty()) {
        return {};
    }

    TPermutation permutation(rows.size());
    std::iota(permutation.begin(), permutation.end(), 0);

    TBinaryYsonComparator comparator(BlobData_, SortOrders_);

    std::stable_sort(permutation.begin(), permutation.end(),
        [&rows, &comparator](ui64 idxA, ui64 idxB) {
            return comparator.CompareRows(rows[idxA], rows[idxB]) < 0;
        }
    );

    return permutation;
}

} // namespace NYql::NFmr
