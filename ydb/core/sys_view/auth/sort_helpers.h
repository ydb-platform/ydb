#pragma once

#include <algorithm>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

namespace NKikimr::NSysView::NAuth {

template<typename TElement, class TCompLess>
void SortBatch(TVector<TElement>& batch, TCompLess compLess, bool requireUnique = true) {
    std::sort(batch.begin(), batch.end(), compLess);

    auto uniqueEnd = std::unique(batch.begin(), batch.end(), [&compLess](const TElement& left, const TElement& right) {
        return !compLess(left, right) && !compLess(right, left);
    });

    if (requireUnique) {
        Y_DEBUG_ABORT_UNLESS(uniqueEnd == batch.end());
    } else {
        batch.erase(uniqueEnd, batch.end());
    }
}

}
