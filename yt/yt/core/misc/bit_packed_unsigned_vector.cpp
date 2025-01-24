#include "bit_packed_unsigned_vector.h"

#include <library/cpp/yt/coding/zig_zag.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> PrepareDiffFromExpected(std::vector<ui32>* values)
{
    ui32 expected = 0;
    ui32 maxDiff = 0;

    if (values->empty()) {
        return {expected, maxDiff};
    }

    expected = DivRound<int>(values->back(), values->size());

    i64 expectedValue = 0;
    for (int i = 0; i < std::ssize(*values); ++i) {
        expectedValue += expected;
        i32 diff = (*values)[i] - expectedValue;
        (*values)[i] = ZigZagEncode32(diff);
        maxDiff = std::max(maxDiff, (*values)[i]);
    }

    return {expected, maxDiff};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
