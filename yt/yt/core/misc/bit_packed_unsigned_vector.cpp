#include "bit_packed_unsigned_vector.h"
#include "numeric_helpers.h"

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void PrepareDiffFromExpected(std::vector<ui32>* values, ui32* expected, ui32* maxDiff)
{
    if (values->empty()) {
        *expected = 0;
        *maxDiff = 0;
        return;
    }

    *expected = DivRound<int>(values->back(), values->size());

    *maxDiff = 0;
    i64 expectedValue = 0;
    for (int i = 0; i < std::ssize(*values); ++i) {
        expectedValue += *expected;
        i32 diff = values->at(i) - expectedValue;
        (*values)[i] = ZigZagEncode32(diff);
        *maxDiff = std::max(*maxDiff, (*values)[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
