#include "aggr.h"

#include <cassert>

#include <util/str_stl.h>

ui32 round_to_nearest_power_of_two(ui64 n) {
    assert((n & (1ull << 63)) == 0);
    for (ui32 i = 62; i; i--) {
        if (n & (1ull << i)) {
            return n & ~(1ull << i) ? i + 1 : i;
        }
    }
    return 0;
}

ui64 hash_keys(ui64 * keys, ui64 keyCount) {
    assert(keyCount > 0);
    ui64 result = THash<ui64>()(*keys++);
    for (ui64 i = 1; i < keyCount; i++) {
        result = CombineHashes(result, THash<ui64>()(*keys++));
    }
    return result;
}