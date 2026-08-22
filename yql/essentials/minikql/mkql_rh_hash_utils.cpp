#include "mkql_rh_hash_utils.h"

namespace NKikimr::NMiniKQL {

ui64 RHHashTableNeedsGrow(ui64 size, ui64 capacity) {
    return size * 2 >= capacity;
}

ui64 CalculateRHHashTableGrowFactor(ui64 currentCapacity) {
    ui64 growFactor;
    if (currentCapacity < 100'000) {
        growFactor = 8;
    } else if (currentCapacity < 1'000'000) {
        growFactor = 4;
    } else {
        growFactor = 2;
    }

    return growFactor;
}

ui64 CalculateRHHashTableCapacity(ui64 targetSize) {
    ui64 capacity = 256;
    while (RHHashTableNeedsGrow(targetSize, capacity)) {
        capacity *= CalculateRHHashTableGrowFactor(capacity);
    }

    return capacity;
}

} // namespace NKikimr::NMiniKQL
